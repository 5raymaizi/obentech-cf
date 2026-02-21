// ======================================================================================
// strategy_mm.go — 跨交易所永续合约套利的 Market Making（做市/挂单）策略执行引擎
//
// 核心思路：
//   在两个不同交易所（swap1, swap2）上，对同一币种的永续合约，通过双边挂限价单来套利，
//   利用两个交易所之间的价差（SR = Spread Rate = swap2价格/swap1价格 - 1）赚取利润。
//
// 文件结构：
//   - openSwap1Swap2 / closeSwap1Swap2    : 开仓/平仓入口（薄封装，加panic恢复）
//   - openSwap1AndSwap2                   : 开仓核心逻辑（~1500行）
//   - closeSwap1AndSwap2                  : 平仓核心逻辑（~1500行，与开仓对称）
//   - reverseSide                         : 工具函数，反转订单方向
//
// 关键术语：
//   SR (Spread Rate)  : 价差率 = swap2价格 / swap1价格 - 1
//   Maker             : 限价单/挂单，手续费低（甚至返佣）
//   Taker             : 市价单/吃单，手续费高但立即成交
//   TickNum           : 挂单价相对盘口最优价的偏移量（单位：最小价格变动）
//   MakerHedge        : 一边成交后价差变差，用maker追另一边来对齐
//   TakerHedge        : 价差极端不利，用taker市价单紧急对冲
//   Exposure/敞口      : 两边成交不对齐的部分，需后续处理
// ======================================================================================
package logic

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"cf_arbitrage/exchange"                    // 交易所客户端接口定义
	actsSpot "cf_arbitrage/exchange/acts/spot"  // 精度计算工具
	"cf_arbitrage/logic/position"              // 仓位管理和订单结果
	"cf_arbitrage/logic/rate"                  // SR相关常量(Swap1MSwap2M等)和模式(SrMean/SrMedian)
	"cf_arbitrage/message"                     // 告警消息推送(飞书/钉钉等)
	utilCommon "cf_arbitrage/util/common"       // 通用工具(超时定时器、panic恢复等)

	common "go.common"       // 通用JSON对象
	"go.common/helper"       // 辅助函数(MustString等)

	ccexgo "github.com/NadiaSama/ccexgo/exchange" // 交易所SDK: OrderSide, SwapSymbol, OrderStatus等
	"github.com/go-kit/kit/log"                   // 结构化日志框架
	"github.com/go-kit/kit/log/level"             // 日志级别(Info/Warn/Error)
	"github.com/shopspring/decimal"               // 高精度小数运算，避免浮点精度问题
)

// openSwap1Swap2 — 开仓入口函数（薄封装）
// 参数:
//   - ctx: 上下文，用于取消/超时控制
//   - swapC1, swapC2: 两个交易所的永续合约客户端
//   - srOpen: 触发开仓的目标价差率
//   - openTradeVolume: 本次开仓的目标交易量
//   - swap1, swap2: 两个交易所当前的深度数据(盘口)
//   - side: 订单方向 (Buy=做空价差, Sell=做多价差)
// 作用: 套一层 defer recover 防止内部panic导致程序崩溃，然后调用真正的开仓逻辑
func (sg *Strategy) openSwap1Swap2(ctx context.Context, swapC1, swapC2 exchange.SwapClient,
	srOpen float64, openTradeVolume float64, swap1 *exchange.Depth, swap2 *exchange.Depth, side ccexgo.OrderSide) (or *position.OrderResult, err error) {
	start := time.Now()
	defer func() {
		if e := recover(); e != nil {
			err = utilCommon.RecoverWithLog(sg.logger, "openSwap1Swap2", start, e)
		}
	}()
	return sg.openSwap1AndSwap2(ctx, swapC1, swapC2, srOpen, openTradeVolume, swap1, swap2, side)
}

// closeSwap1Swap2 — 平仓入口函数（薄封装）
// 参数与 openSwap1Swap2 对称，srClose为触发平仓的目标价差率
// 作用: 套一层 defer recover 防止内部panic，然后调用真正的平仓逻辑
func (sg *Strategy) closeSwap1Swap2(ctx context.Context, swapC1, swapC2 exchange.SwapClient,
	srClose float64, closeTradeVolume float64, swap1 *exchange.Depth, swap2 *exchange.Depth, side ccexgo.OrderSide) (or *position.OrderResult, err error) {
	start := time.Now()
	defer func() {
		if e := recover(); e != nil {
			err = utilCommon.RecoverWithLog(sg.logger, "closeSwap1Swap2", start, e)
		}
	}()
	return sg.closeSwap1AndSwap2(ctx, swapC1, swapC2, srClose, closeTradeVolume, swap1, swap2, side)
}

// openSwap1AndSwap2 — 开仓核心逻辑（约1500行）
//
// 整体流程:
//   阶段1: 初始化参数 → 双边下限价单
//   阶段2: 事件驱动主循环（监听盘口变动、WS订单推送、超时、退出信号）
//          - 根据实时SR决定: 继续等待 / maker追单对冲 / taker紧急对冲
//          - 动态撤单和重新挂单
//   阶段3: checkAndHedge — 撤残单 + taker补齐两边成交差额
//   阶段4: hedge — 最终收尾，记录仓位
//
// 参数:
//   - side == OrderSideBuy  : 做空价差（swap1买入/swap2卖出，期望SR从高位回落获利）
//   - side == OrderSideSell : 做多价差（swap1卖出/swap2买入，期望SR从低位上升获利）
func (sg *Strategy) openSwap1AndSwap2(ctx context.Context, swapC1, swapC2 exchange.SwapClient,
	srOpen float64, openTradeVolume float64, swap1 *exchange.Depth, swap2 *exchange.Depth, side ccexgo.OrderSide) (*position.OrderResult, error) {
	// 【前置检查】确认当前仓位方向是否允许继续开仓（避免方向冲突）
	if err := sg.PosList.CheckPosSideAmount(side, reverseSide(side)); err != nil {
		return nil, err
	}

	// ==================== 变量声明 ====================
	var (
		// --- 聚合订单对象 ---
		// swap1Order/swap2Order 是本次操作的"聚合订单"，可能包含多次撤单重挂和taker补单的累计信息
		// 与交易所返回的 order1/order2（单次挂单）区分开
		swap1Order = &exchange.CfOrder{} // swap1的聚合订单（累计Filled/AvgPrice/Fee）
		swap2Order = &exchange.CfOrder{} // swap2的聚合订单
		srOpenInit float64               // 初始下单时刻的价差率，用于最终记录

		// --- 核心状态标志 ---
		// 已移除 checkExitNowTimer 和 exitNow：直接在 ctx.Done() 时处理退出，避免 select 随机性导致的延迟
		makerHedge         bool // 是否已进入"maker对冲"模式（一边停止挂单，追另一边）
		cancelFilled       bool // 是否因SR撤单阈值触发的makerHedge（与正常makerHedge用不同的tick偏移）
		takerHedge         bool // 是否需要taker紧急对冲（价差极端不利时触发）
		finishedWork       bool // 两边是否都已正常完成成交
		makerReplaceFailed bool // maker重新挂单失败，需要降级到taker对冲

		// --- 撤单/重挂标志 ---
		swap1Cancel      bool // swap1当前挂单需要撤掉
		swap2Cancel      bool // swap2当前挂单需要撤掉
		swap1NeedReplace bool // swap1撤单后需要重新挂单
		swap2NeedReplace bool // swap2撤单后需要重新挂单
		swap1End         bool // swap1已"停工"（不再继续挂新单，等另一边追齐）
		swap2End         bool // swap2已"停工"

		// --- 成交量记录（用于最终的makerInfo统计） ---
		makerAmount1 decimal.Decimal // swap1通过maker成交的数量
		makerAmount2 decimal.Decimal // swap2通过maker成交的数量
		takerAmount1 decimal.Decimal // swap1通过taker补齐的数量
		takerAmount2 decimal.Decimal // swap2通过taker补齐的数量

		logger log.Logger // 带操作ID的logger，便于日志追踪

		// --- SR统计窗口相关（用于最终记录和撤单决策） ---
		srWindow       string          // 下单时的SR统计窗口值（均值或中位数）
		srWindowCancel string          // 触发撤单时记录的SR窗口值
		firstFilled    string          // 哪一边先出现不对齐的成交（"swap1"或"swap2"）
		firstNowSr     string          // 第一次出现不对齐时的实时SR
		firstSrWindow  string          // 第一次不对齐时的SR窗口值
		nowSr          decimal.Decimal // 当前实时计算的综合SR

		timeoutTimer1    *time.Timer // 10分钟告警定时器
		maxDurationTimer *time.Timer // 20分钟强制超时定时器
	)

	// ==================== 加载配置和基础参数 ====================
	cfg := sg.load.Load()          // 热加载最新配置（运行时可动态修改）
	one := decimal.NewFromFloat(1) // 常量1，用于计算 SR = price2/price1 - 1

	// 【tick偏移量】决定挂单价格相对盘口最优价的偏移距离
	// side==Buy(做空价差): swap1在ask侧买入(tick往内偏移), swap2在bid侧卖出(tick往内偏移)
	// side==Sell(做多价差): 方向相反，使用CloseTickNum
	openTickNum1 := float64(cfg.MmSwap1OpenTickNum)
	openTickNum2 := float64(cfg.MmSwap2OpenTickNum)
	if side == ccexgo.OrderSideSell {
		openTickNum1 = float64(cfg.MmSwap1CloseTickNum)
		openTickNum2 = float64(cfg.MmSwap2CloseTickNum)
	}

	// 【价格精度】每个交易所的最小价格变动单位（比如0.01 USDT）
	pricePrecision1 := sg.swapS1.PricePrecision()
	pricePrecision2 := sg.swapS2.PricePrecision()

	// 【数量精度】用于Round下单数量，OKX和Gate的合约面值不同需特殊处理
	exp1 := actsSpot.CalcExponent(sg.swapS1.ContractVal())
	if swapC1.GetExchangeName() == exchange.Okex5 || swapC1.GetExchangeName() == exchange.GateIO {
		exp1 = actsSpot.CalcExponent(decimal.NewFromFloat(sg.swap1ActsSymbol.AmountTickSize * sg.swap1ActsSymbol.ContractValue))
	}
	exp2 := actsSpot.CalcExponent(sg.swapS2.ContractVal())
	if swapC2.GetExchangeName() == exchange.Okex5 || swapC2.GetExchangeName() == exchange.GateIO {
		exp2 = actsSpot.CalcExponent(decimal.NewFromFloat(sg.swap2ActsSymbol.AmountTickSize * sg.swap2ActsSymbol.ContractValue))
	}

	// minOrderAmount1 := sg.swap1ActsSymbol.MinimumOrderAmount
	// minOrderAmount2 := sg.swap2ActsSymbol.MinimumOrderAmount
	// if swapC1.GetExchangeName() == exchange.Okex5 || swapC1.GetExchangeName() == exchange.GateIO {
	// 	minOrderAmount1 = sg.swap1ActsSymbol.MinimumOrderAmount * sg.swap1ActsSymbol.ContractValue
	// }
	// if swapC2.GetExchangeName() == exchange.Okex5 || swapC2.GetExchangeName() == exchange.GateIO {
	// 	minOrderAmount2 = sg.swap2ActsSymbol.MinimumOrderAmount * sg.swap2ActsSymbol.ContractValue
	// }

	// 【获取SR统计窗口值】用于后续的撤单决策和最终记录
	// Buy(做空价差) → 取开仓SR窗口; Sell(做多价差) → 取平仓SR窗口
	if side == ccexgo.OrderSideBuy {
		srWindow = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
	} else {
		srWindow = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
	}

	level.Info(sg.logger).Log("message", "openSwap1AndSwap2", "openTickNum1", openTickNum1, "openTickNum2", openTickNum2, "minOrderAmount1", sg.minOrderAmount1, "minOrderAmount2", sg.minOrderAmount2)

	// ==================== 阶段1: 双边下限价单 ====================
	// 同时在swap1和swap2上挂限价单，返回值:
	//   op        - 操作记录对象（有唯一opID用于追踪整个操作链）
	//   order1    - swap1交易所返回的订单对象（单次挂单）
	//   order2    - swap2交易所返回的订单对象
	//   errMsg    - 错误信息
	//   code      - 结果码: 0=都成功, 1=swap1失败, 2=swap2失败, -1=都失败
	//   placeMode - 下单模式: Swap1MSwap2M(双maker) / Swap1MSwap2T / Swap1TSwap2M
	op, order1, order2, errMsg, code, placeMode := sg.PosList.OpenAllLimit(ctx, swapC1, swapC2, srOpen, openTradeVolume, []float64{openTickNum1, openTickNum2}, swap1, swap2, nil, side, &cfg)
	if op != nil {
		logger = log.With(sg.logger, "id", op.GetOpID())
	} else {
		logger = sg.logger
	}




	// ==================== 处理下单结果 ====================
	switch code {
	case 0: // 【两边都下单成功】→ 记录订单信息，然后进入事件驱动主循环
		// 将交易所返回的order信息拷贝到聚合订单swap1Order/swap2Order
		// 注意: Fee取反(Neg)是因为交易所返回的手续费为正数,内部统一用负数表示支出
		// 如果placeMode==Swap1TSwap2M,说明swap1是taker,没有自己的maker单,用swap2信息推导
		if placeMode != rate.Swap1TSwap2M {
			swap1Order.IDs = append(swap1Order.IDs, order1.ID.String())
			swap1Order.ID = order1.ID
			swap1Order.Price = order1.Price
			swap1Order.Amount = order1.Amount
			swap1Order.Side = order1.Side
			swap1Order.Created = time.Now()
			if !order1.Created.IsZero() {
				swap1Order.Created = order1.Created
			}
			if !order1.Updated.IsZero() {
				swap1Order.Updated = order1.Updated
			}
			swap1Order.Filled = order1.Filled
			swap1Order.AvgPrice = order1.AvgPrice
			swap1Order.Fee = order1.Fee.Neg()
			swap1Order.FeeCurrency = order1.FeeCurrency
			swap1Order.Status = order1.Status
		} else {
			swap1Order.ID = ccexgo.NewStrID("")
			swap1Order.Amount = order2.Amount
			swap1Order.Side = reverseSide(order2.Side)
		}

		if placeMode != rate.Swap1MSwap2T {
			swap2Order.IDs = append(swap2Order.IDs, order2.ID.String())
			swap2Order.ID = order2.ID
			swap2Order.Price = order2.Price
			swap2Order.Amount = order2.Amount
			swap2Order.Side = order2.Side
			swap2Order.Created = time.Now()
			if !order2.Created.IsZero() {
				swap2Order.Created = order2.Created
			}
			if !order2.Updated.IsZero() {
				swap2Order.Updated = order2.Updated
			}
			swap2Order.Filled = order2.Filled
			swap2Order.AvgPrice = order2.AvgPrice
			swap2Order.Fee = order2.Fee.Neg()
			swap2Order.FeeCurrency = order2.FeeCurrency
			swap2Order.Status = order2.Status
		} else {
			swap2Order.ID = ccexgo.NewStrID("")
			swap2Order.Amount = order1.Amount
			swap2Order.Side = reverseSide(order1.Side)
		}

		// 【计算初始SR】根据placeMode确定price1/price2，再算 srOpenInit = price2/price1 - 1
		// Swap1MSwap2T: swap1是maker用挂单价, swap2是taker用盘口价(ask/bid)
		// Swap1TSwap2M: swap1是taker用盘口价, swap2是maker用挂单价
		// 默认(Swap1MSwap2M): 两边都用各自的挂单价
		var price1, price2 decimal.Decimal
		switch placeMode {
		case rate.Swap1MSwap2T:
			price1 = swap1Order.Price
			if side == ccexgo.OrderSideBuy {
				price2 = decimal.NewFromFloat(swap2.Asks[0].Price)
			} else {
				price2 = decimal.NewFromFloat(swap2.Bids[0].Price)
			}
		case rate.Swap1TSwap2M:
			if side == ccexgo.OrderSideBuy {
				price1 = decimal.NewFromFloat(swap1.Bids[0].Price)
			} else {
				price1 = decimal.NewFromFloat(swap1.Asks[0].Price)
			}
			price2 = swap2Order.Price
		default:
			price1 = swap1Order.Price
			price2 = swap2Order.Price
		}
		srOpenInit = price2.Div(price1).Sub(one).InexactFloat64()

		if cfg.SrMode != rate.SrNormal {
			if swap1Order.Filled.IsPositive() {
				firstFilled = "swap1"
				firstNowSr = helper.MustString(srOpenInit)
				if side == ccexgo.OrderSideBuy {
					firstSrWindow = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
				} else {
					firstSrWindow = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
				}
			} else if swap2Order.Filled.IsPositive() {
				firstFilled = "swap2"
				firstNowSr = helper.MustString(srOpenInit)
				if side == ccexgo.OrderSideCloseShort {
					firstSrWindow = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
				} else {
					firstSrWindow = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
				}
			}
		}

	case 1: // 【swap1下单失败，swap2已成功】
		// 处理策略: 尝试撤掉swap2的挂单
		// - 如果撤单前swap2已部分成交 → 记录成交信息，然后跳到taker对冲补齐swap1
		// - 如果placeMode是Swap1MSwap2T(swap1是maker)，swap1失败则整个操作无意义，直接返回
		if placeMode == rate.Swap1MSwap2T {
			return nil, fmt.Errorf("%+v", errMsg)
		}
		swap1Order.Amount = order2.Amount
		swap1Order.Side = reverseSide(order2.Side)

		swap2Order.IDs = append(swap2Order.IDs, order2.ID.String())
		swap2Order.ID = order2.ID
		swap2Order.Price = order2.Price
		swap2Order.Amount = order2.Amount
		swap2Order.Side = order2.Side
		swap2Order.Created = time.Now()
		if !order2.Created.IsZero() {
			swap2Order.Created = order2.Created
		}
		if !order2.Updated.IsZero() {
			swap2Order.Updated = order2.Updated
		}
		swap2Order.Filled = order2.Filled
		swap2Order.AvgPrice = order2.AvgPrice
		swap2Order.Fee = order2.Fee.Neg()
		swap2Order.FeeCurrency = order2.FeeCurrency
		swap2Order.Status = order2.Status

		level.Warn(sg.logger).Log("message", "open swap1 limit failed", "swap2_order", fmt.Sprintf("%+v", order2), "errMsg", errMsg)
		// ?调撤单，有问题异常追踪
		if swap2Order.Status < ccexgo.OrderStatusDone {
			o, err := swapC2.CancelOrder(ctx, order2) // 撤单,查单
			if err != nil {
				errType := swapC2.HandError(err)
				sg.PosList.UpdateBannedTime(errType, false)
				sg.PosList.CheckErrShareBan(errType, swapC2.GetExchangeName())
				if errType.Code == exchange.NoSuchOrder {
					var n int
					for err != nil && n < 5 { // 循环查单5次，每次休眠60ms，理论会查到
						order2, err = swapC2.FetchOrder(ctx, swap2Order)
						errType = swapC2.HandError(err)
						if err != nil && errType.Code != exchange.NoSuchOrder { //TODO 出现限频类错误就不查了,丢到异常订单追踪
							break
						}
						n += 1
						time.Sleep(60 * time.Millisecond)
					}
				}
				if err != nil { // TODO
					// if code == exchange.OtherReason {
					// sg.PosList.AddAbnormalOrder(order2.ID.String(), order2.Side, helper.NowMilliSeconds())
					return nil, fmt.Errorf("%+v\nswap2单边挂单成功，撤单异常\n%+v\nswap1挂单失败\n%+v", swap2Order, err, errMsg)
					// }
				}
			}
			if o.Filled.IsPositive() {
				order2.Status = o.Status
				order2.Filled = o.Filled
				order2.AvgPrice = o.AvgPrice
				order2.Fee = o.Fee
				order2.Updated = o.Updated

				swap2Order.Status = o.Status
				swap2Order.Filled = o.Filled
				swap2Order.AvgPrice = o.AvgPrice
				swap2Order.Fee = o.Fee.Neg()
				swap2Order.FeeCurrency = o.FeeCurrency
				swap2Order.Updated = o.Updated
				level.Warn(sg.logger).Log("message", "open limit swap2 filled", "order", fmt.Sprintf("%+v", order2))
				// return nil, fmt.Errorf("swap2单边挂单成功，撤单成交\n%+v\nswap1挂单失败\n%+v", order2, errMsg)
			}
		}
		swap1Order.Status = ccexgo.OrderStatusFailed
		takerHedge = true
		goto checkAndHedge
	case 2: // 【swap2下单失败，swap1已成功】（与case 1对称）
		// 尝试撤掉swap1的挂单，如果有部分成交则跳到taker对冲
		if placeMode == rate.Swap1TSwap2M {
			return nil, fmt.Errorf("%+v", errMsg)
		}
		swap2Order.Amount = order1.Amount
		swap2Order.Side = reverseSide(order1.Side)

		swap1Order.IDs = append(swap1Order.IDs, order1.ID.String())
		swap1Order.ID = order1.ID
		swap1Order.Price = order1.Price
		swap1Order.Amount = order1.Amount
		swap1Order.Side = order1.Side
		swap1Order.Created = time.Now()
		if !order1.Created.IsZero() {
			swap1Order.Created = order1.Created
		}
		if !order1.Updated.IsZero() {
			swap1Order.Updated = order1.Updated
		}
		swap1Order.Filled = order1.Filled
		swap1Order.AvgPrice = order1.AvgPrice
		swap1Order.Fee = order1.Fee.Neg()
		swap1Order.FeeCurrency = order1.FeeCurrency
		swap1Order.Status = order1.Status
		level.Warn(sg.logger).Log("message", "open swap2 limit failed", "swap1_order", fmt.Sprintf("%+v", order1), "errMsg", errMsg)

		if swap1Order.Status < ccexgo.OrderStatusDone {
			o, err := swapC1.CancelOrder(ctx, order1) // 撤单,查单
			if err != nil {                           // TODO
				errType := swapC1.HandError(err)
				sg.PosList.UpdateBannedTime(errType, false)
				sg.PosList.CheckErrShareBan(errType, swapC1.GetExchangeName())
				return nil, fmt.Errorf("%+v\nswap1单边挂单成功，撤单异常\n%+v\nswap2挂单失败\n%+v", swap1Order, err, errMsg)
			}
			if o.Filled.IsPositive() { // 成交了计入敞口
				order1.Status = o.Status
				order1.Filled = o.Filled
				order1.AvgPrice = o.AvgPrice
				order1.Fee = o.Fee
				order1.Updated = o.Updated

				swap1Order.Status = o.Status
				swap1Order.Filled = o.Filled
				swap1Order.AvgPrice = o.AvgPrice
				swap1Order.Fee = o.Fee.Neg()
				swap1Order.FeeCurrency = o.FeeCurrency
				swap1Order.Updated = o.Updated
				level.Warn(sg.logger).Log("message", "open limit swap1 filled", "order", fmt.Sprintf("%+v", order1))
				// sg.PosList.AddExposure(ctx, order.Filled.Sub(order.Fee).Neg())
				// return nil, fmt.Errorf("swap1单边挂单成功，撤单成交\n%+v\nswap2挂单失败\n%+v", order, errMsg)
			}
		}
		swap2Order.Status = ccexgo.OrderStatusFailed
		takerHedge = true
		goto checkAndHedge

	case -1: // 【两边都下单失败】直接返回错误，本次操作结束
		return nil, fmt.Errorf("%+v", errMsg)
	}





	// ==================== 阶段2前置: 设置保护定时器 ====================
	// 10分钟告警定时器: 超过10分钟还没完成会发告警通知
	timeoutTimer1 = utilCommon.NewTimeOutTimer(ctx, sg.swapS1.String(), 10, fmt.Sprintf("%s MM开仓", op.GetOpID()))
	defer timeoutTimer1.Stop()

	// 20分钟超时保护: 防止协程永久卡住，超时后强制终止并taker对冲
	maxDurationTimer = time.NewTimer(20 * time.Minute)
	defer maxDurationTimer.Stop()





	// ==================== 阶段2: 事件驱动主循环 ====================
	// 用 for { select { ... } } 同时监听多个事件通道:
	//   1. maxDurationTimer.C    — 20分钟超时强制退出
	//   2. ctx.Done()            — 程序退出信号
	//   3. sg.bookTickerEvent    — 盘口行情变动（最核心的决策分支）
	//   4. swapC1.MakerOrder()   — swap1的WebSocket订单状态推送
	//   5. swapC2.MakerOrder()   — swap2的WebSocket订单状态推送
	// 循环持续运行，直到某个分支触发 goto checkAndHedge 跳出
	//
	// 移除 checkExitNowTimer 机制，改为在 ctx.Done() 时直接处理退出逻辑，避免 select 随机选择导致的退出延迟问题
	// 之前的问题：exitNow 标志需要等待 timer 触发才能退出，在高频交易场景可能导致 mmConcurrencyCount 无法递减
	for {
		select {
		case <-maxDurationTimer.C:
			// 【分支A: 20分钟超时保护】强制终止，发紧急告警，跳到taker对冲
			level.Warn(logger).Log("message", "open mm strategy timeout 20 minutes, force exit")
			go message.SendP3Important(ctx, message.NewCommonMsgWithAt(fmt.Sprintf("%s MM开仓超时", sg.swapS1), fmt.Sprintf("操作ID: %s, 运行超过20分钟强制退出", op.GetOpID())))
			takerHedge = true
			goto checkAndHedge
		case <-ctx.Done():
			// 【分支B: 程序退出信号】并发撤掉两边未完成的挂单，更新成交信息，然后taker对冲
			var order1ID, order2ID string
			if order1 != nil {
				order1ID = order1.ID.String()
			}
			if order2 != nil {
				order2ID = order2.ID.String()
			}
			level.Info(logger).Log("message", "open exit now", "order1_id", order1ID, "order2_id", order2ID)
			var wg sync.WaitGroup
			if order1 != nil && order1.Status < ccexgo.OrderStatusDone {
				wg.Go(func() {
					o, err := swapC1.CancelOrder(ctx, order1)
					if err != nil {
						errType := swapC1.HandError(err)
						level.Info(logger).Log("message", "open cancel order failed", "error", errType.String())
						go message.Send(ctx, message.NewOperateFail(fmt.Sprintf("%s swap1部分收尾开仓撤单失败", sg.swapS1), errType.String()))
					} else {
						if !o.Filled.IsZero() {
							if firstFilled == "" && cfg.SrMode != rate.SrNormal {
								firstFilled = "swap1"
								firstNowSr = nowSr.String()
								if side == ccexgo.OrderSideBuy {
									firstSrWindow = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
								} else {
									firstSrWindow = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
								}
							}

							newFilled := o.Filled.Sub(order1.Filled)
							if newFilled.IsPositive() {
								newFee := o.Fee.Sub(order1.Fee)
								if swap1Order.Filled.Add(newFilled).IsPositive() {
									swap1Order.AvgPrice = swap1Order.Filled.Mul(swap1Order.AvgPrice).Add(newFilled.Mul(o.AvgPrice)).Div(swap1Order.Filled.Add(newFilled))
								}
								swap1Order.Filled = swap1Order.Filled.Add(newFilled)
								swap1Order.Fee = swap1Order.Fee.Add(newFee.Neg())
								swap1Order.Updated = o.Updated
							}
						}
						if o.Status != ccexgo.OrderStatusUnknown {
							swap1Order.Status = o.Status
						}
						order1.Status = o.Status
						order1.Filled = o.Filled
						order1.Fee = o.Fee
						order1.Updated = o.Updated
					}
				})
			}
			if order2 != nil && order2.Status < ccexgo.OrderStatusDone {
				wg.Go(func() {
					o, err := swapC2.CancelOrder(ctx, order2)
					if err != nil {
						errType := swapC2.HandError(err)
						level.Info(logger).Log("message", "open cancel order failed", "error", errType.String())
						go message.Send(ctx, message.NewOperateFail(fmt.Sprintf("%s swap2部分收尾开仓撤单失败", sg.swapS2), errType.String()))
					} else {
						if !o.Filled.IsZero() {
							if firstFilled == "" && cfg.SrMode != rate.SrNormal {
								firstFilled = "swap2"
								firstNowSr = nowSr.String()
								if side == ccexgo.OrderSideBuy {
									firstSrWindow = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
								} else {
									firstSrWindow = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
								}
							}

							newFilled := o.Filled.Sub(order2.Filled)
							if newFilled.IsPositive() {
								newFee := o.Fee.Sub(order2.Fee)
								if swap2Order.Filled.Add(newFilled).IsPositive() {
									swap2Order.AvgPrice = swap2Order.Filled.Mul(swap2Order.AvgPrice).Add(newFilled.Mul(o.AvgPrice)).Div(swap2Order.Filled.Add(newFilled))
								}
								swap2Order.Filled = swap2Order.Filled.Add(newFilled)
								swap2Order.Fee = swap2Order.Fee.Add(newFee.Neg())
								swap2Order.Updated = o.Updated
							}
						}
						if o.Status != ccexgo.OrderStatusUnknown {
							swap2Order.Status = o.Status
						}
						order2.Status = o.Status
						order2.Filled = o.Filled
						order2.Fee = o.Fee
						order2.Updated = o.Updated
					}
				})
			}
			wg.Wait()
			takerHedge = true
			goto checkAndHedge
		case <-sg.bookTickerEvent.Done():
			// 【分支C: 盘口行情变动】— 最核心的决策分支
			// 每当任一交易所的盘口(bid1/ask1)发生变化，触发一次完整的决策流程:
			//   步骤C1: 更新订单状态（从WS缓存获取最新成交信息）
			//   步骤C2: 计算当前SR（nowSr和takerSr）
			//   步骤C3: 决策瀑布（4级优先级判断）
			//   步骤C4: 执行撤单
			//   步骤C5: 动态修正标记
			//   步骤C6: 检查是否完成
			//   步骤C7: 重新挂单
			sg.bookTickerEvent.Unset() // 重置事件标志，等待下一次盘口变动
			swap1BookTicker := swapC1.BookTicker()
			swap2BookTicker := swapC2.BookTicker()
			if swap1BookTicker == nil || swap2BookTicker == nil {
				continue
			}

			// --- 步骤C1: 更新订单状态 ---
			// 通过 GetWsOrder 从WebSocket缓存中获取最新的订单成交信息
			// 增量更新: newFilled = 最新成交量 - 上次已知成交量
			// 加权均价: newAvgPrice = (旧成交额 + 新增成交额) / 总成交量
			if order1 != nil && order1.Status < ccexgo.OrderStatusDone {
				o, err := swapC1.GetWsOrder(order1.ID.String())
				if err == nil && (order1.Filled.LessThan(o.Filled) || order1.Updated.Before(o.Updated) || order1.Status < o.Status) {
					newFilled := o.Filled.Sub(order1.Filled)
					if newFilled.IsPositive() {
						newFee := o.Fee.Sub(order1.Fee)
						if swap1Order.Filled.Add(newFilled).IsPositive() {
							swap1Order.AvgPrice = swap1Order.Filled.Mul(swap1Order.AvgPrice).Add(newFilled.Mul(o.AvgPrice)).Div(swap1Order.Filled.Add(newFilled))
						}
						swap1Order.Filled = swap1Order.Filled.Add(newFilled)
						swap1Order.Fee = swap1Order.Fee.Add(newFee.Neg())
					}
					swap1Order.Updated = o.Updated
					swap1Order.Status = o.Status

					order1.Status = o.Status
					order1.Filled = o.Filled
					order1.Fee = o.Fee
					order1.Updated = o.Updated

					if order1.Status >= ccexgo.OrderStatusDone {
						if swap1Cancel {
							swap1Cancel = false
						}
						if swap1Order.Filled.GreaterThanOrEqual(swap1Order.Amount) {
							swap1Order.Status = ccexgo.OrderStatusDone
							// 如果swap2已经成交，则直接对冲
							if swap2Order.Status == ccexgo.OrderStatusDone || (swap2Order.Status == ccexgo.OrderStatusCancel && swap2End) {
								goto checkAndHedge
							}
						}
					}
				}
			}

			if order2 != nil && order2.Status < ccexgo.OrderStatusDone {
				o, err := swapC2.GetWsOrder(order2.ID.String())
				if err == nil && (order2.Filled.LessThan(o.Filled) || order2.Updated.Before(o.Updated) || order2.Status < o.Status) {
					newFilled := o.Filled.Sub(order2.Filled)
					if newFilled.IsPositive() {
						newFee := o.Fee.Sub(order2.Fee)
						if swap2Order.Filled.Add(newFilled).IsPositive() {
							swap2Order.AvgPrice = swap2Order.Filled.Mul(swap2Order.AvgPrice).Add(newFilled.Mul(o.AvgPrice)).Div(swap2Order.Filled.Add(newFilled))
						}
						swap2Order.Filled = swap2Order.Filled.Add(newFilled)
						swap2Order.Fee = swap2Order.Fee.Add(newFee.Neg())
					}
					swap2Order.Updated = o.Updated
					swap2Order.Status = o.Status

					order2.Status = o.Status
					order2.Filled = o.Filled
					order2.Fee = o.Fee
					order2.Updated = o.Updated

					if order2.Status >= ccexgo.OrderStatusDone {
						if swap2Cancel {
							swap2Cancel = false
						}
						if swap2Order.Filled.GreaterThanOrEqual(swap2Order.Amount) {
							swap2Order.Status = ccexgo.OrderStatusDone
							// 如果swap1已经成交，则直接对冲
							if swap1Order.Status == ccexgo.OrderStatusDone || (swap1Order.Status == ccexgo.OrderStatusCancel && swap1End) {
								goto checkAndHedge
							}
						}
					}
				}
			}

			// --- 步骤C2: 计算当前SR ---
			// 获取两边最新的盘口最优价(bid1/ask1)
			swap1Ask1Price := decimal.NewFromFloat(swap1BookTicker.Ask1Price)
			swap1Bid1Price := decimal.NewFromFloat(swap1BookTicker.Bid1Price)
			swap2Ask1Price := decimal.NewFromFloat(swap2BookTicker.Ask1Price)
			swap2Bid1Price := decimal.NewFromFloat(swap2BookTicker.Bid1Price)

			// 计算两个SR:
			//   nowSr   — 假设剩余未成交部分以maker价格成交后的综合SR
			//   takerSr — 假设剩余未成交部分以taker价格（直接吃单）成交后的综合SR
			// takerSr一定比nowSr差（更不利），用于判断是否需要紧急taker对冲
			var (
				takerSr decimal.Decimal
			)
			// tick偏移量: 在makerHedge模式下使用更激进的偏移量(MakerHedgeTickNum)
			// cancelFilled为true时说明是撤单触发的，仍用原始偏移量
			swap1OpenTick := cfg.MmSwap1OpenTickNum
			swap2OpenTick := cfg.MmSwap2OpenTickNum
			swap1CloseTick := cfg.MmSwap1CloseTickNum
			swap2CloseTick := cfg.MmSwap2CloseTickNum

			if makerHedge && !cancelFilled {
				swap1OpenTick = cfg.MmSwap1OpenMakerHedgeTickNum
				swap2OpenTick = cfg.MmSwap2OpenMakerHedgeTickNum
				swap1CloseTick = cfg.MmSwap1CloseMakerHedgeTickNum
				swap2CloseTick = cfg.MmSwap2CloseMakerHedgeTickNum
			}

			// 【情况1: swap1成交更多（swap1多了，需要补swap2）】
			// 估算: 如果剩余差额(needAmount)以maker/taker价格在swap2上成交，综合SR是多少
			// 公式: swap2综合均价 = (swap2已成交额 + 补齐部分估算额) / swap1总成交量
			//       nowSr = swap2综合均价 / swap1均价 - 1
			if swap1Order.Filled.GreaterThan(swap2Order.Filled) {
				swap2Price := swap2Bid1Price.Add(decimal.NewFromInt(int64(swap2OpenTick)).Mul(pricePrecision2))
				needAmount := swap1Order.Filled.Sub(swap2Order.Filled)
				swap2PriceTaker := swap2Bid1Price
				if side == ccexgo.OrderSideSell { // 做多价差
					swap2Price = swap2Ask1Price.Sub(decimal.NewFromInt(int64(swap2CloseTick)).Mul(pricePrecision2))
					swap2PriceTaker = swap2Ask1Price
				}
				swap2Avg := swap2Order.Filled.Mul(swap2Order.AvgPrice).Add(swap2Price.Mul(needAmount)).Div(swap1Order.Filled)
				swap2AvgTaker := swap2Order.Filled.Mul(swap2Order.AvgPrice).Add(swap2PriceTaker.Mul(needAmount)).Div(swap1Order.Filled)

				nowSr = swap2Avg.Div(swap1Order.AvgPrice).Sub(one)
				takerSr = swap2AvgTaker.Div(swap1Order.AvgPrice).Sub(one)
			} else if swap1Order.Filled.LessThan(swap2Order.Filled) {
				swap1Price := swap1Ask1Price.Sub(decimal.NewFromInt(int64(swap1OpenTick)).Mul(pricePrecision1))
				needAmount := swap2Order.Filled.Sub(swap1Order.Filled)
				swap1PriceTaker := swap1Ask1Price
				if side == ccexgo.OrderSideSell {
					swap1Price = swap1Bid1Price.Add(decimal.NewFromInt(int64(swap1CloseTick)).Mul(pricePrecision1))
					swap1PriceTaker = swap1Bid1Price
				}
				swap1Avg := swap1Order.Filled.Mul(swap1Order.AvgPrice).Add(swap1Price.Mul(needAmount)).Div(swap2Order.Filled)
				swap1AvgTaker := swap1Order.Filled.Mul(swap1Order.AvgPrice).Add(swap1PriceTaker.Mul(needAmount)).Div(swap2Order.Filled)

				nowSr = swap2Order.AvgPrice.Div(swap1Avg).Sub(one)
				takerSr = swap2Order.AvgPrice.Div(swap1AvgTaker).Sub(one)
			} else {
				// 【情况3: 两边成交量相等（或都没成交）】
				// 用当前盘口/挂单价计算SR，takerSr取两种极端情况中更差的那个:
				//   srTm — swap1用maker挂单价, swap2用taker吃单价 的SR
				//   srMt — swap1用taker吃单价, swap2用maker挂单价 的SR
				// 做空价差(Buy): takerSr = min(srMt, srTm) (取更低/更差的)
				// 做多价差(Sell): takerSr = max(srMt, srTm) (取更高/更差的)
				var price1, price2 decimal.Decimal
				swap1Price := swap1Ask1Price.Sub(decimal.NewFromInt(int64(swap1OpenTick)).Mul(pricePrecision1))
				swap2Price := swap2Bid1Price.Add(decimal.NewFromInt(int64(swap2OpenTick)).Mul(pricePrecision2))
				if side == ccexgo.OrderSideSell {
					swap1Price = swap1Bid1Price.Add(decimal.NewFromInt(int64(swap1CloseTick)).Mul(pricePrecision1))
					swap2Price = swap2Ask1Price.Sub(decimal.NewFromInt(int64(swap2CloseTick)).Mul(pricePrecision2))
				}
				if order1 != nil {
					price1 = order1.Price
				} else {
					price1 = swap1Price
				}
				if order2 != nil {
					price2 = order2.Price
				} else {
					price2 = swap2Price
				}

				srTm := swap2Bid1Price.Div(price1).Sub(one)
				srMt := price2.Div(swap1Ask1Price).Sub(one)
				takerSr = decimal.Min(srMt, srTm)
				if side == ccexgo.OrderSideSell {
					srTm = swap2Ask1Price.Div(price1).Sub(one)
					srMt = price2.Div(swap1Bid1Price).Sub(one)
					takerSr = decimal.Max(srMt, srTm)
				}
				nowSr = swap2Price.Div(swap1Price).Sub(one)
			}

			// 记录第一次出现不对齐成交时的信息（用于最终的makerInfo统计）
			if firstFilled == "" && cfg.SrMode != rate.SrNormal {
				if swap1Order.Filled.GreaterThan(swap2Order.Filled) {
					firstFilled = "swap1"
					firstNowSr = nowSr.String()
					if side == ccexgo.OrderSideBuy {
						firstSrWindow = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
					} else {
						firstSrWindow = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
					}
				} else if swap1Order.Filled.LessThan(swap2Order.Filled) {
					firstFilled = "swap2"
					firstNowSr = nowSr.String()
					if side == ccexgo.OrderSideBuy {
						firstSrWindow = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
					} else {
						firstSrWindow = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
					}
				}
			}

			// --- 步骤C3: 决策瀑布（4级优先级判断） ---
			// 做空价差(Buy): 期望SR高→开仓; SR下降→不利
			// 做多价差(Sell): 期望SR低→开仓; SR上升→不利
			if side == ccexgo.OrderSideBuy { // ===== 做空价差方向 =====
				// 【撤单条件】cancelNow: SR跌破撤单阈值，或统计窗口SR低于稳定阈值
				cancelNow := nowSr.LessThan(decimal.NewFromFloat(cfg.Queue.MmCancelOpenThreshold))
				if cfg.SrMode != rate.SrNormal && cfg.EnableStatCancel {
					srString := sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
					if srString != "NAN" {
						sr2, _ := decimal.NewFromString(srString)
						cancelNow = cancelNow || sr2.LessThan(decimal.NewFromFloat(cfg.Queue.OpenStableThreshold))
					}
				}

				// 【优先级1: 紧急Taker对冲】takerSr已低于阈值，即使用最好的taker价格，SR都不达标了
				// → 必须立即停止maker，用taker市价单紧急对冲止损
				if takerSr.LessThan(decimal.NewFromFloat(cfg.MmOpenTakerHedgeThreshold)) {
					takerHedge = true
					level.Info(logger).Log("message", "open nowSr < openTakerHedgeThreshold ", "nowSr", nowSr, "takerSr", takerSr, "swap1Ask1Price", swap1Ask1Price, "swap1Bid1Price", swap1Bid1Price, "swap2Ask1Price", swap2Ask1Price, "swap2Bid1Price", swap2Bid1Price)
				} else if nowSr.LessThan(decimal.NewFromFloat(cfg.MmOpenMakerHedgeThreshold)) && !makerHedge {
					// 【优先级2: Maker对冲模式】SR偏差但还没到紧急程度
					// 策略: 成交多的一边停止挂单(End), 成交少的一边撤单后用更激进的tick重挂(追单补齐)
					// 如果新挂单价恰好和当前一样，则不撤单（避免白撤）
					if order1 != nil && order1.Status < ccexgo.OrderStatusDone {
						swap1Cancel = true
					}
					if order2 != nil && order2.Status < ccexgo.OrderStatusDone {
						swap2Cancel = true
					}
					if swap1Order.Filled.GreaterThan(swap2Order.Filled) && !swap2End {
						swap2NeedReplace = true
						swap1End = true
						newSwap2Price := swap2Bid1Price.Add(decimal.NewFromInt(int64(cfg.MmSwap2OpenMakerHedgeTickNum)).Mul(pricePrecision2))
						if order2 != nil && newSwap2Price.Equal(order2.Price) && order2.Status != ccexgo.OrderStatusCancel { // 和当前挂单价一致，则不撤单
							swap2Cancel = false
							swap2NeedReplace = false
						}
						makerHedge = true
					} else if swap1Order.Filled.LessThan(swap2Order.Filled) && !swap1End {
						swap1NeedReplace = true
						swap2End = true
						newSwap1Price := swap1Ask1Price.Sub(decimal.NewFromInt(int64(cfg.MmSwap1OpenMakerHedgeTickNum)).Mul(pricePrecision1))
						if order1 != nil && newSwap1Price.Equal(order1.Price) && order1.Status != ccexgo.OrderStatusCancel { // 和当前挂单价一致，则不撤单
							swap1Cancel = false
							swap1NeedReplace = false
						}
						makerHedge = true
					} else {
						if order1 != nil && order2 != nil && order1.Status >= ccexgo.OrderStatusDone && order2.Status >= ccexgo.OrderStatusDone {
							finishedWork = true
						}
					}
					level.Info(logger).Log("message", "open nowSr < openMakerHedgeThreshold ", "nowSr", nowSr, "swap1Ask1Price", swap1Ask1Price, "swap1Bid1Price", swap1Bid1Price, "swap2Ask1Price", swap2Ask1Price, "swap2Bid1Price", swap2Bid1Price,
						"swap1Cancel", swap1Cancel, "swap2Cancel", swap2Cancel, "swap1NeedReplace", swap1NeedReplace, "swap2NeedReplace", swap2NeedReplace, "makerHedge", makerHedge, "swap1End", swap1End, "swap2End", swap2End)
				} else if cancelNow && !makerHedge {
					// 【优先级3: SR撤单】统计上看价差不稳定（SR低于撤单阈值或统计窗口不利）
					// 逻辑类似优先级2，但标记 cancelFilled=true（后续tick偏移仍用原始值而非更激进的MakerHedge值）
					srWindowCancel = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)

					if order1 != nil && order1.Status < ccexgo.OrderStatusDone {
						swap1Cancel = true
					}
					if order2 != nil && order2.Status < ccexgo.OrderStatusDone {
						swap2Cancel = true
					}
					if swap1Order.Filled.GreaterThan(swap2Order.Filled) && !swap2End {
						swap2NeedReplace = true
						swap1End = true
						newSwap2Price := swap2Bid1Price.Add(decimal.NewFromInt(int64(swap2OpenTick)).Mul(pricePrecision2))
						if order2 != nil && newSwap2Price.Equal(order2.Price) && order2.Status != ccexgo.OrderStatusCancel { // 和当前挂单价一致，则不撤单
							swap2Cancel = false
							swap2NeedReplace = false
						}
						makerHedge = true
						cancelFilled = true
					} else if swap1Order.Filled.LessThan(swap2Order.Filled) && !swap1End {
						swap1NeedReplace = true
						swap2End = true
						newSwap1Price := swap1Ask1Price.Sub(decimal.NewFromInt(int64(swap1OpenTick)).Mul(pricePrecision1))
						if order1 != nil && newSwap1Price.Equal(order1.Price) && order1.Status != ccexgo.OrderStatusCancel { // 和当前挂单价一致，则不撤单
							swap1Cancel = false
							swap1NeedReplace = false
						}
						makerHedge = true
						cancelFilled = true
					} else {
						if order1 != nil && order2 != nil && order1.Status >= ccexgo.OrderStatusDone && order2.Status >= ccexgo.OrderStatusDone {
							finishedWork = true
						}
					}
					level.Info(logger).Log("message", "open nowSr < cancelOpenThreshold ", "nowSr", nowSr, "swap1Ask1Price", swap1Ask1Price, "swap1Bid1Price", swap1Bid1Price, "swap2Ask1Price", swap2Ask1Price, "swap2Bid1Price", swap2Bid1Price,
						"swap1Cancel", swap1Cancel, "swap2Cancel", swap2Cancel, "swap1NeedReplace", swap1NeedReplace, "swap2NeedReplace", swap2NeedReplace, "finishedWork", finishedWork, "swap1End", swap1End, "swap2End", swap2End, "cancelFilled", cancelFilled)
				} else {
					// 【优先级4: 正常追价】SR正常，只检查挂单价是否偏离盘口太远需要重挂
					// priceChange = 盘口最优价 - 当前挂单价
					// 如果偏移超过 ReplaceTickNum × 最小价格精度 → 需要撤单重挂（追价）
					// 或者订单被交易所被动取消（非主动撤单）→ 也需要重挂
					replaceTickNum1 := cfg.MmSwap1OpenReplaceTickNum
					replaceTickNum2 := cfg.MmSwap2OpenReplaceTickNum

					if makerHedge && !cancelFilled {
						replaceTickNum1 = cfg.MmSwap1OpenMakerHedgeReplaceTickNum
						replaceTickNum2 = cfg.MmSwap2OpenMakerHedgeReplaceTickNum
					}

					var priceChange1, priceChange2 decimal.Decimal

					if order1 != nil {
						priceChange1 = swap1Ask1Price.Sub(order1.Price)
						if priceChange1.GreaterThan(decimal.NewFromInt(int64(replaceTickNum1)).Mul(pricePrecision1)) ||
							(!swap1End && order1.Status == ccexgo.OrderStatusCancel) {
							// 如果swap1非主动撤单(一般是被交易所取消)，且未成交，则需要重新下单
							if order1.Status < ccexgo.OrderStatusDone {
								swap1Cancel = true
							}
							if !swap1End && order1.Status != ccexgo.OrderStatusDone {
								swap1NeedReplace = true
							}
						}
					} else {
						if order2.Status == ccexgo.OrderStatusDone {
							swap1NeedReplace = true
						}
					}
					if order2 != nil {
						priceChange2 = order2.Price.Sub(swap2Bid1Price)
						if priceChange2.GreaterThan(decimal.NewFromInt(int64(replaceTickNum2)).Mul(pricePrecision2)) ||
							(!swap2End && order2.Status == ccexgo.OrderStatusCancel) {
							// 如果swap2非主动撤单(一般是被交易所取消)，且未成交，则需要重新下单
							if order2.Status < ccexgo.OrderStatusDone {
								swap2Cancel = true
							}
							if !swap2End && order2.Status != ccexgo.OrderStatusDone {
								swap2NeedReplace = true
							}
						}
					} else {
						if order1.Status == ccexgo.OrderStatusDone {
							swap2NeedReplace = true
						}
					}

					level.Info(logger).Log("message", "open normal check", "nowSr", nowSr, "swap1Ask1Price", swap1Ask1Price, "swap1Bid1Price", swap1Bid1Price, "swap2Ask1Price", swap2Ask1Price, "swap2Bid1Price", swap2Bid1Price,
						"swap1Cancel", swap1Cancel, "swap2Cancel", swap2Cancel, "swap1NeedReplace", swap1NeedReplace, "swap2NeedReplace", swap2NeedReplace, "priceChange1", priceChange1, "priceChange2", priceChange2, "swap1End", swap1End, "swap2End", swap2End,
						"cancelFilled", cancelFilled)
				}

			} else { // 做多价差，和做空价差对称
				cancelNow := nowSr.GreaterThan(decimal.NewFromFloat(cfg.Queue.MmCancelCloseThreshold))
				if cfg.SrMode != rate.SrNormal && cfg.EnableStatCancel {
					srString := sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
					if srString != "NAN" {
						sr2, _ := decimal.NewFromString(srString)
						cancelNow = cancelNow || sr2.GreaterThan(decimal.NewFromFloat(cfg.Queue.CloseStableThreshold))
					}
				}

				if takerSr.GreaterThan(decimal.NewFromFloat(cfg.MmCloseTakerHedgeThreshold)) {
					takerHedge = true
					level.Info(logger).Log("message", "close nowSr > closeTakerHedgeThreshold ", "nowSr", nowSr, "takerSr", takerSr, "swap1Ask1Price", swap1Ask1Price, "swap1Bid1Price", swap1Bid1Price, "swap2Ask1Price", swap2Ask1Price, "swap2Bid1Price", swap2Bid1Price)
				} else if nowSr.GreaterThan(decimal.NewFromFloat(cfg.MmCloseMakerHedgeThreshold)) && !makerHedge {
					if order1 != nil && order1.Status < ccexgo.OrderStatusDone {
						swap1Cancel = true
					}
					if order2 != nil && order2.Status < ccexgo.OrderStatusDone {
						swap2Cancel = true
					}
					if swap1Order.Filled.GreaterThan(swap2Order.Filled) && !swap2End {
						swap2NeedReplace = true
						swap1End = true
						newSwap2Price := swap2Ask1Price.Sub(decimal.NewFromInt(int64(cfg.MmSwap2CloseMakerHedgeTickNum)).Mul(pricePrecision2))
						if order2 != nil && newSwap2Price.Equal(order2.Price) && order2.Status != ccexgo.OrderStatusCancel { // 和当前挂单价一致，则不撤单
							swap2Cancel = false
							swap2NeedReplace = false
						}
						makerHedge = true
					} else if swap1Order.Filled.LessThan(swap2Order.Filled) && !swap1End {
						swap1NeedReplace = true
						swap2End = true
						newSwap1Price := swap1Bid1Price.Add(decimal.NewFromInt(int64(cfg.MmSwap1CloseMakerHedgeTickNum)).Mul(pricePrecision1))
						if order1 != nil && newSwap1Price.Equal(order1.Price) && order1.Status != ccexgo.OrderStatusCancel { // 和当前挂单价一致，则不撤单
							swap1Cancel = false
							swap1NeedReplace = false
						}
						makerHedge = true
					} else {
						if order1 != nil && order2 != nil && order1.Status >= ccexgo.OrderStatusDone && order2.Status >= ccexgo.OrderStatusDone {
							finishedWork = true
						}
					}
					level.Info(logger).Log("message", "close nowSr > closeMakerHedgeThreshold ", "nowSr", nowSr, "swap1Ask1Price", swap1Ask1Price, "swap1Bid1Price", swap1Bid1Price, "swap2Ask1Price", swap2Ask1Price, "swap2Bid1Price", swap2Bid1Price,
						"swap1Cancel", swap1Cancel, "swap2Cancel", swap2Cancel, "swap1NeedReplace", swap1NeedReplace, "swap2NeedReplace", swap2NeedReplace, "makerHedge", makerHedge, "swap1End", swap1End, "swap2End", swap2End)
				} else if cancelNow && !makerHedge {
					srWindowCancel = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)

					if order1 != nil && order1.Status < ccexgo.OrderStatusDone {
						swap1Cancel = true
					}
					if order2 != nil && order2.Status < ccexgo.OrderStatusDone {
						swap2Cancel = true
					}
					if swap1Order.Filled.GreaterThan(swap2Order.Filled) && !swap2End {
						swap2NeedReplace = true
						swap1End = true
						newSwap2Price := swap2Ask1Price.Sub(decimal.NewFromInt(int64(swap2CloseTick)).Mul(pricePrecision2))
						if order2 != nil && newSwap2Price.Equal(order2.Price) && order2.Status != ccexgo.OrderStatusCancel { // 和当前挂单价一致，则不撤单
							swap2Cancel = false
							swap2NeedReplace = false
						}
						makerHedge = true
						cancelFilled = true
					} else if swap1Order.Filled.LessThan(swap2Order.Filled) && !swap1End {
						swap1NeedReplace = true
						swap2End = true
						newSwap1Price := swap1Bid1Price.Add(decimal.NewFromInt(int64(swap1CloseTick)).Mul(pricePrecision1))
						if order1 != nil && newSwap1Price.Equal(order1.Price) && order1.Status != ccexgo.OrderStatusCancel { // 和当前挂单价一致，则不撤单
							swap1Cancel = false
							swap1NeedReplace = false
						}
						makerHedge = true
						cancelFilled = true
					} else {
						if order1 != nil && order2 != nil && order1.Status >= ccexgo.OrderStatusDone && order2.Status >= ccexgo.OrderStatusDone {
							finishedWork = true
						}
					}
					level.Info(logger).Log("message", "close nowSr > cancelCloseThreshold ", "nowSr", nowSr, "swap1Ask1Price", swap1Ask1Price, "swap1Bid1Price", swap1Bid1Price, "swap2Ask1Price", swap2Ask1Price, "swap2Bid1Price", swap2Bid1Price,
						"swap1Cancel", swap1Cancel, "swap2Cancel", swap2Cancel, "swap1NeedReplace", swap1NeedReplace, "swap2NeedReplace", swap2NeedReplace, "finishedWork", finishedWork, "swap1End", swap1End, "swap2End", swap2End, "cancelFilled", cancelFilled)
				} else {
					replaceTickNum1 := cfg.MmSwap1CloseReplaceTickNum
					replaceTickNum2 := cfg.MmSwap2CloseReplaceTickNum

					if makerHedge && !cancelFilled {
						replaceTickNum1 = cfg.MmSwap1CloseMakerHedgeReplaceTickNum
						replaceTickNum2 = cfg.MmSwap2CloseMakerHedgeReplaceTickNum
					}

					var priceChange1, priceChange2 decimal.Decimal

					if order1 != nil {
						priceChange1 = order1.Price.Sub(swap1Bid1Price)
						if priceChange1.GreaterThan(decimal.NewFromInt(int64(replaceTickNum1)).Mul(pricePrecision1)) ||
							(!swap1End && order1.Status == ccexgo.OrderStatusCancel) {
							// 如果swap1非主动撤单(一般是被交易所取消)，且未成交，则需要重新下单
							if order1.Status < ccexgo.OrderStatusDone {
								swap1Cancel = true
							}
							if !swap1End && order1.Status != ccexgo.OrderStatusDone {
								swap1NeedReplace = true
							}
						}
					} else {
						if order2.Status == ccexgo.OrderStatusDone {
							swap1NeedReplace = true
						}
					}
					if order2 != nil {
						priceChange2 = swap2Ask1Price.Sub(order2.Price)
						if priceChange2.GreaterThan(decimal.NewFromInt(int64(replaceTickNum2)).Mul(pricePrecision2)) ||
							(!swap2End && order2.Status == ccexgo.OrderStatusCancel) {
							// 如果swap2非主动撤单(一般是被交易所取消)，且未成交，则需要重新下单
							if order2.Status < ccexgo.OrderStatusDone {
								swap2Cancel = true
							}
							if !swap2End && order2.Status != ccexgo.OrderStatusDone {
								swap2NeedReplace = true
							}
						}
					} else {
						if order1.Status == ccexgo.OrderStatusDone {
							swap2NeedReplace = true
						}
					}
					level.Info(logger).Log("message", "open normal check", "nowSr", nowSr, "swap1Ask1Price", swap1Ask1Price, "swap1Bid1Price", swap1Bid1Price, "swap2Ask1Price", swap2Ask1Price, "swap2Bid1Price", swap2Bid1Price,
						"swap1Cancel", swap1Cancel, "swap2Cancel", swap2Cancel, "swap1NeedReplace", swap1NeedReplace, "swap2NeedReplace", swap2NeedReplace, "priceChange1", priceChange1, "priceChange2", priceChange2, "swap1End", swap1End, "swap2End", swap2End,
						"cancelFilled", cancelFilled)
				}
			}

			// --- 步骤C4: 如果需要taker对冲或两边都完成，跳出循环 ---
			if takerHedge || finishedWork {
				goto checkAndHedge
			}

			// --- 步骤C4续: 执行撤单 ---
			// 并发撤掉需要撤的挂单（swap1Cancel/swap2Cancel标志）
			// 撤单过程中可能有新的成交（部分成交），需要更新聚合订单的Filled/AvgPrice/Fee
			// CancelOrder返回的Fee是累计总手续费，需要用增量(newFee=o.Fee-order.Fee)来更新
			var wg sync.WaitGroup
			if swap1Cancel && order1 != nil {
				wg.Go(func() {
					o, err := swapC1.CancelOrder(ctx, order1)
					if err != nil {
						errType := swapC1.HandError(err)
						level.Warn(logger).Log("message", "cancel order1 failed", "order", fmt.Sprintf("%+v", order1), "error", errType.String())
						go message.Send(ctx, message.NewOperateFail(fmt.Sprintf("%s swap1部分中途开仓撤单失败", sg.swapS1), errType.String()))
					} else {
						if !o.Filled.IsZero() {
							if firstFilled == "" && cfg.SrMode != rate.SrNormal {
								firstFilled = "swap1"
								firstNowSr = nowSr.String()
								if side == ccexgo.OrderSideBuy {
									firstSrWindow = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
								} else {
									firstSrWindow = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
								}
							}

							newFilled := o.Filled.Sub(order1.Filled)
							if newFilled.IsPositive() {
								newFee := o.Fee.Sub(order1.Fee) // CancelOrder撤单查单是总手续费
								if swap1Order.Filled.Add(newFilled).IsPositive() {
									swap1Order.AvgPrice = swap1Order.Filled.Mul(swap1Order.AvgPrice).Add(newFilled.Mul(o.AvgPrice)).Div(swap1Order.Filled.Add(newFilled))
								}
								swap1Order.Filled = swap1Order.Filled.Add(newFilled)
								swap1Order.Fee = swap1Order.Fee.Add(newFee.Neg()) //撤单主动查出来的是总手续费，swap取反保持统一
								swap1Order.Updated = o.Updated
								if o.FeeCurrency != `` && swap1Order.FeeCurrency == `` {
									swap1Order.FeeCurrency = o.FeeCurrency
								}
							}
						}
						if swap1Order.Filled.GreaterThanOrEqual(swap1Order.Amount) {
							swap1Order.Status = ccexgo.OrderStatusDone
						}
						order1.Status = o.Status
						order1.Filled = o.Filled
						order1.Fee = o.Fee
						order1.Updated = o.Updated
						swap1Cancel = false
					}
				})
			} else if swap1Cancel {
				swap1Cancel = false
			}

			if swap2Cancel && order2 != nil {
				wg.Go(func() {
					o, err := swapC2.CancelOrder(ctx, order2) // 撤单,查单
					if err != nil {
						errType := swapC2.HandError(err)
						level.Warn(logger).Log("message", "cancel order2 failed", "order", fmt.Sprintf("%+v", order2), "error", errType.String())
						go message.Send(ctx, message.NewOperateFail(fmt.Sprintf("%s swap2部分中途开仓撤单失败", sg.swapS2), errType.String()))
					} else {
						if !o.Filled.IsZero() {
							if firstFilled == "" && cfg.SrMode != rate.SrNormal {
								firstFilled = "swap2"
								firstNowSr = nowSr.String()
								if side == ccexgo.OrderSideBuy {
									firstSrWindow = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
								} else {
									firstSrWindow = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
								}
							}

							newFilled := o.Filled.Sub(order2.Filled)
							if newFilled.IsPositive() {
								newFee := o.Fee.Sub(order2.Fee)
								if swap2Order.Filled.Add(newFilled).IsPositive() {
									swap2Order.AvgPrice = swap2Order.Filled.Mul(swap2Order.AvgPrice).Add(newFilled.Mul(o.AvgPrice)).Div(swap2Order.Filled.Add(newFilled))
								}
								swap2Order.Filled = swap2Order.Filled.Add(newFilled)
								swap2Order.Fee = swap2Order.Fee.Add(newFee.Neg())
								swap2Order.Updated = o.Updated
								if o.FeeCurrency != `` && swap2Order.FeeCurrency == `` {
									swap2Order.FeeCurrency = o.FeeCurrency
								}
							}
						}
						if swap2Order.Filled.GreaterThanOrEqual(swap2Order.Amount) {
							swap2Order.Status = ccexgo.OrderStatusDone
						}
						order2.Status = o.Status
						order2.Filled = o.Filled
						order2.Fee = o.Fee
						order2.Updated = o.Updated
						swap2Cancel = false
					}
				})
			} else if swap2Cancel {
				swap2Cancel = false
			}
			wg.Wait()

			// --- 步骤C5: 动态修正标记 ---
			// 撤单过程中可能有新的成交导致两边数量关系反转
			// 例: 之前 swap1多 → 标记swap1End, 但撤单时swap2又成交了 → swap2反而更多了
			// 此时需要反转End/NeedReplace标记，确保追的是正确的一边
			if order1 != nil && order2 != nil && makerHedge && !swap1Cancel && !swap2Cancel && order1.Status >= ccexgo.OrderStatusDone && order2.Status >= ccexgo.OrderStatusDone {
				if swap1End && swap2Order.Filled.GreaterThan(swap1Order.Filled) {
					// swap1标记结束但swap2成交更多 → 反转：swap2停工，追swap1
					swap2End = true
					swap2NeedReplace = false
					swap1End = false
					swap1NeedReplace = true
					level.Warn(logger).Log("message", "reverse swap1 and swap2 flag", "reason", "swap2 filled GreaterThan swap1")
				} else if swap2End && swap1Order.Filled.GreaterThan(swap2Order.Filled) {
					// swap2标记结束但swap1成交更多 → 反转：swap1停工，追swap2
					swap1End = true
					swap1NeedReplace = false
					swap2End = false
					swap2NeedReplace = true
					level.Warn(logger).Log("message", "reverse swap1 and swap2 flag", "reason", "swap1 filled GreaterThan swap2")
				}
			}

			// --- 步骤C6: 检查是否可以结束循环 ---
			// 条件1: 某一边已满额成交 且 另一边也已完成
			if (swap1Order.Filled.GreaterThanOrEqual(swap1Order.Amount) && !swap2Cancel && swap2Order.Status == ccexgo.OrderStatusDone) ||
				(swap2Order.Filled.GreaterThanOrEqual(swap2Order.Amount) && !swap1Cancel && swap1Order.Status == ccexgo.OrderStatusDone) {
				goto checkAndHedge
			}

			// 条件2: 无需撤单、至少一边无需重挂、两边订单都是终态、且成交差额小于最小下单量
			// → 差额太小无法再挂单，视为实质完成
			if !swap1Cancel && !swap2Cancel && (!swap1NeedReplace || !swap2NeedReplace) && order1 != nil && order2 != nil {
				if order1.Status >= ccexgo.OrderStatusDone && order2.Status >= ccexgo.OrderStatusDone {
					diff := swap1Order.Filled.Sub(swap2Order.Filled).Abs()
					maxMinAmount := math.Max(sg.minOrderAmount1, sg.minOrderAmount2)
					if diff.LessThan(decimal.NewFromFloat(maxMinAmount)) {
						goto checkAndHedge
					}
				}
			}

			// --- 步骤C7: 重新挂单(Replace) ---
			// 【swap1重挂单】条件: 当前swap1订单已终结(Done/Cancel) 且 swap1NeedReplace=true
			if (order1 == nil || order1.Status >= ccexgo.OrderStatusDone) && swap1NeedReplace {
				// 计算重挂数量:
				//   正常模式: openAmount = 总目标量 - 已成交量
				//   makerHedge模式: openAmount = swap2已成交 - swap1已成交（只补差额）
				openAmount := swap1Order.Amount.Sub(swap1Order.Filled).InexactFloat64()
				tickNum := cfg.MmSwap1OpenTickNum
				if makerHedge {
					openAmount = swap2Order.Filled.Sub(swap1Order.Filled).InexactFloat64()
					if !cancelFilled {
						tickNum = cfg.MmSwap1OpenMakerHedgeTickNum
					}
				}
				if side == ccexgo.OrderSideSell {
					tickNum = cfg.MmSwap1CloseTickNum
					if makerHedge && !cancelFilled {
						tickNum = cfg.MmSwap1CloseMakerHedgeTickNum
					}
				}
				if openAmount < sg.minOrderAmount1 {
					swap1Order.Status = ccexgo.OrderStatusDone
					swap1NeedReplace = false
					// 如果swap2不需要重挂单, 当前swap1已经不满足重挂精度, 则直接去taker
					if !swap2Cancel && !swap2NeedReplace {
						makerReplaceFailed = true
						goto checkAndHedge
					}
					goto replace2
				}
				// 调用 OpenSwapMaker 在swap1上重新挂限价单
				_, o, errMsg1, err := sg.PosList.OpenSwapMaker(ctx, swapC1, srOpen, openAmount, float64(tickNum), sg.swap1OrderBook, op, side, 0, false)
				if err != nil {
					order1.Status = ccexgo.OrderStatusCancel
					errType := swapC1.HandError(err)
					level.Warn(logger).Log("message", "replace open swap1 failed", "openAmount", openAmount, "tickNum", tickNum, "errMsg1", errMsg1)
					go message.Send(ctx, message.NewOperateFail(fmt.Sprintf("%s swap1部分开仓重挂失败", sg.swapS1), errMsg1))
					// NewOrderRejected(风控拒绝) → 跳过swap1去处理swap2
					// 其他错误 → makerReplaceFailed, 降级到taker对冲
					if errType.Code != exchange.NewOrderRejected {
						makerReplaceFailed = true
						errMsg = errMsg1
						goto checkAndHedge
					} else {
						goto replace2
					}
				}
				order1 = o
				swap1NeedReplace = false
				swap1Order.ID = order1.ID
				swap1Order.IDs = append(swap1Order.IDs, order1.ID.String())
				if swap1Order.Created.IsZero() {
					swap1Order.Created = order1.Created
					if strings.Contains(swapC1.GetExchangeName(), exchange.Binance) && swap1Order.Created.IsZero() {
						swap1Order.Created = time.Now()
					}
				}
				// 考虑下单即成交的情况
				if !o.Filled.IsZero() {
					if firstFilled == "" && cfg.SrMode != rate.SrNormal {
						firstFilled = "swap1"
						firstNowSr = nowSr.String()
						if side == ccexgo.OrderSideBuy {
							firstSrWindow = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
						} else {
							firstSrWindow = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
						}
					}

					newFilled := o.Filled
					newFee := o.Fee
					if swap1Order.Filled.Add(newFilled).IsPositive() {
						swap1Order.AvgPrice = swap1Order.Filled.Mul(swap1Order.AvgPrice).Add(newFilled.Mul(o.AvgPrice)).Div(swap1Order.Filled.Add(newFilled))
					}
					swap1Order.Filled = swap1Order.Filled.Add(newFilled)
					swap1Order.Fee = swap1Order.Fee.Add(newFee.Neg())

					swap1Order.Status = o.Status
					swap1Order.Updated = o.Updated
					if o.FeeCurrency != `` && swap1Order.FeeCurrency == `` {
						swap1Order.FeeCurrency = o.FeeCurrency
					}
				}
			}

		replace2:
			if (order2 == nil || order2.Status >= ccexgo.OrderStatusDone) && swap2NeedReplace {
				openAmount := swap2Order.Amount.Sub(swap2Order.Filled).InexactFloat64()
				tickNum := cfg.MmSwap2OpenTickNum
				if makerHedge {
					openAmount = swap1Order.Filled.Sub(swap2Order.Filled).InexactFloat64()
					if !cancelFilled {
						tickNum = cfg.MmSwap2OpenMakerHedgeTickNum
					}
				}
				if side == ccexgo.OrderSideSell {
					tickNum = cfg.MmSwap2CloseTickNum
					if makerHedge && !cancelFilled {
						tickNum = cfg.MmSwap2CloseMakerHedgeTickNum
					}
				}
				if openAmount < sg.minOrderAmount2 {
					swap2Order.Status = ccexgo.OrderStatusDone
					swap2NeedReplace = false
					// 如果swap1不需要重挂单, 当前swap2已经不满足重挂精度, 则直接去taker
					if !swap1Cancel && !swap1NeedReplace {
						makerReplaceFailed = true
						goto checkAndHedge
					}
					continue
				}
				_, o, errMsg1, err := sg.PosList.OpenSwapMaker(ctx, swapC2, srOpen, openAmount, float64(tickNum), sg.swap2OrderBook, op, reverseSide(side), 0, false)
				if err != nil {
					errType := swapC2.HandError(err)
					level.Warn(logger).Log("message", "replace open swap2 failed", "openAmount", openAmount, "tickNum", tickNum, "errMsg1", errMsg1)
					go message.Send(ctx, message.NewOperateFail(fmt.Sprintf("%s swap2部分开仓重挂失败", sg.swapS2), errMsg1))
					if errType.Code != exchange.NewOrderRejected { // 直接去taker？
						makerReplaceFailed = true
						errMsg = errMsg1
						goto checkAndHedge
					} else {
						continue
					}
				}
				order2 = o
				swap2NeedReplace = false
				swap2Order.ID = order2.ID
				swap2Order.IDs = append(swap2Order.IDs, order2.ID.String())
				if swap2Order.Created.IsZero() {
					swap2Order.Created = order2.Created
					if strings.Contains(swapC2.GetExchangeName(), exchange.Binance) && swap2Order.Created.IsZero() {
						swap2Order.Created = time.Now()
					}
				}
				// 考虑下单即成交的情况
				if !o.Filled.IsZero() {
					if firstFilled == "" && cfg.SrMode != rate.SrNormal {
						firstFilled = "swap2"
						firstNowSr = nowSr.String()
						if side == ccexgo.OrderSideBuy {
							firstSrWindow = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
						} else {
							firstSrWindow = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
						}
					}

					newFilled := o.Filled
					newFee := o.Fee
					if swap2Order.Filled.Add(newFilled).IsPositive() {
						swap2Order.AvgPrice = swap2Order.Filled.Mul(swap2Order.AvgPrice).Add(newFilled.Mul(o.AvgPrice)).Div(swap2Order.Filled.Add(newFilled))
					}
					swap2Order.Filled = swap2Order.Filled.Add(newFilled)
					swap2Order.Fee = swap2Order.Fee.Add(newFee.Neg())

					swap2Order.Status = o.Status
					swap2Order.Updated = o.Updated

					if o.FeeCurrency != `` && swap2Order.FeeCurrency == `` {
						swap2Order.FeeCurrency = o.FeeCurrency
					}
				}
			}

		case o := <-swapC1.MakerOrder(swap1Order.ID.String()):
			// 【分支D: swap1 WebSocket订单推送】
			// 交易所通过WS推送订单状态变化（部分成交、完全成交、取消等）
			// 只处理比当前已知状态更新的推送（Filled更大 或 Updated更晚 或 Status更高）
			if o == nil { // channel超时关闭了
				continue
			}
			level.Info(logger).Log("message", "get swap1 ws order notify", "id", o.ID.String(), "amount", o.Amount, "filled", o.Filled, "fee", o.Fee, "avg_price", o.AvgPrice, "status", o.Status)

			if order1.ID.String() == o.ID.String() && (order1.Filled.LessThan(o.Filled) || order1.Updated.Before(o.Updated) || order1.Status < o.Status) {
				newFilled := o.Filled.Sub(order1.Filled)
				if newFilled.IsPositive() {
					// 手续费处理差异: Bybit推送的Fee是累计总额; 其他交易所推送的是本次增量
					newFee := o.Fee
					if swapC1.GetExchangeName() == exchange.Bybit5 {
						newFee = o.Fee.Sub(order1.Fee)
					}
					if swap1Order.Filled.Add(newFilled).IsPositive() {
						swap1Order.AvgPrice = swap1Order.Filled.Mul(swap1Order.AvgPrice).Add(newFilled.Mul(o.AvgPrice)).Div(swap1Order.Filled.Add(newFilled))
					}
					swap1Order.Filled = swap1Order.Filled.Add(newFilled)
					swap1Order.Fee = swap1Order.Fee.Add(newFee.Neg())

					if swapC1.GetExchangeName() == exchange.Bybit5 {
						order1.Fee = o.Fee
					} else {
						order1.Fee = order1.Fee.Add(o.Fee)
					}

					if firstFilled == "" && cfg.SrMode != rate.SrNormal {
						if swap1Order.Filled.GreaterThan(swap2Order.Filled) {
							firstFilled = "swap1"
							firstNowSr = nowSr.String()
							if side == ccexgo.OrderSideBuy {
								firstSrWindow = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
							} else {
								firstSrWindow = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
							}
						} else if swap1Order.Filled.LessThan(swap2Order.Filled) {
							firstFilled = "swap2"
							firstNowSr = nowSr.String()
							if side == ccexgo.OrderSideBuy {
								firstSrWindow = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
							} else {
								firstSrWindow = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
							}
						}
					}
				}

				swap1Order.Status = o.Status
				swap1Order.Updated = o.Updated
				if swap1Order.Created.IsZero() {
					swap1Order.Created = o.Created
				}

				order1.Status = o.Status
				order1.Filled = o.Filled
				order1.Updated = o.Updated
			}

			if o.FeeCurrency != `` {
				swap1Order.FeeCurrency = o.FeeCurrency
			}
			// if !strings.Contains(swapC1.GetExchangeName(), exchange.Binance) { // 币安合约ws推送没有创建时间
			// 	swap1Order.Created = o.Created
			// }

			if order1.Status >= ccexgo.OrderStatusDone {
				if swap1Cancel {
					swap1Cancel = false
				}
			}

			if swap1Order.Status != ccexgo.OrderStatusDone {
				continue
			}
			if swap2Order.Status == ccexgo.OrderStatusDone || (swap2Order.Status == ccexgo.OrderStatusCancel && swap2End) {
				goto checkAndHedge
			}

		case o := <-swapC2.MakerOrder(swap2Order.ID.String()):
			if o == nil {
				continue
			}
			level.Info(logger).Log("message", "get swap2 ws order notify", "id", o.ID.String(), "amount", o.Amount, "filled", o.Filled, "fee", o.Fee, "avg_price", o.AvgPrice, "status", o.Status)
			if order2.ID.String() == o.ID.String() && (order2.Filled.LessThan(o.Filled) || order2.Updated.Before(o.Updated) || order2.Status < o.Status) {
				newFilled := o.Filled.Sub(order2.Filled)
				if newFilled.IsPositive() {
					newFee := o.Fee
					if swapC2.GetExchangeName() == exchange.Bybit5 {
						newFee = o.Fee.Sub(order2.Fee)
					}
					if swap2Order.Filled.Add(newFilled).IsPositive() {
						swap2Order.AvgPrice = swap2Order.Filled.Mul(swap2Order.AvgPrice).Add(newFilled.Mul(o.AvgPrice)).Div(swap2Order.Filled.Add(newFilled))
					}
					swap2Order.Filled = swap2Order.Filled.Add(newFilled)
					swap2Order.Fee = swap2Order.Fee.Add(newFee.Neg())

					if swapC2.GetExchangeName() == exchange.Bybit5 {
						order2.Fee = o.Fee
					} else {
						order2.Fee = order2.Fee.Add(o.Fee)
					}
					if firstFilled == "" && cfg.SrMode != rate.SrNormal {
						if swap1Order.Filled.GreaterThan(swap2Order.Filled) {
							firstFilled = "swap1"
							firstNowSr = nowSr.String()
							if side == ccexgo.OrderSideBuy {
								firstSrWindow = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
							} else {
								firstSrWindow = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
							}
						} else if swap1Order.Filled.LessThan(swap2Order.Filled) {
							firstFilled = "swap2"
							firstNowSr = nowSr.String()
							if side == ccexgo.OrderSideBuy {
								firstSrWindow = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
							} else {
								firstSrWindow = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
							}
						}
					}
				}

				swap2Order.Status = o.Status
				swap2Order.Updated = o.Updated
				if swap2Order.Created.IsZero() {
					swap2Order.Created = o.Created
				}

				order2.Status = o.Status
				order2.Filled = o.Filled
				order2.Updated = o.Updated
			}

			if o.FeeCurrency != `` {
				swap2Order.FeeCurrency = o.FeeCurrency
			}
			// if !strings.Contains(swapC2.GetExchangeName(), exchange.Binance) { // 币安合约ws推送没有创建时间
			// 	swap2Order.Created = o.Created
			// }

			if order2.Status >= ccexgo.OrderStatusDone {
				if swap2Cancel {
					swap2Cancel = false
				}
			}

			if swap2Order.Status != ccexgo.OrderStatusDone {
				continue
			}
			if swap1Order.Status == ccexgo.OrderStatusDone || (swap1Order.Status == ccexgo.OrderStatusCancel && swap1End) {
				goto checkAndHedge
			}

		}
	}




	// ==================== 阶段3: checkAndHedge — Taker对冲补齐 ====================
	// 从主循环跳出后到达这里，执行:
	//   1. 撤掉所有未完成的挂单
	//   2. 计算两边成交差额 (swap1Filled - swap2Filled)
	//   3. 用市价单(taker)在成交少的一边补齐差额
checkAndHedge:
	makerAmount1 = swap1Order.Filled // 记录maker部分的成交量（在taker补齐之前）
	makerAmount2 = swap2Order.Filled
	if takerHedge || finishedWork || makerReplaceFailed {
		// 【步骤1: 撤掉swap1的未完成挂单】
		if order1 != nil && order1.Status < ccexgo.OrderStatusDone {
			level.Info(logger).Log("message", "taker hedge cancel order", "order", order1.ID, "finished_work", finishedWork, "maker_replace_failed", makerReplaceFailed)
			o, err := swapC1.CancelOrder(ctx, order1)
			if err != nil {
				errType := swapC1.HandError(err)
				level.Warn(logger).Log("message", "taker hedge cancel order failed", "order", fmt.Sprintf("%+v", order1), "error", errType.String())
				go message.Send(ctx, message.NewOperateFail(fmt.Sprintf("%s swap1部分开仓taker撤单失败", sg.swapS1), errType.String()))
			} else {
				if !o.Filled.IsZero() {
					newFilled := o.Filled.Sub(order1.Filled)
					if newFilled.IsPositive() {
						newFee := o.Fee.Sub(order1.Fee)
						if swap1Order.Filled.Add(newFilled).IsPositive() {
							swap1Order.AvgPrice = swap1Order.Filled.Mul(swap1Order.AvgPrice).Add(newFilled.Mul(o.AvgPrice)).Div(swap1Order.Filled.Add(newFilled))
						}
						swap1Order.Filled = swap1Order.Filled.Add(newFilled)
						swap1Order.Fee = swap1Order.Fee.Add(newFee.Neg())
						swap1Order.Updated = o.Updated
						if o.FeeCurrency != `` && swap1Order.FeeCurrency == `` {
							swap1Order.FeeCurrency = o.FeeCurrency
						}
					}
				}
				swap1Order.Status = ccexgo.OrderStatusCancel
			}
		}
		makerAmount1 = swap1Order.Filled

		if order2 != nil && order2.Status < ccexgo.OrderStatusDone {
			level.Info(logger).Log("message", "taker hedge cancel order2", "order", order2.ID, "finished_work", finishedWork)
			o, err := swapC2.CancelOrder(ctx, order2) // 撤单,查单
			if err != nil {
				errType := swapC2.HandError(err)
				level.Warn(logger).Log("message", "taker hedge cancel order2 failed", "order", fmt.Sprintf("%+v", order2), "error", errType.String())
				go message.Send(ctx, message.NewOperateFail(fmt.Sprintf("%s swap2部分开仓taker撤单失败", sg.swapS2), errType.String()))
			} else {
				if !o.Filled.IsZero() {
					newFilled := o.Filled.Sub(order2.Filled)
					if newFilled.IsPositive() {
						newFee := o.Fee.Sub(order2.Fee)
						if swap2Order.Filled.Add(newFilled).IsPositive() {
							swap2Order.AvgPrice = swap2Order.Filled.Mul(swap2Order.AvgPrice).Add(newFilled.Mul(o.AvgPrice)).Div(swap2Order.Filled.Add(newFilled))
						}
						swap2Order.Filled = swap2Order.Filled.Add(newFilled)
						swap2Order.Fee = swap2Order.Fee.Add(newFee.Neg())
						swap2Order.Updated = o.Updated
						if o.FeeCurrency != `` && swap2Order.FeeCurrency == `` {
							swap2Order.FeeCurrency = o.FeeCurrency
						}
					}
				}
				swap2Order.Status = ccexgo.OrderStatusCancel
			}
		}
		makerAmount2 = swap2Order.Filled

		// 【步骤2: 计算两边成交差额，确定需要在哪边用taker补齐】
		// needTakerHedgeAmount > 0: swap1多了 → 在swap2上taker补齐
		// needTakerHedgeAmount < 0: swap2多了 → 在swap1上taker补齐
		// needTakerHedgeAmount = 0: 两边一样，不需要补齐
		needTakerHedgeAmount := swap1Order.Filled.Sub(swap2Order.Filled)
		var (
			swapClient     exchange.SwapClient
			placeSide      ccexgo.OrderSide
			minOrderAmount float64
			swapName       string
			swapSymbol     ccexgo.SwapSymbol
		)
		if needTakerHedgeAmount.IsPositive() {
			swapClient = swapC2
			placeSide = reverseSide(side)
			needTakerHedgeAmount = needTakerHedgeAmount.Round(int32(exp2))
			minOrderAmount = sg.minOrderAmount2
			swapName = `swap2`
			swapSymbol = sg.swapS2
		} else if needTakerHedgeAmount.IsNegative() {
			swapClient = swapC1
			placeSide = side
			needTakerHedgeAmount = needTakerHedgeAmount.Abs().Round(int32(exp1))
			minOrderAmount = sg.minOrderAmount1
			swapName = `swap1`
			swapSymbol = sg.swapS1
		} else {
			goto hedge // 两边成交量完全一致，无需taker补齐
		}

		if needTakerHedgeAmount.LessThan(decimal.NewFromFloat(minOrderAmount)) {
			goto hedge // 差额小于最小下单量，无法下单，忽略
		}

		// 【步骤3: 执行市价单补齐差额】
		or, err := swapClient.MarketOrder(ctx, placeSide, needTakerHedgeAmount, decimal.Zero, nil)
		if err != nil {
			level.Warn(logger).Log("message", "taker hedge market order failed", "error", err.Error())
			errType := swapClient.HandError(err)
			sg.PosList.UpdateBannedTime(errType, false)
			sg.PosList.CheckErrShareBan(errType, swapClient.GetExchangeName())
			msg := message.NewOperateFail(fmt.Sprintf("%s %s(%s)部分开仓taker对冲失败", swapSymbol, swapName, swapClient.GetExchangeName()), errType.String())
			if errType.Code != exchange.OtherReason {
				go message.Send(ctx, msg)
			} else {
				go message.SendP3Important(ctx, msg)
			}
		} else {
			if !or.Filled.IsZero() {
				addInfo := func(swapOrder *exchange.CfOrder, o *exchange.CfOrder, swapSymbol ccexgo.SwapSymbol) {
					swapOrder.AvgPrice = swapOrder.Filled.Mul(swapOrder.AvgPrice).Add(o.Filled.Mul(o.AvgPrice)).Div(swapOrder.Filled.Add(o.Filled))
					swapOrder.Filled = swapOrder.Filled.Add(o.Filled)
					swapOrder.Updated = o.Updated
					swapOrder.Status = ccexgo.OrderStatusCancel
					swapOrder.IDs = append(swapOrder.IDs, o.ID.String())
					if o.Filled.Equal(swapOrder.Amount) || swapOrder.ID == nil {
						swapOrder.ID = o.ID
					}
					if swapOrder.FeeCurrency == "" {
						swapOrder.FeeCurrency = o.FeeCurrency
					} else {
						if swapOrder.FeeCurrency != o.FeeCurrency && o.FeeCurrency != `` {
							swapOrder.FeeCurrency += `_` + o.FeeCurrency
						}
					}
					// 币安taker可能是bnb抵扣费, 要计算出对应的usdt费
					fee := o.Fee
					if strings.Contains(swapClient.GetExchangeName(), exchange.Binance) && o.FeeCurrency == `BNB` && swapSymbol.String() != `BNBUSDT` {
						bnbEqualFee := o.Filled.Mul(o.AvgPrice).Mul(swapClient.FeeRate().OpenTaker.Mul(decimal.NewFromFloat(0.9))).Neg()
						fee = bnbEqualFee
					}
					swapOrder.Fee = swapOrder.Fee.Add(fee)
					if swapOrder.Created.IsZero() {
						if !o.Created.IsZero() {
							swapOrder.Created = o.Created
						} else {
							swapOrder.Created = o.Updated
						}
					}
				}
				if swapName == `swap1` {
					addInfo(swap1Order, or, sg.swapS1)
					takerAmount1 = or.Filled
				} else {
					addInfo(swap2Order, or, sg.swapS2)
					takerAmount2 = or.Filled
				}
			}
		}
	}
	// ==================== 阶段4: hedge — 最终收尾和仓位记录 ====================
hedge:
	check := func() (*position.OrderResult, error) {
		// 【情况1: 两边都没成交】本次操作实际上什么都没发生，返回nil
		if swap1Order.Filled.IsZero() && swap2Order.Filled.IsZero() {
			if errMsg != "" {
				return nil, fmt.Errorf("%s", errMsg)
			}
			return nil, nil
		}
		// 【情况2: 有一边成交为0】产生了单边敞口
		// 将非零一边的成交量计入敞口(Exposure)，等待后续自动敞口处理机制来补齐
		if swap1Order.Filled.IsZero() || swap2Order.Filled.IsZero() {
			var errMsg1 string
			if swap1Order.Filled.IsZero() {
				sg.PosList.AddSwapExposure(ctx, swap2Order.Filled, "swap2")
				errMsg1 = fmt.Sprintf("swap1(%s[%s]) 成交数量为0, swap2(%s[%s]) 成交数量为%s", swapC1.GetExchangeName(), sg.swapS1, swapC2.GetExchangeName(), sg.swapS2, swap2Order.Filled.String())
			}
			if swap2Order.Filled.IsZero() {
				sg.PosList.AddSwapExposure(ctx, swap1Order.Filled, "swap1")
				errMsg1 = fmt.Sprintf("swap2(%s[%s]) 成交数量为0, swap1(%s[%s]) 成交数量为%s", swapC2.GetExchangeName(), sg.swapS2, swapC1.GetExchangeName(), sg.swapS1, swap1Order.Filled.String())
			}
			if errMsg != "" {
				return nil, fmt.Errorf("%s", errMsg)
			}
			return nil, fmt.Errorf("%s, 已计入敞口等待自动处理", errMsg1)
		}
		// 【情况3: 正常——两边都有成交】构建makerInfo并调用最终对冲写入仓位记录
		makerInfo := common.NewJsonObject()
		makerInfo["sr_open_init"] = srOpenInit       // 初始价差
		makerInfo["maker_amount1"] = makerAmount1.String() // swap1 maker成交量
		makerInfo["maker_amount2"] = makerAmount2.String() // swap2 maker成交量
		makerInfo["taker_amount1"] = takerAmount1.String() // swap1 taker补齐量
		makerInfo["taker_amount2"] = takerAmount2.String() // swap2 taker补齐量

		switch cfg.SrMode {
		case rate.SrMean:
			makerInfo["sr_mean"] = srWindow
			makerInfo["sr_mean_cancel"] = srWindowCancel
			makerInfo["first_filled"] = firstFilled
			makerInfo["first_sr"] = firstNowSr
			makerInfo["first_sr_mean"] = firstSrWindow
		case rate.SrMedian:
			makerInfo["sr_median"] = srWindow
			makerInfo["sr_median_cancel"] = srWindowCancel
			makerInfo["first_filled"] = firstFilled
			makerInfo["first_sr"] = firstNowSr
			makerInfo["first_sr_median"] = firstSrWindow
		}
		op.SetMakerInfo(makerInfo)

		// 调用 OpenAllLimitHedge 将两边的成交信息写入仓位记录，完成本次开仓操作
		level.Info(logger).Log("message", "ready to hedge open", "swap1_filled", swap1Order.Filled.String(), "swap1_avg_price", swap1Order.AvgPrice.String(), "usdt", swap1Order.Filled.Mul(swap1Order.AvgPrice).String(),
			"swap2_filled", swap2Order.Filled, "swap2_avg_price", swap2Order.AvgPrice)
		return sg.PosList.OpenAllLimitHedge(ctx, swapC1, swapC2, swap1Order, swap2Order, sg.queue.GetSwap1Depth(), sg.queue.GetSwap2Depth(), op)
	}

	return check()
}









func (sg *Strategy) closeSwap1AndSwap2(ctx context.Context, swapC1, swapC2 exchange.SwapClient,
	srClose float64, closeTradeVolume float64, swap1 *exchange.Depth, swap2 *exchange.Depth, side ccexgo.OrderSide) (*position.OrderResult, error) {
	var (
		swap1Order  = &exchange.CfOrder{} // swap1聚合订单
		swap2Order  = &exchange.CfOrder{} // swap2聚合订单
		srCloseInit float64               // 初始平仓价差率

		// 核心状态标志（含义与开仓完全相同，参见 openSwap1AndSwap2 的注释）
		// 已移除 checkExitNowTimer 和 exitNow：直接在 ctx.Done() 时处理退出，避免 select 随机性导致的延迟
		makerHedge         bool // 是否已进入maker对冲模式
		cancelFilled       bool // 是否因SR撤单阈值触发
		takerHedge         bool // 是否需要taker紧急对冲
		finishedWork       bool // 两边是否都已完成
		makerReplaceFailed bool // maker重挂失败需降级taker

		swap1Cancel      bool // 撤单/重挂标志（含义同开仓）
		swap2Cancel      bool
		swap1NeedReplace bool
		swap2NeedReplace bool
		swap1End         bool
		swap2End         bool

		makerAmount1 decimal.Decimal // 成交量统计（含义同开仓）
		makerAmount2 decimal.Decimal
		takerAmount1 decimal.Decimal
		takerAmount2 decimal.Decimal

		logger log.Logger

		srWindow       string          // SR统计窗口相关（含义同开仓）
		srWindowCancel string
		firstFilled    string
		firstNowSr     string
		firstSrWindow  string
		nowSr          decimal.Decimal
		frozenAmount   decimal.Decimal // 【平仓特有】冻结的可平量，失败时需要归还

		timeoutTimer1    *time.Timer
		maxDurationTimer *time.Timer
	)
	cfg := sg.load.Load()
	one := decimal.NewFromInt(1)

	// tick偏移量: CloseShort用OpenTickNum, CloseLong用CloseTickNum
	closeTickNum1 := float64(cfg.MmSwap1OpenTickNum)
	closeTickNum2 := float64(cfg.MmSwap2OpenTickNum)
	if side == ccexgo.OrderSideCloseLong {
		closeTickNum1 = float64(cfg.MmSwap1CloseTickNum)
		closeTickNum2 = float64(cfg.MmSwap2CloseTickNum)
	}
	pricePrecision1 := sg.swapS1.PricePrecision()
	pricePrecision2 := sg.swapS2.PricePrecision()

	exp1 := actsSpot.CalcExponent(sg.swapS1.ContractVal())
	if swapC1.GetExchangeName() == exchange.Okex5 || swapC1.GetExchangeName() == exchange.GateIO {
		exp1 = actsSpot.CalcExponent(decimal.NewFromFloat(sg.swap1ActsSymbol.AmountTickSize * sg.swap1ActsSymbol.ContractValue))
	}
	exp2 := actsSpot.CalcExponent(sg.swapS2.ContractVal())
	if swapC2.GetExchangeName() == exchange.Okex5 || swapC2.GetExchangeName() == exchange.GateIO {
		exp2 = actsSpot.CalcExponent(decimal.NewFromFloat(sg.swap2ActsSymbol.AmountTickSize * sg.swap2ActsSymbol.ContractValue))
	}

	// minOrderAmount1 := sg.swap1ActsSymbol.MinimumOrderAmount
	// minOrderAmount2 := sg.swap2ActsSymbol.MinimumOrderAmount
	// if swapC1.GetExchangeName() == exchange.Okex5 || swapC1.GetExchangeName() == exchange.GateIO {
	// 	minOrderAmount1 = sg.swap1ActsSymbol.MinimumOrderAmount * sg.swap1ActsSymbol.ContractValue
	// }
	// if swapC2.GetExchangeName() == exchange.Okex5 || swapC2.GetExchangeName() == exchange.GateIO {
	// 	minOrderAmount2 = sg.swap2ActsSymbol.MinimumOrderAmount * sg.swap2ActsSymbol.ContractValue
	// }

	if side == ccexgo.OrderSideCloseShort {
		srWindow = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
	} else {
		srWindow = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
	}

	level.Info(sg.logger).Log("message", "closeSwap1AndSwap2", "closeTickNum1", closeTickNum1, "closeTickNum2", closeTickNum2, "minOrderAmount1", sg.minOrderAmount1, "minOrderAmount2", sg.minOrderAmount2)

	// 【双边下平仓限价单】与开仓的 OpenAllLimit 对称
	op, order1, order2, errMsg, code, placeMode := sg.PosList.CloseAllLimit(ctx, swapC1, swapC2, srClose, closeTradeVolume, []float64{closeTickNum1, closeTickNum2}, swap1, swap2, nil, side, &cfg)
	if op != nil {
		logger = log.With(sg.logger, "id", op.GetOpID())
	} else {
		logger = sg.logger
	}

	switch code {
	case 0:
		if placeMode != rate.Swap1TSwap2M {
			swap1Order.IDs = append(swap1Order.IDs, order1.ID.String())
			swap1Order.ID = order1.ID
			swap1Order.Price = order1.Price
			swap1Order.Amount = order1.Amount
			swap1Order.Side = order1.Side
			swap1Order.Created = time.Now()
			if !order1.Created.IsZero() {
				swap1Order.Created = order1.Created
			}
			if !order1.Updated.IsZero() {
				swap1Order.Updated = order1.Updated
			}
			swap1Order.Filled = order1.Filled
			swap1Order.AvgPrice = order1.AvgPrice
			swap1Order.Fee = order1.Fee.Neg()
			swap1Order.FeeCurrency = order1.FeeCurrency
			swap1Order.Status = order1.Status
		} else {
			swap1Order.ID = ccexgo.NewStrID("")
			swap1Order.Amount = order2.Amount
			swap1Order.Side = reverseSide(order2.Side)
		}

		if placeMode != rate.Swap1MSwap2T {
			swap2Order.IDs = append(swap2Order.IDs, order2.ID.String())
			swap2Order.ID = order2.ID
			swap2Order.Price = order2.Price
			swap2Order.Amount = order2.Amount
			swap2Order.Side = order2.Side
			swap2Order.Created = time.Now()
			if !order2.Created.IsZero() {
				swap2Order.Created = order2.Created
			}
			if !order2.Updated.IsZero() {
				swap2Order.Updated = order2.Updated
			}
			swap2Order.Filled = order2.Filled
			swap2Order.AvgPrice = order2.AvgPrice
			swap2Order.Fee = order2.Fee.Neg()
			swap2Order.FeeCurrency = order2.FeeCurrency
			swap2Order.Status = order2.Status
		} else {
			swap2Order.ID = ccexgo.NewStrID("")
			swap2Order.Amount = order1.Amount
			swap2Order.Side = reverseSide(order1.Side)
		}

		var price1, price2 decimal.Decimal
		switch placeMode {
		case rate.Swap1MSwap2T:
			price1 = swap1Order.Price
			if side == ccexgo.OrderSideCloseShort {
				price2 = decimal.NewFromFloat(swap2.Asks[0].Price)
			} else {
				price2 = decimal.NewFromFloat(swap2.Bids[0].Price)
			}
		case rate.Swap1TSwap2M:
			if side == ccexgo.OrderSideCloseShort {
				price1 = decimal.NewFromFloat(swap1.Bids[0].Price)
			} else {
				price1 = decimal.NewFromFloat(swap1.Asks[0].Price)
			}
			price2 = swap2Order.Price
		default:
			price1 = swap1Order.Price
			price2 = swap2Order.Price
		}

		srCloseInit = price2.Div(price1).Sub(one).InexactFloat64()

		if cfg.SrMode != rate.SrNormal {
			if swap1Order.Filled.IsPositive() {
				firstFilled = "swap1"
				firstNowSr = helper.MustString(srCloseInit)
				if side == ccexgo.OrderSideCloseShort {
					firstSrWindow = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
				} else {
					firstSrWindow = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
				}
			} else if swap2Order.Filled.IsPositive() {
				firstFilled = "swap2"
				firstNowSr = helper.MustString(srCloseInit)
				if side == ccexgo.OrderSideCloseShort {
					firstSrWindow = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
				} else {
					firstSrWindow = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
				}
			}
		}

		frozenAmount = swap1Order.Amount

	// ?一个单失败，是不是就认为价差不符合了
	case 1: // swap1失败,循环撤swap2，查成交,发失败告警
		if placeMode == rate.Swap1MSwap2T {
			return nil, fmt.Errorf("%+v", errMsg)
		}

		swap1Order.Amount = order2.Amount
		swap1Order.Side = reverseSide(order2.Side)

		swap2Order.IDs = append(swap2Order.IDs, order2.ID.String())
		swap2Order.ID = order2.ID
		swap2Order.Price = order2.Price
		swap2Order.Amount = order2.Amount
		swap2Order.Side = order2.Side
		swap2Order.Created = time.Now()
		if !order2.Created.IsZero() {
			swap2Order.Created = order2.Created
		}
		if !order2.Updated.IsZero() {
			swap2Order.Updated = order2.Updated
		}
		swap2Order.Filled = order2.Filled
		swap2Order.AvgPrice = order2.AvgPrice
		swap2Order.Fee = order2.Fee.Neg()
		swap2Order.FeeCurrency = order2.FeeCurrency
		swap2Order.Status = order2.Status

		level.Warn(sg.logger).Log("message", "close swap1 limit failed", "swap2_order", fmt.Sprintf("%+v", order2), "errMsg", errMsg)
		// ?调撤单，有问题异常追踪
		if swap2Order.Status < ccexgo.OrderStatusDone {
			o, err := swapC2.CancelOrder(ctx, order2) // 撤单,查单
			if err != nil {
				errType := swapC2.HandError(err)
				sg.PosList.UpdateBannedTime(errType, false)
				sg.PosList.CheckErrShareBan(errType, swapC2.GetExchangeName())
				if errType.Code == exchange.NoSuchOrder {
					var n int
					for err != nil && n < 5 { // 最多循环查单5次，每次休眠60ms，理论会查到
						order2, err = swapC2.FetchOrder(ctx, swap2Order)
						errType = swapC2.HandError(err)
						if err != nil && errType.Code != exchange.NoSuchOrder {
							break
						}
						n += 1
						time.Sleep(60 * time.Millisecond)
					}

				}
				if err != nil {
					// sg.PosList.AddAbnormalOrder(order2.ID.String(), order2.Side, helper.NowMilliSeconds())
					return nil, fmt.Errorf("%+v\nswap2单边挂单成功，撤单查单异常\n%+v\nswap1挂单失败\n%+v", swap2Order, err, errMsg)
					// }
				}
			}
			if o.Filled.IsPositive() { // TODO成交了，自动处理
				order2.Status = o.Status
				order2.Filled = o.Filled
				order2.AvgPrice = o.AvgPrice
				order2.Fee = o.Fee
				order2.Updated = o.Updated

				swap2Order.Status = o.Status
				swap2Order.Filled = o.Filled
				swap2Order.AvgPrice = o.AvgPrice
				swap2Order.Fee = o.Fee.Neg()
				swap2Order.FeeCurrency = o.FeeCurrency
				swap2Order.Updated = o.Updated
				level.Warn(sg.logger).Log("message", "close limit swap2 filled", "order", fmt.Sprintf("%+v", order2))
				// return nil, fmt.Errorf("swap2单边挂单成功，撤单成交\n%+v\nswap1挂单失败\n%+v", order2, errMsg)
			}
		}
		swap1Order.Status = ccexgo.OrderStatusFailed
		takerHedge = true
		goto checkAndHedge

	case 2: // swap2单失败,循环撤swap1，查成交,发失败告警
		if placeMode == rate.Swap1TSwap2M {
			return nil, fmt.Errorf("%+v", errMsg)
		}
		swap2Order.Amount = order1.Amount
		swap2Order.Side = reverseSide(order1.Side)

		swap1Order.IDs = append(swap1Order.IDs, order1.ID.String())
		swap1Order.ID = order1.ID
		swap1Order.Price = order1.Price
		swap1Order.Amount = order1.Amount
		swap1Order.Side = order1.Side
		swap1Order.Created = time.Now()
		if !order1.Created.IsZero() {
			swap1Order.Created = order1.Created
		}
		if !order1.Updated.IsZero() {
			swap1Order.Updated = order1.Updated
		}
		swap1Order.Filled = order1.Filled
		swap1Order.AvgPrice = order1.AvgPrice
		swap1Order.Fee = order1.Fee.Neg()
		swap1Order.FeeCurrency = order1.FeeCurrency
		swap1Order.Status = order1.Status

		level.Warn(sg.logger).Log("message", "close swap2 limit failed", "swap1_order", fmt.Sprintf("%+v", order1), "errMsg", errMsg)

		if swap1Order.Status < ccexgo.OrderStatusDone {
			o, err := swapC1.CancelOrder(ctx, order1) // 撤单,查单
			if err != nil {                           // TODO
				errType := swapC1.HandError(err)
				sg.PosList.UpdateBannedTime(errType, false)
				sg.PosList.CheckErrShareBan(errType, swapC1.GetExchangeName())
				return nil, fmt.Errorf("%+v\nswap1单边挂单成功，撤单查单异常\n%+v\nswap2挂单失败\n%+v", swap1Order, err, errMsg)
			}
			if o.Filled.IsPositive() { // 成交了直接去taker对冲
				order1.Status = o.Status
				order1.Filled = o.Filled
				order1.AvgPrice = o.AvgPrice
				order1.Fee = o.Fee
				order1.Updated = o.Updated

				swap1Order.Status = o.Status
				swap1Order.Filled = o.Filled
				swap1Order.AvgPrice = o.AvgPrice
				swap1Order.Fee = o.Fee.Neg()
				swap1Order.FeeCurrency = o.FeeCurrency
				swap1Order.Updated = o.Updated
				level.Warn(sg.logger).Log("message", "close limit swap1 filled", "order", fmt.Sprintf("%+v", order1))
				// sg.PosList.AddExposure(ctx, order.Filled.Sub(order.Fee).Neg())
				// return nil, fmt.Errorf("swap1单边挂单成功，撤单成交\n%+v\nswap2挂单失败\n%+v", order, errMsg)
			}
		}
		swap2Order.Status = ccexgo.OrderStatusFailed
		takerHedge = true
		goto checkAndHedge

	case -1: // 两个单都失败,返回原因？
		if strings.Contains(errMsg, "最小下单数量") {
			level.Warn(sg.logger).Log("message", "min order amount error", "errMsg", errMsg)
			if strings.Contains(errMsg, "成功") {
				return nil, nil
			}
		}
		return nil, fmt.Errorf("%+v", errMsg)
	}

	timeoutTimer1 = utilCommon.NewTimeOutTimer(ctx, sg.swapS1.String(), 10, fmt.Sprintf("%s MM平仓", op.GetOpID()))
	defer timeoutTimer1.Stop()

	// 20分钟超时保护，防止协程永久卡住
	maxDurationTimer = time.NewTimer(20 * time.Minute)
	defer maxDurationTimer.Stop()

	// 移除 checkExitNowTimer 机制，改为在 ctx.Done() 时直接处理退出逻辑，避免 select 随机选择导致的退出延迟问题
	// 之前的问题：exitNow 标志需要等待 timer 触发才能退出，在高频交易场景可能导致 mmConcurrencyCount 无法递减
	for {
		select {
		case <-maxDurationTimer.C:
			// 超过20分钟，强制终止流程
			level.Warn(logger).Log("message", "close mm strategy timeout 20 minutes, force exit")
			go message.SendP3Important(ctx, message.NewCommonMsgWithAt(fmt.Sprintf("%s MM平仓超时", sg.swapS1), fmt.Sprintf("操作ID: %s, 运行超过20分钟强制退出", op.GetOpID())))
			takerHedge = true
			goto checkAndHedge
		case <-ctx.Done():
			// 收到退出信号，立即执行撤单和清理逻辑（复用原 checkExitNowTimer 的处理逻辑）
			var order1ID, order2ID string
			if order1 != nil {
				order1ID = order1.ID.String()
			}
			if order2 != nil {
				order2ID = order2.ID.String()
			}
			level.Info(logger).Log("message", "close exit now", "order1_id", order1ID, "order2_id", order2ID)
			var wg sync.WaitGroup
			if order1 != nil && order1.Status < ccexgo.OrderStatusDone {
				wg.Go(func() {
					o, err := swapC1.CancelOrder(ctx, order1)
					if err != nil {
						errType := swapC1.HandError(err)
						level.Info(logger).Log("message", "open cancel order failed", "error", errType.String())
						go message.Send(ctx, message.NewOperateFail(fmt.Sprintf("%s swap1部分收尾平仓撤单失败", sg.swapS1), errType.String()))
					} else {
						if !o.Filled.IsZero() {
							if firstFilled == "" && cfg.SrMode != rate.SrNormal {
								firstFilled = "swap1"
								firstNowSr = nowSr.String()
								if side == ccexgo.OrderSideCloseShort {
									firstSrWindow = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
								} else {
									firstSrWindow = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
								}
							}

							newFilled := o.Filled.Sub(order1.Filled)
							if newFilled.IsPositive() {
								newFee := o.Fee.Sub(order1.Fee)
								if swap1Order.Filled.Add(newFilled).IsPositive() {
									swap1Order.AvgPrice = swap1Order.Filled.Mul(swap1Order.AvgPrice).Add(newFilled.Mul(o.AvgPrice)).Div(swap1Order.Filled.Add(newFilled))
								}
								swap1Order.Filled = swap1Order.Filled.Add(newFilled)
								swap1Order.Fee = swap1Order.Fee.Add(newFee.Neg())
								swap1Order.Updated = o.Updated
							}
						}
						if o.Status != ccexgo.OrderStatusUnknown {
							swap1Order.Status = o.Status
						}
						order1.Status = o.Status
						order1.Filled = o.Filled
						order1.Fee = o.Fee
						order1.Updated = o.Updated
					}
				})
			}
			if order2 != nil && order2.Status < ccexgo.OrderStatusDone {
				wg.Go(func() {
					o, err := swapC2.CancelOrder(ctx, order2)
					if err != nil {
						errType := swapC2.HandError(err)
						level.Info(logger).Log("message", "open cancel order failed", "error", errType.String())
						go message.Send(ctx, message.NewOperateFail(fmt.Sprintf("%s swap2部分收尾平仓撤单失败", sg.swapS2), errType.String()))
					} else {
						if !o.Filled.IsZero() {
							if firstFilled == "" && cfg.SrMode != rate.SrNormal {
								firstFilled = "swap2"
								firstNowSr = nowSr.String()
								if side == ccexgo.OrderSideCloseShort {
									firstSrWindow = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
								} else {
									firstSrWindow = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
								}
							}

							newFilled := o.Filled.Sub(order2.Filled)
							if newFilled.IsPositive() {
								newFee := o.Fee.Sub(order2.Fee)
								if swap2Order.Filled.Add(newFilled).IsPositive() {
									swap2Order.AvgPrice = swap2Order.Filled.Mul(swap2Order.AvgPrice).Add(newFilled.Mul(o.AvgPrice)).Div(swap2Order.Filled.Add(newFilled))
								}
								swap2Order.Filled = swap2Order.Filled.Add(newFilled)
								swap2Order.Fee = swap2Order.Fee.Add(newFee.Neg())
								swap2Order.Updated = o.Updated
							}
						}
						if o.Status != ccexgo.OrderStatusUnknown {
							swap2Order.Status = o.Status
						}
						order2.Status = o.Status
						order2.Filled = o.Filled
						order2.Fee = o.Fee
						order2.Updated = o.Updated
					}
				})
			}
			wg.Wait()
			takerHedge = true
			goto checkAndHedge
		case <-sg.bookTickerEvent.Done():
			sg.bookTickerEvent.Unset()
			swap1BookTicker := swapC1.BookTicker()
			swap2BookTicker := swapC2.BookTicker()
			if swap1BookTicker == nil || swap2BookTicker == nil {
				continue
			}

			// 先更新下订单信息
			if order1 != nil && order1.Status < ccexgo.OrderStatusDone {
				o, err := swapC1.GetWsOrder(order1.ID.String())
				if err == nil && (order1.Filled.LessThan(o.Filled) || order1.Updated.Before(o.Updated) || order1.Status < o.Status) {
					newFilled := o.Filled.Sub(order1.Filled)
					if newFilled.IsPositive() {
						newFee := o.Fee.Sub(order1.Fee)
						if swap1Order.Filled.Add(newFilled).IsPositive() {
							swap1Order.AvgPrice = swap1Order.Filled.Mul(swap1Order.AvgPrice).Add(newFilled.Mul(o.AvgPrice)).Div(swap1Order.Filled.Add(newFilled))
						}
						swap1Order.Filled = swap1Order.Filled.Add(newFilled)
						swap1Order.Fee = swap1Order.Fee.Add(newFee.Neg())
					}
					swap1Order.Updated = o.Updated
					swap1Order.Status = o.Status

					order1.Filled = o.Filled
					order1.Status = o.Status
					order1.Fee = o.Fee
					order1.Updated = o.Updated

					if order1.Status >= ccexgo.OrderStatusDone {
						if swap1Cancel {
							swap1Cancel = false
						}
						if swap1Order.Filled.GreaterThanOrEqual(swap1Order.Amount) {
							swap1Order.Status = ccexgo.OrderStatusDone
							// 如果swap2已经成交，则直接对冲
							if swap2Order.Status == ccexgo.OrderStatusDone || (swap2Order.Status == ccexgo.OrderStatusCancel && swap2End) {
								goto checkAndHedge
							}
						}
					}
				}
			}

			if order2 != nil && order2.Status < ccexgo.OrderStatusDone {
				o, err := swapC2.GetWsOrder(order2.ID.String())
				if err == nil && (order2.Filled.LessThan(o.Filled) || order2.Updated.Before(o.Updated) || order2.Status < o.Status) {
					newFilled := o.Filled.Sub(order2.Filled)
					if newFilled.IsPositive() {
						newFee := o.Fee.Sub(order2.Fee)
						if swap2Order.Filled.Add(newFilled).IsPositive() {
							swap2Order.AvgPrice = swap2Order.Filled.Mul(swap2Order.AvgPrice).Add(newFilled.Mul(o.AvgPrice)).Div(swap2Order.Filled.Add(newFilled))
						}
						swap2Order.Filled = swap2Order.Filled.Add(newFilled)
						swap2Order.Fee = swap2Order.Fee.Add(newFee.Neg())
					}
					swap2Order.Updated = o.Updated
					swap2Order.Status = o.Status

					order2.Filled = o.Filled
					order2.Status = o.Status
					order2.Fee = o.Fee
					order2.Updated = o.Updated

					if order2.Status >= ccexgo.OrderStatusDone {
						if swap2Cancel {
							swap2Cancel = false
						}
						if swap2Order.Filled.GreaterThanOrEqual(swap2Order.Amount) {
							swap2Order.Status = ccexgo.OrderStatusDone
							// 如果swap1已经成交，则直接对冲
							if swap1Order.Status == ccexgo.OrderStatusDone || (swap1Order.Status == ccexgo.OrderStatusCancel && swap1End) {
								goto checkAndHedge
							}
						}
					}
				}
			}

			swap1Bid1Price := decimal.NewFromFloat(swap1BookTicker.Bid1Price)
			swap1Ask1Price := decimal.NewFromFloat(swap1BookTicker.Ask1Price)
			swap2Bid1Price := decimal.NewFromFloat(swap2BookTicker.Bid1Price)
			swap2Ask1Price := decimal.NewFromFloat(swap2BookTicker.Ask1Price)

			// 价差判断
			var (
				takerSr decimal.Decimal
			)

			swap1OpenTick := cfg.MmSwap1OpenTickNum
			swap2OpenTick := cfg.MmSwap2OpenTickNum
			swap1CloseTick := cfg.MmSwap1CloseTickNum
			swap2CloseTick := cfg.MmSwap2CloseTickNum

			if makerHedge && !cancelFilled {
				swap1OpenTick = cfg.MmSwap1OpenMakerHedgeTickNum
				swap2OpenTick = cfg.MmSwap2OpenMakerHedgeTickNum
				swap1CloseTick = cfg.MmSwap1CloseMakerHedgeTickNum
				swap2CloseTick = cfg.MmSwap2CloseMakerHedgeTickNum
			}

			if swap1Order.Filled.GreaterThan(swap2Order.Filled) {
				swap2Price := swap2Bid1Price.Add(decimal.NewFromInt(int64(swap2OpenTick)).Mul(pricePrecision2))
				needAmount := swap1Order.Filled.Sub(swap2Order.Filled)
				swap2PriceTaker := swap2Bid1Price
				if side == ccexgo.OrderSideCloseLong { // sell
					swap2Price = swap2Ask1Price.Sub(decimal.NewFromInt(int64(swap2CloseTick)).Mul(pricePrecision2))
					swap2PriceTaker = swap2Ask1Price
				}
				swap2Avg := swap2Order.Filled.Mul(swap2Order.AvgPrice).Add(swap2Price.Mul(needAmount)).Div(swap1Order.Filled)
				swap2AvgTaker := swap2Order.Filled.Mul(swap2Order.AvgPrice).Add(swap2PriceTaker.Mul(needAmount)).Div(swap1Order.Filled)
				nowSr = swap2Avg.Div(swap1Order.AvgPrice).Sub(one)
				takerSr = swap2AvgTaker.Div(swap1Order.AvgPrice).Sub(one)
			} else if swap1Order.Filled.LessThan(swap2Order.Filled) {
				swap1Price := swap1Ask1Price.Sub(decimal.NewFromInt(int64(swap1OpenTick)).Mul(pricePrecision1))
				needAmount := swap2Order.Filled.Sub(swap1Order.Filled)
				swap1PriceTaker := swap1Ask1Price
				if side == ccexgo.OrderSideCloseLong { // sell
					swap1Price = swap1Bid1Price.Add(decimal.NewFromInt(int64(swap1CloseTick)).Mul(pricePrecision1))
					swap1PriceTaker = swap1Bid1Price
				}
				swap1Avg := swap1Order.Filled.Mul(swap1Order.AvgPrice).Add(swap1Price.Mul(needAmount)).Div(swap2Order.Filled)
				swap1AvgTaker := swap1Order.Filled.Mul(swap1Order.AvgPrice).Add(swap1PriceTaker.Mul(needAmount)).Div(swap2Order.Filled)

				nowSr = swap2Order.AvgPrice.Div(swap1Avg).Sub(one)
				takerSr = swap2Order.AvgPrice.Div(swap1AvgTaker).Sub(one)
			} else {
				var price1, price2 decimal.Decimal
				swap1Price := swap1Ask1Price.Sub(decimal.NewFromInt(int64(swap1OpenTick)).Mul(pricePrecision1))
				swap2Price := swap2Bid1Price.Add(decimal.NewFromInt(int64(swap2OpenTick)).Mul(pricePrecision2))
				if side == ccexgo.OrderSideCloseLong { // sell
					swap1Price = swap1Bid1Price.Add(decimal.NewFromInt(int64(swap1CloseTick)).Mul(pricePrecision1))
					swap2Price = swap2Ask1Price.Sub(decimal.NewFromInt(int64(swap2CloseTick)).Mul(pricePrecision2))
				}
				if order1 != nil {
					price1 = order1.Price
				} else {
					price1 = swap1Price
				}
				if order2 != nil {
					price2 = order2.Price
				} else {
					price2 = swap2Price
				}
				srTm := swap2Bid1Price.Div(price1).Sub(one)
				srMt := price2.Div(swap1Ask1Price).Sub(one)
				takerSr = decimal.Min(srMt, srTm)
				if side == ccexgo.OrderSideCloseLong { // sell
					srTm = swap2Ask1Price.Div(price1).Sub(one)
					srMt = price2.Div(swap1Bid1Price).Sub(one)
					takerSr = decimal.Max(srMt, srTm)
				}
				nowSr = swap2Price.Div(swap1Price).Sub(one)
			}

			if firstFilled == "" && cfg.SrMode != rate.SrNormal {
				if swap1Order.Filled.GreaterThan(swap2Order.Filled) {
					firstFilled = "swap1"
					firstNowSr = nowSr.String()
					if side == ccexgo.OrderSideCloseShort {
						firstSrWindow = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
					} else {
						firstSrWindow = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
					}
				} else if swap1Order.Filled.LessThan(swap2Order.Filled) {
					firstFilled = "swap2"
					firstNowSr = nowSr.String()
					if side == ccexgo.OrderSideCloseShort {
						firstSrWindow = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
					} else {
						firstSrWindow = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
					}
				}
			}

			if side == ccexgo.OrderSideCloseShort { // 平空， buy
				cancelNow := nowSr.LessThan(decimal.NewFromFloat(cfg.Queue.MmCancelOpenThreshold))
				if cfg.SrMode != rate.SrNormal && cfg.EnableStatCancel {
					srString := sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
					if srString != "NAN" {
						sr2, _ := decimal.NewFromString(srString)
						cancelNow = cancelNow || sr2.LessThan(decimal.NewFromFloat(cfg.Queue.OpenStableThreshold))
					}
				}

				if takerSr.LessThan(decimal.NewFromFloat(cfg.MmOpenTakerHedgeThreshold)) {
					takerHedge = true
					level.Info(logger).Log("message", "close nowSr < openTakerHedgeThreshold ", "nowSr", nowSr, "takerSr", takerSr, "swap1Ask1Price", swap1Ask1Price, "swap1Bid1Price", swap1Bid1Price, "swap2Ask1Price", swap2Ask1Price, "swap2Bid1Price", swap2Bid1Price)
				} else if nowSr.LessThan(decimal.NewFromFloat(cfg.MmOpenMakerHedgeThreshold)) && !makerHedge {
					if order1 != nil && order1.Status < ccexgo.OrderStatusDone {
						swap1Cancel = true
					}
					if order2 != nil && order2.Status < ccexgo.OrderStatusDone {
						swap2Cancel = true
					}
					if swap1Order.Filled.GreaterThan(swap2Order.Filled) && !swap2End {
						swap2NeedReplace = true
						swap1End = true
						newSwap2Price := swap2Bid1Price.Add(decimal.NewFromInt(int64(cfg.MmSwap2OpenMakerHedgeTickNum)).Mul(pricePrecision2))
						if order2 != nil && newSwap2Price.Equal(order2.Price) && order2.Status != ccexgo.OrderStatusCancel { // 和当前挂单价一致，则不撤单
							swap2Cancel = false
							swap2NeedReplace = false
						}
						makerHedge = true
					} else if swap1Order.Filled.LessThan(swap2Order.Filled) && !swap1End {
						swap1NeedReplace = true
						swap2End = true
						newSwap1Price := swap1Ask1Price.Sub(decimal.NewFromInt(int64(swap1OpenTick)).Mul(pricePrecision1))
						if order1 != nil && newSwap1Price.Equal(order1.Price) && order1.Status != ccexgo.OrderStatusCancel { // 和当前挂单价一致，则不撤单
							swap1Cancel = false
							swap1NeedReplace = false
						}
						makerHedge = true
					} else {
						if order1 != nil && order2 != nil && order1.Status >= ccexgo.OrderStatusDone && order2.Status >= ccexgo.OrderStatusDone {
							finishedWork = true
						}
					}
					level.Info(logger).Log("message", "close nowSr < openMakerHedgeThreshold ", "nowSr", nowSr, "swap1Ask1Price", swap1Ask1Price, "swap1Bid1Price", swap1Bid1Price, "swap2Ask1Price", swap2Ask1Price, "swap2Bid1Price", swap2Bid1Price,
						"swap1Cancel", swap1Cancel, "swap2Cancel", swap2Cancel, "swap1NeedReplace", swap1NeedReplace, "swap2NeedReplace", swap2NeedReplace, "makerHedge", makerHedge, "swap1End", swap1End, "swap2End", swap2End)
				} else if cancelNow && !makerHedge { // sr撤单
					srWindowCancel = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)

					if order1 != nil && order1.Status < ccexgo.OrderStatusDone {
						swap1Cancel = true
					}
					if order2 != nil && order2.Status < ccexgo.OrderStatusDone {
						swap2Cancel = true
					}
					if swap1Order.Filled.GreaterThan(swap2Order.Filled) && !swap2End {
						swap2NeedReplace = true
						swap1End = true
						newSwap2Price := swap2Bid1Price.Add(decimal.NewFromInt(int64(swap2OpenTick)).Mul(pricePrecision2))
						if order2 != nil && newSwap2Price.Equal(order2.Price) && order2.Status != ccexgo.OrderStatusCancel { // 和当前挂单价一致，则不撤单
							swap2Cancel = false
							swap2NeedReplace = false
						}
						makerHedge = true
						cancelFilled = true
					} else if swap1Order.Filled.LessThan(swap2Order.Filled) && !swap1End {
						swap1NeedReplace = true
						swap2End = true
						newSwap1Price := swap1Ask1Price.Sub(decimal.NewFromInt(int64(swap1OpenTick)).Mul(pricePrecision1))
						if order1 != nil && newSwap1Price.Equal(order1.Price) && order1.Status != ccexgo.OrderStatusCancel { // 和当前挂单价一致，则不撤单
							swap1Cancel = false
							swap1NeedReplace = false
						}
						makerHedge = true
						cancelFilled = true
					} else {
						if order1 != nil && order2 != nil && order1.Status >= ccexgo.OrderStatusDone && order2.Status >= ccexgo.OrderStatusDone {
							finishedWork = true
						}
					}
					level.Info(logger).Log("message", "close nowSr < cancelOpenThreshold ", "nowSr", nowSr, "swap1Ask1Price", swap1Ask1Price, "swap1Bid1Price", swap1Bid1Price, "swap2Ask1Price", swap2Ask1Price, "swap2Bid1Price", swap2Bid1Price,
						"swap1Cancel", swap1Cancel, "swap2Cancel", swap2Cancel, "swap1NeedReplace", swap1NeedReplace, "swap2NeedReplace", swap2NeedReplace, "finishedWork", finishedWork, "swap1End", swap1End, "swap2End", swap2End, "cancelFilled", cancelFilled)
				} else {
					replaceTickNum1 := cfg.MmSwap1OpenReplaceTickNum
					replaceTickNum2 := cfg.MmSwap2OpenReplaceTickNum

					if makerHedge && !cancelFilled {
						replaceTickNum1 = cfg.MmSwap1OpenMakerHedgeReplaceTickNum
						replaceTickNum2 = cfg.MmSwap2OpenMakerHedgeReplaceTickNum
					}

					var priceChange1, priceChange2 decimal.Decimal

					if order1 != nil {
						priceChange1 = swap1Ask1Price.Sub(order1.Price)
						if priceChange1.GreaterThan(decimal.NewFromInt(int64(replaceTickNum1)).Mul(pricePrecision1)) ||
							(!swap1End && order1.Status == ccexgo.OrderStatusCancel) {
							// 如果swap1非主动撤单(一般是被交易所取消)，且未成交，则需要重新下单
							if order1.Status < ccexgo.OrderStatusDone {
								swap1Cancel = true
							}
							if !swap1End && order1.Status != ccexgo.OrderStatusDone {
								swap1NeedReplace = true
							}
						}
					} else {
						if order2.Status == ccexgo.OrderStatusDone {
							swap1NeedReplace = true
						}
					}
					if order2 != nil {
						priceChange2 = order2.Price.Sub(swap2Bid1Price)
						if priceChange2.GreaterThan(decimal.NewFromInt(int64(replaceTickNum2)).Mul(pricePrecision2)) ||
							(!swap2End && order2.Status == ccexgo.OrderStatusCancel) {
							// 如果swap2非主动撤单(一般是被交易所取消)，且未成交，则需要重新下单
							if order2.Status < ccexgo.OrderStatusDone {
								swap2Cancel = true
							}
							if !swap2End && order2.Status != ccexgo.OrderStatusDone {
								swap2NeedReplace = true
							}
						}
					} else {
						if order1.Status == ccexgo.OrderStatusDone {
							swap2NeedReplace = true
						}
					}
					level.Info(logger).Log("message", "close normal check", "nowSr", nowSr, "swap1Ask1Price", swap1Ask1Price, "swap1Bid1Price", swap1Bid1Price, "swap2Ask1Price", swap2Ask1Price, "swap2Bid1Price", swap2Bid1Price,
						"swap1Cancel", swap1Cancel, "swap2Cancel", swap2Cancel, "swap1NeedReplace", swap1NeedReplace, "swap2NeedReplace", swap2NeedReplace, "priceChange1", priceChange1, "priceChange2", priceChange2, "swap1End", swap1End, "swap2End", swap2End,
						"cancelFilled", cancelFilled)
				}

			} else { // 平多, sell
				cancelNow := nowSr.GreaterThan(decimal.NewFromFloat(cfg.Queue.MmCancelCloseThreshold))
				if cfg.SrMode != rate.SrNormal && cfg.EnableStatCancel {
					srString := sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
					if srString != "NAN" {
						sr2, _ := decimal.NewFromString(srString)
						cancelNow = cancelNow || sr2.GreaterThan(decimal.NewFromFloat(cfg.Queue.CloseStableThreshold))
					}
				}

				if takerSr.GreaterThan(decimal.NewFromFloat(cfg.MmCloseTakerHedgeThreshold)) {
					takerHedge = true
					level.Info(logger).Log("message", "close nowSr > closeTakerHedgeThreshold ", "nowSr", nowSr, "takerSr", takerSr, "swap1Ask1Price", swap1Ask1Price, "swap1Bid1Price", swap1Bid1Price, "swap2Ask1Price", swap2Ask1Price, "swap2Bid1Price", swap2Bid1Price)
				} else if nowSr.GreaterThan(decimal.NewFromFloat(cfg.MmCloseMakerHedgeThreshold)) && !makerHedge {
					if order1 != nil && order1.Status < ccexgo.OrderStatusDone {
						swap1Cancel = true
					}
					if order2 != nil && order2.Status < ccexgo.OrderStatusDone {
						swap2Cancel = true
					}
					if swap1Order.Filled.GreaterThan(swap2Order.Filled) && !swap1End {
						swap2NeedReplace = true
						swap1End = true
						newSwap2Price := swap2Ask1Price.Sub(decimal.NewFromInt(int64(cfg.MmSwap2CloseMakerHedgeTickNum)).Mul(pricePrecision2))
						if order2 != nil && newSwap2Price.Equal(order2.Price) && order2.Status != ccexgo.OrderStatusCancel { // 和当前挂单价一致，则不撤单
							swap2Cancel = false
							swap2NeedReplace = false
						}
						makerHedge = true
					} else if swap1Order.Filled.LessThan(swap2Order.Filled) && !swap2End {
						swap1NeedReplace = true
						swap2End = true
						newSwap1Price := swap1Bid1Price.Add(decimal.NewFromInt(int64(cfg.MmSwap1CloseMakerHedgeTickNum)).Mul(pricePrecision1))
						if order1 != nil && newSwap1Price.Equal(order1.Price) && order1.Status != ccexgo.OrderStatusCancel { // 和当前挂单价一致，则不撤单
							swap1Cancel = false
							swap1NeedReplace = false
						}
						makerHedge = true
					} else {
						if order1 != nil && order2 != nil && order1.Status >= ccexgo.OrderStatusDone && order2.Status >= ccexgo.OrderStatusDone {
							finishedWork = true
						}
					}
					level.Info(logger).Log("message", "close nowSr > closeMakerHedgeThreshold ", "nowSr", nowSr, "swap1Ask1Price", swap1Ask1Price, "swap1Bid1Price", swap1Bid1Price, "swap2Ask1Price", swap2Ask1Price, "swap2Bid1Price", swap2Bid1Price,
						"swap1Cancel", swap1Cancel, "swap2Cancel", swap2Cancel, "swap1NeedReplace", swap1NeedReplace, "swap2NeedReplace", swap2NeedReplace, "makerHedge", makerHedge, "swap1End", swap1End, "swap2End", swap2End)
				} else if cancelNow && !makerHedge {
					srWindowCancel = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)

					if order1 != nil && order1.Status < ccexgo.OrderStatusDone {
						swap1Cancel = true
					}
					if order2 != nil && order2.Status < ccexgo.OrderStatusDone {
						swap2Cancel = true
					}
					if swap1Order.Filled.GreaterThan(swap2Order.Filled) && !swap2End {
						swap2NeedReplace = true
						swap1End = true
						newSwap2Price := swap2Ask1Price.Sub(decimal.NewFromInt(int64(swap2CloseTick)).Mul(pricePrecision2))
						if order2 != nil && newSwap2Price.Equal(order2.Price) && order2.Status != ccexgo.OrderStatusCancel { // 和当前挂单价一致，则不撤单
							swap2Cancel = false
							swap2NeedReplace = false
						}
						makerHedge = true
						cancelFilled = true
					} else if swap1Order.Filled.LessThan(swap2Order.Filled) && !swap1End {
						swap1NeedReplace = true
						swap2End = true
						newSwap1Price := swap1Bid1Price.Add(decimal.NewFromInt(int64(swap1CloseTick)).Mul(pricePrecision1))
						if order1 != nil && newSwap1Price.Equal(order1.Price) && order1.Status != ccexgo.OrderStatusCancel { // 和当前挂单价一致，则不撤单
							swap1Cancel = false
							swap1NeedReplace = false
						}
						makerHedge = true
						cancelFilled = true
					} else {
						if order1 != nil && order2 != nil && order1.Status >= ccexgo.OrderStatusDone && order2.Status >= ccexgo.OrderStatusDone {
							finishedWork = true
						}
					}
					level.Info(logger).Log("message", "close nowSr > cancelCloseThreshold ", "nowSr", nowSr, "swap1Ask1Price", swap1Ask1Price, "swap1Bid1Price", swap1Bid1Price, "swap2Ask1Price", swap2Ask1Price, "swap2Bid1Price", swap2Bid1Price,
						"swap1Cancel", swap1Cancel, "swap2Cancel", swap2Cancel, "swap1NeedReplace", swap1NeedReplace, "swap2NeedReplace", swap2NeedReplace, "finishedWork", finishedWork, "swap1End", swap1End, "swap2End", swap2End, "cancelFilled", cancelFilled)
				} else {
					replaceTickNum1 := cfg.MmSwap1CloseReplaceTickNum
					replaceTickNum2 := cfg.MmSwap2CloseReplaceTickNum

					if makerHedge && !cancelFilled {
						replaceTickNum1 = cfg.MmSwap1CloseMakerHedgeReplaceTickNum
						replaceTickNum2 = cfg.MmSwap2CloseMakerHedgeReplaceTickNum
					}

					var priceChange1, priceChange2 decimal.Decimal

					if order1 != nil {
						priceChange1 = order1.Price.Sub(swap1Bid1Price)
						if priceChange1.GreaterThan(decimal.NewFromInt(int64(replaceTickNum1)).Mul(pricePrecision1)) ||
							(!swap1End && order1.Status == ccexgo.OrderStatusCancel) {
							// 如果swap1非主动撤单(一般是被交易所取消)，且未成交，则需要重新下单
							if order1.Status < ccexgo.OrderStatusDone {
								swap1Cancel = true
							}
							if !swap1End && order1.Status != ccexgo.OrderStatusDone {
								swap1NeedReplace = true
							}
						}
					} else {
						if order2.Status == ccexgo.OrderStatusDone {
							swap1NeedReplace = true
						}
					}
					if order2 != nil {
						priceChange2 = swap2Ask1Price.Sub(order2.Price)
						if priceChange2.GreaterThan(decimal.NewFromInt(int64(replaceTickNum2)).Mul(pricePrecision2)) ||
							(!swap2End && order2.Status == ccexgo.OrderStatusCancel) {
							// 如果swap2非主动撤单(一般是被交易所取消)，且未成交，则需要重新下单
							if order2.Status < ccexgo.OrderStatusDone {
								swap2Cancel = true
							}
							if !swap2End && order2.Status != ccexgo.OrderStatusDone {
								swap2NeedReplace = true
							}
						}
					} else {
						if order1.Status == ccexgo.OrderStatusDone {
							swap2NeedReplace = true
						}
					}
					level.Info(logger).Log("message", "close normal check", "nowSr", nowSr, "swap1Ask1Price", swap1Ask1Price, "swap1Bid1Price", swap1Bid1Price, "swap2Ask1Price", swap2Ask1Price, "swap2Bid1Price", swap2Bid1Price,
						"swap1Cancel", swap1Cancel, "swap2Cancel", swap2Cancel, "swap1NeedReplace", swap1NeedReplace, "swap2NeedReplace", swap2NeedReplace, "priceChange1", priceChange1, "priceChange2", priceChange2, "swap1End", swap1End, "swap2End", swap2End,
						"cancelFilled", cancelFilled)
				}
			}

			if takerHedge || finishedWork { // 如果是taker hedge，则需要判断是否需要撤单
				goto checkAndHedge
			}

			var wg sync.WaitGroup
			if swap1Cancel && order1 != nil {
				wg.Go(func() {
					o, err := swapC1.CancelOrder(ctx, order1) // 撤单,查单
					if err != nil {
						errType := swapC1.HandError(err)
						level.Warn(logger).Log("message", "close cancel order1 failed", "order", fmt.Sprintf("%+v", order1), "error", errType.String())
						go message.Send(ctx, message.NewOperateFail(fmt.Sprintf("%s swap1部分中途平仓撤单失败", sg.swapS1), errType.String()))
					} else {
						if !o.Filled.IsZero() {
							if firstFilled == "" && cfg.SrMode != rate.SrNormal {
								firstFilled = "swap1"
								firstNowSr = nowSr.String()
								if side == ccexgo.OrderSideCloseShort {
									firstSrWindow = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
								} else {
									firstSrWindow = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
								}
							}

							newFilled := o.Filled.Sub(order1.Filled)
							if newFilled.IsPositive() {
								newFee := o.Fee.Sub(order1.Fee)
								if swap1Order.Filled.Add(newFilled).IsPositive() {
									swap1Order.AvgPrice = swap1Order.Filled.Mul(swap1Order.AvgPrice).Add(newFilled.Mul(o.AvgPrice)).Div(swap1Order.Filled.Add(newFilled))
								}
								swap1Order.Filled = swap1Order.Filled.Add(newFilled)
								swap1Order.Fee = swap1Order.Fee.Add(newFee.Neg()) //撤单主动查出来的是总手续费，swap取反保持统一
								swap1Order.Updated = o.Updated
								if o.FeeCurrency != `` && swap1Order.FeeCurrency == `` {
									swap1Order.FeeCurrency = o.FeeCurrency
								}
							}
						}
						if swap1Order.Filled.GreaterThanOrEqual(swap1Order.Amount) {
							swap1Order.Status = ccexgo.OrderStatusDone
						}
						order1.Status = o.Status
						order1.Filled = o.Filled
						order1.Fee = o.Fee
						order1.Updated = o.Updated
						swap1Cancel = false
					}
				})
			} else if swap1Cancel {
				swap1Cancel = false
			}
			if swap2Cancel && order2 != nil {
				wg.Go(func() {
					o, err := swapC2.CancelOrder(ctx, order2) // 撤单,查单
					if err != nil {
						errType := swapC2.HandError(err)
						level.Warn(logger).Log("message", "close cancel order2 failed", "order", fmt.Sprintf("%+v", order2), "error", errType.String())
						go message.Send(ctx, message.NewOperateFail(fmt.Sprintf("%s swap2部分中途平仓撤单失败", sg.swapS2), errType.String()))
					} else {
						if !o.Filled.IsZero() {
							if firstFilled == "" && cfg.SrMode != rate.SrNormal {
								firstFilled = "swap2"
								firstNowSr = nowSr.String()
								if side == ccexgo.OrderSideCloseShort {
									firstSrWindow = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
								} else {
									firstSrWindow = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
								}
							}

							newFilled := o.Filled.Sub(order2.Filled)
							if newFilled.IsPositive() {
								newFee := o.Fee.Sub(order2.Fee)
								if swap2Order.Filled.Add(newFilled).IsPositive() {
									swap2Order.AvgPrice = swap2Order.Filled.Mul(swap2Order.AvgPrice).Add(newFilled.Mul(o.AvgPrice)).Div(swap2Order.Filled.Add(newFilled))
								}
								swap2Order.Filled = swap2Order.Filled.Add(newFilled)
								swap2Order.Fee = swap2Order.Fee.Add(newFee.Neg())
								swap2Order.Updated = o.Updated
								if o.FeeCurrency != `` && swap2Order.FeeCurrency == `` {
									swap2Order.FeeCurrency = o.FeeCurrency
								}
							}
						}
						if swap2Order.Filled.GreaterThanOrEqual(swap2Order.Amount) {
							swap2Order.Status = ccexgo.OrderStatusDone
						}
						order2.Status = o.Status
						order2.Filled = o.Filled
						order2.Fee = o.Fee
						order2.Updated = o.Updated
						swap2Cancel = false
					}
				})
			} else if swap2Cancel {
				swap2Cancel = false
			}
			wg.Wait()

			// 撤单后这里需要动态更新标记
			// 预先打的标记不一定准确
			if order1 != nil && order2 != nil && makerHedge && !swap1Cancel && !swap2Cancel && order1.Status >= ccexgo.OrderStatusDone && order2.Status >= ccexgo.OrderStatusDone {
				if swap1End && swap2Order.Filled.GreaterThan(swap1Order.Filled) {
					// 如果swap1标记结束，但是swap2的成交数量大于swap1, 则改标记swap2结束，swap1需要重挂单
					swap2End = true
					swap2NeedReplace = false
					swap1End = false
					swap1NeedReplace = true
					level.Warn(logger).Log("message", "reverse swap1 and swap2 flag", "reason", "swap2 filled GreaterThan swap1")
				} else if swap2End && swap1Order.Filled.GreaterThan(swap2Order.Filled) {
					// 如果swap2标记结束，但是swap1的成交数量大于swap2, 则改标记swap1结束，swap2需要重挂单
					swap1End = true
					swap1NeedReplace = false
					swap2End = false
					swap2NeedReplace = true
					level.Warn(logger).Log("message", "reverse swap1 and swap2 flag", "reason", "swap1 filled GreaterThan swap2")
				}
			}

			if (swap1Order.Filled.GreaterThanOrEqual(swap1Order.Amount) && !swap2Cancel && swap2Order.Status == ccexgo.OrderStatusDone) ||
				(swap2Order.Filled.GreaterThanOrEqual(swap2Order.Amount) && !swap1Cancel && swap1Order.Status == ccexgo.OrderStatusDone) {
				goto checkAndHedge
			}

			// 都不需要撤单, 并且至少有一边不需要重挂单, 并且两边成交数量一致，当前挂单都是终结态，则直接去写记录
			if !swap1Cancel && !swap2Cancel && (!swap1NeedReplace || !swap2NeedReplace) && order1 != nil && order2 != nil {
				if order1.Status >= ccexgo.OrderStatusDone && order2.Status >= ccexgo.OrderStatusDone {
					diff := swap1Order.Filled.Sub(swap2Order.Filled).Abs()
					maxMinAmount := math.Max(sg.minOrderAmount1, sg.minOrderAmount2)
					if diff.LessThan(decimal.NewFromFloat(maxMinAmount)) {
						goto checkAndHedge
					}
				}
			}

			if (order1 == nil || order1.Status >= ccexgo.OrderStatusDone) && swap1NeedReplace {
				closeAmount := swap1Order.Amount.Sub(swap1Order.Filled).InexactFloat64()
				tickNum := cfg.MmSwap1OpenTickNum
				if makerHedge {
					closeAmount = swap2Order.Filled.Sub(swap1Order.Filled).InexactFloat64()
					if !cancelFilled {
						tickNum = cfg.MmSwap1OpenMakerHedgeTickNum
					}
				}
				if side == ccexgo.OrderSideCloseLong {
					tickNum = cfg.MmSwap1CloseTickNum
					if makerHedge && !cancelFilled {
						tickNum = cfg.MmSwap1CloseMakerHedgeTickNum
					}
				}
				if closeAmount < sg.minOrderAmount1 {
					swap1Order.Status = ccexgo.OrderStatusDone
					swap1NeedReplace = false
					// 如果swap2不需要重挂单, 当前swap1已经不满足重挂精度, 则直接去taker
					if !swap2Cancel && !swap2NeedReplace {
						makerReplaceFailed = true
						goto checkAndHedge
					}
					goto replace2
				}
				_, o, errMsg1, err := sg.PosList.CloseSwapMaker(ctx, swapC1, srClose, closeAmount, float64(tickNum), sg.swap1OrderBook, op, side, 0, false, false)
				if err != nil {
					errType := swapC1.HandError(err)
					level.Warn(logger).Log("message", "replace close swap1 failed", "closeAmount", closeAmount, "tickNum", tickNum, "errMsg1", errMsg1)
					go message.Send(ctx, message.NewOperateFail(fmt.Sprintf("%s swap1部分平仓重挂失败", sg.swapS1), errMsg1))
					if errType.Code != exchange.NewOrderRejected {
						makerReplaceFailed = true
						errMsg = errMsg1
						goto checkAndHedge
					} else {
						goto replace2
					}
				}
				order1 = o
				swap1NeedReplace = false
				swap1Order.ID = order1.ID
				swap1Order.IDs = append(swap1Order.IDs, order1.ID.String())
				if swap1Order.Created.IsZero() {
					swap1Order.Created = order1.Created
					if strings.Contains(swapC1.GetExchangeName(), exchange.Binance) && swap1Order.Created.IsZero() {
						swap1Order.Created = time.Now()
					}
				}
				// 考虑下单即成交的情况
				if !o.Filled.IsZero() {
					if firstFilled == "" && cfg.SrMode != rate.SrNormal {
						firstFilled = "swap1"
						firstNowSr = nowSr.String()
						if side == ccexgo.OrderSideCloseShort {
							firstSrWindow = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
						} else {
							firstSrWindow = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
						}
					}

					newFilled := o.Filled
					newFee := o.Fee
					if swap1Order.Filled.Add(newFilled).IsPositive() {
						swap1Order.AvgPrice = swap1Order.Filled.Mul(swap1Order.AvgPrice).Add(newFilled.Mul(o.AvgPrice)).Div(swap1Order.Filled.Add(newFilled))
					}
					swap1Order.Filled = swap1Order.Filled.Add(newFilled)
					swap1Order.Fee = swap1Order.Fee.Add(newFee.Neg())

					swap1Order.Status = o.Status
					swap1Order.Updated = o.Updated
					if o.FeeCurrency != `` && swap1Order.FeeCurrency == `` {
						swap1Order.FeeCurrency = o.FeeCurrency
					}
				}
			}

		replace2:
			if (order2 == nil || order2.Status >= ccexgo.OrderStatusDone) && swap2NeedReplace {
				closeAmount := swap2Order.Amount.Sub(swap2Order.Filled).InexactFloat64()
				tickNum := cfg.MmSwap2OpenTickNum
				if makerHedge {
					closeAmount = swap1Order.Filled.Sub(swap2Order.Filled).InexactFloat64()
					if !cancelFilled {
						tickNum = cfg.MmSwap2OpenMakerHedgeTickNum
					}
				}
				if side == ccexgo.OrderSideCloseLong {
					tickNum = cfg.MmSwap2CloseTickNum
					if makerHedge && !cancelFilled {
						tickNum = cfg.MmSwap2CloseMakerHedgeTickNum
					}
				}
				if closeAmount < sg.minOrderAmount2 {
					swap2Order.Status = ccexgo.OrderStatusDone
					swap2NeedReplace = false
					// 如果swap1不需要重挂单, 当前swap2已经不满足重挂精度, 则直接去taker
					if !swap1Cancel && !swap1NeedReplace {
						makerReplaceFailed = true
						goto checkAndHedge
					}
					continue
				}
				_, o, errMsg1, err := sg.PosList.CloseSwapMaker(ctx, swapC2, srClose, closeAmount, float64(tickNum), sg.swap2OrderBook, op, reverseSide(side), 0, false, false)
				if err != nil {
					errType := swapC2.HandError(err)
					level.Warn(logger).Log("message", "replace close swap2 failed", "closeAmount", closeAmount, "tickNum", tickNum, "errMsg1", errMsg1)
					go message.Send(ctx, message.NewOperateFail(fmt.Sprintf("%s swap2部分平仓重挂失败", sg.swapS2), errMsg1))
					if errType.Code != exchange.NewOrderRejected {
						makerReplaceFailed = true
						errMsg = errMsg1
						goto checkAndHedge
					} else {
						continue
					}
				}
				order2 = o
				swap2NeedReplace = false
				swap2Order.ID = order2.ID
				swap2Order.IDs = append(swap2Order.IDs, order2.ID.String())
				if swap2Order.Created.IsZero() {
					swap2Order.Created = order2.Created
					if strings.Contains(swapC2.GetExchangeName(), exchange.Binance) && swap2Order.Created.IsZero() {
						swap2Order.Created = time.Now()
					}
				}
				// 考虑下单即成交的情况
				if !o.Filled.IsZero() {
					if firstFilled == "" && cfg.SrMode != rate.SrNormal {
						firstFilled = "swap2"
						firstNowSr = nowSr.String()
						if side == ccexgo.OrderSideCloseShort {
							firstSrWindow = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
						} else {
							firstSrWindow = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
						}
					}

					newFilled := o.Filled
					newFee := o.Fee
					if swap2Order.Filled.Add(newFilled).IsPositive() {
						swap2Order.AvgPrice = swap2Order.Filled.Mul(swap2Order.AvgPrice).Add(newFilled.Mul(o.AvgPrice)).Div(swap2Order.Filled.Add(newFilled))
					}
					swap2Order.Filled = swap2Order.Filled.Add(newFilled)
					swap2Order.Fee = swap2Order.Fee.Add(newFee.Neg())

					swap2Order.Status = o.Status
					swap2Order.Updated = o.Updated

					if o.FeeCurrency != `` && swap2Order.FeeCurrency == `` {
						swap2Order.FeeCurrency = o.FeeCurrency
					}
				}
			}

		case o := <-swapC1.MakerOrder(swap1Order.ID.String()): // 判断是否完全成交或取消
			if o == nil { // channel超时关闭了
				continue
			}
			if o.ID != order1.ID {
				level.Warn(logger).Log("message", "skip swap1 unmatched order", "want", order1.ID.String(), "got", o.ID.String(), "amount", o.Amount, "fee", o.Fee, "filled", o.Filled)
			}
			level.Info(logger).Log("message", "get swap1 ws order notify", "id", o.ID.String(), "amount", o.Amount, "filled", o.Filled, "fee", o.Fee, "avg_price", o.AvgPrice, "status", o.Status)
			if order1.ID.String() == o.ID.String() && (order1.Filled.LessThan(o.Filled) || order1.Updated.Before(o.Updated) || order1.Status < o.Status) {
				newFilled := o.Filled.Sub(order1.Filled)
				if newFilled.IsPositive() {
					newFee := o.Fee
					if swapC1.GetExchangeName() == exchange.Bybit5 {
						newFee = o.Fee.Sub(order1.Fee)
					}
					if swap1Order.Filled.Add(newFilled).IsPositive() {
						swap1Order.AvgPrice = swap1Order.Filled.Mul(swap1Order.AvgPrice).Add(newFilled.Mul(o.AvgPrice)).Div(swap1Order.Filled.Add(newFilled))
					}
					swap1Order.Filled = swap1Order.Filled.Add(newFilled)
					swap1Order.Fee = swap1Order.Fee.Add(newFee.Neg())

					if swapC1.GetExchangeName() == exchange.Bybit5 {
						order1.Fee = o.Fee
					} else {
						order1.Fee = order1.Fee.Add(o.Fee)
					}

					if firstFilled == "" && cfg.SrMode != rate.SrNormal {
						if swap1Order.Filled.GreaterThan(swap2Order.Filled) {
							firstFilled = "swap1"
							firstNowSr = nowSr.String()
							if side == ccexgo.OrderSideCloseShort {
								firstSrWindow = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
							} else {
								firstSrWindow = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
							}
						} else if swap1Order.Filled.LessThan(swap2Order.Filled) {
							firstFilled = "swap2"
							firstNowSr = nowSr.String()
							if side == ccexgo.OrderSideCloseShort {
								firstSrWindow = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
							} else {
								firstSrWindow = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
							}
						}
					}
				}

				swap1Order.Status = o.Status
				swap1Order.Updated = o.Updated
				if swap1Order.Created.IsZero() {
					swap1Order.Created = o.Created
				}

				order1.Status = o.Status
				order1.Filled = o.Filled
				order1.Updated = o.Updated
			}

			if o.FeeCurrency != `` {
				swap1Order.FeeCurrency = o.FeeCurrency
			}

			if order1.Status >= ccexgo.OrderStatusDone {
				if swap1Cancel {
					swap1Cancel = false
				}
			}

			if swap1Order.Status != ccexgo.OrderStatusDone {
				continue
			}
			if swap2Order.Status == ccexgo.OrderStatusDone || (swap2Order.Status == ccexgo.OrderStatusCancel && swap2End) {
				goto checkAndHedge
			}
		case o := <-swapC2.MakerOrder(swap2Order.ID.String()):
			if o == nil { // channel超时关闭了
				continue
			}
			if o.ID != order2.ID {
				level.Warn(logger).Log("message", "skip swap unmatched order", "want", order2.ID.String(), "got", o.ID.String(), "amount", o.Amount, "fee", o.Fee, "filled", o.Filled)
			}
			level.Info(logger).Log("message", "get swap2 ws order notify", "id", o.ID.String(), "amount", o.Amount, "filled", o.Filled, "fee", o.Fee, "avg_price", o.AvgPrice, "status", o.Status)
			if order2.ID.String() == o.ID.String() && (order2.Filled.LessThan(o.Filled) || order2.Updated.Before(o.Updated) || order2.Status < o.Status) {
				newFilled := o.Filled.Sub(order2.Filled)
				if newFilled.IsPositive() {
					newFee := o.Fee
					if swapC2.GetExchangeName() == exchange.Bybit5 {
						newFee = o.Fee.Sub(order2.Fee)
					}
					if swap2Order.Filled.Add(newFilled).IsPositive() {
						swap2Order.AvgPrice = swap2Order.Filled.Mul(swap2Order.AvgPrice).Add(newFilled.Mul(o.AvgPrice)).Div(swap2Order.Filled.Add(newFilled))
					}
					swap2Order.Filled = swap2Order.Filled.Add(newFilled)
					swap2Order.Fee = swap2Order.Fee.Add(newFee.Neg())

					if swapC2.GetExchangeName() == exchange.Bybit5 {
						order2.Fee = o.Fee
					} else {
						order2.Fee = order2.Fee.Add(o.Fee)
					}

					if firstFilled == "" && cfg.SrMode != rate.SrNormal {
						if swap1Order.Filled.GreaterThan(swap2Order.Filled) {
							firstFilled = "swap1"
							firstNowSr = nowSr.String()
							if side == ccexgo.OrderSideCloseShort {
								firstSrWindow = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
							} else {
								firstSrWindow = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
							}
						} else if swap1Order.Filled.LessThan(swap2Order.Filled) {
							firstFilled = "swap2"
							firstNowSr = nowSr.String()
							if side == ccexgo.OrderSideCloseShort {
								firstSrWindow = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
							} else {
								firstSrWindow = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
							}
						}
					}
				}

				swap2Order.Status = o.Status
				swap2Order.Updated = o.Updated
				if swap2Order.Created.IsZero() {
					swap2Order.Created = o.Created
				}

				order2.Status = o.Status
				order2.Filled = o.Filled
				order2.Updated = o.Updated
			}

			if o.FeeCurrency != `` {
				swap2Order.FeeCurrency = o.FeeCurrency
			}

			if order2.Status >= ccexgo.OrderStatusDone {
				if swap2Cancel {
					swap2Cancel = false
				}
			}

			if swap2Order.Status != ccexgo.OrderStatusDone {
				continue
			}

			if swap1Order.Status == ccexgo.OrderStatusDone || (swap1Order.Status == ccexgo.OrderStatusCancel && swap1End) {
				goto checkAndHedge
			}

		}
	}
checkAndHedge:
	makerAmount1 = swap1Order.Filled
	makerAmount2 = swap2Order.Filled
	if takerHedge || finishedWork || makerReplaceFailed {
		if order1 != nil && order1.Status < ccexgo.OrderStatusDone {
			level.Info(logger).Log("message", "close taker hedge cancel order", "order", order1.ID, "finished_work", finishedWork, "maker_replace_failed", makerReplaceFailed)
			o, err := swapC1.CancelOrder(ctx, order1) // 撤单,查单
			if err != nil {
				errType := swapC1.HandError(err)
				level.Warn(logger).Log("message", "close taker hedge cancel order failed", "order", fmt.Sprintf("%+v", order1), "error", errType.String())
				go message.Send(ctx, message.NewOperateFail(fmt.Sprintf("%s swap1部分平仓taker撤单失败", sg.swapS1), errType.String()))
			} else {
				if !o.Filled.IsZero() {
					newFilled := o.Filled.Sub(order1.Filled)
					if newFilled.IsPositive() {
						newFee := o.Fee.Sub(order1.Fee)
						if swap1Order.Filled.Add(newFilled).IsPositive() {
							swap1Order.AvgPrice = swap1Order.Filled.Mul(swap1Order.AvgPrice).Add(newFilled.Mul(o.AvgPrice)).Div(swap1Order.Filled.Add(newFilled))
						}
						swap1Order.Filled = swap1Order.Filled.Add(newFilled)
						swap1Order.Fee = swap1Order.Fee.Add(newFee.Neg())
						swap1Order.Updated = o.Updated
						if o.FeeCurrency != `` && swap1Order.FeeCurrency == `` {
							swap1Order.FeeCurrency = o.FeeCurrency
						}
					}
				}
				swap1Order.Status = ccexgo.OrderStatusCancel
			}
		}
		makerAmount1 = swap1Order.Filled
		if order2 != nil && order2.Status < ccexgo.OrderStatusDone {
			level.Info(logger).Log("message", "close taker hedge cancel order2", "order", order2.ID, "finished_work", finishedWork)
			o, err := swapC2.CancelOrder(ctx, order2) // 撤单,查单
			if err != nil {
				errType := swapC2.HandError(err)
				level.Warn(logger).Log("message", "close taker hedge cancel order2 failed", "order", fmt.Sprintf("%+v", order2), "error", errType.String())
				go message.Send(ctx, message.NewOperateFail(fmt.Sprintf("%s swap2部分平仓taker撤单失败", sg.swapS2), errType.String()))
			} else {
				if !o.Filled.IsZero() {
					newFilled := o.Filled.Sub(order2.Filled)
					if newFilled.IsPositive() {
						newFee := o.Fee.Sub(order2.Fee)
						if swap2Order.Filled.Add(newFilled).IsPositive() {
							swap2Order.AvgPrice = swap2Order.Filled.Mul(swap2Order.AvgPrice).Add(newFilled.Mul(o.AvgPrice)).Div(swap2Order.Filled.Add(newFilled))
						}
						swap2Order.Filled = swap2Order.Filled.Add(newFilled)
						swap2Order.Fee = swap2Order.Fee.Add(newFee.Neg())
						swap2Order.Updated = o.Updated
						if o.FeeCurrency != `` && swap2Order.FeeCurrency == `` {
							swap2Order.FeeCurrency = o.FeeCurrency
						}
					}
				}
				swap2Order.Status = ccexgo.OrderStatusCancel
			}
		}
		makerAmount2 = swap2Order.Filled

		// 看哪边成交多，少的一边用taker对冲对齐
		needTakerHedgeAmount := swap1Order.Filled.Sub(swap2Order.Filled)
		var (
			swapClient     exchange.SwapClient
			placeSide      ccexgo.OrderSide
			minOrderAmount float64
			swapName       string
			swapSymbol     ccexgo.SwapSymbol
		)
		if needTakerHedgeAmount.IsPositive() {
			swapClient = swapC2
			placeSide = reverseSide(side)
			needTakerHedgeAmount = needTakerHedgeAmount.Round(int32(exp2))
			minOrderAmount = sg.minOrderAmount2
			swapName = `swap2`
			swapSymbol = sg.swapS2
		} else if needTakerHedgeAmount.IsNegative() {
			swapClient = swapC1
			placeSide = side
			needTakerHedgeAmount = needTakerHedgeAmount.Abs().Round(int32(exp1))
			minOrderAmount = sg.minOrderAmount1
			swapName = `swap1`
			swapSymbol = sg.swapS1
		} else {
			goto hedge
		}

		if needTakerHedgeAmount.LessThan(decimal.NewFromFloat(minOrderAmount)) {
			goto hedge
		}

		or, err := swapClient.MarketOrder(ctx, placeSide, needTakerHedgeAmount, decimal.Zero, nil)
		if err != nil {
			level.Warn(logger).Log("message", "close taker hedge market order failed", "error", err.Error())
			errType := swapClient.HandError(err)
			sg.PosList.UpdateBannedTime(errType, false)
			sg.PosList.CheckErrShareBan(errType, swapClient.GetExchangeName())
			msg := message.NewOperateFail(fmt.Sprintf("%s %s(%s)部分平仓taker对冲失败", swapSymbol, swapName, swapClient.GetExchangeName()), swapClient.HandError(err).String())
			if errType.Code != exchange.OtherReason {
				go message.Send(ctx, msg)
			} else {
				go message.SendP3Important(ctx, msg)
			}
		} else {
			if !or.Filled.IsZero() {
				addInfo := func(swapOrder *exchange.CfOrder, o *exchange.CfOrder, swapSymbol ccexgo.SwapSymbol) {
					swapOrder.AvgPrice = swapOrder.Filled.Mul(swapOrder.AvgPrice).Add(o.Filled.Mul(o.AvgPrice)).Div(swapOrder.Filled.Add(o.Filled))
					swapOrder.Filled = swapOrder.Filled.Add(o.Filled)
					swapOrder.Updated = o.Updated
					swapOrder.Status = ccexgo.OrderStatusCancel
					swapOrder.IDs = append(swapOrder.IDs, o.ID.String())
					if o.Filled.Equal(swapOrder.Amount) || swapOrder.ID == nil {
						swapOrder.ID = o.ID
					}
					if swapOrder.FeeCurrency == "" {
						swapOrder.FeeCurrency = o.FeeCurrency
					} else {
						if swapOrder.FeeCurrency != o.FeeCurrency && o.FeeCurrency != `` {
							swapOrder.FeeCurrency += `_` + o.FeeCurrency
						}
					}
					// 币安taker可能是bnb抵扣费, 要计算出对应的usdt费
					fee := o.Fee
					if strings.Contains(swapClient.GetExchangeName(), exchange.Binance) && o.FeeCurrency == `BNB` && swapSymbol.String() != `BNBUSDT` {
						bnbEqualFee := o.Filled.Mul(o.AvgPrice).Mul(swapClient.FeeRate().CloseTaker.Mul(decimal.NewFromFloat(0.9))).Neg()
						fee = bnbEqualFee
					}
					swapOrder.Fee = swapOrder.Fee.Add(fee)
					if swapOrder.Created.IsZero() {
						if !o.Created.IsZero() {
							swapOrder.Created = o.Created
						} else {
							swapOrder.Created = o.Updated
						}
					}
				}
				if swapName == `swap1` {
					addInfo(swap1Order, or, sg.swapS1)
					takerAmount1 = or.Filled
				} else {
					addInfo(swap2Order, or, sg.swapS2)
					takerAmount2 = or.Filled
				}
			}
		}
	}

	// ==================== 平仓阶段4: hedge — 最终收尾 ====================
	// 与开仓的hedge区别: 平仓需要处理 frozenAmount（冻结的可平量）
	// 如果平仓失败/没有成交，需要 UnfreezeCloseAmount 归还冻结量
hedge:
	check := func() (*position.OrderResult, error) {
		// 【情况1: 两边都没成交】归还冻结量，返回nil
		if swap1Order.Filled.IsZero() && swap2Order.Filled.IsZero() {
			if frozenAmount.IsPositive() {
				sg.PosList.UnfreezeCloseAmount(frozenAmount) // 归还冻结的可平量
			}
			if errMsg != "" {
				return nil, fmt.Errorf("%s", errMsg)
			}
			return nil, nil
		}
		// 【情况2: 有一边成交为0】计入敞口(注意平仓用Neg)，归还冻结量
		if swap1Order.Filled.IsZero() || swap2Order.Filled.IsZero() {
			var errMsg1 string
			if swap1Order.Filled.IsZero() {
				sg.PosList.AddSwapExposure(ctx, swap2Order.Filled.Neg(), "swap2")
				errMsg1 = fmt.Sprintf("swap1(%s[%s]) 成交数量为0, swap2(%s[%s]) 成交数量为%s", swapC1.GetExchangeName(), sg.swapS1, swapC2.GetExchangeName(), sg.swapS2, swap2Order.Filled.String())
			}
			if swap2Order.Filled.IsZero() {
				sg.PosList.AddSwapExposure(ctx, swap1Order.Filled.Neg(), "swap1")
				errMsg1 = fmt.Sprintf("swap2(%s[%s]) 成交数量为0, swap1(%s[%s]) 成交数量为%s", swapC2.GetExchangeName(), sg.swapS2, swapC1.GetExchangeName(), sg.swapS1, swap1Order.Filled.String())
			}
			sg.PosList.UnfreezeCloseAmount(frozenAmount) // 归还冻结的可平量
			// 如果errMsg存在则要把errMsg返回，否则返回已计入敞口等待自动处理
			if errMsg != "" {
				return nil, fmt.Errorf("%s", errMsg)
			}
			return nil, fmt.Errorf("%s, 已计入敞口等待自动处理", errMsg1)
		}
		makerInfo := common.NewJsonObject()
		makerInfo["sr_close_init"] = srCloseInit
		makerInfo["maker_amount1"] = makerAmount1.String()
		makerInfo["maker_amount2"] = makerAmount2.String()
		makerInfo["taker_amount1"] = takerAmount1.String()
		makerInfo["taker_amount2"] = takerAmount2.String()

		switch cfg.SrMode {
		case rate.SrMean:
			makerInfo["sr_mean"] = srWindow
			makerInfo["sr_mean_cancel"] = srWindowCancel
			makerInfo["first_filled"] = firstFilled
			makerInfo["first_sr"] = firstNowSr
			makerInfo["first_sr_mean"] = firstSrWindow
		case rate.SrMedian:
			makerInfo["sr_window"] = srWindow
			makerInfo["sr_median_cancel"] = srWindowCancel
			makerInfo["first_filled"] = firstFilled
			makerInfo["first_sr"] = firstNowSr
			makerInfo["first_sr_median"] = firstSrWindow
		}
		op.SetMakerInfo(makerInfo)

		// 调用 CloseAllLimitHedge 将两边的成交信息写入仓位记录，完成本次平仓操作
		// 与开仓不同: 多传了 cfg.TotalLimit 和 frozenAmount 参数
		level.Info(logger).Log("message", "ready to hedge close", "swap1_filled", swap1Order.Filled.String(), "swap1_avg_price", swap1Order.AvgPrice.String(), "usdt", swap1Order.Filled.Mul(swap1Order.AvgPrice).String(),
			"swap2_filled", swap2Order.Filled, "swap_avg_price", swap2Order.AvgPrice)

		return sg.PosList.CloseAllLimitHedge(ctx, swapC1, swapC2, swap1Order, swap2Order, sg.swap1OrderBook, sg.swap2OrderBook, op, cfg.TotalLimit, frozenAmount)
	}

	return check()
}


func reverseSide(side ccexgo.OrderSide) ccexgo.OrderSide {
	switch side {
	case ccexgo.OrderSideBuy:
		return ccexgo.OrderSideSell
	case ccexgo.OrderSideSell:
		return ccexgo.OrderSideBuy
	case ccexgo.OrderSideCloseLong:
		return ccexgo.OrderSideCloseShort
	case ccexgo.OrderSideCloseShort:
		return ccexgo.OrderSideCloseLong
	}
	return side
}
