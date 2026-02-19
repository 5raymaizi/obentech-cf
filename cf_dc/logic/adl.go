package logic

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"cf_arbitrage/exchange"
	actsSpot "cf_arbitrage/exchange/acts/spot"
	"cf_arbitrage/message"
	utilCommon "cf_arbitrage/util/common"

	common "go.common"
	"go.common/apis"

	ccexgo "github.com/NadiaSama/ccexgo/exchange"
	"github.com/go-kit/kit/log/level"
	"github.com/go-redis/redis/v7"
	"github.com/shopspring/decimal"
)

// ADL处理相关的常量配置
const (
	// Redis相关配置
	adlRedisLockTTL     = 300 * time.Second // 锁TTL，从10分钟减少到5分钟
	adlRedisKeyTTL      = 600 * time.Second // 优先级键TTL
	adlRedisLockRefresh = 60 * time.Second  // 锁刷新间隔

	// ADL处理逻辑配置
	adlExposureMinValue     = 100                    // 最小敞口阈值（USDT）
	adlMaxSingleTradeAmount = 5000                   // 单次交易最大金额（USDT）
	adlProcessMaxTimeout    = 3 * time.Minute        // 最大处理超时时间
	adlFailureThreshold     = 3                      // 连续失败阈值
	adlEmptyExposureLimit   = 100                    // 连续获取不到敞口的最大次数
	adlTryInterval          = 100 * time.Millisecond // 尝试间隔
	adlReportInterval       = 100 * time.Millisecond // 上报间隔
	adlLeaveInterval        = 30 * time.Second       // 闲置时间隔
	adlProcessCooldown      = 3 * time.Second        // 处理冷却时间
)

type ADLProcessor struct {
	InADL        bool            `json:"in_adl"`
	ADLNotFinish bool            `json:"adl_not_finish"` // 出现adl后人工检查完毕需将该标识置为false
	Pair         string          `json:"pair"`           // 例如 "BTCUSDT"
	Coin         string          `json:"coin"`           // eg: BTC
	Exposure1    decimal.Decimal `json:"exposure1"`      // 币本位是张，u本位是币
	Side1        apis.OrderSide  `json:"side1"`
	Exposure2    decimal.Decimal `json:"exposure2"` // 币本位是张，u本位是币
	Side2        apis.OrderSide  `json:"side2"`
}

func NewADLProcessor(pair, coin string) *ADLProcessor {
	return &ADLProcessor{
		Pair: pair,
		Coin: coin,
	}
}

func (p *ADLProcessor) priorityKey() string {
	return fmt.Sprintf("CF_DC:ADL:%s", p.Pair)
}

func (p *ADLProcessor) lockKey() string {
	return fmt.Sprintf("CF_DC:ADL:%s:LOCK", p.Coin)
}

// adl后持续上报敞口
func (sg *Strategy) reportADLExposure(adlEvent, event *common.Event) {
	ticker := utilCommon.NewTimerWithInterval(adlReportInterval)
	defer func() {
		ticker.Stop()
		level.Info(sg.logger).Log("message", "reportADLExposure quit")
	}()

	level.Warn(sg.logger).Log("message", "reportADLExposure start")

	// 主循环持续上报敞口
	for {
		select {
		case <-event.Done():
			return
		case <-adlEvent.Done():
			if ticker.GetInterval() != adlReportInterval {
				ticker = utilCommon.NewTimerWithInterval(adlReportInterval)
			}
		case <-ticker.C:
			// 获取当前敞口
			exposure, price := sg.getADLExposure()
			if exposure.IsZero() && price.IsZero() { // 说明是获取敞口有问题
				ticker = utilCommon.NewTimerWithInterval(adlReportInterval)
				continue
			}

			// if exposure.IsNegative() {
			// 	level.Warn(sg.logger).Log("message", "ADL exposure is negative", "exposure", exposure.String())
			// 	sg.ADLProcessor.InADL = false
			// 	ticker = utilCommon.NewTimerWithInterval(adlLeaveInterval)
			// 	continue
			// }

			// 计算敞口数值用于排序
			exposureValue := exposure.Mul(price).Abs()
			// if sg.inverse {
			// 	exposureValue = exposure.Mul(sg.swapS.ContractVal())
			// }

			if !sg.ADLProcessor.InADL && exposureValue.LessThan(decimal.NewFromInt(adlExposureMinValue)) {
				ticker = utilCommon.NewTimerWithInterval(adlLeaveInterval)
				continue
			}

			if exposureValue.GreaterThan(decimal.NewFromInt(adlExposureMinValue)) && !sg.ADLProcessor.InADL && sg.TotalFailedTimes < adlFailureThreshold { // 自动开启adl状态，如果敞口大于最小值，并且没有在adl状态，并且连续失败次数小于阈值
				sg.ADLProcessor.InADL = true
				adlEvent.Set()
			}
			level.Info(sg.logger).Log(
				"message", "reporting ADL exposure",
				"env", sg.envName,
				"exposure", exposure.String(),
				"price", price.String(),
				"value", exposureValue.String(),
			)

			// 原子化上报（ZADD + EXPIRE）
			pipe := sg.rdb.TxPipeline()
			pipe.ZAdd(sg.ADLProcessor.priorityKey(), &redis.Z{
				Score:  -math.Abs(exposureValue.InexactFloat64()), // 按负值排名
				Member: sg.envName,
			})
			pipe.Expire(sg.ADLProcessor.priorityKey(), adlRedisKeyTTL)
			if _, err := pipe.Exec(); err != nil {
				level.Warn(sg.logger).Log("message", "reportADLExposure failed", "err", err)
			}

			if !sg.ADLProcessor.InADL || sg.TotalFailedTimes >= adlFailureThreshold || exposure.IsZero() { // adl状态退出
				if exposure.IsZero() && sg.ADLProcessor.InADL {
					sg.ADLProcessor.InADL = false
				}
			}

			if sg.ADLProcessor.InADL {
				ticker = utilCommon.NewTimerWithInterval(adlReportInterval)
			} else {
				ticker = utilCommon.NewTimerWithInterval(adlLeaveInterval)
			}
		}
	}
}

// 接受信号处理ADL的函数
func (sg *Strategy) ProcessADL(adlEvent, event *common.Event) {
	tryTimer := utilCommon.NewTimerWithInterval(adlTryInterval)
	defer func() {
		tryTimer.Stop()
		level.Info(sg.logger).Log("message", "TryProcessingADL proc quit")
	}()
	for {
		select {
		case <-event.Done():
			return
		case <-tryTimer.C:
			if !sg.ADLProcessor.InADL || sg.TotalFailedTimes >= adlFailureThreshold {
				tryTimer = utilCommon.NewTimerWithInterval(adlLeaveInterval)
				continue
			}
			if sg.TryProcessingADL() { // 尝试去处理
				tryTimer = utilCommon.NewTimerWithInterval(adlProcessCooldown) // 成功处理后冷却
			} else {
				tryTimer = utilCommon.NewTimerWithInterval(adlTryInterval)
			}
		case <-adlEvent.Done():
			adlEvent.Unset()
			if tryTimer.GetInterval() != adlTryInterval {
				tryTimer = utilCommon.NewTimerWithInterval(adlTryInterval)
			}
		}
	}
}

// TryProcessingADL 尝试获取锁并处理ADL
func (sg *Strategy) TryProcessingADL() bool {
	// 检查是否是优先级最高的环境
	rank, err := sg.rdb.ZRank(sg.ADLProcessor.priorityKey(), sg.envName).Result()
	if err != nil || rank != 0 {
		return false // 不是第一名直接放弃
	}

	// 尝试获取锁
	lockKey := sg.ADLProcessor.lockKey()
	locked, err := sg.rdb.SetNX(lockKey, sg.envName, adlRedisLockTTL).Result()
	if err != nil || !locked {
		return false
	}

	// 创建锁刷新通道和结束通知
	lockDone := make(chan struct{})
	var wg sync.WaitGroup

	// 启动锁刷新goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(adlRedisLockRefresh)
		defer ticker.Stop()

		for {
			select {
			case <-lockDone:
				return
			case <-ticker.C:
				// 刷新锁TTL
				if ok, err := sg.rdb.Expire(lockKey, adlRedisLockTTL).Result(); !ok || err != nil {
					level.Warn(sg.logger).Log("message", "Failed to refresh ADL lock", "error", err)
					return
				}
				level.Info(sg.logger).Log("message", "Refreshed ADL lock")
			}
		}
	}()

	// 确保锁释放和恢复处理panic
	defer func() {
		// 通知锁刷新goroutine退出
		close(lockDone)
		// 等待goroutine完成
		wg.Wait()
		// 删除锁
		sg.rdb.Del(lockKey)

		// 恢复处理panic
		if r := recover(); r != nil {
			level.Error(sg.logger).Log("message", "processADL panic", "error", r)
			// 记录ADL处理失败，并确保下次可以尝试
			sg.addFailed()
		}
		sg.serialize()
	}()

	// 处理ADL敞口
	sg.processADLExposure()
	return true
}

// getADLExposure 获取当前敞口和价格
func (sg *Strategy) getADLExposure() (decimal.Decimal, decimal.Decimal) {
	if sg.swap1OrderBook == nil || len(sg.swap1OrderBook.Bids) == 0 {
		return decimal.Zero, decimal.Zero
	}
	price := decimal.NewFromFloat(sg.swap1OrderBook.Bids[0].Price)

	// 用获取到的adl数量去处理
	return sg.ADLProcessor.Exposure1.Sub(sg.ADLProcessor.Exposure2), price
}

// adl处理逻辑 https://digifinex.larksuite.com/docx/LMTGdihAnoH3w5xuX3QuHBaBsMd
func (sg *Strategy) processADLExposure() {
	// 创建带有超时的上下文，防止无限处理
	ctx, cancel := context.WithTimeout(context.Background(), adlProcessMaxTimeout)
	defer cancel()

	// 发送开始处理消息
	startMsg := message.NewCommonMsgWithAt(
		fmt.Sprintf("%s ADL自动处理", sg.swapS1),
		"开始进行ADL处理, 请关注",
	)
	go message.SendP3Important(sg.ctx, startMsg)

	var (
		processEnd         bool
		count, failedCount int64
		reason             string
		firstExposure      decimal.Decimal
	)

	processStart := time.Now()
	// cv := sg.swapS1.ContractVal()
	// cv2 := sg.swapS2.ContractVal()
	// 主处理循环
	for !processEnd {
		select {
		case <-ctx.Done():
			// 处理超时
			reason = "处理超时，达到最大处理时间限制"
			break
		default:
			// 检查连续失败次数
			if sg.TotalFailedTimes >= adlFailureThreshold {
				reason = "连续失败三次"
				break
			}

			if count != 0 {
				// 短暂延迟，防止处理太快
				time.Sleep(50 * time.Millisecond)
			}

			// 获取当前敞口
			exposure, price := sg.getADLExposure()
			if exposure.IsZero() && price.IsZero() {
				failedCount++
				if failedCount > adlEmptyExposureLimit {
					reason = "连续多次获取不到敞口"
					break
				}
				time.Sleep(time.Millisecond * 100)
				continue
			}

			// 记录首次敞口
			if count == 0 {
				firstExposure = exposure
			}

			exposureAbs := exposure.Abs()
			// 记录敞口信息
			exposureValue := exposureAbs.Mul(price)
			// if sg.inverse {
			// 	exposureValue = exposure.Mul(cv)
			// }
			level.Info(sg.logger).Log(
				"message", "ADL exposure",
				"exposure", exposure.String(),
				"price", price.String(),
				"value", exposureValue.String(),
				"count", count,
			)

			// 检查敞口是否已经低于阈值
			if exposureValue.LessThan(decimal.NewFromInt(adlExposureMinValue)) {
				processEnd = true
				reason = fmt.Sprintf("处理完成,当前敞口已小于%dUSDT", adlExposureMinValue)
				sg.slMu.Lock()
				if sg.ADLProcessor.Exposure1.Equal(sg.ADLProcessor.Exposure2) {
					sg.ADLProcessor.Exposure1 = decimal.Zero
					sg.ADLProcessor.Exposure2 = decimal.Zero
				}
				sg.slMu.Unlock()
				break
			}

			// 检查深度数据
			swap1BidBookAmount := calcBookBis5Amount(sg.swap1OrderBook)
			swap2BidBookAmount := calcBookBis5Amount(sg.swap2OrderBook)
			if swap1BidBookAmount.IsZero() && swap2BidBookAmount.IsZero() {
				failedCount++
				level.Warn(sg.logger).Log("message", "No depth data available", "spotAmount", swap1BidBookAmount, "swapAmount", swap2BidBookAmount)
				time.Sleep(time.Millisecond * 100)
				continue
			}

			// 处理订单
			count++
			// 计算下单数量，取最小值以保证安全
			maxAmountUSDT := decimal.NewFromInt(adlMaxSingleTradeAmount)
			swap1NeedAmount := decimal.Min(
				swap1BidBookAmount,
				maxAmountUSDT.Div(price),
				exposureAbs.Div(decimal.NewFromInt(2)),
			)
			swap2NeedAmount := decimal.Min(
				swap2BidBookAmount,
				maxAmountUSDT.Div(price),
				exposureAbs.Div(decimal.NewFromInt(2)),
			)

			exp1 := 0
			exp2 := 0
			if sg.inverse {
				// if sg.exchange == exchange.Okex5 {
				// 	exp = actsSpot.CalcExponent(decimal.NewFromFloat(sg.swapActsSymbol.AmountTickSize))
				// }
				// swap1NeedAmount = decimal.Min(
				// 	swap1BidBookAmount,
				// 	maxAmountUSDT.Div(price),
				// 	exposure.Div(decimal.NewFromInt(2)).Mul(cv).Div(price),
				// )
				// swap2NeedAmount = decimal.Min(
				// 	swap2BidBookAmount,
				// 	maxAmountUSDT.Div(cv),
				// 	exposure.Div(decimal.NewFromInt(2)),
				// ).RoundFloor(int32(exp))

			} else {
				exp1 = actsSpot.CalcExponent(sg.swapS1.ContractVal())
				if sg.swap1OrderBook.Exchange == exchange.Okex5 || sg.swap1OrderBook.Exchange == exchange.GateIO { // 个数精度
					exp1 = actsSpot.CalcExponent(decimal.NewFromFloat(sg.swap1ActsSymbol.AmountTickSize * sg.swap1ActsSymbol.ContractValue))
				}
				swap1NeedAmount = swap1NeedAmount.RoundFloor(int32(exp1))
				// if !newSwapNeedAmount.Equal(swap2NeedAmount) {
				// 	swap1NeedAmount = swap1NeedAmount.Add(swap2NeedAmount.Sub(newSwapNeedAmount))
				// swap2NeedAmount = newSwapNeedAmount
				// }
				exp2 = actsSpot.CalcExponent(sg.swapS2.ContractVal())
				if sg.swap2OrderBook.Exchange == exchange.Okex5 || sg.swap2OrderBook.Exchange == exchange.GateIO { // 个数精度
					exp2 = actsSpot.CalcExponent(decimal.NewFromFloat(sg.swap2ActsSymbol.AmountTickSize * sg.swap2ActsSymbol.ContractValue))
				}
				swap2NeedAmount = swap2NeedAmount.RoundFloor(int32(exp2))
			}

			// if swap2NeedAmount.LessThan(sg.swapS2.AmountMin()) { // 合约不够下，转移到现货
			// 	if sg.inverse {
			// 		swap1NeedAmount = swap1NeedAmount.Add(swap2NeedAmount.Mul(sg.swapS2.ContractVal()).Div(price))
			// 	} else {
			// 		swap1NeedAmount = swap1NeedAmount.Add(swap2NeedAmount)
			// 	}
			// 	swap2NeedAmount = decimal.Zero
			// }

			// 执行ADL处理
			level.Info(sg.logger).Log(
				"message", "Processing ADL orders",
				"swap1Amount", swap1NeedAmount.String(),
				"swap2Amount", swap2NeedAmount.String(),
				"price", price,
				"attempt", count,
			)

			var side1 ccexgo.OrderSide
			if exposure.IsNegative() {
				// sell
				side1 = ccexgo.OrderSideCloseLong
				if sg.ADLProcessor.Side2 == apis.OrderSideCloseLong { // 做多价差
					side1 = ccexgo.OrderSideCloseShort
				}
			} else {
				// buy
				side1 = ccexgo.OrderSideBuy
				if sg.ADLProcessor.Side1 == apis.OrderSideCloseShort { // 做多价差
					side1 = ccexgo.OrderSideSell
				}
			}

			// 处理ADL订单
			swap1Order, swap2Order, err := sg.PosList.ProcessADL(sg.ctx, swap1NeedAmount, swap2NeedAmount, price, side1)

			// 创建日志消息
			warnStr := fmt.Sprintf("第%d次处理\n", count)
			if swap1Order == nil && swap2Order == nil { // 同时下单失败
				sg.addFailed()
				failedCount++
				if err != nil {
					warnStr += fmt.Sprintf("下单出错:%s", err)
					level.Error(sg.logger).Log("message", "ADL order failed", "error", err)
				} else {
					level.Warn(sg.logger).Log("message", "ADL order returned nil")
				}
			} else {
				sg.resetFailed()
				if swap1Order != nil {
					warnStr += fmt.Sprintf("swap1订单成交:%+v\n", swap1Order)
					amt := swap1Order.Filled
					// if sg.inverse {
					// 	amt = swap1Order.Filled.Mul(swap1Order.AvgPrice).Div(cv).Round(int32(exp1))
					// }
					sg.slMu.Lock()
					if !sg.ADLProcessor.Exposure1.IsZero() && !sg.ADLProcessor.Exposure2.IsZero() {
						if side1 == ccexgo.OrderSideBuy || side1 == ccexgo.OrderSideSell {
							sg.ADLProcessor.Exposure1 = sg.ADLProcessor.Exposure1.Sub(amt)
						} else {
							sg.ADLProcessor.Exposure2 = sg.ADLProcessor.Exposure2.Sub(amt)
						}
					} else if !sg.ADLProcessor.Exposure1.IsZero() {
						sg.ADLProcessor.Exposure1 = sg.ADLProcessor.Exposure1.Sub(amt)
					} else if !sg.ADLProcessor.Exposure2.IsZero() {
						sg.ADLProcessor.Exposure2 = sg.ADLProcessor.Exposure2.Sub(amt)
					}
					sg.slMu.Unlock()

					level.Info(sg.logger).Log("message", "swap1 order completed", "order", fmt.Sprintf("%+v", swap1Order), "now_exposure", sg.ADLProcessor.Exposure1)
				}
				if swap2Order != nil {
					warnStr += fmt.Sprintf("swap2订单成交:%+v\n", swap2Order)
					amt := swap2Order.Filled
					sg.slMu.Lock()
					if !sg.ADLProcessor.Exposure1.IsZero() && !sg.ADLProcessor.Exposure2.IsZero() {
						if side1 == ccexgo.OrderSideBuy || side1 == ccexgo.OrderSideSell {
							sg.ADLProcessor.Exposure1 = sg.ADLProcessor.Exposure1.Sub(amt)
						} else {
							sg.ADLProcessor.Exposure2 = sg.ADLProcessor.Exposure2.Sub(amt)
						}
					} else if !sg.ADLProcessor.Exposure1.IsZero() {
						sg.ADLProcessor.Exposure1 = sg.ADLProcessor.Exposure1.Sub(amt)
					} else if !sg.ADLProcessor.Exposure2.IsZero() {
						sg.ADLProcessor.Exposure2 = sg.ADLProcessor.Exposure2.Sub(amt)
					}
					sg.slMu.Unlock()

					level.Info(sg.logger).Log("message", "Swap2 order completed", "order", fmt.Sprintf("%+v", swap2Order), "now_exposure", sg.ADLProcessor.Exposure1)
				}
				if err != nil {
					warnStr += fmt.Sprintf("一边下单出现错误:%s", err)
					level.Warn(sg.logger).Log("message", "Partial ADL order error", "error", err)
				}
			}

			// 发送处理消息
			processMsg := message.NewCommonMsg(fmt.Sprintf("%s ADL自动处理", sg.swapS1), warnStr)
			go message.SendP3Important(sg.ctx, processMsg)
		}

		// 如果已设置退出原因，跳出循环
		if reason != "" {
			break
		}
	}

	// 获取最终敞口
	endExposure, _ := sg.getADLExposure()
	processDuration := time.Since(processStart)

	unit := sg.spotS.Base()
	if sg.inverse {
		unit = "张"
	}
	// 发送结束消息
	endMsgBody := fmt.Sprintf(
		"处理结束，处理前敞口%s %s, 处理后剩余敞口%s %s, 处理时间: %s, 退出自动处理原因: %s",
		firstExposure.Truncate(8), unit,
		endExposure.Truncate(8), unit,
		processDuration.String(),
		reason,
	)
	endMsg := message.NewCommonMsgWithAt(fmt.Sprintf("%s ADL自动处理退出", sg.swapS1), endMsgBody)
	go message.SendP3Important(sg.ctx, endMsg)

	// 记录处理完成日志
	level.Info(sg.logger).Log(
		"message", "ADL process completed",
		"reason", reason,
		"initialExposure", firstExposure.String(),
		"finalExposure", endExposure.String(),
		"duration", processDuration.String(),
		"orderCount", count,
	)

	// 如果处理成功完成，设置InADL为false
	if processEnd {
		sg.ADLProcessor.InADL = false
	}
}

// calcBookBis5Amount 计算前5档深度总量
func calcBookBis5Amount(depth *exchange.Depth) (total decimal.Decimal) {
	if depth == nil || len(depth.Bids) == 0 {
		return decimal.Zero
	}

	maxIdx := 5
	if len(depth.Bids) < maxIdx {
		maxIdx = len(depth.Bids)
	}

	for idx := 0; idx < maxIdx; idx++ {
		total = total.Add(decimal.NewFromFloat(depth.Bids[idx].Amount))
	}
	return
}
