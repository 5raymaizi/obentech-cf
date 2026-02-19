package logic

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"cf_arbitrage/exchange"
	actsSpot "cf_arbitrage/exchange/acts/spot"
	"cf_arbitrage/logic/position"
	"cf_arbitrage/logic/rate"
	"cf_arbitrage/message"
	utilCommon "cf_arbitrage/util/common"

	common "go.common"
	"go.common/helper"

	ccexgo "github.com/NadiaSama/ccexgo/exchange"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/shopspring/decimal"
)

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

func (sg *Strategy) openSwap1AndSwap2(ctx context.Context, swapC1, swapC2 exchange.SwapClient,
	srOpen float64, openTradeVolume float64, swap1 *exchange.Depth, swap2 *exchange.Depth, side ccexgo.OrderSide) (*position.OrderResult, error) {
	if err := sg.PosList.CheckPosSideAmount(side, reverseSide(side)); err != nil {
		return nil, err
	}

	var (
		swap1Order = &exchange.CfOrder{}
		swap2Order = &exchange.CfOrder{}
		srOpenInit float64

		// 已移除 checkExitNowTimer 和 exitNow：直接在 ctx.Done() 时处理退出，避免 select 随机性导致的延迟
		makerHedge         bool // 是否是maker hedge
		cancelFilled       bool // 是否是cancel 成交
		takerHedge         bool
		finishedWork       bool
		makerReplaceFailed bool // maker挂单失败，需要taker

		swap1Cancel      bool
		swap2Cancel      bool
		swap1NeedReplace bool
		swap2NeedReplace bool
		swap1End         bool
		swap2End         bool

		makerAmount1 decimal.Decimal
		makerAmount2 decimal.Decimal
		takerAmount1 decimal.Decimal
		takerAmount2 decimal.Decimal

		logger log.Logger // 便于追踪

		srWindow       string
		srWindowCancel string
		// 记录第一次成交的一边名称和sr
		firstFilled   string // swap1或swap2
		firstNowSr    string
		firstSrWindow string // mean或median
		nowSr         decimal.Decimal

		timeoutTimer1    *time.Timer
		maxDurationTimer *time.Timer
	)
	cfg := sg.load.Load()
	one := decimal.NewFromFloat(1)

	openTickNum1 := float64(cfg.MmSwap1OpenTickNum)
	openTickNum2 := float64(cfg.MmSwap2OpenTickNum)
	if side == ccexgo.OrderSideSell {
		openTickNum1 = float64(cfg.MmSwap1CloseTickNum)
		openTickNum2 = float64(cfg.MmSwap2CloseTickNum)
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

	if side == ccexgo.OrderSideBuy {
		srWindow = sg.queue.GetSrOpen2(rate.Swap1MSwap2M, cfg.SrMode)
	} else {
		srWindow = sg.queue.GetSrClose2(rate.Swap1MSwap2M, cfg.SrMode)
	}

	level.Info(sg.logger).Log("message", "openSwap1AndSwap2", "openTickNum1", openTickNum1, "openTickNum2", openTickNum2, "minOrderAmount1", sg.minOrderAmount1, "minOrderAmount2", sg.minOrderAmount2)

	op, order1, order2, errMsg, code, placeMode := sg.PosList.OpenAllLimit(ctx, swapC1, swapC2, srOpen, openTradeVolume, []float64{openTickNum1, openTickNum2}, swap1, swap2, nil, side, &cfg)
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

	case -1: // 两个单都失败,返回原因？
		return nil, fmt.Errorf("%+v", errMsg)
	}

	timeoutTimer1 = utilCommon.NewTimeOutTimer(ctx, sg.swapS1.String(), 10, fmt.Sprintf("%s MM开仓", op.GetOpID()))
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
			level.Warn(logger).Log("message", "open mm strategy timeout 20 minutes, force exit")
			go message.SendP3Important(ctx, message.NewCommonMsgWithAt(fmt.Sprintf("%s MM开仓超时", sg.swapS1), fmt.Sprintf("操作ID: %s, 运行超过20分钟强制退出", op.GetOpID())))
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
		case <-sg.bookTickerEvent.Done(): // bookTicker变动, 同步检测CancelClose
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

			swap1Ask1Price := decimal.NewFromFloat(swap1BookTicker.Ask1Price)
			swap1Bid1Price := decimal.NewFromFloat(swap1BookTicker.Bid1Price)
			swap2Ask1Price := decimal.NewFromFloat(swap2BookTicker.Ask1Price)
			swap2Bid1Price := decimal.NewFromFloat(swap2BookTicker.Bid1Price)

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

			if side == ccexgo.OrderSideBuy { // 做空价差
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
					level.Info(logger).Log("message", "open nowSr < openTakerHedgeThreshold ", "nowSr", nowSr, "takerSr", takerSr, "swap1Ask1Price", swap1Ask1Price, "swap1Bid1Price", swap1Bid1Price, "swap2Ask1Price", swap2Ask1Price, "swap2Bid1Price", swap2Bid1Price)
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
					level.Info(logger).Log("message", "open nowSr < cancelOpenThreshold ", "nowSr", nowSr, "swap1Ask1Price", swap1Ask1Price, "swap1Bid1Price", swap1Bid1Price, "swap2Ask1Price", swap2Ask1Price, "swap2Bid1Price", swap2Bid1Price,
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

					level.Info(logger).Log("message", "open normal check", "nowSr", nowSr, "swap1Ask1Price", swap1Ask1Price, "swap1Bid1Price", swap1Bid1Price, "swap2Ask1Price", swap2Ask1Price, "swap2Bid1Price", swap2Bid1Price,
						"swap1Cancel", swap1Cancel, "swap2Cancel", swap2Cancel, "swap1NeedReplace", swap1NeedReplace, "swap2NeedReplace", swap2NeedReplace, "priceChange1", priceChange1, "priceChange2", priceChange2, "swap1End", swap1End, "swap2End", swap2End,
						"cancelFilled", cancelFilled)
				}

			} else { // 做多价差
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

			if takerHedge || finishedWork { // 如果是taker hedge，则需要判断是否需要撤单
				goto checkAndHedge
			}

			var wg sync.WaitGroup
			if swap1Cancel && order1 != nil {
				wg.Go(func() {
					o, err := swapC1.CancelOrder(ctx, order1) // 撤单,查单
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
					maxMinAmount := math.Max(sg.minOrderAmount1, sg.minOrderAmount2) // 两边刚好都部分成交且相差一个tick会出现？
					if diff.LessThan(decimal.NewFromFloat(maxMinAmount)) {           // 相差小于最小下单量，则认为成交数量一致
						goto checkAndHedge
					}
				}
			}

			if (order1 == nil || order1.Status >= ccexgo.OrderStatusDone) && swap1NeedReplace {
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
				_, o, errMsg1, err := sg.PosList.OpenSwapMaker(ctx, swapC1, srOpen, openAmount, float64(tickNum), sg.swap1OrderBook, op, side, 0, false)
				if err != nil {
					order1.Status = ccexgo.OrderStatusCancel
					errType := swapC1.HandError(err)
					level.Warn(logger).Log("message", "replace open swap1 failed", "openAmount", openAmount, "tickNum", tickNum, "errMsg1", errMsg1)
					go message.Send(ctx, message.NewOperateFail(fmt.Sprintf("%s swap1部分开仓重挂失败", sg.swapS1), errMsg1))
					if errType.Code != exchange.NewOrderRejected { // 直接去taker？
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

		case o := <-swapC1.MakerOrder(swap1Order.ID.String()): // 判断是否完全成交或取消
			if o == nil { // channel超时关闭了
				continue
			}
			level.Info(logger).Log("message", "get swap1 ws order notify", "id", o.ID.String(), "amount", o.Amount, "filled", o.Filled, "fee", o.Fee, "avg_price", o.AvgPrice, "status", o.Status)

			if order1.ID.String() == o.ID.String() && (order1.Filled.LessThan(o.Filled) || order1.Updated.Before(o.Updated) || order1.Status < o.Status) {
				newFilled := o.Filled.Sub(order1.Filled)
				if newFilled.IsPositive() {
					newFee := o.Fee
					if swapC1.GetExchangeName() == exchange.Bybit5 { // bybit5 是总手续费
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
checkAndHedge:
	makerAmount1 = swap1Order.Filled
	makerAmount2 = swap2Order.Filled
	if takerHedge || finishedWork || makerReplaceFailed {
		if order1 != nil && order1.Status < ccexgo.OrderStatusDone {
			level.Info(logger).Log("message", "taker hedge cancel order", "order", order1.ID, "finished_work", finishedWork, "maker_replace_failed", makerReplaceFailed)
			o, err := swapC1.CancelOrder(ctx, order1) // 撤单,查单
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
hedge:
	check := func() (*position.OrderResult, error) {
		if swap1Order.Filled.IsZero() && swap2Order.Filled.IsZero() {
			if errMsg != "" {
				return nil, fmt.Errorf("%s", errMsg)
			}
			return nil, nil
		}
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
			// 如果errMsg存在则要把errMsg返回，否则返回已计入敞口等待自动处理
			if errMsg != "" {
				return nil, fmt.Errorf("%s", errMsg)
			}
			return nil, fmt.Errorf("%s, 已计入敞口等待自动处理", errMsg1)
		}
		makerInfo := common.NewJsonObject()
		makerInfo["sr_open_init"] = srOpenInit
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
			makerInfo["sr_median"] = srWindow
			makerInfo["sr_median_cancel"] = srWindowCancel
			makerInfo["first_filled"] = firstFilled
			makerInfo["first_sr"] = firstNowSr
			makerInfo["first_sr_median"] = firstSrWindow
		}
		op.SetMakerInfo(makerInfo)

		level.Info(logger).Log("message", "ready to hedge open", "swap1_filled", swap1Order.Filled.String(), "swap1_avg_price", swap1Order.AvgPrice.String(), "usdt", swap1Order.Filled.Mul(swap1Order.AvgPrice).String(),
			"swap2_filled", swap2Order.Filled, "swap2_avg_price", swap2Order.AvgPrice)
		return sg.PosList.OpenAllLimitHedge(ctx, swapC1, swapC2, swap1Order, swap2Order, sg.queue.GetSwap1Depth(), sg.queue.GetSwap2Depth(), op)
	}

	return check()
}

func (sg *Strategy) closeSwap1AndSwap2(ctx context.Context, swapC1, swapC2 exchange.SwapClient,
	srClose float64, closeTradeVolume float64, swap1 *exchange.Depth, swap2 *exchange.Depth, side ccexgo.OrderSide) (*position.OrderResult, error) {
	var (
		swap1Order  = &exchange.CfOrder{}
		swap2Order  = &exchange.CfOrder{}
		srCloseInit float64

		// 已移除 checkExitNowTimer 和 exitNow：直接在 ctx.Done() 时处理退出，避免 select 随机性导致的延迟
		makerHedge         bool // 是否是maker hedge
		cancelFilled       bool // 是否是cancel 成交
		takerHedge         bool
		finishedWork       bool
		makerReplaceFailed bool // maker挂单失败，需要taker

		swap1Cancel      bool
		swap2Cancel      bool
		swap1NeedReplace bool
		swap2NeedReplace bool
		swap1End         bool // 结束挂单
		swap2End         bool

		makerAmount1 decimal.Decimal
		makerAmount2 decimal.Decimal
		takerAmount1 decimal.Decimal
		takerAmount2 decimal.Decimal

		logger log.Logger // 便于追踪

		srWindow       string
		srWindowCancel string
		// 记录第一次成交的一边名称和sr
		firstFilled   string // swap1或swap2
		firstNowSr    string
		firstSrWindow string // mean或median
		nowSr         decimal.Decimal
		frozenAmount  decimal.Decimal

		timeoutTimer1    *time.Timer
		maxDurationTimer *time.Timer
	)
	cfg := sg.load.Load()
	one := decimal.NewFromInt(1)

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

hedge:
	check := func() (*position.OrderResult, error) {
		if swap1Order.Filled.IsZero() && swap2Order.Filled.IsZero() {
			if frozenAmount.IsPositive() {
				sg.PosList.UnfreezeCloseAmount(frozenAmount)
			}
			if errMsg != "" {
				return nil, fmt.Errorf("%s", errMsg)
			}
			return nil, nil
		}
		// 如果有一边成交为0, 则计入敞口等待自动处理, 部分成交或者另一边taker失败可能会出现这种情况
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
			sg.PosList.UnfreezeCloseAmount(frozenAmount)
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
