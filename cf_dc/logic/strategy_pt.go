package logic

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cf_arbitrage/exchange"
	"cf_arbitrage/exchange/acts"
	"cf_arbitrage/logic/config"
	"cf_arbitrage/logic/position"
	"cf_arbitrage/logic/rate"
	"cf_arbitrage/message"

	gocommon "go.common"

	ccexgo "github.com/NadiaSama/ccexgo/exchange"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/shopspring/decimal"
)

func (sg *Strategy) preTakerOpenSwapAndHedge(ctx context.Context, swapC1 exchange.SwapClient, swapC2 exchange.SwapClient,
	srOpen float64, openTradeVolume float64, swap1, swap2 *exchange.Depth, side1 ccexgo.OrderSide, cfg *config.Config, mode rate.RunMode) (*position.OrderResult, error) {
	takerSwapC := swapC1
	makerSwapC := swapC2
	takerSide := side1
	makerSide := reverseSide(side1)
	makerSwapDepth := swap2
	mSwapActsSymbol := sg.swap2ActsSymbol
	tSwapSymbol := sg.swapS1
	mSwapSymbol := sg.swapS2
	takerSwapName := "swap1"
	makerSwapName := "swap2"
	if mode == rate.Swap1MSwap2T {
		takerSwapC = swapC2
		makerSwapC = swapC1
		takerSide = reverseSide(side1)
		makerSide = side1
		makerSwapDepth = swap1
		mSwapActsSymbol = sg.swap1ActsSymbol
		tSwapSymbol = sg.swapS2
		mSwapSymbol = sg.swapS1
		takerSwapName = "swap2"
		makerSwapName = "swap1"
	}

	op, swapOrderT, err := sg.PosList.OpenSwapTakerOrder(ctx, cfg, takerSwapC, srOpen, openTradeVolume, takerSide, mode)
	if err != nil {
		return nil, err
	}

	// 币安taker可能是bnb抵扣费, 要计算出对应的usdt费
	var bnbEqualFee decimal.Decimal
	if strings.Contains(takerSwapC.GetExchangeName(), exchange.Binance) && swapOrderT.FeeCurrency == `BNB` && tSwapSymbol.String() != `BNBUSDT` {
		bnbEqualFee = swapOrderT.Filled.Mul(swapOrderT.AvgPrice).Mul(takerSwapC.FeeRate().OpenTaker.Mul(decimal.NewFromFloat(0.9))).Neg()
		swapOrderT.Fee = bnbEqualFee
	}

	one := decimal.NewFromInt(1)
	size := swapOrderT.Filled

	var (
		errInfo      string
		myPlaceOrder *exchange.CfOrder
		// multiDonePrice []string
		//与bestPrice的距离
		// multiDistance []string
		// 挂单数量
		multiAmount []string
		// 实际成交数量
		// multiDoneAmount []string
		// 实际成交实际
		// multiDoneTime []string
		// 挂单价格
		multiPrice []string
		srOpenInit float64
		swapOrderM *exchange.CfOrder
	)
	// cv, _ := sg.swapS.ContractVal().Float64()
	// bestPrice := (swap1.Asks[0].Price + swap1.Bids[0].Price) / 2

	openTradeOrderVolumes := []float64{size.InexactFloat64()}
	openTradeOrderTickNums := []float64{float64(cfg.PreTakerOpenTickNum)}
	if side1 == ccexgo.OrderSideSell {
		openTradeOrderTickNums[0] = float64(cfg.PreTakerCloseTickNum)
	}
	logger := log.With(sg.logger, "id", op.GetOpID())

	// 20分钟超时保护，防止协程永久卡住
	maxDurationTimer := time.NewTimer(20 * time.Minute)
	defer maxDurationTimer.Stop()

	// 撤单和清理的通用逻辑（声明在早期，避免 goto 跳过变量声明）
	var cancelAndExit func()

	_, o, errMsg, err := sg.PosList.OpenSwapMaker(ctx, makerSwapC, srOpen, openTradeOrderVolumes[0], openTradeOrderTickNums[0], makerSwapDepth, nil, makerSide, cfg.MakerType, true)
	if err != nil { // 第一次下单失败,直接退出开仓，后面满足条件会再进入开仓
		level.Info(logger).Log("message", "first open order failed", "err", err.Error())

		if strings.Contains(errMsg, "订单会立即成交") { // 直接去taker
			swapOrderM, err = makerSwapC.MarketOrder(ctx, makerSide, size, decimal.Zero, nil)
			if err == nil {
				swapOrderM.IDs = []string{swapOrderM.ID.String()}
				fee := swapOrderM.Fee
				if strings.Contains(makerSwapC.GetExchangeName(), exchange.Binance) && swapOrderM.FeeCurrency == `BNB` && sg.swapS2.String() != `BNBUSDT` {
					bnbEqualFee := swapOrderM.Filled.Mul(swapOrderM.AvgPrice).Mul(makerSwapC.FeeRate().OpenTaker.Mul(decimal.NewFromFloat(0.9))).Neg()
					fee = bnbEqualFee
				}
				swapOrderM.Fee = fee
				goto end
			} else {
				errType := makerSwapC.HandError(err)
				sg.PosList.UpdateBannedTime(errType, false)
				sg.PosList.CheckErrShareBan(errType, makerSwapC.GetExchangeName())
				errInfo += fmt.Sprintf("%s[%s] taker下单失败, 错误原因:%s\n", makerSwapName, mSwapSymbol, errType.String())
			}
		}

		if errMsg != "" {
			level.Info(logger).Log("message", "开仓订单创建失败", "err", errMsg)
			errInfo += errMsg + "\n"
		}
		if strings.Contains(errMsg, "自动处理") {
			errInfo = fmt.Sprintf("【系统自动处理】%s挂单失败, %s敞口将自动处理, %s", makerSwapName, takerSwapName, errInfo)
		} else {
			errInfo = fmt.Sprintf("【人工处理】%s挂单失败, %s敞口将自动处理, %s出现未知错误, 请人工处理\n%s", makerSwapName, takerSwapName, makerSwapName, errInfo)
		}

		sg.PosList.AddSwapExposure(ctx, swapOrderT.Filled, takerSwapName)
		return nil, fmt.Errorf("%+v", errInfo)
	}
	if o.Status >= ccexgo.OrderStatusDone {
		o.Status = ccexgo.OrderStatusOpen
		o.Filled = decimal.Zero
		o.AvgPrice = decimal.Zero
		o.Fee = decimal.Zero
		o.FeeCurrency = ``
		o.IsMaker = false
	}
	if o.ID.String() != `` {
		myPlaceOrder = o
		// 生成记录
		multiPrice = append(multiPrice, o.Price.String())
		multiAmount = append(multiAmount, o.Amount.String())
	}
	// TODO 没有一个挂单成功，多挂单这里会有问题，其余订单不一定是挂失败了，要具体判断错误
	// if len(orderResults) == 0 {
	// 	return nil, fmt.Errorf("%+v", errInfo)
	// } else {
	// 	if errInfo != "" { // TODO 先发个告警吧
	// 		go message.SendImportant(context.Background(), message.NewCommonMsgWithAt(fmt.Sprintf("%s 合约部分开仓挂单失败", sg.swapS), errInfo))
	// 	}
	// }
	srOpenInit = myPlaceOrder.Price.Div(swapOrderT.AvgPrice).Sub(one).InexactFloat64()
	if mode == rate.Swap1MSwap2T {
		srOpenInit = swapOrderT.AvgPrice.Div(myPlaceOrder.Price).Sub(one).InexactFloat64()
	}
	swapOrderM = &exchange.CfOrder{
		IDs: []string{myPlaceOrder.ID.String()},
		Order: ccexgo.Order{
			ID:       myPlaceOrder.ID,
			Side:     myPlaceOrder.Side,
			AvgPrice: decimal.NewFromFloat(0),
			Filled:   decimal.NewFromFloat(0),
			Created:  time.Now(),
		},
	}
	if !myPlaceOrder.Created.IsZero() {
		swapOrderM.Created = myPlaceOrder.Created
	}

	// if strings.Contains(sg.exchange, exchange.Binance) { // 币安开启bnb抵扣逻辑
	// 	bnb, err = sg.PosList.GetBNBInfo()
	// 	if err == nil {
	// 		if time.Now().Unix()-bnb.Swap.Ts < 90 && bnb.Swap.Total*bnb.Ask1 > cfg.BNBMinimumValue {
	// 			usdmUseBnbFee = true
	// 		}
	// 	}
	// }

	//!注意合约订单手续费需取反变成负数
	// 实现撤单和清理的通用逻辑
	cancelAndExit = func() {
		// 撤单,查单,去对冲
		// TODO:先实现顺序撤单
		o, err = makerSwapC.CancelOrder(ctx, myPlaceOrder)
		if err != nil {
			errType := makerSwapC.HandError(err)
			sg.PosList.UpdateBannedTime(errType, false)
			sg.PosList.CheckErrShareBan(errType, makerSwapC.GetExchangeName())
			level.Warn(logger).Log("message", "open cancel order failed", "err", err.Error())
			err = fmt.Errorf("%s %s[%s]撤单或查单失败, 错误原因:%s, 需要人工处理", makerSwapName, mSwapSymbol, myPlaceOrder.ID.String(), errType.String())
			go message.SendP3Important(context.Background(), message.NewCommonMsgWithAt(fmt.Sprintf("%s %s部分开仓失败", sg.swapS2, makerSwapName), err.Error()))
			return
		}

		level.Info(logger).Log("message", "open checkCancel cancel order success", "order_id", myPlaceOrder.ID.String())
		if !o.Filled.IsZero() {
			newFilled := o.Filled.Sub(myPlaceOrder.Filled)
			if newFilled.IsPositive() {
				newFee := o.Fee.Sub(myPlaceOrder.Fee)
				swapOrderM.AvgPrice = swapOrderM.Filled.Mul(swapOrderM.AvgPrice).Add(newFilled.Mul(o.AvgPrice)).Div(swapOrderM.Filled.Add(newFilled))
				swapOrderM.Filled = swapOrderM.Filled.Add(newFilled)
				swapOrderM.Fee = swapOrderM.Fee.Add(newFee.Neg()) //撤单主动查出来的是总手续费
				swapOrderM.FeeCurrency = o.FeeCurrency
				swapOrderM.Status = o.Status
				swapOrderM.IsMaker = o.IsMaker
				// if !strings.Contains(makerSwapC.GetExchangeName(), exchange.Binance) {
				// 	swapOrderM.Created = o.Created
				// }
				swapOrderM.Updated = o.Updated
				// 生成记录
				// common.CreatOrderDoneField(ctx, logger, bestPrice, swap2.Symbol.PricePrecision(), orderResults[i], &multiDonePrice, &multiDoneAmount, &multiDistance, &multiDoneTime)
			}

			myPlaceOrder.Status = o.Status
			myPlaceOrder.Filled = o.Filled
			myPlaceOrder.Fee = o.Fee
		}
		if swapOrderM.Filled.LessThan(size) { // taker差值
			remainAmount := size.Sub(swapOrderM.Filled)
			if remainAmount.GreaterThanOrEqual(decimal.NewFromFloat(mSwapActsSymbol.MinimumOrderAmount)) {
				// swapRestStart := time.Now()
				o, err = makerSwapC.MarketOrder(ctx, makerSide, remainAmount, decimal.Zero, nil)
				// swapRestEnd := time.Now()
				if err == nil {
					swapOrderM.ID = o.ID
					swapOrderM.IDs = append(swapOrderM.IDs, o.ID.String())
					swapOrderM.AvgPrice = swapOrderM.Filled.Mul(swapOrderM.AvgPrice).Add(o.Filled.Mul(o.AvgPrice)).Div(swapOrderM.Filled.Add(o.Filled))
					swapOrderM.Filled = swapOrderM.Filled.Add(o.Filled)
					if swapOrderM.FeeCurrency == `` {
						swapOrderM.FeeCurrency = o.FeeCurrency
					} else {
						swapOrderM.FeeCurrency += `_` + o.FeeCurrency
					}
					swapOrderM.Status = o.Status
					swapOrderM.IsMaker = o.IsMaker
					// if !strings.Contains(sg.exchange, exchange.Binance) { // 币安合约ws推送没有创建时间
					// 	swapOrder.Created = o.Created
					// }
					swapOrderM.Updated = o.Updated

					fee := o.Fee
					if strings.Contains(makerSwapC.GetExchangeName(), exchange.Binance) && o.FeeCurrency == `BNB` && mSwapSymbol.String() != `BNBUSDT` {
						bnbEqualFee := o.Filled.Mul(o.AvgPrice).Mul(makerSwapC.FeeRate().OpenTaker.Mul(decimal.NewFromFloat(0.9))).Neg()
						fee = bnbEqualFee
					}
					swapOrderM.Fee = swapOrderM.Fee.Add(fee)

				} else {
					errType := makerSwapC.HandError(err)
					sg.PosList.UpdateBannedTime(errType, false)
					sg.PosList.CheckErrShareBan(errType, makerSwapC.GetExchangeName())
					level.Warn(logger).Log("message", "open exit cancel order failed", "err", err.Error())
					err = fmt.Errorf("preTaker maker方进行taker对冲失败, 错误原因: %s", errType.String())
				}
			}
		}
	}

	for {
		select {
		case <-maxDurationTimer.C:
			// 超过20分钟，强制终止流程
			level.Warn(logger).Log("message", "PT open strategy timeout 20 minutes, force exit", "order_id", myPlaceOrder.ID.String())
			go message.SendP3Important(ctx, message.NewCommonMsgWithAt(fmt.Sprintf("%s PT开仓超时", sg.swapS1), fmt.Sprintf("操作ID: %s, 订单ID: %s, 运行超过20分钟强制退出", op.GetOpID(), myPlaceOrder.ID.String())))
			cancelAndExit()
			goto end
		case <-ctx.Done():
			level.Info(logger).Log("message", "open exit now")
			cancelAndExit()
			goto end
		case <-sg.bookTickerEvent.Done(): // bookTicker变动
			sg.bookTickerEvent.Unset()

			var swapMBookTicker *exchange.BookTicker
			if mode == rate.Swap1MSwap2T {
				swapMBookTicker = swapC1.BookTicker()
			} else if mode == rate.Swap1TSwap2M {
				swapMBookTicker = swapC2.BookTicker()
			}
			if /*spotBookTicker == nil ||*/ swapMBookTicker == nil {
				level.Warn(logger).Log("message", "open book ticker is nil", "swapMBookTicker", fmt.Sprintf("%+v", swapMBookTicker), "mode", mode)
				continue
			}
			/*
				ahAsk := cf_factor.CalculateAhAskRatio(spotBookTicker, swapBookTicker, cv, sg.inverse)

				sr := sg.queue.CancelOpen(orders[0].Price.InexactFloat64(), rate.SpotTSwapM)
				if ahAsk <= cfg.Factor.TMAhOpenThreshold && !sr.Ok {
					continue
				}
			*/

			priceChange := myPlaceOrder.Price.Sub(decimal.NewFromFloat(swapMBookTicker.Bid1Price))
			replaceTickNum := cfg.PreTakerReplaceOpenTickNum
			if makerSide == ccexgo.OrderSideBuy {
				priceChange = decimal.NewFromFloat(swapMBookTicker.Ask1Price).Sub(myPlaceOrder.Price)
				replaceTickNum = cfg.PreTakerReplaceCloseTickNum
			}
			var (
				needReplace bool
				needCancel  bool
			)
			if myPlaceOrder.Status == ccexgo.OrderStatusCancel {
				needReplace = true
			} else if !priceChange.LessThanOrEqual(decimal.NewFromInt(int64(replaceTickNum)).Mul(mSwapSymbol.PricePrecision())) { // 重下
				needCancel = true
				needReplace = true
			}

			if !needReplace {
				continue
			}

			level.Info(logger).Log("message", "open book ticker cancel order", "order_price", myPlaceOrder.Price, "bidM_price", swapMBookTicker.Bid1Price, "book_bid", makerSwapDepth.Bids[0].Price,
				"price_change", priceChange, "askM_price", swapMBookTicker.Ask1Price, "book_ask", makerSwapDepth.Asks[0].Price,
				"swap_bt_id", swapMBookTicker.ID, "needCancel", needCancel, "needReplace", needReplace, "mode", mode)

			if needCancel {
				o, err = makerSwapC.CancelOrder(ctx, myPlaceOrder)
				if err != nil {
					errType := makerSwapC.HandError(err)
					sg.PosList.UpdateBannedTime(errType, false)
					sg.PosList.CheckErrShareBan(errType, makerSwapC.GetExchangeName())
					level.Info(logger).Log("message", "open cancel order failed", "err", err.Error())
					err = fmt.Errorf("%s %s[%s]撤单或查单失败, 错误原因:%s, 需要人工处理", makerSwapName, mSwapSymbol, myPlaceOrder.ID.String(), errType.String())
					go message.SendP3Important(context.Background(), message.NewCommonMsgWithAt(fmt.Sprintf("%s %s部分开仓失败", sg.swapS2, makerSwapName), err.Error()))
					goto end
				}

				level.Info(logger).Log("message", "open book ticker cancel order success", "order_id", myPlaceOrder.ID.String(), "mode", mode)
				if !o.Filled.IsZero() {
					newFilled := o.Filled.Sub(myPlaceOrder.Filled)
					if newFilled.IsPositive() {
						newFee := o.Fee.Sub(myPlaceOrder.Fee)
						swapOrderM.AvgPrice = swapOrderM.Filled.Mul(swapOrderM.AvgPrice).Add(newFilled.Mul(o.AvgPrice)).Div(swapOrderM.Filled.Add(newFilled))
						swapOrderM.Filled = swapOrderM.Filled.Add(newFilled)
						swapOrderM.Fee = swapOrderM.Fee.Add(newFee.Neg()) //撤单主动查出来的是总手续费
						swapOrderM.FeeCurrency = o.FeeCurrency
						swapOrderM.Status = o.Status
						swapOrderM.IsMaker = o.IsMaker
						// if !strings.Contains(makerSwapC.GetExchangeName(), exchange.Binance) && swapOrderM.Created.IsZero() { // 币安合约ws推送没有创建时间
						// 	swapOrderM.Created = o.Created
						// }
					}
					swapOrderM.Updated = o.Updated

					myPlaceOrder.Status = o.Status
					myPlaceOrder.Filled = o.Filled
					myPlaceOrder.Fee = o.Fee

					// 生成记录
					// common.CreatOrderDoneField(ctx, logger, bestPrice, swap2.Symbol.PricePrecision(), orderResults[i], &multiDonePrice, &multiDoneAmount, &multiDistance, &multiDoneTime)
				}
			}

			if swapOrderM.Filled.GreaterThanOrEqual(size) {
				goto end
			}

			// 重新挂maker
			gotoEnd := false
			for !gotoEnd {
				// 计算是重挂还是直接taker
				remainAmount := size.Sub(swapOrderM.Filled)
				minOrderAmount := decimal.NewFromFloat(mSwapActsSymbol.MinimumOrderAmount)
				if makerSwapC.GetExchangeName() == exchange.Okex5 || makerSwapC.GetExchangeName() == exchange.GateIO {
					minOrderAmount = minOrderAmount.Mul(decimal.NewFromFloat(mSwapActsSymbol.ContractValue))
				}
				if remainAmount.LessThan(minOrderAmount) {
					gotoEnd = true
					break
				}

				swapMBookTicker = swapC2.BookTicker()
				if mode == rate.Swap1MSwap2T {
					swapMBookTicker = swapC1.BookTicker()
				}

				newSwapMakerPrice := decimal.NewFromFloat(swapMBookTicker.Bid1Price).Add(mSwapSymbol.PricePrecision().Mul(decimal.NewFromInt(int64(cfg.PreTakerOpenTickNum))))
				nowSr := newSwapMakerPrice.Div(swapOrderT.AvgPrice)
				if mode == rate.Swap1MSwap2T {
					nowSr = swapOrderT.AvgPrice.Div(newSwapMakerPrice)
				}
				needTaker := nowSr.Sub(one).LessThan(decimal.NewFromFloat(srOpenInit - cfg.StopLossSrDelta))
				if makerSide == ccexgo.OrderSideBuy {
					newSwapMakerPrice = decimal.NewFromFloat(swapMBookTicker.Ask1Price).Sub(mSwapSymbol.PricePrecision().Mul(decimal.NewFromInt(int64(cfg.PreTakerCloseTickNum))))
					nowSr = newSwapMakerPrice.Div(swapOrderT.AvgPrice)
					if mode == rate.Swap1MSwap2T {
						nowSr = swapOrderT.AvgPrice.Div(newSwapMakerPrice)
					}
					needTaker = nowSr.Sub(one).GreaterThan(decimal.NewFromFloat(srOpenInit + cfg.StopLossSrDelta))
				}

				level.Info(logger).Log("message", "open replace taker or maker order", "newSwapMakerPrice", newSwapMakerPrice, "Bid1Price", swapMBookTicker.Bid1Price, "book_bid", makerSwapDepth.Bids[0].Price,
					"ask1_price", swapMBookTicker.Ask1Price, "book_ask", makerSwapDepth.Asks[0].Price, "remainAmount", remainAmount, "needTaker", needTaker, "mode", mode)
				if needTaker { // 直接taker对冲
					// swapRestStart := time.Now()
					o, err = makerSwapC.MarketOrder(ctx, makerSide, remainAmount, decimal.Zero, nil)
					// swapRestEnd := time.Now()
					if err == nil {
						swapOrderM.ID = o.ID
						swapOrderM.IDs = append(swapOrderM.IDs, o.ID.String())
						swapOrderM.AvgPrice = swapOrderM.Filled.Mul(swapOrderM.AvgPrice).Add(o.Filled.Mul(o.AvgPrice)).Div(swapOrderM.Filled.Add(o.Filled))
						swapOrderM.Filled = swapOrderM.Filled.Add(o.Filled)
						if swapOrderM.FeeCurrency == `` {
							swapOrderM.FeeCurrency = o.FeeCurrency
						} else {
							swapOrderM.FeeCurrency += `_` + o.FeeCurrency
						}
						swapOrderM.Status = o.Status
						swapOrderM.IsMaker = o.IsMaker
						// if !strings.Contains(sg.exchange, exchange.Binance) { // 币安合约ws推送没有创建时间
						// 	swapOrder.Created = o.Created
						// }
						swapOrderM.Updated = o.Updated

						fee := o.Fee
						if strings.Contains(makerSwapC.GetExchangeName(), exchange.Binance) && o.FeeCurrency == `BNB` && mSwapSymbol.String() != `BNBUSDT` {
							bnbEqualFee := o.Filled.Mul(o.AvgPrice).Mul(makerSwapC.FeeRate().OpenTaker.Mul(decimal.NewFromFloat(0.9))).Neg()
							fee = bnbEqualFee
						}
						swapOrderM.Fee = swapOrderM.Fee.Add(fee)
					} else {
						errType := makerSwapC.HandError(err)
						sg.PosList.UpdateBannedTime(errType, false)
						sg.PosList.CheckErrShareBan(errType, makerSwapC.GetExchangeName())
						level.Warn(logger).Log("message", "open replace taker order failed", "err", err.Error())
						err = fmt.Errorf("preTaker maker方进行taker对冲失败, 错误原因: %s", errType.String())
					}

					gotoEnd = true
					break
				}

				openTradeOrderVolumes = []float64{remainAmount.InexactFloat64()}
				_, o, errMsg, err = sg.PosList.OpenSwapMaker(ctx, makerSwapC, srOpen, openTradeOrderVolumes[0], openTradeOrderTickNums[0], makerSwapDepth, nil, makerSide, cfg.MakerType, true)
				if err != nil { // 第一次下单失败,直接退出开仓，后面满足条件会再进入开仓
					errType := makerSwapC.HandError(err)
					if errType.Code == exchange.NewOrderRejected {
						continue
					}
					level.Info(logger).Log("message", "open order failed", "err", err.Error())
					if errMsg != "" {
						level.Info(logger).Log("message", "开仓订单创建失败", "err", errMsg)
						errInfo += errMsg + "\n"
					}
					if strings.Contains(errMsg, "自动处理") {
						errInfo = fmt.Sprintf("【系统自动处理】%s挂单失败, %s敞口将自动处理, %s", makerSwapName, takerSwapName, errInfo)
						msg := message.NewCommonMsg(fmt.Sprintf("%s %s再次开仓挂单失败", sg.swapS2, makerSwapName), errInfo)
						go message.Send(context.Background(), msg)
						err = fmt.Errorf("%s", errInfo)
					} else {
						errInfo = fmt.Sprintf("【人工处理】%s挂单失败, %s敞口将自动处理, %s出现未知错误, 请人工处理\n%s", makerSwapName, takerSwapName, makerSwapName, errInfo)
						if swapOrderM.Filled.IsPositive() {
							msg := message.NewCommonMsgWithAt(fmt.Sprintf("%s %s再次开仓挂单失败", sg.swapS2, makerSwapName), errInfo)
							go message.SendP3Important(context.Background(), msg)
						} else {
							err = fmt.Errorf("%s", errInfo)
						}
					}

					// return nil, fmt.Errorf("%+v", errInfo)
					gotoEnd = true
				} else {
					break
				}
			}
			if gotoEnd {
				goto end
			}

			myPlaceOrder = o
			if o.ID.String() != `` {
				// 生成记录
				multiPrice = append(multiPrice, o.Price.String())
				multiAmount = append(multiAmount, o.Amount.String())
			}
			swapOrderM.ID = myPlaceOrder.ID
			swapOrderM.IDs = append(swapOrderM.IDs, myPlaceOrder.ID.String())
			// 考虑下单即成交的情况
			if o.Status == ccexgo.OrderStatusDone {
				newFee := o.Fee
				swapOrderM.Fee = swapOrderM.Fee.Add(newFee.Neg())
				swapOrderM.AvgPrice = swapOrderM.Filled.Mul(swapOrderM.AvgPrice).Add(o.Filled.Mul(o.AvgPrice)).Div(swapOrderM.Filled.Add(o.Filled))
				swapOrderM.Filled = swapOrderM.Filled.Add(o.Filled)
				if o.FeeCurrency != `` && swapOrderM.FeeCurrency == `` {
					swapOrderM.FeeCurrency = o.FeeCurrency
				}
				swapOrderM.IsMaker = o.IsMaker
				swapOrderM.Status = o.Status
				swapOrderM.Updated = o.Updated
			}

		case o := <-makerSwapC.MakerOrder(swapOrderM.ID.String()): // 判断是否完全成交或取消
			if o == nil { // channel超时关闭了
				continue
			}
			if o.ID != myPlaceOrder.ID {
				level.Warn(logger).Log("message", "skip unmatched order", "want", swapOrderM.ID.String(), "got", o.ID.String(), "amount", o.Amount, "fee", o.Fee, "filled", o.Filled)
				continue
			}
			level.Info(logger).Log("message", "get ws order notify", "id", o.ID.String(), "amount", o.Amount, "filled", o.Filled, "fee", o.Fee, "avg_price", o.AvgPrice, "status", o.Status)
			if myPlaceOrder.Status == ccexgo.OrderStatusDone || myPlaceOrder.Status == ccexgo.OrderStatusCancel {
				continue
			}
			if o.Status == ccexgo.OrderStatusDone || o.Status == ccexgo.OrderStatusCancel {
				if o.Filled.IsZero() && o.Status == ccexgo.OrderStatusCancel { // 没有成交
					myPlaceOrder.Status = ccexgo.OrderStatusCancel
					continue
				}
				newFilled := o.Filled.Sub(myPlaceOrder.Filled)
				if newFilled.IsPositive() {
					newFee := o.Fee // bybit5总成交fee，其余逐笔成交fee
					swapOrderM.Fee = swapOrderM.Fee.Add(newFee.Neg())
					// !剩余的挂单撤单,多挂单这里会有问题
					swapOrderM.AvgPrice = swapOrderM.Filled.Mul(swapOrderM.AvgPrice).Add(o.Filled.Mul(o.AvgPrice)).Div(swapOrderM.Filled.Add(o.Filled))
					swapOrderM.Filled = swapOrderM.Filled.Add(o.Filled)
					if o.FeeCurrency != `` {
						swapOrderM.FeeCurrency = o.FeeCurrency
					}
					swapOrderM.IsMaker = o.IsMaker
					// if !strings.Contains(makerSwapC.GetExchangeName(), exchange.Binance) && o.Created.IsZero() {
					// 	swapOrderM.Created = o.Created
					// }
					myPlaceOrder.Fee = myPlaceOrder.Fee.Add(newFee)
				}

				swapOrderM.Status = o.Status
				swapOrderM.Updated = o.Updated

				myPlaceOrder.Status = o.Status
				myPlaceOrder.Filled = o.Filled
				myPlaceOrder.Updated = o.Updated
				// 添加记录
				// common.CreatOrderDoneField(ctx, logger, bestPrice, swap2.Symbol.PricePrecision(), o, &multiDonePrice, &multiDoneAmount, &multiDistance, &multiDoneTime)
				// var reaminingOrder []*exchange.CfOrder
				// level.Info(logger).Log("message", "open done cancel order", "order_id", myPlaceOrder.ID.String())
				// if o.ID == myPlaceOrder.ID {
				// 	continue
				// }
				// reaminingOrder = append(reaminingOrder, myPlaceOrder)
				// reaminingOrder, err = makerSwapC.CancelOrders(ctx, reaminingOrder)
				// if err != nil {
				// 	level.Info(logger).Log("message", "open cancel order failed", "err", err.Error())
				// 	go message.SendImportant(context.Background(), message.NewCommonMsgWithAt(fmt.Sprintf("%s %s部分开仓失败", sg.swapS2, makerSwapName), err.Error()))
				// }

				// for i := range reaminingOrder {
				// 	level.Info(logger).Log("message", "open done cancel order", "order_id", reaminingOrder[i].ID.String())
				// 	if !float.Equal(reaminingOrder[i].Filled.InexactFloat64(), .0) {
				// 		swapOrderM.AvgPrice = swapOrderM.Filled.Mul(swapOrderM.AvgPrice).Add(reaminingOrder[i].Filled.Mul(reaminingOrder[i].AvgPrice)).Div(swapOrderM.Filled.Add(reaminingOrder[i].Filled))
				// 		swapOrderM.Filled = swapOrderM.Filled.Add(reaminingOrder[i].Filled)
				// 		swapOrderM.Fee = swapOrderM.Fee.Add(reaminingOrder[i].Fee.Neg()) //撤单主动查出来的是总手续费
				// 		swapOrderM.FeeCurrency = reaminingOrder[i].FeeCurrency
				// 		swapOrderM.Status = reaminingOrder[i].Status
				// 		swapOrderM.IsMaker = reaminingOrder[i].IsMaker

				// 		// 生成记录
				// 		// common.CreatOrderDoneField(ctx, logger, bestPrice, swap2.Symbol.PricePrecision(), reaminingOrder[i], &multiDonePrice, &multiDoneAmount, &multiDistance, &multiDoneTime)
				// 	}
				// }
				// 可能被交易所挂单即撤单
				if swapOrderM.Filled.GreaterThanOrEqual(size) {
					goto end
				}
			} else {
				if makerSwapC.GetExchangeName() != exchange.Bybit5 {
					swapOrderM.Fee = swapOrderM.Fee.Add(o.Fee.Neg())
					myPlaceOrder.Fee = myPlaceOrder.Fee.Add(o.Fee)
				}
				swapOrderM.FeeCurrency = o.FeeCurrency
			}

		}
	}
end:
	//从成交到撤单与查单完全完成的延迟
	finishWork := func() (*position.OrderResult, error) {
		if !swapOrderM.Filled.IsPositive() {
			sg.PosList.AddSwapExposure(ctx, swapOrderT.Filled, takerSwapName)
			level.Warn(logger).Log("message", "preTakerOpenSwapAndHedge", "swapName", takerSwapName)
			if err != nil {
				return nil, err
			}
			return nil, nil
		}

		// 开仓张数
		realOpenVolume := swapOrderM.Filled
		makerInfo := gocommon.NewJsonObject()
		makerInfo["srOpenInit"] = srOpenInit
		makerInfo["swapIDs"] = strings.Join(swapOrderM.IDs, `_`)
		makerInfo["realOpenVolume"] = realOpenVolume.String()
		makerInfo["srOpenDone"] = sg.queue.GetSrOpen(acts.CcexSide2Acts(makerSide), swapOrderM.AvgPrice.InexactFloat64(), mode)
		makerInfo["multiPrice"] = strings.Join(multiPrice, "_")
		makerInfo["multiAmount"] = strings.Join(multiAmount, "_")
		op.SetMakerInfo(makerInfo)

		level.Info(logger).Log("message", "pre taker open finished", "mode", mode)
		var swap1Order, swap2Order *exchange.CfOrder
		if mode == rate.Swap1MSwap2T {
			swap1Order = swapOrderM
			swap2Order = swapOrderT
		} else {
			swap1Order = swapOrderT
			swap2Order = swapOrderM
		}
		op, err = sg.PosList.PreTakerOpenFinished(ctx, swapC1, swapC2, srOpen, swap1Order, swap2Order, swap1, swap2, op, mode)
		if err != nil {
			return nil, err
		}
		return op, nil
	}
	return finishWork()
}

func (sg *Strategy) preTakerCloseSwapAndHedge(ctx context.Context, swapC1, swapC2 exchange.SwapClient,
	srClose float64, closeTradeVolume float64, swap1, swap2 *exchange.Depth, side1 ccexgo.OrderSide, cfg *config.Config, mode rate.RunMode) (*position.OrderResult, error) {
	takerSwapC := swapC1
	makerSwapC := swapC2
	takerSide := side1
	makerSide := reverseSide(side1)
	makerSwapDepth := swap2
	mSwapActsSymbol := sg.swap2ActsSymbol
	tSwapSymbol := sg.swapS1
	mSwapSymbol := sg.swapS2
	takerSwapName := "swap1"
	makerSwapName := "swap2"
	if mode == rate.Swap1MSwap2T {
		takerSwapC = swapC2
		makerSwapC = swapC1
		takerSide = reverseSide(side1)
		makerSide = side1
		makerSwapDepth = swap1
		mSwapActsSymbol = sg.swap1ActsSymbol
		tSwapSymbol = sg.swapS2
		mSwapSymbol = sg.swapS1
		takerSwapName = "swap2"
		makerSwapName = "swap1"
	}

	op, swapOrderT, err := sg.PosList.CloseSwapTakerOrder(ctx, cfg, takerSwapC, srClose, closeTradeVolume, takerSide, mode)
	if err != nil {
		return nil, err
	}

	// 币安taker可能是bnb抵扣费, 要计算出对应的usdt费
	var bnbEqualFee decimal.Decimal
	if strings.Contains(takerSwapC.GetExchangeName(), exchange.Binance) && swapOrderT.FeeCurrency == `BNB` && tSwapSymbol.String() != `BNBUSDT` {
		bnbEqualFee = swapOrderT.Filled.Mul(swapOrderT.AvgPrice).Mul(takerSwapC.FeeRate().OpenTaker.Mul(decimal.NewFromFloat(0.9))).Neg()
		swapOrderT.Fee = bnbEqualFee
	}

	one := decimal.NewFromInt(1)
	size := swapOrderT.Filled

	var (
		errInfo string
		// myPlaceOrder []*exchange.CfOrder
		myPlaceOrder *exchange.CfOrder
		// multiDonePrice []string
		//与bestPrice的距离
		// multiDistance []string
		// 挂单数量
		multiAmount []string
		// 实际成交数量
		// multiDoneAmount []string
		// 实际成交实际
		// multiDoneTime []string
		// 挂单价格
		multiPrice   []string
		srCloseInit  float64
		swapOrderM   *exchange.CfOrder
		frozenAmount decimal.Decimal
	)

	tradeOrderVolumes := []float64{size.InexactFloat64()}
	tradeOrderTickNums := []float64{float64(cfg.PreTakerCloseTickNum)}
	if side1 == ccexgo.OrderSideCloseShort {
		tradeOrderTickNums[0] = float64(cfg.PreTakerOpenTickNum)
	}
	logger := log.With(sg.logger, "id", op.GetOpID())

	// 20分钟超时保护，防止协程永久卡住
	maxDurationTimer := time.NewTimer(20 * time.Minute)
	defer maxDurationTimer.Stop()

	// 撤单和清理的通用逻辑（声明在早期，避免 goto 跳过变量声明）
	var cancelAndExit func()

	_, o, errMsg, err := sg.PosList.CloseSwapMaker(ctx, makerSwapC, srClose, tradeOrderVolumes[0], tradeOrderTickNums[0], makerSwapDepth, nil, makerSide, cfg.MakerType, true, false)
	if err != nil { // 第一次下单失败,直接退出开仓，后面满足条件会再进入开仓
		level.Info(logger).Log("message", "first close order failed", "err", err.Error())

		if strings.Contains(errMsg, "订单会立即成交") { // 直接去taker
			swapOrderM, err = makerSwapC.MarketOrder(ctx, makerSide, size, decimal.Zero, nil)
			if err == nil {
				frozenAmount = swapOrderM.Filled
				swapOrderM.IDs = []string{swapOrderM.ID.String()}
				fee := swapOrderM.Fee
				if strings.Contains(makerSwapC.GetExchangeName(), exchange.Binance) && swapOrderM.FeeCurrency == `BNB` && mSwapSymbol.String() != `BNBUSDT` {
					bnbEqualFee := swapOrderM.Filled.Mul(swapOrderM.AvgPrice).Mul(makerSwapC.FeeRate().OpenTaker.Mul(decimal.NewFromFloat(0.9))).Neg()
					fee = bnbEqualFee
				}
				swapOrderM.Fee = fee
				goto end
			} else {
				errType := makerSwapC.HandError(err)
				sg.PosList.UpdateBannedTime(errType, false)
				sg.PosList.CheckErrShareBan(errType, makerSwapC.GetExchangeName())
				errInfo += fmt.Sprintf("%s[%s] taker下单失败, 错误原因:%s\n", makerSwapName, mSwapSymbol, errType.String())
			}
		}

		if errMsg != "" {
			level.Info(logger).Log("message", "平仓订单创建失败", "err", errMsg)
			errInfo += errMsg + "\n"
		}
		if strings.Contains(errMsg, "自动处理") {
			errInfo = fmt.Sprintf("【系统自动处理】%s挂单失败, %s敞口将自动处理, %s", makerSwapName, takerSwapName, errInfo)
		} else {
			errInfo = fmt.Sprintf("【人工处理】%s挂单失败, %s敞口将自动处理, %s出现未知错误, 请人工处理\n%s", makerSwapName, takerSwapName, makerSwapName, errInfo)
		}

		sg.PosList.AddSwapExposure(ctx, swapOrderT.Filled.Neg(), takerSwapName)
		return nil, fmt.Errorf("%+v", errInfo)
	}

	if o.Status >= ccexgo.OrderStatusDone {
		o.Status = ccexgo.OrderStatusOpen
		o.Filled = decimal.Zero
		o.AvgPrice = decimal.Zero
		o.Fee = decimal.Zero
		o.FeeCurrency = ``
		o.IsMaker = false
	}
	if o.ID.String() != `` {
		myPlaceOrder = o
		// 生成记录
		multiPrice = append(multiPrice, o.Price.String())
		multiAmount = append(multiAmount, o.Amount.String())
	}
	frozenAmount = o.Filled

	// TODO 没有一个挂单成功，多挂单这里会有问题，其余订单不一定是挂失败了，要具体判断错误
	// if len(orderResults) == 0 {
	// 	return nil, fmt.Errorf("%+v", errInfo)
	// } else {
	// 	if errInfo != "" { // TODO 先发个告警吧
	// 		go message.SendImportant(context.Background(), message.NewCommonMsgWithAt(fmt.Sprintf("%s 合约部分开仓挂单失败", sg.swapS), errInfo))
	// 	}
	// }
	srCloseInit = myPlaceOrder.Price.Div(swapOrderT.AvgPrice).Sub(one).InexactFloat64()
	if mode == rate.Swap1MSwap2T {
		srCloseInit = swapOrderT.AvgPrice.Div(myPlaceOrder.Price).Sub(one).InexactFloat64()
	}
	swapOrderM = &exchange.CfOrder{
		IDs: []string{myPlaceOrder.ID.String()},
		Order: ccexgo.Order{
			ID:       myPlaceOrder.ID,
			Side:     myPlaceOrder.Side,
			AvgPrice: decimal.NewFromFloat(0),
			Filled:   decimal.NewFromFloat(0),
			Created:  time.Now(),
		},
	}
	if !myPlaceOrder.Created.IsZero() {
		swapOrderM.Created = myPlaceOrder.Created
	}

	//!注意合约订单手续费需取反变成负数
	// 实现撤单和清理的通用逻辑
	cancelAndExit = func() {
		// 撤单,查单,去对冲
		// TODO:先实现顺序撤单
		o, err = makerSwapC.CancelOrder(ctx, myPlaceOrder)
		if err != nil {
			errType := makerSwapC.HandError(err)
			sg.PosList.UpdateBannedTime(errType, false)
			sg.PosList.CheckErrShareBan(errType, makerSwapC.GetExchangeName())
			level.Info(logger).Log("message", "close cancel order failed", "err", err.Error())
			err = fmt.Errorf("%s %s[%s]撤单或查单失败, 错误原因:%s, 需要人工处理", makerSwapName, mSwapSymbol, myPlaceOrder.ID.String(), errType.String())
			go message.SendP3Important(context.Background(), message.NewCommonMsgWithAt(fmt.Sprintf("%s %s部分平仓失败", sg.swapS2, makerSwapName), err.Error()))
			return
		}

		level.Info(logger).Log("message", "close checkCancel cancel order success", "order_id", myPlaceOrder.ID.String())
		if !o.Filled.IsZero() {
			newFilled := o.Filled.Sub(myPlaceOrder.Filled)
			if newFilled.IsPositive() {
				newFee := o.Fee.Sub(myPlaceOrder.Fee)
				swapOrderM.AvgPrice = swapOrderM.Filled.Mul(swapOrderM.AvgPrice).Add(newFilled.Mul(o.AvgPrice)).Div(swapOrderM.Filled.Add(newFilled))
				swapOrderM.Filled = swapOrderM.Filled.Add(newFilled)
				swapOrderM.Fee = swapOrderM.Fee.Add(newFee.Neg()) //撤单主动查出来的是总手续费
				swapOrderM.FeeCurrency = o.FeeCurrency
				swapOrderM.Status = o.Status
				swapOrderM.IsMaker = o.IsMaker
				// if !strings.Contains(makerSwapC.GetExchangeName(), exchange.Binance) {
				// 	swapOrderM.Created = o.Created
				// }
				// 生成记录
				// common.CreatOrderDoneField(ctx, logger, bestPrice, swap2.Symbol.PricePrecision(), orderResults[i], &multiDonePrice, &multiDoneAmount, &multiDistance, &multiDoneTime)
			}
			swapOrderM.Updated = o.Updated

			myPlaceOrder.Status = o.Status
			myPlaceOrder.Filled = o.Filled
			myPlaceOrder.Fee = o.Fee
		}
		if swapOrderM.Filled.LessThan(size) { // taker差值
			remainAmount := size.Sub(swapOrderM.Filled)
			if remainAmount.GreaterThanOrEqual(decimal.NewFromFloat(mSwapActsSymbol.MinimumOrderAmount)) {
				// swapRestStart := time.Now()
				o, err = makerSwapC.MarketOrder(ctx, makerSide, remainAmount, decimal.Zero, nil)
				// swapRestEnd := time.Now()
				if err == nil {
					swapOrderM.ID = o.ID
					swapOrderM.IDs = append(swapOrderM.IDs, o.ID.String())
					swapOrderM.AvgPrice = swapOrderM.Filled.Mul(swapOrderM.AvgPrice).Add(o.Filled.Mul(o.AvgPrice)).Div(swapOrderM.Filled.Add(o.Filled))
					swapOrderM.Filled = swapOrderM.Filled.Add(o.Filled)
					if swapOrderM.FeeCurrency == `` {
						swapOrderM.FeeCurrency = o.FeeCurrency
					} else {
						swapOrderM.FeeCurrency += `_` + o.FeeCurrency
					}
					swapOrderM.Status = o.Status
					swapOrderM.IsMaker = o.IsMaker
					// if !strings.Contains(sg.exchange, exchange.Binance) { // 币安合约ws推送没有创建时间
					// 	swapOrder.Created = o.Created
					// }
					swapOrderM.Updated = o.Updated

					fee := o.Fee
					if strings.Contains(makerSwapC.GetExchangeName(), exchange.Binance) && o.FeeCurrency == `BNB` && mSwapSymbol.String() != `BNBUSDT` {
						bnbEqualFee := o.Filled.Mul(o.AvgPrice).Mul(makerSwapC.FeeRate().OpenTaker.Mul(decimal.NewFromFloat(0.9))).Neg()
						fee = bnbEqualFee
					}
					swapOrderM.Fee = swapOrderM.Fee.Add(fee)
				} else {
					errType := makerSwapC.HandError(err)
					sg.PosList.UpdateBannedTime(errType, false)
					sg.PosList.CheckErrShareBan(errType, makerSwapC.GetExchangeName())
					level.Warn(logger).Log("message", "close exit cancel order failed", "err", errType.String())
					err = fmt.Errorf("preTaker maker方进行taker对冲失败, 错误原因: %s", errType.String())
				}
			}
		}
	}

	for {
		select {
		case <-maxDurationTimer.C:
			// 超过20分钟，强制终止流程
			level.Warn(logger).Log("message", "PT close strategy timeout 20 minutes, force exit", "order_id", myPlaceOrder.ID.String())
			go message.SendP3Important(ctx, message.NewCommonMsgWithAt(fmt.Sprintf("%s PT平仓超时", sg.swapS1), fmt.Sprintf("操作ID: %s, 订单ID: %s, 运行超过20分钟强制退出", op.GetOpID(), myPlaceOrder.ID.String())))
			cancelAndExit()
			goto end
		case <-ctx.Done():
			level.Info(logger).Log("message", "close exit now")
			cancelAndExit()
			goto end
		case <-sg.bookTickerEvent.Done(): // bookTicker变动
			sg.bookTickerEvent.Unset()

			var swapMBookTicker *exchange.BookTicker
			if mode == rate.Swap1MSwap2T {
				swapMBookTicker = swapC1.BookTicker()
			} else if mode == rate.Swap1TSwap2M {
				swapMBookTicker = swapC2.BookTicker()
			}
			if /*spotBookTicker == nil ||*/ swapMBookTicker == nil {
				level.Warn(logger).Log("message", "close book ticker is nil", "swapMBookTicker", fmt.Sprintf("%+v", swapMBookTicker), "mode", mode)
				continue
			}
			/*
				ahAsk := cf_factor.CalculateAhAskRatio(spotBookTicker, swapBookTicker, cv, sg.inverse)

				sr := sg.queue.CancelOpen(orders[0].Price.InexactFloat64(), rate.SpotTSwapM)
				if ahAsk <= cfg.Factor.TMAhOpenThreshold && !sr.Ok {
					continue
				}
			*/

			// TODO 这里需要修改
			priceChange := decimal.NewFromFloat(swapMBookTicker.Ask1Price).Sub(myPlaceOrder.Price)
			replaceTickNum := cfg.PreTakerReplaceCloseTickNum
			if makerSide == ccexgo.OrderSideCloseLong {
				priceChange = myPlaceOrder.Price.Sub(decimal.NewFromFloat(swapMBookTicker.Bid1Price))
				replaceTickNum = cfg.PreTakerReplaceOpenTickNum
			}
			var (
				needReplace bool
				needCancel  bool
			)
			if myPlaceOrder.Status == ccexgo.OrderStatusCancel {
				needReplace = true
			} else if !priceChange.LessThanOrEqual(decimal.NewFromInt(int64(replaceTickNum)).Mul(mSwapSymbol.PricePrecision())) { // 重下
				needCancel = true
				needReplace = true
			}

			if !needReplace {
				continue
			}

			level.Info(logger).Log("message", "close book ticker cancel order", "order_price", myPlaceOrder.Price, "ask_price", swapMBookTicker.Ask1Price,
				"price_change", priceChange, "bid1_price", swapMBookTicker.Bid1Price, "book_bid", makerSwapDepth.Bids[0].Price,
				"book_ask", makerSwapDepth.Asks[0].Price, "swap_bt_id", swapMBookTicker.ID, "sg.swapS.PricePrecision", mSwapSymbol.PricePrecision(),
				"needCancel", needCancel, "needReplace", needReplace, "mode", mode)
			if needCancel {
				o, err = makerSwapC.CancelOrder(ctx, myPlaceOrder)
				if err != nil {
					errType := makerSwapC.HandError(err)
					sg.PosList.UpdateBannedTime(errType, false)
					sg.PosList.CheckErrShareBan(errType, makerSwapC.GetExchangeName())
					level.Info(logger).Log("message", "close cancel order failed", "err", err.Error())
					err = fmt.Errorf("%s %s[%s]撤单或查单失败, 错误原因:%s, 需要人工处理", makerSwapName, mSwapSymbol, myPlaceOrder.ID.String(), errType.String())
					go message.SendP3Important(context.Background(), message.NewCommonMsgWithAt(fmt.Sprintf("%s %s部分平仓失败", sg.swapS2, makerSwapName), err.Error()))
					goto end
				}

				level.Info(logger).Log("message", "close book ticker cancel order success", "order_id", myPlaceOrder.ID.String(), "mode", mode)
				if !o.Filled.IsZero() {
					newFilled := o.Filled.Sub(myPlaceOrder.Filled)
					if newFilled.IsPositive() {
						newFee := o.Fee.Sub(myPlaceOrder.Fee)
						swapOrderM.AvgPrice = swapOrderM.Filled.Mul(swapOrderM.AvgPrice).Add(newFilled.Mul(o.AvgPrice)).Div(swapOrderM.Filled.Add(newFilled))
						swapOrderM.Filled = swapOrderM.Filled.Add(newFilled)
						swapOrderM.Fee = swapOrderM.Fee.Add(newFee.Neg()) //撤单主动查出来的是总手续费
						swapOrderM.FeeCurrency = o.FeeCurrency
						swapOrderM.Status = o.Status
						swapOrderM.IsMaker = o.IsMaker
						// if !strings.Contains(makerSwapC.GetExchangeName(), exchange.Binance) { // 币安合约ws推送没有创建时间
						// 	swapOrderM.Created = o.Created
						// }
					}
					swapOrderM.Updated = o.Updated

					myPlaceOrder.Status = o.Status
					myPlaceOrder.Filled = o.Filled
					myPlaceOrder.Fee = o.Fee
					// 生成记录
					// common.CreatOrderDoneField(ctx, logger, bestPrice, swap2.Symbol.PricePrecision(), orderResults[i], &multiDonePrice, &multiDoneAmount, &multiDistance, &multiDoneTime)
				}
			}

			if swapOrderM.Filled.GreaterThanOrEqual(size) {
				goto end
			}

			gotoEnd := false
			for !gotoEnd {
				// 计算是重挂还是直接taker
				remainAmount := size.Sub(swapOrderM.Filled)
				minOrderAmount := decimal.NewFromFloat(mSwapActsSymbol.MinimumOrderAmount)
				if makerSwapC.GetExchangeName() == exchange.Okex5 || makerSwapC.GetExchangeName() == exchange.GateIO {
					minOrderAmount = minOrderAmount.Mul(decimal.NewFromFloat(mSwapActsSymbol.ContractValue))
				}
				if remainAmount.LessThan(minOrderAmount) {
					gotoEnd = true
					break
				}
				swapMBookTicker = swapC2.BookTicker()
				if mode == rate.Swap1MSwap2T {
					swapMBookTicker = swapC1.BookTicker()
				}

				newSwapMakerPrice := decimal.NewFromFloat(swapMBookTicker.Ask1Price).Sub(mSwapSymbol.PricePrecision().Mul(decimal.NewFromInt(int64(cfg.PreTakerCloseTickNum))))
				needTaker := newSwapMakerPrice.Div(swapOrderT.AvgPrice).Sub(one).GreaterThan(decimal.NewFromFloat(srCloseInit + cfg.StopLossSrDelta))
				if makerSide == ccexgo.OrderSideCloseLong {
					newSwapMakerPrice = decimal.NewFromFloat(swapMBookTicker.Bid1Price).Add(mSwapSymbol.PricePrecision().Mul(decimal.NewFromInt(int64(cfg.PreTakerOpenTickNum))))
					needTaker = newSwapMakerPrice.Div(swapOrderT.AvgPrice).Sub(one).LessThan(decimal.NewFromFloat(srCloseInit - cfg.StopLossSrDelta))
				}
				level.Info(logger).Log("message", "close replace taker or maker order", "newSwapMakerPrice", newSwapMakerPrice, "Ask1Price", swapMBookTicker.Ask1Price, "book_ask", makerSwapDepth.Asks[0].Price,
					"Bid1Price", swapMBookTicker.Bid1Price, "book_bid", makerSwapDepth.Bids[0].Price, "remainAmount", remainAmount, "srCloseInit", srCloseInit, "needTaker", needTaker, "mode", mode)
				if needTaker { // 直接taker对冲
					// swapRestStart := time.Now()
					o, err = makerSwapC.MarketOrder(ctx, makerSide, remainAmount, decimal.Zero, nil)
					// swapRestEnd := time.Now()
					if err == nil {
						swapOrderM.ID = o.ID
						swapOrderM.IDs = append(swapOrderM.IDs, o.ID.String())
						swapOrderM.AvgPrice = swapOrderM.Filled.Mul(swapOrderM.AvgPrice).Add(o.Filled.Mul(o.AvgPrice)).Div(swapOrderM.Filled.Add(o.Filled))
						swapOrderM.Filled = swapOrderM.Filled.Add(o.Filled)
						if swapOrderM.FeeCurrency == `` {
							swapOrderM.FeeCurrency = o.FeeCurrency
						} else {
							swapOrderM.FeeCurrency += `_` + o.FeeCurrency
						}
						swapOrderM.Status = o.Status
						swapOrderM.IsMaker = o.IsMaker
						// if !strings.Contains(sg.exchange, exchange.Binance) { // 币安合约ws推送没有创建时间
						// 	swapOrder.Created = o.Created
						// }
						swapOrderM.Updated = o.Updated

						fee := o.Fee
						if strings.Contains(makerSwapC.GetExchangeName(), exchange.Binance) && o.FeeCurrency == `BNB` && mSwapSymbol.String() != `BNBUSDT` {
							bnbEqualFee := o.Filled.Mul(o.AvgPrice).Mul(makerSwapC.FeeRate().OpenTaker.Mul(decimal.NewFromFloat(0.9))).Neg()
							fee = bnbEqualFee
						}
						swapOrderM.Fee = swapOrderM.Fee.Add(fee)
					} else {
						errType := makerSwapC.HandError(err)
						sg.PosList.UpdateBannedTime(errType, false)
						sg.PosList.CheckErrShareBan(errType, makerSwapC.GetExchangeName())
						level.Warn(logger).Log("message", "close replace taker order failed", "err", errType.String())
						err = fmt.Errorf("preTaker maker方进行taker对冲失败, 错误原因: %s", errType.String())
					}

					gotoEnd = true
					break
				}

				tradeOrderVolumes = []float64{remainAmount.InexactFloat64()}
				// 重新挂maker
				_, o, errMsg, err = sg.PosList.CloseSwapMaker(ctx, makerSwapC, srClose, tradeOrderVolumes[0], tradeOrderTickNums[0], makerSwapDepth, nil, makerSide, cfg.MakerType, true, false)
				if err != nil {
					errType := makerSwapC.HandError(err)
					if errType.Code == exchange.NewOrderRejected {
						continue
					}
					level.Warn(logger).Log("message", "close order failed", "err", err.Error())
					if errMsg != "" {
						level.Info(logger).Log("message", "平仓订单创建失败", "err", errMsg)
						errInfo += errMsg + "\n"
					}
					if strings.Contains(errMsg, "自动处理") {
						errInfo = fmt.Sprintf("【系统自动处理】%s挂单失败, %s敞口将自动处理, %s", makerSwapName, takerSwapName, errInfo)
						msg := message.NewCommonMsg(fmt.Sprintf("%s %s再次平仓挂单失败", sg.swapS2, makerSwapName), errInfo)
						go message.Send(context.Background(), msg)
						err = fmt.Errorf("%s", errInfo)
					} else {
						errInfo = fmt.Sprintf("【人工处理】%s挂单失败, %s敞口将自动处理, %s出现未知错误, 请人工处理\n%s", makerSwapName, takerSwapName, makerSwapName, errInfo)
						if swapOrderM.Filled.IsPositive() {
							msg := message.NewCommonMsgWithAt(fmt.Sprintf("%s %s再次平仓挂单失败", sg.swapS2, makerSwapName), errInfo)
							go message.SendP3Important(context.Background(), msg)
						} else {
							err = fmt.Errorf("%s", errInfo)
						}
					}

					// return nil, fmt.Errorf("%+v", errInfo)
					gotoEnd = true
				} else {
					break
				}
			}

			if gotoEnd {
				goto end
			}

			myPlaceOrder = o
			if o.ID.String() != `` {
				// 生成记录
				multiPrice = append(multiPrice, o.Price.String())
				multiAmount = append(multiAmount, o.Amount.String())
			}
			swapOrderM.ID = myPlaceOrder.ID
			swapOrderM.IDs = append(swapOrderM.IDs, myPlaceOrder.ID.String())
			// 考虑下单即成交的情况
			if o.Status == ccexgo.OrderStatusDone {
				newFee := o.Fee
				swapOrderM.Fee = swapOrderM.Fee.Add(newFee.Neg())
				swapOrderM.AvgPrice = swapOrderM.Filled.Mul(swapOrderM.AvgPrice).Add(o.Filled.Mul(o.AvgPrice)).Div(swapOrderM.Filled.Add(o.Filled))
				swapOrderM.Filled = swapOrderM.Filled.Add(o.Filled)
				if o.FeeCurrency != `` && swapOrderM.FeeCurrency == `` {
					swapOrderM.FeeCurrency = o.FeeCurrency
				}
				swapOrderM.IsMaker = o.IsMaker
				swapOrderM.Status = o.Status
				swapOrderM.Updated = o.Updated
			}

		case o := <-makerSwapC.MakerOrder(swapOrderM.ID.String()): // 判断是否完全成交或取消
			if o == nil { // channel超时关闭了
				continue
			}
			if o.ID != myPlaceOrder.ID {
				level.Warn(logger).Log("message", "skip unmatched order", "want", swapOrderM.ID.String(), "got", o.ID.String(), "amount", o.Amount, "fee", o.Fee, "filled", o.Filled)
				continue
			}
			level.Info(logger).Log("message", "get ws order notify", "id", o.ID.String(), "amount", o.Amount, "filled", o.Filled, "fee", o.Fee, "avg_price", o.AvgPrice, "status", o.Status)
			if myPlaceOrder.Status == ccexgo.OrderStatusDone || myPlaceOrder.Status == ccexgo.OrderStatusCancel {
				continue
			}

			if o.Status == ccexgo.OrderStatusDone || o.Status == ccexgo.OrderStatusCancel {
				if o.Filled.IsZero() && o.Status == ccexgo.OrderStatusCancel { // 没有成交
					myPlaceOrder.Status = ccexgo.OrderStatusCancel
					continue
				}
				newFilled := o.Filled.Sub(myPlaceOrder.Filled)
				if newFilled.IsPositive() {
					newFee := o.Fee // bybit5总成交fee，其余逐笔成交fee
					swapOrderM.Fee = swapOrderM.Fee.Add(newFee.Neg())
					// !剩余的挂单撤单,多挂单这里会有问题
					swapOrderM.AvgPrice = swapOrderM.Filled.Mul(swapOrderM.AvgPrice).Add(o.Filled.Mul(o.AvgPrice)).Div(swapOrderM.Filled.Add(o.Filled))
					swapOrderM.Filled = swapOrderM.Filled.Add(o.Filled)
					if o.FeeCurrency != `` {
						swapOrderM.FeeCurrency = o.FeeCurrency
					}
					swapOrderM.IsMaker = o.IsMaker
					// if !strings.Contains(makerSwapC.GetExchangeName(), exchange.Binance) && o.Created.IsZero() {
					// 	swapOrderM.Created = o.Created
					// }
					myPlaceOrder.Fee = myPlaceOrder.Fee.Add(newFee)
				}
				swapOrderM.Status = o.Status
				swapOrderM.Updated = o.Updated

				myPlaceOrder.Status = o.Status
				myPlaceOrder.Filled = o.Filled
				myPlaceOrder.Updated = o.Updated
				// 添加记录
				// common.CreatOrderDoneField(ctx, logger, bestPrice, swap2.Symbol.PricePrecision(), o, &multiDonePrice, &multiDoneAmount, &multiDistance, &multiDoneTime)
				// var reaminingOrder []*exchange.CfOrder
				// level.Info(logger).Log("message", "close done cancel order", "order_id", myPlaceOrder.ID.String())
				// for i := range myPlaceOrder {
				// 	if o.ID == myPlaceOrder.ID {
				// 		continue
				// 	}
				// 	reaminingOrder = append(reaminingOrder, myPlaceOrder)
				// }
				// reaminingOrder, err = makerSwapC.CancelOrders(ctx, reaminingOrder)
				// if err != nil {
				// 	level.Info(logger).Log("message", "close cancel order failed", "err", err.Error())
				// 	go message.SendImportant(context.Background(), message.NewCommonMsgWithAt(fmt.Sprintf("%s %s部分平仓失败", sg.swapS2, makerSwapName), err.Error()))
				// }

				// for i := range reaminingOrder {
				// 	level.Info(logger).Log("message", "close done cancel order", "order_id", reaminingOrder[i].ID.String())
				// 	if !float.Equal(reaminingOrder[i].Filled.InexactFloat64(), .0) {
				// 		swapOrderM.AvgPrice = swapOrderM.Filled.Mul(swapOrderM.AvgPrice).Add(reaminingOrder[i].Filled.Mul(reaminingOrder[i].AvgPrice)).Div(swapOrderM.Filled.Add(reaminingOrder[i].Filled))
				// 		swapOrderM.Filled = swapOrderM.Filled.Add(reaminingOrder[i].Filled)
				// 		swapOrderM.Fee = swapOrderM.Fee.Add(reaminingOrder[i].Fee.Neg()) //撤单主动查出来的是总手续费
				// 		swapOrderM.FeeCurrency = reaminingOrder[i].FeeCurrency
				// 		swapOrderM.Status = reaminingOrder[i].Status
				// 		swapOrderM.IsMaker = reaminingOrder[i].IsMaker
				// 		// 生成记录
				// 		// common.CreatOrderDoneField(ctx, logger, bestPrice, swap2.Symbol.PricePrecision(), reaminingOrder[i], &multiDonePrice, &multiDoneAmount, &multiDistance, &multiDoneTime)
				// 	}
				// }

				if swapOrderM.Filled.GreaterThanOrEqual(size) {
					goto end
				}
			} else {
				if makerSwapC.GetExchangeName() != exchange.Bybit5 {
					swapOrderM.Fee = swapOrderM.Fee.Add(o.Fee.Neg())
					myPlaceOrder.Fee = myPlaceOrder.Fee.Add(o.Fee)
				}
				swapOrderM.FeeCurrency = o.FeeCurrency
			}

		}
	}
end:
	//从成交到撤单与查单完全完成的延迟
	finishWork := func() (*position.OrderResult, error) {
		if !swapOrderM.Filled.IsPositive() {
			if frozenAmount.IsPositive() {
				sg.PosList.UnfreezeCloseAmount(frozenAmount)
			}
			sg.PosList.AddSwapExposure(ctx, swapOrderT.Filled.Neg(), takerSwapName)
			level.Warn(logger).Log("message", "preTakerCloseSwapAndHedge", "takerSwapName", takerSwapName)
			if err != nil {
				return nil, err
			}
			return nil, nil
		}

		// 开仓张数
		makerInfo := gocommon.NewJsonObject()
		makerInfo["srCloseInit"] = srCloseInit
		makerInfo["swapIDs"] = strings.Join(swapOrderM.IDs, `_`)
		makerInfo["srCloseDone"] = sg.queue.GetSrClose(acts.CcexSide2Acts(makerSide), swapOrderM.AvgPrice.InexactFloat64(), mode)
		makerInfo["multiPrice"] = strings.Join(multiPrice, "_")
		makerInfo["multiAmount"] = strings.Join(multiAmount, "_")
		op.SetMakerInfo(makerInfo)

		level.Info(logger).Log("message", "pre taker close finished", "mode", mode)
		var swap1Order, swap2Order *exchange.CfOrder
		if mode == rate.Swap1MSwap2T {
			swap1Order = swapOrderM
			swap2Order = swapOrderT
		} else {
			swap1Order = swapOrderT
			swap2Order = swapOrderM
		}
		op, err = sg.PosList.PreTakerCloseFinished(ctx, swapC1, swapC2, srClose, swap1Order, swap2Order, swap1, swap2, op, mode, cfg.TotalLimit, frozenAmount)
		if err != nil {
			return nil, err
		}
		return op, nil
	}
	return finishWork()
}
