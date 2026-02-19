package logic

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"cf_arbitrage/exchange"
	"cf_arbitrage/exchange/acts"
	"cf_arbitrage/logic/config"
	"cf_arbitrage/logic/position"
	"cf_arbitrage/logic/rate"
	"cf_arbitrage/message"
	utilCommon "cf_arbitrage/util/common"

	common "go.common"
	"go.common/float"

	ccexgo "github.com/NadiaSama/ccexgo/exchange"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/shopspring/decimal"
)

func (sg *Strategy) openSwap1AndHedge(ctx context.Context, spotC exchange.Client, swapC1 exchange.SwapClient, swapC2 exchange.SwapClient,
	srOpen float64, openTradeVolume float64, swap1 *exchange.Depth, swap2 *exchange.Depth, side ccexgo.OrderSide, cfg *config.Config, mode rate.RunMode) (op *position.OrderResult, err error) {
	start := time.Now()
	defer func() {
		if e := recover(); e != nil {
			err = utilCommon.RecoverWithLog(sg.logger, "openSwap1AndHedge", start, e)
		}
	}()
	if cfg.PTMode == config.PreTaker {
		if mode == rate.Swap1TSwap2M { // mt tm 的 preTaker 都是要传side1, 实际都是传的maker方的side, 所以这里要反转
			side = reverseSide(side)
		}
		return sg.preTakerOpenSwapAndHedge(ctx, swapC1, swapC2, srOpen, openTradeVolume, swap1, swap2, side, cfg, mode)
	}
	if mode == rate.Swap1MSwap2T {
		return sg.openSwap1HedgeSwap2(ctx, spotC, swapC1, swapC2, srOpen, openTradeVolume, swap1, swap2, side)
	}
	return sg.openSwap2HedgeSwap1(ctx, spotC, swapC1, swapC2, srOpen, openTradeVolume, swap1, swap2, side)
}

func (sg *Strategy) closeSwapAndHedge(ctx context.Context, spotC exchange.Client, swapC1 exchange.SwapClient, swapC2 exchange.SwapClient,
	srClose float64, closeTradeVolume float64, swap1 *exchange.Depth, swap2 *exchange.Depth, side ccexgo.OrderSide, cfg *config.Config, mode rate.RunMode) (op *position.OrderResult, err error) {
	start := time.Now()
	defer func() {
		if e := recover(); e != nil {
			err = utilCommon.RecoverWithLog(sg.logger, "closeSwapAndHedge", start, e)
		}
	}()
	if cfg.PTMode == config.PreTaker {
		if mode == rate.Swap1TSwap2M { // mt tm 的 preTaker 都是要传side1, 实际都是传的maker方的side, 所以这里要反转
			side = reverseSide(side)
		}
		return sg.preTakerCloseSwapAndHedge(ctx, swapC1, swapC2, srClose, closeTradeVolume, swap1, swap2, side, cfg, mode)
	}

	if mode == rate.Swap1MSwap2T {
		return sg.closeSwap1HedgeSwap2(ctx, spotC, swapC1, swapC2, srClose, closeTradeVolume, swap1, swap2, side)
	}
	return sg.closeSwap2HedgeSwap1(ctx, spotC, swapC1, swapC2, srClose, closeTradeVolume, swap1, swap2, side)
}

// swap1开多或开空
func (sg *Strategy) openSwap1HedgeSwap2(ctx context.Context, spotC exchange.Client, swapC1 exchange.SwapClient, swapC2 exchange.SwapClient,
	srOpen float64, openTradeVolume float64, swap1 *exchange.Depth, swap2 *exchange.Depth, side ccexgo.OrderSide) (*position.OrderResult, error) {
	var (
		swap1Order *exchange.CfOrder
		logger     log.Logger // 便于追踪
	)
	config := sg.load.Load()
	op, order, errMsg, err := sg.PosList.OpenSwapMaker(ctx, swapC1, srOpen, openTradeVolume, float64(config.OpenTickNum), swap1, nil, side, config.MakerType, false)
	if err != nil || order.ID.String() == `` || order.Status == ccexgo.OrderStatusFailed || order.Status == ccexgo.OrderStatusCancel { // 第一次下单失败,直接退出开仓，后面满足条件会再进入开仓
		if err == nil {
			return nil, fmt.Errorf("订单异常(取消或失败), 可能是触发了交易所限制, 可联系开发查看\n%+v", order)
		}
		level.Info(sg.logger).Log("message", "first open order failed", "error", err.Error())
		if errMsg != "" {
			return nil, fmt.Errorf("%+v", errMsg)
		}
		return nil, err
	}
	swap1Order = &exchange.CfOrder{
		Order: ccexgo.Order{
			ID:       order.ID,
			Side:     order.Side,
			AvgPrice: decimal.NewFromFloat(0),
			Filled:   decimal.NewFromFloat(0),
			Created:  time.Now(),
		},
	}
	if !order.Created.IsZero() {
		swap1Order.Created = order.Created
	}

	logger = log.With(sg.logger, "id", op.GetOpID())

	// 20分钟超时保护，防止协程永久卡住
	maxDurationTimer := time.NewTimer(20 * time.Minute)
	defer maxDurationTimer.Stop()

	// 撤单和清理的通用逻辑
	cancelAndExit := func() {
		var o *exchange.CfOrder
		o, err = swapC1.CancelOrder(ctx, order) // error
		if err != nil {
			errType := swapC1.HandError(err)
			level.Info(logger).Log("message", "open cancel order failed", "error", err.Error())
			err = fmt.Errorf("%s撤单或查单失败, 错误原因:%s, 需要人工处理", order.ID.String(), errType.String())
			go message.Send(ctx, message.NewOperateFail(fmt.Sprintf("%s swap1部分开仓失败", sg.swapS1), err.Error()))
			return
		}
		order = o
		if !order.Filled.IsZero() {
			swap1Order.AvgPrice = swap1Order.Filled.Mul(swap1Order.AvgPrice).Add(order.Filled.Mul(order.AvgPrice)).Div(swap1Order.Filled.Add(order.Filled))
			swap1Order.Filled = swap1Order.Filled.Add(order.Filled)
			swap1Order.Fee = order.Fee.Neg()                                   //撤单主动查出来的是总手续费，swap取反保持统一
			if !strings.Contains(swapC1.GetExchangeName(), exchange.Binance) { // 币安合约ws推送没有创建时间
				swap1Order.Created = order.Created
			}
			swap1Order.Updated = order.Updated
		}
	}

	for {
		select {
		case <-maxDurationTimer.C:
			// 超过20分钟，强制终止流程
			level.Warn(logger).Log("message", "MT open strategy timeout 20 minutes, force exit", "order_id", order.ID.String())
			go message.SendP3Important(ctx, message.NewCommonMsgWithAt(fmt.Sprintf("%s MT开仓超时", sg.swapS1), fmt.Sprintf("操作ID: %s, 订单ID: %s, 运行超过20分钟强制退出", op.GetOpID(), order.ID.String())))
			cancelAndExit()
			goto OpenHedgeSwap2
		case <-ctx.Done():
			level.Info(logger).Log("message", "open exit now", "order_id", order.ID.String())
			cancelAndExit()
			goto OpenHedgeSwap2
		case <-sg.bookTickerEvent.Done(): // bookTicker变动, 同步检测
			sg.bookTickerEvent.Unset()
			var (
				sr     rate.Sr
				cancel bool
			)
			if side == ccexgo.OrderSideBuy {
				sr = sg.queue.CancelOpen(order.Price.InexactFloat64(), rate.Swap1MSwap2T)
			} else {
				sr = sg.queue.CancelClose(order.Price.InexactFloat64(), rate.Swap1MSwap2T)
			}

			var (
				priceChange float64
				price0      float64
				price1      float64
			)
			mid := sg.queue.GetSwap1Mid()
			swap1BookTicker := swapC1.BookTicker()
			if swap1BookTicker == nil {
				continue
			}
			depth := sg.queue.GetSwap1Depth()
			depth.Asks[0].Price = swap1BookTicker.Ask1Price
			depth.Bids[0].Price = swap1BookTicker.Bid1Price
			depth.Asks[0].Amount = swap1BookTicker.Ask1Amount
			depth.Bids[0].Amount = swap1BookTicker.Bid1Amount

			if sr.Ok {
				cancel = true
			} else {
				pricePrecision := sg.swapS1.PricePrecision().InexactFloat64()
				if side == ccexgo.OrderSideBuy {
					if config.MakerType == 0 {
						priceChange = depth.Asks[0].Price - order.Price.InexactFloat64()
					} else {
						if float.Equal(mid, .0) { // 刚切换配置是0
							continue
						}
						price0 = depth.Bids[0].Price
						price1 = depth.Bids[1].Price
						if float.Equal(depth.Bids[0].Price, order.Price.InexactFloat64()) && float.Equal(depth.Bids[0].Amount, order.Amount.InexactFloat64()) {
							price0 = depth.Bids[1].Price
							price1 = depth.Bids[2].Price
						}
						priceChange = mid - price0
					}
				} else {
					if config.MakerType == 0 {
						priceChange = order.Price.InexactFloat64() - depth.Bids[0].Price
					} else {
						if float.Equal(mid, .0) { // 刚切换配置是0
							continue
						}
						price0 = depth.Asks[0].Price
						price1 = depth.Asks[1].Price
						if float.Equal(depth.Asks[0].Price, order.Price.InexactFloat64()) && float.Equal(depth.Asks[0].Amount, order.Amount.InexactFloat64()) {
							price0 = depth.Asks[1].Price
							price1 = depth.Asks[2].Price
						}
						priceChange = price0 - mid
					}
				}
				if config.MakerType == 0 {
					if math.Round(priceChange/pricePrecision) > float64(config.ReplaceOpenTickNum) {
						cancel = true
					}
				} else {
					if math.Round(priceChange/pricePrecision) > float64(config.SpreadLimitNum1) {
						cancel = true
					}
					if !cancel && config.MakerType == 2 {
						if side == ccexgo.OrderSideBuy { // 买
							if math.Round((price0-price1)/pricePrecision) > float64(config.SpreadLimitNum2) {
								cancel = true
							}
						} else {
							if math.Round((price1-price0)/pricePrecision) > float64(config.SpreadLimitNum2) {
								cancel = true
							}
						}
					}
				}
			}

			if cancel { // 撤单,查单,去对冲
				level.Info(logger).Log("message", "open replace order", "order_id", order.ID.String(), "side", order.Side.String(), "price", order.Price, "maker_type", config.MakerType,
					"price0", price0, "price1", price1, "price_change", priceChange, "ask0", depth.Asks[0].Price, "ask1", depth.Asks[1].Price, "ask2", depth.Asks[2].Price,
					"ask0_amount", depth.Asks[0].Amount, "bid0", depth.Bids[0].Price, "bid1", depth.Bids[1].Price, "bid2", depth.Bids[2].Price, "bid0_amount", depth.Bids[0].Amount,
					"sr_ok", sr.Ok, "sr_spread", sr.Spread)

				var o *exchange.CfOrder
				o, err = swapC1.CancelOrder(ctx, order) // error
				if err != nil {
					errType := swapC1.HandError(err)
					sg.PosList.UpdateBannedTime(errType, false)
					sg.PosList.CheckErrShareBan(errType, swapC1.GetExchangeName())
					level.Info(logger).Log("message", "open cancel order failed", "error", err.Error())
					err = fmt.Errorf("swap1 %s[%s]撤单或查单失败, 错误原因:%s, 需要人工处理", sg.swapS1, order.ID.String(), errType.String())
					go message.Send(ctx, message.NewOperateFail(fmt.Sprintf("%s swap1部分开仓失败", sg.swapS1), err.Error()))
					goto OpenHedgeSwap2
				}
				order = o
				if !float.Equal(order.Filled.InexactFloat64(), .0) {
					swap1Order.AvgPrice = swap1Order.Filled.Mul(swap1Order.AvgPrice).Add(order.Filled.Mul(order.AvgPrice)).Div(swap1Order.Filled.Add(order.Filled))
					swap1Order.Filled = swap1Order.Filled.Add(order.Filled)
					swap1Order.Fee = order.Fee.Neg()                                   //撤单主动查出来的是总手续费，swap取反保持统一
					if !strings.Contains(swapC1.GetExchangeName(), exchange.Binance) { // 币安合约ws推送没有创建时间
						swap1Order.Created = order.Created
					}
					swap1Order.Updated = order.Updated
				}
				goto OpenHedgeSwap2
			}

		case o := <-swapC1.MakerOrder(order.ID.String()): // 判断是否完全成交或取消
			if o == nil { // channel超时关闭了
				continue
			}
			if o.ID != order.ID {
				level.Warn(logger).Log("message", "skip unmatch order", "want", order.ID.String(), "got", o.ID.String(), "amount", o.Amount, "fee", o.Fee, "filled", o.Filled)
				continue
			}
			level.Info(logger).Log("message", "get ws order notify", "id", o.ID.String(), "amount", o.Amount, "filled", o.Filled, "fee", o.Fee, "avg_price", o.AvgPrice, "status", o.Status)
			if o.Status == ccexgo.OrderStatusDone || o.Status == ccexgo.OrderStatusCancel {
				if swap1Order.Filled.Add(o.Filled).IsPositive() {
					swap1Order.AvgPrice = swap1Order.Filled.Mul(swap1Order.AvgPrice).Add(o.Filled.Mul(o.AvgPrice)).Div(swap1Order.Filled.Add(o.Filled))
				}
				swap1Order.Filled = swap1Order.Filled.Add(o.Filled)
				swap1Order.Fee = swap1Order.Fee.Add(o.Fee.Neg()) // 不需要取反，webSockets返回的已经取反了
				if o.FeeCurrency != `` {
					swap1Order.FeeCurrency = o.FeeCurrency
				}
				if !strings.Contains(swapC1.GetExchangeName(), exchange.Binance) { // 币安合约ws推送没有创建时间
					swap1Order.Created = o.Created
				}
				swap1Order.Updated = o.Updated
				goto OpenHedgeSwap2
			}
		}
	}

OpenHedgeSwap2:

	hedgeWithSwap := func() (*position.OrderResult, error) {
		if swap1Order.Filled.IsZero() {
			if err != nil {
				return nil, err
			}
			return nil, nil
		}
		makerInfo := common.NewJsonObject()
		makerInfo["sr_open_done"] = sg.queue.GetSrOpen(acts.CcexSide2Acts(side), swap1Order.AvgPrice.InexactFloat64(), rate.Swap1MSwap2T)
		op.SetMakerInfo(makerInfo)

		level.Info(logger).Log("message", "open hedge swap2", "filled amount", swap1Order.Filled.String(), "avgPrice", swap1Order.AvgPrice.String(), "usdt", swap1Order.Filled.Mul(swap1Order.AvgPrice).String())
		return sg.PosList.OpenHedgeSwap2(ctx, spotC, swapC1, swapC2, swap1Order, sg.queue.GetSwap2Depth(), op)
	}

	return hedgeWithSwap()
}

func (sg *Strategy) closeSwap1HedgeSwap2(ctx context.Context, spotC exchange.Client, swapC1 exchange.SwapClient, swapC2 exchange.SwapClient,
	srClose float64, closeTradeVolume float64, swap1 *exchange.Depth, swap2 *exchange.Depth, side ccexgo.OrderSide) (*position.OrderResult, error) {
	var (
		swap1Order *exchange.CfOrder
		logger     log.Logger // 便于追踪
	)
	config := sg.load.Load()
	op, order, errMsg, err := sg.PosList.CloseSwapMaker(ctx, swapC1, srClose, closeTradeVolume, float64(config.CloseTickNum), swap1, nil, side, config.MakerType, false, true)
	if err != nil || order.ID.String() == `` || order.Status == ccexgo.OrderStatusFailed || order.Status == ccexgo.OrderStatusCancel { // 第一次下单失败,直接退出平仓，后面满足条件会再进入平仓
		if err == nil {
			return nil, fmt.Errorf("订单异常(取消或失败), 可能是触发了交易所限制, 可联系开发查看\n%+v", order)
		}
		level.Info(sg.logger).Log("message", "first close order failed", "error", err.Error())
		if errMsg != "" {
			return nil, fmt.Errorf("%+v", errMsg)
		}
		return nil, err
	}

	frozenAmount := order.Amount
	swap1Order = &exchange.CfOrder{
		Order: ccexgo.Order{
			ID:       order.ID,
			Side:     order.Side,
			AvgPrice: decimal.NewFromFloat(0),
			Filled:   decimal.NewFromFloat(0),
			Created:  time.Now(),
		},
	}
	if !order.Created.IsZero() {
		swap1Order.Created = order.Created
	}

	logger = log.With(sg.logger, "id", op.GetOpID())

	// 20分钟超时保护，防止协程永久卡住
	maxDurationTimer := time.NewTimer(20 * time.Minute)
	defer maxDurationTimer.Stop()

	// 撤单和清理的通用逻辑
	cancelAndExit := func() {
		var o *exchange.CfOrder
		o, err = swapC1.CancelOrder(ctx, order) // 撤单,查单
		if err != nil {
			errType := swapC1.HandError(err)
			level.Info(logger).Log("message", "close cancel order failed", "error", err.Error())
			err = fmt.Errorf("%s撤单或查单失败, 错误原因:%s, 需要人工处理", order.ID.String(), errType.String())
			go message.Send(ctx, message.NewOperateFail(fmt.Sprintf("%s swap1部分平仓失败", sg.swapS1), err.Error()))
			return
		}
		order = o
		if !order.Filled.IsZero() {
			swap1Order.AvgPrice = swap1Order.Filled.Mul(swap1Order.AvgPrice).Add(order.Filled.Mul(order.AvgPrice)).Div(swap1Order.Filled.Add(order.Filled))
			swap1Order.Filled = swap1Order.Filled.Add(order.Filled)
			swap1Order.Fee = order.Fee.Neg()                                   //撤单主动查出来的是总手续费
			if !strings.Contains(swapC1.GetExchangeName(), exchange.Binance) { // 币安合约ws推送没有创建时间
				swap1Order.Created = order.Created
			}
			swap1Order.Updated = order.Updated
		}
	}

	for {
		select {
		case <-maxDurationTimer.C:
			// 超过20分钟，强制终止流程
			level.Warn(logger).Log("message", "MT close strategy timeout 20 minutes, force exit", "order_id", order.ID.String())
			go message.SendP3Important(ctx, message.NewCommonMsgWithAt(fmt.Sprintf("%s MT平仓超时", sg.swapS1), fmt.Sprintf("操作ID: %s, 订单ID: %s, 运行超过20分钟强制退出", op.GetOpID(), order.ID.String())))
			cancelAndExit()
			goto CloseHedgeSwap2
		case <-ctx.Done():
			level.Info(logger).Log("message", "close exit now", "order_id", order.ID.String())
			cancelAndExit()
			goto CloseHedgeSwap2
		case <-sg.bookTickerEvent.Done(): // bookTicker变动, 同步检测CancelClose
			sg.bookTickerEvent.Unset()
			var (
				sr     rate.Sr
				cancel bool
			)
			if side == ccexgo.OrderSideCloseShort {
				sr = sg.queue.CancelOpen(order.Price.InexactFloat64(), rate.Swap1MSwap2T)
			} else {
				sr = sg.queue.CancelClose(order.Price.InexactFloat64(), rate.Swap1MSwap2T)
			}

			var (
				priceChange float64
				price0      float64
				price1      float64
			)
			mid := sg.queue.GetSwap1Mid()
			swap1BookTicker := swapC1.BookTicker()
			if swap1BookTicker == nil {
				continue
			}
			depth := sg.queue.GetSwap1Depth()
			depth.Asks[0].Price = swap1BookTicker.Ask1Price
			depth.Bids[0].Price = swap1BookTicker.Bid1Price
			depth.Asks[0].Amount = swap1BookTicker.Ask1Amount
			depth.Bids[0].Amount = swap1BookTicker.Bid1Amount

			if sr.Ok {
				cancel = true
			} else {
				pricePrecision := sg.swapS1.PricePrecision().InexactFloat64()
				if side == ccexgo.OrderSideCloseLong { //卖
					if config.MakerType == 0 {
						priceChange = order.Price.InexactFloat64() - depth.Bids[0].Price
					} else {
						if float.Equal(mid, .0) { // 刚切换配置是0
							continue
						}
						price0 = depth.Asks[0].Price
						price1 = depth.Asks[1].Price
						if float.Equal(depth.Asks[0].Price, order.Price.InexactFloat64()) && float.Equal(depth.Asks[0].Amount, order.Amount.InexactFloat64()) {
							price0 = depth.Asks[1].Price
							price1 = depth.Asks[2].Price
						}
						priceChange = price0 - mid
					}
				} else { // 平空，买
					if config.MakerType == 0 {
						priceChange = depth.Asks[0].Price - order.Price.InexactFloat64()
					} else {
						if float.Equal(mid, .0) { // 刚切换配置是0
							continue
						}
						price0 = depth.Bids[0].Price
						price1 = depth.Bids[1].Price
						if float.Equal(depth.Bids[0].Price, order.Price.InexactFloat64()) && float.Equal(depth.Bids[0].Amount, order.Amount.InexactFloat64()) {
							price0 = depth.Bids[1].Price
							price1 = depth.Bids[2].Price
						}
						priceChange = mid - price0
					}
				}
				if config.MakerType == 0 {
					if math.Round(priceChange/pricePrecision) > float64(config.ReplaceCloseTickNum) {
						cancel = true
					}
				} else {
					if math.Round(priceChange/pricePrecision) > float64(config.SpreadLimitNum1) {
						cancel = true
					}
					if !cancel && config.MakerType == 2 {
						if side == ccexgo.OrderSideCloseLong {
							if math.Round((price1-price0)/pricePrecision) > float64(config.SpreadLimitNum2) {
								cancel = true
							}
						} else {
							if math.Round((price0-price1)/pricePrecision) > float64(config.SpreadLimitNum2) {
								cancel = true
							}
						}
					}
				}
			}

			if cancel { // 撤单,查单,去对冲
				level.Info(logger).Log("message", "close replace order", "order_id", order.ID.String(), "side", order.Side.String(), "price", order.Price, "maker_type", config.MakerType,
					"price0", price0, "price1", price1, "price_change", priceChange, "ask0", depth.Asks[0].Price, "ask1", depth.Asks[1].Price, "ask2", depth.Asks[2].Price,
					"ask0_amount", depth.Asks[0].Amount, "bid0", depth.Bids[0].Price, "bid1", depth.Bids[1].Price, "bid2", depth.Bids[2].Price, "bid0_amount", depth.Bids[0].Amount,
					"sr_ok", sr.Ok, "sr_spread", sr.Spread)

				var o *exchange.CfOrder
				o, err = swapC1.CancelOrder(ctx, order) // error
				if err != nil {
					errType := swapC1.HandError(err)
					sg.PosList.UpdateBannedTime(errType, false)
					sg.PosList.CheckErrShareBan(errType, swapC1.GetExchangeName())
					level.Info(logger).Log("message", "close cancel order failed", "error", err.Error())
					err = fmt.Errorf("swap1 %s[%s]撤单或查单失败, 错误原因:%s, 需要人工处理", sg.swapS1, order.ID.String(), errType.String())
					go message.Send(ctx, message.NewOperateFail(fmt.Sprintf("%s swap1部分平仓失败", sg.swapS1), err.Error()))
					goto CloseHedgeSwap2
				}
				order = o
				if !float.Equal(order.Filled.InexactFloat64(), .0) {
					swap1Order.AvgPrice = swap1Order.Filled.Mul(swap1Order.AvgPrice).Add(order.Filled.Mul(order.AvgPrice)).Div(swap1Order.Filled.Add(order.Filled))
					swap1Order.Filled = swap1Order.Filled.Add(order.Filled)
					swap1Order.Fee = order.Fee.Neg()                                   //撤单主动查出来的是总手续费
					if !strings.Contains(swapC1.GetExchangeName(), exchange.Binance) { // 币安合约ws推送没有创建时间
						swap1Order.Created = order.Created
					}
					swap1Order.Updated = order.Updated
				}
				goto CloseHedgeSwap2
			}

		case o := <-swapC1.MakerOrder(order.ID.String()): // 判断是否完全成交或取消
			if o == nil { // channel超时关闭了
				continue
			}
			if o.ID != order.ID {
				level.Warn(logger).Log("message", "skip unmatch order", "want", order.ID.String(), "got", o.ID.String(), "amount", o.Amount, "fee", o.Fee, "filled", o.Filled)
				continue
			}
			level.Info(logger).Log("message", "get ws order notify", "id", o.ID.String(), "amount", o.Amount, "filled", o.Filled, "fee", o.Fee, "avg_price", o.AvgPrice, "status", o.Status)
			if o.Status == ccexgo.OrderStatusDone || o.Status == ccexgo.OrderStatusCancel {
				if swap1Order.Filled.Add(o.Filled).IsPositive() {
					swap1Order.AvgPrice = swap1Order.Filled.Mul(swap1Order.AvgPrice).Add(o.Filled.Mul(o.AvgPrice)).Div(swap1Order.Filled.Add(o.Filled))
				}
				swap1Order.Filled = swap1Order.Filled.Add(o.Filled)
				swap1Order.Fee = swap1Order.Fee.Add(o.Fee.Neg())
				if o.FeeCurrency != `` {
					swap1Order.FeeCurrency = o.FeeCurrency
				}
				if !strings.Contains(swapC1.GetExchangeName(), exchange.Binance) { // 币安合约ws推送没有创建时间
					swap1Order.Created = o.Created
				}
				swap1Order.Updated = o.Updated
				goto CloseHedgeSwap2
			}
		}
	}
CloseHedgeSwap2:
	// swap1Order.Updated = time.Now()
	hedgeWithSwap := func() (*position.OrderResult, error) {
		if swap1Order.Filled.IsZero() {
			sg.PosList.UnfreezeCloseAmount(frozenAmount)
			if err != nil {
				return nil, err
			}
			return nil, nil
		}
		makerInfo := common.NewJsonObject()
		makerInfo["sr_close_done"] = sg.queue.GetSrClose(acts.CcexSide2Acts(side), swap1Order.AvgPrice.InexactFloat64(), rate.Swap1MSwap2T)
		op.SetMakerInfo(makerInfo)
		return sg.PosList.CloseHedgeSwap2(ctx, spotC, swapC1, swapC2, swap1Order, sg.queue.GetSwap2Depth(), op, config.TotalLimit, frozenAmount)
	}
	return hedgeWithSwap()
}
