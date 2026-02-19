package position

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"cf_arbitrage/exchange"
	actsSpot "cf_arbitrage/exchange/acts/spot"
	"cf_arbitrage/logic/config"
	"cf_arbitrage/logic/rate"

	"cf_factor"

	ccexgo "github.com/NadiaSama/ccexgo/exchange"
	"github.com/NadiaSama/ccexgo/misc/ctxlog"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/log/level"
	"github.com/shopspring/decimal"
)

func (li *listImpl) OpenAllLimit(ctx context.Context, swapC1, swapC2 exchange.SwapClient,
	srOpen float64, openTradeUnit float64, openTickNum []float64, swap1 *exchange.Depth,
	swap2 *exchange.Depth, op *OrderResult, side ccexgo.OrderSide, cfg *config.Config) (*OrderResult, *exchange.CfOrder, *exchange.CfOrder, string, int, rate.RunMode) {
	return li.allLimitOpenSwap1LongSwap2Short(ctx, swapC1, swapC2, srOpen, openTradeUnit, openTickNum, swap1, swap2, op, side, cfg)
}

func (li *listImpl) CloseAllLimit(ctx context.Context, swapC1, swapC2 exchange.SwapClient,
	srClose float64, closeTradeUnit float64, closeTickNum []float64, swap1 *exchange.Depth,
	swap2 *exchange.Depth, op *OrderResult, side ccexgo.OrderSide, cfg *config.Config) (*OrderResult, *exchange.CfOrder, *exchange.CfOrder, string, int, rate.RunMode) {
	return li.allLimitCloseSpotLongSwapShort(ctx, swapC1, swapC2, srClose, closeTradeUnit, closeTickNum, swap1, swap2, op, side, cfg)
}

func (li *listImpl) allLimitOpenSwap1LongSwap2Short(ctx context.Context, swapC1, swapC2 exchange.SwapClient,
	srOpen float64, openTradeUnit float64, openTickNum []float64, swap1 *exchange.Depth,
	swap2 *exchange.Depth, op *OrderResult, side ccexgo.OrderSide, cfg *config.Config) (*OrderResult, *exchange.CfOrder, *exchange.CfOrder, string, int, rate.RunMode) {
	li.mu.Lock()
	id := fmt.Sprintf("%s_%d", time.Now().Format("060102_150405"), li.PosID)
	if op == nil {
		li.PosID += 1
	}
	li.mu.Unlock()
	logger := log.With(li.logger, "id", id, "mode", rate.Swap1MSwap2M)
	// one := decimal.NewFromFloat(1)

	swap2PriceExp := actsSpot.CalcExponent(li.swap2.PricePrecision())
	swap1PriceExp := actsSpot.CalcExponent(li.swap1.PricePrecision())
	amountExp := li.amountMinExp

	swap1Price := decimal.NewFromFloat(swap1.Asks[0].Price).Sub(li.swap1.PricePrecision().Mul(decimal.NewFromFloat(openTickNum[0]))).Round(int32(swap1PriceExp))
	swap2Price := decimal.NewFromFloat(swap2.Bids[0].Price).Add(li.swap2.PricePrecision().Mul(decimal.NewFromFloat(openTickNum[1]))).Round(int32(swap2PriceExp))

	if side == ccexgo.OrderSideSell {
		swap1Price = decimal.NewFromFloat(swap1.Bids[0].Price).Add(li.swap1.PricePrecision().Mul(decimal.NewFromFloat(openTickNum[0]))).Round(int32(swap1PriceExp))
		swap2Price = decimal.NewFromFloat(swap2.Asks[0].Price).Sub(li.swap2.PricePrecision().Mul(decimal.NewFromFloat(openTickNum[1]))).Round(int32(swap2PriceExp))
	}

	amount1 := decimal.NewFromFloat(openTradeUnit).Div(swap1Price).Round(int32(amountExp)) // 想买的个数

	level.Info(logger).Log("message", "allLimitOpenSwap1LongSwap2Short", "swap1Price", swap1Price, "swap2Price", swap2Price, "amount1", amount1)

	placeSize1 := amount1.Div(li.swap1.ContractVal())
	placeSize2 := amount1.Div(li.swap2.ContractVal())
	if placeSize1.LessThan(decimal.NewFromFloat(li.swap1ActsSymbol.MinimumOrderAmount)) || placeSize2.LessThan(decimal.NewFromFloat(li.swap2ActsSymbol.MinimumOrderAmount)) {
		errMsg := fmt.Sprintf("下单数量小于最小下单数量 swap1=%s(min=%f) swap2=%s(min=%f)", placeSize1.String(), li.swap1ActsSymbol.MinimumOrderAmount, placeSize2.String(), li.swap2ActsSymbol.MinimumOrderAmount)
		return nil, nil, nil, errMsg, -1, rate.Swap1MSwap2M
	}
	usdt := amount1.Mul(swap1Price)

	ret := NewOrderResult()
	if op == nil {
		ret.AddAny("pos_id", id)
		ret.Start()
		ret.AddAny("sr_open", srOpen)
		ret.AddAny("open_trade_unit", amount1)
		ret.AddAny("open_usdt", usdt)
		ret.SetOpID(id)
	} else {
		ret = op
	}
	var (
		placeMode              rate.RunMode
		swap1Order, swap2Order *exchange.CfOrder
		errMsg1, errMsg2       string
		err1, err2             error
	)
	// 根据cfg.MMSwitchSide决定是否只挂单一边
	switch cfg.MMSwitchSide {
	case 1:
		placeMode = rate.Swap1MSwap2T
	case 2:
		placeMode = rate.Swap1TSwap2M
	case 3:
		// 这里再用imb判断
		var isBuy bool
		if side == ccexgo.OrderSideSell {
			isBuy = true
		}
		tradeMode := cf_factor.CalculateMMIMBPass(swap1.OrderBook, swap2.OrderBook, &cfg.Factor, isBuy)
		if tradeMode == cf_factor.SpotMSwapT {
			placeMode = rate.Swap1MSwap2T
		} else if tradeMode == cf_factor.SpotTSwapM {
			placeMode = rate.Swap1TSwap2M
		} else {
			placeMode = rate.Swap1MSwap2M
		}
	default:
		placeMode = rate.Swap1MSwap2M
	}
	var wg sync.WaitGroup
	if placeMode == rate.Swap1MSwap2M {
		wg.Add(2)
	} else {
		wg.Add(1)
	}
	if placeMode != rate.Swap1TSwap2M {
		go func() {
			defer wg.Done()
			swap1Order, err1 = swapC1.LimitOrder(ctxlog.SetLog(ctx, logger), side, amount1, swap1Price.InexactFloat64())
			if err1 != nil {
				errType := swapC1.HandError(err1)
				level.Error(logger).Log("message", "create swap1 limit order failed", "side", side, "err", err1.Error())
				errMsg1 = fmt.Sprintf("创建swap1%s单失败 usdt=%s, amount=%s, price=%f\n错误原因:%s", side, usdt.String(), amount1.String(), swap1Price.InexactFloat64(), errType.String())
			} else {
				level.Info(logger).Log("message", "create swap1 limit order", "side", side, "order", fmt.Sprintf("%+v", swap1Order))
			}
		}()
	}

	if placeMode != rate.Swap1MSwap2T {
		swap2Side := reverseSide(side)
		go func() {
			defer wg.Done()
			swap2Order, err2 = swapC2.LimitOrder(ctxlog.SetLog(ctx, logger), swap2Side, amount1, swap2Price.InexactFloat64())
			if err2 != nil {
				errType := swapC2.HandError(err2)
				level.Error(logger).Log("message", "create swap2 limit order failed", "side", swap2Side, "err", err2.Error())
				errMsg2 = fmt.Sprintf("创建swap2%s单失败 usdt=%s, amount=%s, price=%f\n错误原因:%s", swap2Side, usdt.String(), amount1.String(), swap2Price.InexactFloat64(), errType.String())
			} else {
				level.Info(logger).Log("message", "create swap2 limit order", "side", swap2Side, "order", fmt.Sprintf("%+v", swap2Order))
			}
		}()
	}

	wg.Wait()

	if err1 != nil && err2 != nil { // 两个单都失败
		return nil, nil, nil, fmt.Sprintf("%s\n%s", errMsg1, errMsg2), -1, placeMode
	}

	if err1 != nil || err2 != nil { // 一个单失败
		if err1 != nil {
			return ret, nil, swap2Order, errMsg1, 1, placeMode
		}
		return ret, swap1Order, nil, errMsg2, 2, placeMode
	}

	return ret, swap1Order, swap2Order, "", 0, placeMode
}

func (li *listImpl) allLimitCloseSpotLongSwapShort(ctx context.Context, swapC1 exchange.Client, swapC2 exchange.SwapClient,
	srClose float64, closeTradeUnit float64, closeTickNum []float64, swap1 *exchange.Depth,
	swap2 *exchange.Depth, op *OrderResult, side ccexgo.OrderSide, cfg *config.Config) (*OrderResult, *exchange.CfOrder, *exchange.CfOrder, string, int, rate.RunMode) {
	id := fmt.Sprintf("%d_%d", time.Now().UnixNano(), li.PosID)
	// if op == nil {
	// 	li.PosID += 1
	// }
	logger := log.With(li.logger, "id", id, "mode", rate.Swap1MSwap2M)
	// one := decimal.NewFromFloat(1)
	// swapCF2 := swapC2.FeeRate()

	swap2PriceExp := actsSpot.CalcExponent(li.swap2.PricePrecision())
	swap1PriceExp := actsSpot.CalcExponent(li.swap1.PricePrecision())
	amountExp := li.amountMinExp

	swap1Price := decimal.NewFromFloat(swap1.Bids[0].Price).Add(li.swap1.PricePrecision().Mul(decimal.NewFromFloat(closeTickNum[0]))).Round(int32(swap1PriceExp))
	swap2Price := decimal.NewFromFloat(swap2.Asks[0].Price).Sub(li.swap2.PricePrecision().Mul(decimal.NewFromFloat(closeTickNum[1]))).Round(int32(swap2PriceExp))
	if side == ccexgo.OrderSideCloseShort {
		swap1Price = decimal.NewFromFloat(swap1.Asks[0].Price).Sub(li.swap1.PricePrecision().Mul(decimal.NewFromFloat(closeTickNum[0]))).Round(int32(swap1PriceExp))
		swap2Price = decimal.NewFromFloat(swap2.Bids[0].Price).Add(li.swap2.PricePrecision().Mul(decimal.NewFromFloat(closeTickNum[1]))).Round(int32(swap2PriceExp))
	}
	li.mu.RLock()
	localPos2Amount := li.LocalPos2Amount
	swap1ShortPos := li.Swap1ShortPos.PosAmount
	swap2LongPos := li.Swap2LongPos.PosAmount
	swap1LongPos := li.Swap1LongPos.PosAmount
	swap2ShortPos := li.Swap2ShortPos.PosAmount
	li.mu.RUnlock()

	swap2Side := reverseSide(side)

	amount1 := decimal.Min(decimal.NewFromFloat(closeTradeUnit).Div(swap1Price), localPos2Amount).Round(int32(amountExp)) // 平的个数

	if side == ccexgo.OrderSideCloseShort { // 平空
		amount1 = decimal.Min(amount1, swap1ShortPos, swap2LongPos) // 平的个数
	} else {
		amount1 = decimal.Min(amount1, swap1LongPos, swap2ShortPos) // 平的个数
	}

	placeSize1 := amount1.Div(li.swap1.ContractVal())
	placeSize2 := amount1.Div(li.swap2.ContractVal())
	if placeSize1.LessThan(decimal.NewFromFloat(li.swap1ActsSymbol.MinimumOrderAmount)) || placeSize2.LessThan(decimal.NewFromFloat(li.swap2ActsSymbol.MinimumOrderAmount)) {
		errMsg := fmt.Sprintf("下单数量小于最小下单数量 swap1=%s(min=%f) swap2=%s(min=%f)", placeSize1.String(), li.swap1ActsSymbol.MinimumOrderAmount, placeSize2.String(), li.swap2ActsSymbol.MinimumOrderAmount)
		// 给大的一边先处理，另一边依赖自动清理仓位
		// 用交易所的大的一边先处理, 可能本地仓位为0了
		if side == ccexgo.OrderSideCloseShort { // 平空
			amount1 = decimal.Min(swap1ShortPos, swap2LongPos) // 平的个数
		} else {
			amount1 = decimal.Min(swap1LongPos, swap2ShortPos) // 平的个数
		}
		placeSize1 = amount1.Div(li.swap1.ContractVal())
		placeSize2 = amount1.Div(li.swap2.ContractVal())
		if placeSize1.GreaterThanOrEqual(placeSize2) {
			swap1Order, err := swapC1.MarketOrder(ctxlog.SetLog(ctx, logger), side, amount1, decimal.Zero, nil)
			if err != nil {
				errType := swapC1.HandError(err)
				li.UpdateBannedTime(errType, false)
				li.CheckErrShareBan(errType, swapC1.GetExchangeName())
				errMsg += fmt.Sprintf("\n创建swap1%s单失败 amount=%s 错误原因:%s", side, amount1.String(), errType.String())
			} else {
				level.Info(logger).Log("message", "create swap1 market order", "side", side, "order", fmt.Sprintf("%+v", swap1Order))
				errMsg += fmt.Sprintf("\n创建swap1%s单成功 amount=%s", side, amount1.String())
				li.TotalSwap1Exposure = decimal.Zero
				li.TotalSwap2Exposure = decimal.Zero
			}
		} else {
			swap2Order, err := swapC2.MarketOrder(ctxlog.SetLog(ctx, logger), swap2Side, amount1, decimal.Zero, nil)
			if err != nil {
				errType := swapC2.HandError(err)
				li.UpdateBannedTime(errType, false)
				li.CheckErrShareBan(errType, swapC2.GetExchangeName())
				errMsg += fmt.Sprintf("\n创建swap2%s单失败 amount=%s 错误原因:%s", swap2Side, amount1.String(), errType.String())
			} else {
				level.Info(logger).Log("message", "create swap2 market order", "side", swap2Side, "order", fmt.Sprintf("%+v", swap2Order))
				errMsg += fmt.Sprintf("\n创建swap2%s单成功 amount=%s", swap2Side, amount1.String())
				li.TotalSwap1Exposure = decimal.Zero
				li.TotalSwap2Exposure = decimal.Zero
			}
		}
		return nil, nil, nil, errMsg, -1, rate.Swap1MSwap2M
	}
	usdt := amount1.Mul(swap1Price)

	level.Info(logger).Log("message", "allLimitCloseSpotLongSwapShort", "swap1Price", swap1Price, "swap2Price", swap2Price, "amount1", amount1)

	ret := NewOrderResult()
	if op == nil {
		ret.Start()
		ret.AddAny("sr_close", srClose)
		ret.SetOpID(id)
	} else {
		ret = op
	}
	var (
		placeMode              rate.RunMode
		swap1Order, swap2Order *exchange.CfOrder
		errMsg1, errMsg2       string
		err1, err2             error
	)
	// 根据cfg.MMSwitchSide决定是否只挂单一边
	switch cfg.MMSwitchSide {
	case 1:
		placeMode = rate.Swap1MSwap2T
	case 2:
		placeMode = rate.Swap1TSwap2M
	case 3:
		// 这里再用imb判断
		var isBuy bool
		if side == ccexgo.OrderSideCloseLong { // sell
			isBuy = true
		}
		tradeMode := cf_factor.CalculateMMIMBPass(swap1.OrderBook, swap2.OrderBook, &cfg.Factor, isBuy)
		if tradeMode == cf_factor.SpotMSwapT {
			placeMode = rate.Swap1MSwap2T
		} else if tradeMode == cf_factor.SpotTSwapM {
			placeMode = rate.Swap1TSwap2M
		} else {
			placeMode = rate.Swap1MSwap2M
		}
	default:
		placeMode = rate.Swap1MSwap2M

	}

	frozenAmount, success := li.TryFreezeCloseAmount(amount1, true)
	if !success {
		errMsg := fmt.Sprintf("剩余本地可平数量不足, 无法平仓, 请求数量: %s", amount1.String())
		return nil, nil, nil, errMsg, -1, placeMode
	}
	amount1 = frozenAmount

	var wg sync.WaitGroup
	if placeMode == rate.Swap1MSwap2M {
		wg.Add(2)
	} else {
		wg.Add(1)
	}
	if placeMode != rate.Swap1TSwap2M {
		go func() {
			defer wg.Done()
			swap1Order, err1 = swapC1.LimitOrder(ctxlog.SetLog(ctx, logger), side, amount1, swap1Price.InexactFloat64())
			if err1 != nil {
				errType := swapC1.HandError(err1)
				level.Error(logger).Log("message", "create swap1 limit order failed", "side", side, "err", err1.Error())
				errMsg1 = fmt.Sprintf("创建swap1%s单失败 usdt=%s, amount=%s, price=%f\n错误原因:%s", side, usdt.String(), amount1.String(), swap1Price.InexactFloat64(), errType.String())
			} else {
				level.Info(logger).Log("message", "create swap1 limit order", "side", side, "order", fmt.Sprintf("%+v", swap1Order))
			}
		}()
	}

	if placeMode != rate.Swap1MSwap2T {
		go func() {
			defer wg.Done()
			swap2Order, err2 = swapC2.LimitOrder(ctxlog.SetLog(ctx, logger), swap2Side, amount1, swap2Price.InexactFloat64())
			if err2 != nil {
				errType := swapC2.HandError(err2)
				level.Error(logger).Log("message", "create swap2 limit order failed", "side", swap2Side, "err", err2.Error())
				errMsg2 = fmt.Sprintf("创建swap2%s单失败 usdt=%s, amount=%s, price=%f\n错误原因:%s", swap2Side, usdt.String(), amount1.String(), swap2Price.InexactFloat64(), errType.String())
			} else {
				level.Info(logger).Log("message", "create swap2 limit order", "side", swap2Side, "order", fmt.Sprintf("%+v", swap2Order))
			}
		}()
	}

	wg.Wait()

	if err1 != nil && err2 != nil { // 两个单都失败
		li.UnfreezeCloseAmount(amount1)
		return nil, nil, nil, fmt.Sprintf("%s\n%s", errMsg1, errMsg2), -1, placeMode
	}

	if err1 != nil || err2 != nil { // 一个单失败
		li.UnfreezeCloseAmount(amount1)
		if err1 != nil {
			return ret, nil, swap2Order, errMsg1, 1, placeMode
		}
		return ret, swap1Order, nil, errMsg2, 2, placeMode
	}

	return ret, swap1Order, swap2Order, "", 0, placeMode
}

func (li *listImpl) OpenAllLimitHedge(ctx context.Context, swapC1, swapC2 exchange.SwapClient,
	swap1Order *exchange.CfOrder, swap2Order *exchange.CfOrder, swap *exchange.Depth, spot *exchange.Depth, op *OrderResult) (*OrderResult, error) {
	id := op.GetOpID()
	logger := log.With(li.logger, "id", id, "mode", rate.Swap1MSwap2M)
	cv1 := li.swap1.ContractVal()
	// cv2 := li.swap2.ContractVal()
	one := decimal.NewFromInt(1)

	exp1 := li.amount1Exp
	exp2 := li.amount2Exp

	swap1Order.Filled = swap1Order.Filled.Round(int32(exp1))
	swap2Order.Filled = swap2Order.Filled.Round(int32(exp2))

	// 对于u本位来说都是币
	filled1 := swap1Order.Filled
	filled2 := swap2Order.Filled

	// 重算filled1及filled2, 以小的为标准
	if filled1.GreaterThan(filled2) {
		filled1 = filled2
		li.AddSwapExposure(ctx, swap1Order.Filled.Sub(filled1), "swap1")
	} else if filled1.LessThan(filled2) {
		filled2 = filled1
		li.AddSwapExposure(ctx, swap2Order.Filled.Sub(filled2), "swap2")
	}

	level.Info(logger).Log("message", "OpenAllLimitHedge", "swap2Order", fmt.Sprintf("%+v", swap2Order), "swap1Order", fmt.Sprintf("%+v", swap1Order))

	swap1Ccy := filled1
	swap2Ccy := filled2

	realSrOpen := swap2Order.AvgPrice.Div(swap1Order.AvgPrice).Sub(one)
	// realUsdt := swap2Order.Filled.Mul(cv).Div(swapOrder.AvgPrice).Mul(one.Add(swapC2.FeeRate().OpenTaker)).Div(one.Sub(spotC.FeeRate().OpenMaker)).Mul(spotOrder.AvgPrice)
	unit1 := decimal.Max(swap1Order.Filled, swap2Order.Filled)
	unit2 := decimal.Max(swap1Order.Filled, swap2Order.Filled)
	ccy1 := swap1Ccy
	ccy2 := swap2Ccy
	unit1Real := swap1Ccy
	unit2Real := swap1Ccy.Mul(swap2Order.AvgPrice).Div(cv1)
	fee1Real := swap1Order.Fee.Mul(unit1Real).Div(swap1Order.Filled)
	fee2Real := swap2Order.Fee.Mul(unit2Real).Div(swap2Order.Filled)
	ccyDeposit1 := swap1Ccy.Div(li.level1)
	ccyDeposit2 := swap2Ccy.Div(li.level2)
	ccyDeposit2Real := ccyDeposit2.Mul(unit2Real).Div(swap2Order.Filled)
	if !li.inverse {
		unit1 = filled1
		unit2 = filled2
		ccy1 = swap1Ccy.Mul(swap1Order.AvgPrice)
		ccy2 = swap2Ccy.Mul(swap2Order.AvgPrice)
		unit1Real = unit1
		unit2Real = unit2
		fee1Real = swap1Order.Fee.Mul(unit1Real).Div(swap1Order.Filled)
		fee2Real = swap2Order.Fee.Mul(unit2Real).Div(swap2Order.Filled)
		ccyDeposit1 = ccy1.Div(li.level1)
		ccyDeposit2 = ccy2.Div(li.level2)
		ccyDeposit2Real = ccyDeposit2
	}
	ccyDeposit := ccyDeposit1.Add(ccyDeposit2)
	openExposure := ccy1.Sub(ccy2) // 币本位是币，u本位是u

	op.AddAny("sr_open_real", realSrOpen)
	op.AddOrder("swap1", swap1Order)
	op.AddOrder("swap2", swap2Order)
	op.AddDepth("swap2", swap)
	op.AddAny("unit1_real", unit1Real)
	op.AddAny("unit2_real", unit2Real)
	op.AddAny("fee1", swap1Order.Fee)
	op.AddAny("fee1_real", fee1Real)
	op.AddAny("fee2", swap2Order.Fee)
	op.AddAny("fee2_real", fee2Real)
	op.AddAny("ccy1", ccy1)
	op.AddAny("ccy2", ccy2)
	op.AddAny("deposit1", ccyDeposit1)
	op.AddAny("deposit1_real", ccyDeposit1)
	op.AddAny("deposit2", ccyDeposit2)
	op.AddAny("deposit2_real", ccyDeposit2Real)
	op.AddAny("deposit", ccyDeposit)
	op.AddAny("deposit_real", ccyDeposit1.Add(ccyDeposit2Real))
	op.AddAny("open_exposure", openExposure) // 开仓敞口
	op.AddAny("mode", rate.Swap1MSwap2M)
	op.AddAny("maker_info", op.GetMakerInfo())
	op.End()

	r, _ := realSrOpen.Float64()
	li.mu.Lock()
	li.Poss = append(li.Poss, Pos{
		ID:              id,
		SrOpen:          r,
		Direction:       swap2Order.Side.String(),
		Unit1:           unit1,
		Unit1Real:       unit1Real,
		Unit2:           unit2,
		Unit2Real:       unit2Real,
		Fee1:            swap1Order.Fee,
		Fee1Real:        fee1Real,
		Fee2:            swap2Order.Fee,
		Fee2Real:        fee2Real,
		OpenPrice1:      swap1Order.AvgPrice,
		OpenPrice2:      swap2Order.AvgPrice,
		Ccy1:            ccy1,
		Ccy2:            ccy2,
		OpenExposure:    openExposure,
		CcyDeposit1:     ccyDeposit1,
		CcyDeposit1Real: ccyDeposit1,
		CcyDeposit2:     ccyDeposit2,
		CcyDeposit2Real: ccyDeposit2Real,
		CcyDeposit:      ccyDeposit,
		CcyDepositReal:  ccyDeposit1.Add(ccyDeposit2Real),
		Mode:            rate.Swap1MSwap2M,
	})

	li.TotalExposure = li.TotalExposure.Add(openExposure)
	li.updateLocalTotalPos()
	li.mu.Unlock()

	li.calcAvgOpenSpread(unit1, realSrOpen, true)

	if err := li.updateAccountInfo(ctx, nil, swapC1, swapC2, logger); err != nil {
		level.Warn(logger).Log("message", "update account info failed", "err", err.Error(), "sleep to", li.BannedUntil)
		li.UpdateBannedTime(nil, true)
	}

	op.SetCcy(ccyDeposit1, ccyDeposit2)
	return op, nil
}

func (li *listImpl) CloseAllLimitHedge(ctx context.Context, swapC1, swapC2 exchange.SwapClient,
	swap1Order *exchange.CfOrder, swap2Order *exchange.CfOrder, swap *exchange.Depth, spot *exchange.Depth,
	op *OrderResult, totalLimit float64, frozenAmount decimal.Decimal) (*OrderResult, error) {

	id := fmt.Sprintf("%d_%d", time.Now().Unix(), li.PosID)
	logger := log.With(li.logger, "id", id, "mode", rate.Swap1MSwap2M, "func", "CloseAllLimitHedge")
	cv1 := li.swap1.ContractVal()
	cv2 := li.swap2.ContractVal()
	one := decimal.NewFromInt(1)
	swapCF := swapC1.FeeRate()
	// spotCF := swapC1.FeeRate()

	exp1 := li.amount1Exp
	exp2 := li.amount2Exp

	swap1Order.Filled = swap1Order.Filled.Round(int32(exp1))
	swap2Order.Filled = swap2Order.Filled.Round(int32(exp2))

	level.Info(logger).Log("message", "order info", "swap1Order", fmt.Sprintf("%+v", swap1Order), "swap2Order", fmt.Sprintf("%+v", swap2Order))

	//这里要遍历poss，得到张数对应的现货量
	// swapPrice := swap2Order.AvgPrice
	filled := decimal.Min(swap1Order.Filled, swap2Order.Filled)
	avg := swap1Order.AvgPrice
	cv := cv1
	swapC := swapC1
	actsSymbol := li.swap1ActsSymbol
	if filled.Equal(swap2Order.Filled) {
		avg = swap2Order.AvgPrice
		cv = cv2
		swapC = swapC2
		actsSymbol = li.swap2ActsSymbol
		swapCF = swapC2.FeeRate()
	}
	exp := 0
	if swapC.GetExchangeName() == exchange.Okex5 { // 可能只有okx才有小数张，取决于acts接口转换导致？
		exp = actsSpot.CalcExponent(decimal.NewFromFloat(actsSymbol.AmountTickSize))
	}

	// swap1Order Fee 或者 swap2Order Fee 可能因为unit缩减需要重算, 所以需要重算fee1Real, fee2Real
	fee1Real := swap1Order.Fee.Mul(filled).Div(swap1Order.Filled)
	fee2Real := swap2Order.Fee.Mul(filled).Div(swap2Order.Filled)

	li.mu.Lock()
	cp, closed, newPos := li.splitPos2(cv, swapCF.OpenMaker, filled.InexactFloat64(), avg.InexactFloat64(), nil, swap, totalLimit, swap2Order.Side, rate.Swap1MSwap2M, exp)

	if cp == nil {
		li.mu.Unlock()
		li.UnfreezeCloseAmount(frozenAmount)
		//no available pos to close
		return nil, fmt.Errorf("【人工处理】 CloseAllLimitHedge 平仓选不出仓位,请检查！！！")
	}

	if cp.Unit2.IsZero() {
		li.mu.Unlock()
		li.UnfreezeCloseAmount(frozenAmount)
		level.Warn(logger).Log("message", "unit2 is zero, return")
		return nil, nil
	}

	li.Poss = newPos
	li.updateLocalTotalPos()
	li.mu.Unlock()
	li.UnfreezeCloseAmount(frozenAmount)

	// if swap1Order.Filled.IsZero() || swap2Order.Filled.IsZero() {
	// 	var errMsg string
	// 	if swap1Order.Filled.IsZero() {
	// 		li.TotalSwap1Exposure = li.TotalSwap1Exposure.Add(swap2Order.Filled)
	// 		errMsg = fmt.Sprintf("swap1(%s) 成交数量为0, swap2(%s) 成交数量为%s", swapC1.GetExchangeName(), swapC2.GetExchangeName(), swap2Order.Filled.String())
	// 	}
	// 	if swap2Order.Filled.IsZero() {
	// 		li.TotalSwap2Exposure = li.TotalSwap2Exposure.Add(swap1Order.Filled)
	// 		errMsg = fmt.Sprintf("swap2(%s) 成交数量为0, swap1(%s) 成交数量为%s", swapC2.GetExchangeName(), swapC1.GetExchangeName(), swap1Order.Filled.String())
	// 	}
	// 	li.Poss = newPos
	// 	return nil, fmt.Errorf("%s, 已去掉仓位, 已计入敞口等待自动处理", errMsg)
	// }

	if !swap1Order.Filled.Equal(filled) {
		li.AddSwapExposure(ctx, swap1Order.Filled.Sub(filled).Neg(), "swap1")
	}
	if !swap2Order.Filled.Equal(filled) {
		li.AddSwapExposure(ctx, swap2Order.Filled.Sub(filled).Neg(), "swap2")
	}

	level.Info(logger).Log("message", "loop for pos", "traverse", cp.traverse, "deal_count", cp.count,
		"order_unit", cp.Unit2, "funding1", cp.Funding1, "funding_real", cp.Funding1Real, "funding2", cp.Funding2, "finding2_real", cp.Funding2Real, "fee1", cp.Fee1, "fee1_real", cp.Fee1Real,
		"fee2", cp.Fee2, "fee2_real", cp.Fee2Real, "deposit1", cp.CcyDeposit1, "deposit1_real", cp.CcyDeposit1Real, "deposit2", cp.CcyDeposit2Real)

	op.SetCcy(cp.CcyDeposit1, cp.CcyDeposit2)

	updatePosIDs := []string{}
	var (
		swap1Profit     decimal.Decimal
		swap2Profit     decimal.Decimal
		swap2ProfitReal decimal.Decimal
	)

	for _, pos := range closed {
		profit1 := pos.Unit1.Mul(cv1).Mul(one.Div(swap1Order.AvgPrice).Sub(one.Div(pos.OpenPrice1)))
		profit2 := pos.Unit2.Mul(cv2).Mul(one.Div(pos.OpenPrice2).Sub(one.Div(swap2Order.AvgPrice)))
		profitReal2 := pos.Unit2Real.Mul(cv2).Mul(one.Div(pos.OpenPrice2).Sub(one.Div(swap2Order.AvgPrice)))
		if !li.inverse {
			profit1 = pos.Unit1.Mul(pos.OpenPrice1.Sub(swap1Order.AvgPrice))
			profit2 = pos.Unit2.Mul(swap2Order.AvgPrice.Sub(pos.OpenPrice2))
			profitReal2 = pos.Unit2Real.Mul(swap2Order.AvgPrice.Sub(pos.OpenPrice2))
		}
		if swap2Order.Side == ccexgo.OrderSideCloseShort { //平空,buy
			profit1 = profit1.Neg()
			profit2 = profit2.Neg()
			profitReal2 = profitReal2.Neg()
		}
		swap1Profit = swap1Profit.Add(profit1)
		swap2Profit = swap2Profit.Add(profit2)
		swap2ProfitReal = swap2ProfitReal.Add(profitReal2)
		updatePosIDs = append(updatePosIDs, pos.ID)
	}
	swap1Earn := swap1Profit.Add(cp.Funding1).Add(cp.Fee1Real).Add(fee1Real) // 币本位是币，u本位是u
	swap2Earn := swap2Profit.Add(cp.Funding2).Add(cp.Fee2Real).Add(fee2Real)
	swap2EarnReal := swap2ProfitReal.Add(cp.Funding2Real).Add(cp.Fee2Real).Add(fee2Real)

	level.Info(logger).Log("message", "close swap2 order", "id", swap2Order.ID.String(), "unit2", cp.Unit2,
		"swap2_avg_price", swap2Order.AvgPrice, "swap2_profit", swap2Profit, "swap2_earn", swap2Earn, "swap2_order_fee", swap2Order.Fee,
		"swap2_fee_real", fee2Real, "swap1_profit", swap1Profit, "swap1_earn", swap1Earn, "swap2_earn_real", swap2EarnReal, "swap1_fee_real", fee1Real)

	li.calcAvgOpenSpread(cp.Unit1, decimal.Zero, false)

	if err := li.updateAccountInfo(ctx, nil, swapC1, swapC2, logger); err != nil {
		li.UpdateBannedTime(nil, true)
		level.Warn(logger).Log("message", "update account info failed", "err", err.Error(), "sleep to", li.BannedUntil)
	}

	//计算盈亏
	cost := cp.CcyDeposit1.Add(cp.CcyDeposit2)
	earn := swap1Earn.Add(swap2Earn)
	costReal := cp.CcyDeposit1Real.Add(cp.CcyDeposit2Real)
	realEarn := swap1Earn.Add(swap2EarnReal)

	op.AddAny("sr_close_real", swap2Order.AvgPrice.Div(swap1Order.AvgPrice).Sub(decimal.NewFromInt(1)))
	op.AddOrder("swap1", swap1Order)
	op.AddOrder("swap2", swap2Order)
	op.AddDepth("swap2", swap)
	op.AddAny("swap1_profit", swap1Profit)
	op.AddAny("swap2_profit", swap2Profit)
	op.AddAny("fee1", cp.Fee1)
	op.AddAny("fee1_real", cp.Fee1Real)
	op.AddAny("fee2", cp.Fee2)
	op.AddAny("fee2_real", cp.Fee2Real)
	op.AddAny("funding1", cp.Funding1)
	op.AddAny("funding1_real", cp.Funding1Real)
	op.AddAny("funding2", cp.Funding2)
	op.AddAny("funding2_real", cp.Funding2Real)
	op.AddAny("deposit1", cp.CcyDeposit1)
	op.AddAny("deposit2", cp.CcyDeposit2)
	op.AddAny("deposit1_real", cp.CcyDeposit1Real)
	op.AddAny("deposit2_real", cp.CcyDeposit2Real)
	op.AddAny("swap1_earn", swap1Earn)
	op.AddAny("swap1_earn_real", swap1Earn)
	op.AddAny("swap2_earn", swap2Earn)
	op.AddAny("swap2_earn_real", swap2EarnReal)
	op.AddAny("earn", earn)
	op.AddAny("earn_rate", earn.Div(cost).Mul(decimal.NewFromFloat(100)))
	op.AddAny("real_earn", realEarn)
	op.AddAny("real_earn_rate", realEarn.Div(costReal).Mul(decimal.NewFromFloat(100)))
	// op.AddDecimal("close_exposure", closeExposure)
	op.AddAny("mode", rate.Swap1MSwap2M)
	op.AddAny("pos_ids", strings.Join(updatePosIDs, ","))
	op.AddAny("maker_info", op.GetMakerInfo())
	op.End()
	return op, nil
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

func (li *listImpl) CheckPosSideAmount(side1, side2 ccexgo.OrderSide) error {
	li.mu.RLock()
	swap1ShortPos := li.Swap1ShortPos.PosAmount
	swap1LongPos := li.Swap1LongPos.PosAmount
	swap2ShortPos := li.Swap2ShortPos.PosAmount
	swap2LongPos := li.Swap2LongPos.PosAmount
	li.mu.RUnlock()

	if (side1 == ccexgo.OrderSideBuy && !swap1ShortPos.IsZero()) || (side1 == ccexgo.OrderSideSell && !swap1LongPos.IsZero()) {
		return fmt.Errorf(`【人工处理】计算swap(%s) %s %s,但是另一方向持仓不为0, 非连续出现可忽略`, li.swap1C.GetExchangeName(), side1, li.swap1.String())
	}
	if (side2 == ccexgo.OrderSideBuy && !swap2ShortPos.IsZero()) || (side2 == ccexgo.OrderSideSell && !swap2LongPos.IsZero()) {
		return fmt.Errorf(`【人工处理】计算swap(%s) %s %s,但是另一方向持仓不为0, 非连续出现可忽略`, li.swap2C.GetExchangeName(), side2, li.swap2.String())
	}
	if (side1 == ccexgo.OrderSideCloseLong && !swap1ShortPos.IsZero()) || (side1 == ccexgo.OrderSideCloseShort && !swap1LongPos.IsZero()) {
		return fmt.Errorf(`【人工处理】计算swap(%s) %s %s,但是另一方向持仓不为0, 非连续出现可忽略`, li.swap1C.GetExchangeName(), side1, li.swap1.String())
	}
	if (side2 == ccexgo.OrderSideCloseLong && !swap2ShortPos.IsZero()) || (side2 == ccexgo.OrderSideCloseShort && !swap2LongPos.IsZero()) {
		return fmt.Errorf(`【人工处理】计算swap(%s) %s %s,但是另一方向持仓不为0, 非连续出现可忽略`, li.swap2C.GetExchangeName(), side2, li.swap2.String())
	}
	return nil
}
