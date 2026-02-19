package position

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cf_arbitrage/exchange"
	actsSpot "cf_arbitrage/exchange/acts/spot"
	"cf_arbitrage/logic/rate"
	utilCommon "cf_arbitrage/util/common"

	ccexgo "github.com/NadiaSama/ccexgo/exchange"
	"github.com/NadiaSama/ccexgo/misc/ctxlog"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/log/level"
	"github.com/shopspring/decimal"
)

// Open 开仓
func (li *listImpl) OpenSwapMaker(ctx context.Context, swapC exchange.Client,
	srOpen float64, openTradeUnit float64, openTickNum float64,
	swap *exchange.Depth, op *OrderResult, side ccexgo.OrderSide, makerType int, preTaker bool) (*OrderResult, *exchange.CfOrder, string, error) {
	// if li.isversed { //币本位
	return li.openSwap1(ctx, swapC, srOpen, openTradeUnit, openTickNum, swap, op, side, makerType, preTaker)
	// }
	// return li.openSpotShort(ctx, swapC, srOpen, openTradeUnit, openTickNum, swap, op)
}

// 开多或开空
func (li *listImpl) openSwap1(ctx context.Context, swapC exchange.Client,
	srOpen float64, openTradeUnit float64, openTickNum float64,
	swap *exchange.Depth, op *OrderResult, side ccexgo.OrderSide, makerType int, preTaker bool) (*OrderResult, *exchange.CfOrder, string, error) {
	li.mu.Lock()
	id := fmt.Sprintf("%s_%d", time.Now().Format("060102_150405"), li.PosID)
	if op == nil {
		li.PosID += 1
	} else {
		id = op.GetOpID()
	}
	li.mu.Unlock()
	logger := log.With(li.logger, "id", id, "mode", rate.Swap1MSwap2T)

	cv := li.swap1.ContractVal()
	actsSymbol := li.swap1ActsSymbol
	// 如果swap1和swap2是同一个交易所，但是symbol不一样，则需要根据swap的symbol来确定contract val
	if swapC.GetExchangeName() == li.swap2C.GetExchangeName() {
		if (li.swap1C.GetExchangeName() != li.swap2C.GetExchangeName()) ||
			(li.swap1C.GetExchangeName() == li.swap2C.GetExchangeName() && swap.Symbol.String() == li.swap2.String()) {
			cv = li.swap2.ContractVal()
			actsSymbol = li.swap2ActsSymbol
		}
	}
	exp := 0
	if swapC.GetExchangeName() == exchange.Okex5 { // 可能只有okx才有小数张，取决于acts接口转换导致？
		exp = actsSpot.CalcExponent(decimal.NewFromFloat(actsSymbol.AmountTickSize))
	}
	amt := decimal.NewFromFloat(openTradeUnit).Div(cv).Round(int32(exp)) // 张
	swap1Side := side
	pricePrecision := li.swap1.PricePrecision()
	if swapC.GetExchangeName() == li.swap2C.GetExchangeName() {
		if (li.swap1C.GetExchangeName() != li.swap2C.GetExchangeName()) ||
			(li.swap1C.GetExchangeName() == li.swap2C.GetExchangeName() && swap.Symbol.String() == li.swap2.String()) {
			swap1Side = reverseSide(side)
			pricePrecision = li.swap2.PricePrecision()
		}
	}

	if err := li.CheckPosSideAmount(swap1Side, reverseSide(swap1Side)); err != nil {
		return nil, nil, err.Error(), err
	}

	priceExp := actsSpot.CalcExponent(pricePrecision)
	var (
		price     float64
		bookPrice decimal.Decimal
	)
	usdt := amt.Mul(cv)
	if side == ccexgo.OrderSideBuy {
		bookPrice = decimal.NewFromFloat(swap.Asks[0].Price)
		// price = decimal.NewFromFloat((swap1.Asks[0].Price + swap1.Bids[0].Price) / 2).Sub(li.swap1.PricePrecision().Mul(decimal.NewFromFloat(openTickNum))).Round(int32(priceExp)).InexactFloat64()
		price = bookPrice.Sub(pricePrecision.Mul(decimal.NewFromFloat(openTickNum))).Round(int32(priceExp)).InexactFloat64()
		if makerType != 0 {
			price = swap.Bids[0].Price
		}
	} else {
		bookPrice = decimal.NewFromFloat(swap.Bids[0].Price)
		// price = decimal.NewFromFloat((swap1.Asks[0].Price + swap1.Bids[0].Price) / 2).Add(li.swap1.PricePrecision().Mul(decimal.NewFromFloat(openTickNum))).Round(int32(priceExp)).InexactFloat64()
		price = bookPrice.Add(pricePrecision.Mul(decimal.NewFromFloat(openTickNum))).Round(int32(priceExp)).InexactFloat64()
		if makerType != 0 {
			price = swap.Asks[0].Price
		}
	}
	if !li.inverse {
		amt = usdt
		usdt = usdt.Mul(decimal.NewFromFloat(price))
	}
	ret := NewOrderResult()
	if op == nil {
		ret.AddAny("pos_id", id)
		ret.Start()
		ret.AddAny("sr_open", srOpen)
		ret.AddAny("open_trade_unit", amt)
		ret.AddAny("open_usdt", usdt)
		ret.SetOpID(id)
	} else {
		ret = op
	}
	swapOrder, err := swapC.LimitOrder(ctxlog.SetLog(ctx, logger), side, amt, price)
	if err != nil {
		errType := swapC.HandError(err)
		li.UpdateBannedTime(errType, false)
		li.CheckErrShareBan(errType, swapC.GetExchangeName())
		level.Error(logger).Log("message", "create swap limit order failed", "side", side, "err", err.Error())
		if preTaker {
			processType := errToProcessType(errType)
			if processType == SystemProcess {
				return ret, nil, fmt.Sprintf("创建swap(%s)%s单失败 系统自动处理 usdt=%s, amount=%+v, price=%+v\n错误原因:%s", swapC.GetExchangeName(), side, usdt.String(), amt, price, errType.String()), err
			}
		}
		return ret, nil, fmt.Sprintf("创建swap(%s)%s单失败 usdt=%s, amount=%s, price=%f\n错误原因:%s", swapC.GetExchangeName(), side, usdt.String(), amt, price, errType.String()), err
	}

	level.Info(logger).Log("message", "create swap open limit order", "swapC", swapC.GetExchangeName(), "side", side, "pricePrecision", pricePrecision, "openTickNum", openTickNum, "bookPrice", bookPrice, "order", fmt.Sprintf("%+v", swapOrder))

	return ret, swapOrder, "", nil
}

// func (li *listImpl) openSpotShort(ctx context.Context, spotC exchange.Client,
// 	srOpen float64, openTradeUnit float64, openTickNum float64,
// 	spot *exchange.Depth, op *OrderResult) (*OrderResult, *exchange.CfOrder, string, error) {
// 	id := fmt.Sprintf("%s_%d", time.Now().Format("20060102"), li.PosID)
// 	if op == nil {
// 		li.PosID += 1
// 	}
// 	logger := log.With(li.logger, "id", id, "mode", rate.SpotMSwapT)

// 	amt := decimal.NewFromFloat(openTradeUnit) // 实际想卖的币
// 	exp := actsSpot.CalcExponent(li.spot.AmountPrecision())
// 	priceExp := actsSpot.CalcExponent(li.spot.PricePrecision())
// 	placeCurrency := amt.Round(int32(exp)) // 约到最小下单精度
// 	usdt := placeCurrency.Mul(decimal.NewFromFloat(spot.Bids[0].Price))
// 	price := decimal.NewFromFloat((spot.Asks[0].Price + spot.Bids[0].Price) / 2).Add(li.spot.PricePrecision().Mul(decimal.NewFromFloat(openTickNum))).Round(int32(priceExp)).InexactFloat64()
// 	ret := NewOrderResult()
// 	if op == nil {
// 		ret.AddField("pos_id", id)
// 		ret.Start()
// 		ret.AddFloat("sr_open", srOpen)
// 		ret.AddDecimal("open_trade_unit", amt)
// 		ret.AddDecimal("open_usdt", usdt)
// 	} else {
// 		ret = op
// 	}
// 	spotOrder, err := spotC.LimitOrder(ctxlog.SetLog(ctx, logger), ccexgo.OrderSideSell, placeCurrency, price)
// 	if err != nil {
// 		errType := spotC.HandError(err)
// 		level.Error(logger).Log("message", "create spot limit sell order failed", err.Error())
// 		return ret, nil, fmt.Sprintf("创建现货卖单失败 usdt=%s, amount=%s, price=%f\n错误原因:%s", usdt.String(), placeCurrency.String(), price, errType.String()),
// 			errors.WithMessagef(err, "create spot limit sell order fail usdt=%s, amount=%s, price=%f", usdt.String(), placeCurrency.String(), price)
// 	}

// 	level.Info(logger).Log("message", "create spot limit buy order", "order", fmt.Sprintf("%+v", spotOrder))

// 	return ret, spotOrder, "", nil
// }

func (li *listImpl) OpenHedgeSwap2(ctx context.Context, spotC exchange.Client, swapC1 exchange.SwapClient,
	swapC2 exchange.SwapClient, swap1Order *exchange.CfOrder, swap *exchange.Depth, op *OrderResult) (or *OrderResult, err error) {
	start := time.Now()
	defer func() {
		if e := recover(); e != nil {
			err = utilCommon.RecoverWithLog(li.logger, "OpenHedgeSwap2", start, e)
		}
	}()

	// if li.isversed {
	return li.openHedgeSwap2(ctx, spotC, swapC1, swapC2, swap1Order, swap, op)
	// }
	// return li.openHedgeSwapLong(ctx, spotC, swapC, swap1Order, swap, op)
}

// swap2开空或开多
func (li *listImpl) openHedgeSwap2(ctx context.Context, spotC exchange.Client, swapC1 exchange.SwapClient,
	swapC2 exchange.SwapClient, swap1Order *exchange.CfOrder, swap *exchange.Depth, op *OrderResult) (*OrderResult, error) {

	id := op.GetOpID()
	logger := log.With(li.logger, "id", id, "mode", rate.Swap1MSwap2T)
	cv1 := li.swap1.ContractVal()
	cv2 := li.swap2.ContractVal()
	one := decimal.NewFromInt(1)

	// tf := swap1Order.Filled.Mul(one.Sub(spotC.FeeRate().OpenMaker)) // 实际不转敞口个数，对冲敞口
	swap1Ccy := swap1Order.Filled.Mul(cv1).Div(swap1Order.AvgPrice)
	if !li.inverse {
		swap1Ccy = swap1Order.Filled
	}

	// shouldAmt := tf.Mul(decimal.NewFromFloat(swap.Bids[0].Price)).Div(one.Add(swapC2.FeeRate().OpenTaker)).Div(cv2)
	// amt := shouldAmt.Truncate(0)        // 截断
	var (
		swap2Side ccexgo.OrderSide
		price     float64
	)
	if swap1Order.Side == ccexgo.OrderSideBuy {
		swap2Side = ccexgo.OrderSideSell
		price = swap.Bids[0].Price
	} else {
		swap2Side = ccexgo.OrderSideBuy
		price = swap.Asks[0].Price
	}
	shouldAmt := swap1Ccy.Mul(decimal.NewFromFloat(price)).Div(cv2) // 小数张
	amt := shouldAmt.Round(0)
	if !li.inverse {
		shouldAmt = swap1Ccy.Div(cv2)
		exp := 0
		if swapC2.GetExchangeName() == exchange.Okex5 { // 可能只有okx才有小数张，取决于acts接口转换导致？
			exp = actsSpot.CalcExponent(decimal.NewFromFloat(li.swap2ActsSymbol.AmountTickSize))
		}
		amt = shouldAmt.RoundFloor(int32(exp)).Mul(cv2)
	}

	if amt.Equal(decimal.Zero) { // 直接计入swap1敞口
		li.AddSwapExposure(ctx, swap1Order.Filled, "swap1")
		level.Warn(logger).Log("message", "amt is zero", "filled", swap1Order.Filled, "fee", swap1Order.Fee, "price", price, "total_swap1_exposure", li.TotalSwap1Exposure)
		// li.TotalExposure = li.TotalExposure.Add(swap1Order.Filled.Sub(spotOrder.Fee))
		return nil, fmt.Errorf("swap1成交价值算出swap2开0张,swap1成交数量=%s,price=%f, 当前swap1敞口=%s", swap1Ccy, price, li.TotalSwap1Exposure.String())
	}
	// firstAmount := li.PosAmount
	// var duration time.Duration
	// newTf := amt.Mul(cv).Div(decimal.NewFromFloat(swap.Bids[0].Price)).Mul(one.Add(swapC2.FeeRate().OpenTaker))
	// newRealTf := newTf.Mul(decimal.NewFromInt(4)).Div(decimal.NewFromInt(5)).Truncate(8)

	// if firstAmount.Mul(cv).LessThan(decimal.NewFromInt(3000)) {
	// 	ts := time.Now()
	// 	if err := li.transfer.Transfer(ctx, exchange.AccontTypeSpot, exchange.AccontTypeSwap, li.spot.Base(), newRealTf); err != nil {
	// 		balance, e := spotC.FetchBalance(ctx)
	// 		if e != nil {
	// 			level.Warn(logger).Log("message", "fetch balance failed", "error", e.Error())
	// 			balance = []ccexgo.Balance{}
	// 		}
	// 		return nil, fmt.Errorf("[%s]划转现货到合约失败(下单前) tf=%s balance=%+v\n错误原因:%s", rate.SpotMSwapT, newRealTf, balance, swapC2.HandError(err))
	// 	}
	// 	duration = time.Since(ts)
	// 	level.Info(logger).Log("message", "transfer from spot to swap", "amount", newRealTf)
	// }

	swap2Order, err := swapC2.MarketOrder(ctx, swap2Side, amt, decimal.Zero, nil)
	if err != nil {
		errType := swapC2.HandError(err)
		processType := errToProcessType(errType)
		if processType == SystemProcess {
			li.AddSwapExposure(ctx, swap1Order.Filled, "swap1")
			if errType.Code == exchange.AccountInsufficientBalance {
				return nil, fmt.Errorf("【人工处理】[%s]swap2(%s)开仓失败 amount=%s, side=%s, swap1成交, swap1Order=%+v \n错误原因:%s, 已计入swap1敞口, 需要手动处理余额问题", rate.Swap1MSwap2T, swapC2.GetExchangeName(), amt.String(), swap2Side, swap1Order, errType)
			}
			return nil, fmt.Errorf("【%s】[%s]swap2(%s)开仓失败 amount=%s, side=%s, swap1成交, swap1Order=%+v \n错误原因:%s, 已计入swap1敞口", processType, rate.Swap1MSwap2T, swapC2.GetExchangeName(), amt.String(), swap2Side, swap1Order, errType)
		} else {
			return nil, fmt.Errorf("【人工处理】[%s]swap2(%s)开仓失败 amount=%s, side=%s, swap1成交, swap1Order=%+v \n错误原因:%s", rate.Swap1MSwap2T, swapC2.GetExchangeName(), amt.String(), swap2Side, swap1Order, errType)
		}
	}

	// 币安taker可能是bnb抵扣费, 要计算出对应的usdt费
	var bnbEqualFee decimal.Decimal
	if strings.Contains(swapC2.GetExchangeName(), exchange.Binance) && swap2Order.FeeCurrency == `BNB` && li.swap2.String() != `BNBUSDT` {
		bnbEqualFee = swap2Order.Filled.Mul(swap2Order.AvgPrice).Mul(swapC2.FeeRate().OpenTaker.Mul(decimal.NewFromFloat(0.9))).Neg()
		swap2Order.Fee = bnbEqualFee
	}

	level.Info(logger).Log("message", "create swap2 market sell order", "id", swap2Order.ID.String(), "side", swap2Side, "bnb_equal_fee", bnbEqualFee)

	// if firstAmount.Mul(cv).GreaterThanOrEqual(decimal.NewFromInt(3000)) {
	// 	newTf = amt.Mul(cv).Div(swapOrder.AvgPrice).Mul(one.Add(swapC2.FeeRate().OpenTaker))
	// 	newRealTf = newTf.Mul(decimal.NewFromInt(4)).Div(decimal.NewFromInt(5)).Truncate(8)
	// 	ts := time.Now()
	// 	if err := li.transfer.Transfer(ctx, exchange.AccontTypeSpot, exchange.AccontTypeSwap, li.spot.Base(), newRealTf); err != nil {
	// 		balance, e := spotC.FetchBalance(ctx)
	// 		if e != nil {
	// 			level.Warn(logger).Log("message", "fetch balance failed", "error", e.Error())
	// 			balance = []ccexgo.Balance{}
	// 		}
	// 		newErr := fmt.Errorf("[%s]划转现货到合约失败(下单后) tf=%s balance=%+v\n错误原因:%s", rate.SpotMSwapT, newRealTf, balance, swapC2.HandError(err))
	// 		if err = message.SendImportant(ctx, message.NewOperateFail(fmt.Sprintf("%s 开仓失败", li.swap2.String()), newErr.Error())); err != nil {
	// 			level.Warn(logger).Log("message", "send lark failed", "err", err.Error())
	// 		}
	// 		// return nil, fmt.Errorf("[%s]划转现货到合约失败(下单后) tf=%s balance=%+v\n错误原因:%s", rate.SpotMSwapT, newRealTf, balance, swapC.HandError(err))
	// 	}
	// 	duration = time.Since(ts)
	// 	level.Info(logger).Log("message", "transfer from spot to swap", "amount", newRealTf)
	// }

	// op.AddDecimal("spot_to_swap_amount", newRealTf)
	// op.AddDecimal("spot_keep", newTf.Sub(newRealTf))
	// op.AddField("spot_to_swap_duration", duration.String())

	// 对于u本位来说都是币
	filled1 := swap1Order.Filled
	filled2 := swap2Order.Filled

	// 重算filled1及filled2, 以小的为标准, 实际上swap1挂单全部成交或者部分成交，一定是swap1 >= swap2
	// 所以其实是给swap1加敞口
	if filled1.GreaterThan(filled2) {
		filled1 = filled2
		li.AddSwapExposure(ctx, swap1Order.Filled.Sub(filled1), "swap1")
	} else if filled1.LessThan(filled2) {
		filled2 = filled1
		li.AddSwapExposure(ctx, swap2Order.Filled.Sub(filled2), "swap2")
	}

	swap2Ccy := filled2.Mul(cv2).Div(swap2Order.AvgPrice)

	realSrOpen := swap2Order.AvgPrice.Div(swap1Order.AvgPrice).Sub(one)
	// realUsdt := swap2Order.Filled.Mul(cv).Div(swapOrder.AvgPrice).Mul(one.Add(swapC2.FeeRate().OpenTaker)).Div(one.Sub(spotC.FeeRate().OpenMaker)).Mul(spotOrder.AvgPrice)
	unit1 := filled1
	unit2 := filled2
	ccy1 := swap1Ccy
	ccy2 := swap2Ccy
	unit1Real := swap1Ccy
	unit2Real := swap1Ccy.Mul(swap2Order.AvgPrice).Div(cv1)
	fee1Real := swap1Order.Fee
	fee2Real := swap2Order.Fee.Mul(unit2Real).Div(filled2)
	ccyDeposit1 := swap1Ccy.Div(li.level1)
	ccyDeposit2 := swap2Ccy.Div(li.level2)
	ccyDeposit2Real := ccyDeposit2.Mul(unit2Real).Div(filled2)
	if !li.inverse {
		swap1Ccy = filled1
		swap2Ccy = filled2
		ccy1 = swap1Ccy.Mul(swap1Order.AvgPrice)
		ccy2 = swap2Ccy.Mul(swap2Order.AvgPrice)
		unit1Real = swap1Ccy
		unit2Real = swap2Ccy
		fee1Real = swap1Order.Fee.Mul(unit1Real).Div(swap1Order.Filled)
		fee2Real = swap2Order.Fee.Mul(unit2Real).Div(swap2Order.Filled)
		ccyDeposit1 = ccy1.Div(li.level1)
		ccyDeposit2 = ccy2.Div(li.level2)
		ccyDeposit2Real = ccyDeposit2.Mul(unit2Real).Div(swap2Order.Filled)
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
	op.AddAny("mode", "swap1_maker")
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
		Mode:            rate.Swap1MSwap2T,
	})

	li.TotalExposure = li.TotalExposure.Add(openExposure)
	li.updateLocalTotalPos()
	li.mu.Unlock()

	li.calcAvgOpenSpread(unit1, realSrOpen, true)
	if err := li.updateAccountInfo(ctx, spotC, swapC1, swapC2, logger); err != nil {
		level.Warn(logger).Log("message", "update account info failed", "err", err.Error(), "sleep", "80s")
		// time.Sleep(80 * time.Second) // 休眠80s
		li.UpdateBannedTime(nil, true)
	}

	op.SetCcy(ccyDeposit1, ccyDeposit2)
	return op, nil
}

// openHedgeSwapLong 合约开多
// func (li *listImpl) openHedgeSwapLong(ctx context.Context, spotC exchange.Client, swapC exchange.SwapClient,
// 	spotOrder *exchange.CfOrder, swap *exchange.Depth, op *OrderResult) (*OrderResult, error) {

// id := fmt.Sprintf("%s_%d", time.Now().Format("20060102"), li.PosID)
// logger := log.With(li.logger, "id", id, "mode", rate.SpotMSwapT)
// cv := li.swap.ContractVal()
// one := decimal.NewFromInt(1)

// tf := spotOrder.Filled.Mul(spotOrder.AvgPrice).Mul(one.Sub(spotC.FeeRate().OpenMaker)).Div(one.Add(swapC.FeeRate().OpenTaker)) // 实际不转敞口个数，对冲敞口

// shouldAmt := tf.Div(decimal.NewFromFloat(swap.Asks[0].Price)).Div(cv) // 得到张数
// if int(shouldAmt.Truncate(0).InexactFloat64()) == 0 {                 // 直接计入敞口
// 	level.Warn(logger).Log("message", "amt is zero", "filled", spotOrder.Filled, "fee", spotOrder.Fee)
// 	li.TotalExposure = li.TotalExposure.Add(spotOrder.Filled.Mul(spotOrder.AvgPrice).Sub(spotOrder.Fee))
// 	return op, fmt.Errorf("现货成交价值小于合约面值,跳过开仓, amount=%s, price=%s, usdt=%s", spotOrder.Filled.String(), spotOrder.AvgPrice.String(), spotOrder.Filled.Mul(spotOrder.AvgPrice).String())
// }
// amt := shouldAmt.Truncate(0).Mul(cv) // 截断到整数*面值得到个数

// firstAmount := li.SwapAmount
// var duration time.Duration
// newTf := amt.Mul(decimal.NewFromFloat(swap.Asks[0].Price)).Mul(one.Add(swapC.FeeRate().OpenTaker))
// newRealTf := newTf.Mul(decimal.NewFromInt(4)).Div(decimal.NewFromInt(5)).Truncate(8)

// if firstAmount.LessThan(decimal.NewFromInt(3000)) {
// 	ts := time.Now()
// 	if err := li.transfer.Transfer(ctx, exchange.AccontTypeSpot, exchange.AccontTypeSwap, li.spot.Quote(), newRealTf); err != nil {
// 		balance, e := spotC.FetchBalance(ctx)
// 		if e != nil {
// 			level.Warn(logger).Log("message", "fetch balance failed", "error", e.Error())
// 			balance = []ccexgo.Balance{}
// 		}
// 		return nil, fmt.Errorf("[%s]划转现货到合约失败(下单前) tf=%s balance=%+v\n错误原因:%s", rate.SpotMSwapT, newRealTf, balance, swapC.HandError(err))
// 	}
// 	duration = time.Since(ts)
// 	level.Info(logger).Log("message", "transfer from spot to swap", "amount", newRealTf)
// }

// swapOrder, err := swapC.MarketOrder(ctx, ccexgo.OrderSideBuy, amt, decimal.Zero)
// if err != nil {
// 	return nil, fmt.Errorf("[%s]合约开仓失败 amount=%s\n错误原因:%s", rate.SpotMSwapT, amt.String(), swapC.HandError(err))
// }
// level.Info(logger).Log("message", "create swap market buy order", "id", swapOrder.ID.String())

// op.AddOrder("spot", spotOrder)
// op.AddOrder("swap", swapOrder)

// if firstAmount.GreaterThanOrEqual(decimal.NewFromInt(3000)) {
// 	newTf = amt.Mul(swapOrder.AvgPrice).Mul(one.Add(swapC.FeeRate().OpenTaker))
// 	newRealTf = newTf.Mul(decimal.NewFromInt(4)).Div(decimal.NewFromInt(5)).Truncate(8)
// 	ts := time.Now()
// 	if err := li.transfer.Transfer(ctx, exchange.AccontTypeSpot, exchange.AccontTypeSwap, li.spot.Quote(), newRealTf); err != nil {
// 		balance, e := spotC.FetchBalance(ctx)
// 		if e != nil {
// 			level.Warn(logger).Log("message", "fetch balance failed", "error", e.Error())
// 			balance = []ccexgo.Balance{}
// 		}
// 		newErr := fmt.Errorf("[%s]划转现货到合约失败(下单后) tf=%s balance=%+v\n错误原因:%s", rate.SpotMSwapT, newRealTf, balance, swapC.HandError(err))
// 		if err = message.SendImportant(ctx, message.NewOperateFail(fmt.Sprintf("%s 开仓失败", li.swap.String()), newErr.Error())); err != nil {
// 			level.Warn(logger).Log("message", "send lark failed", "err", err.Error())
// 		}
// 		// return nil, fmt.Errorf("[%s]划转现货到合约失败(下单后) tf=%s balance=%+v\n错误原因:%s", rate.SpotMSwapT, newRealTf, balance, swapC.HandError(err))
// 	}
// 	duration = time.Since(ts)
// 	level.Info(logger).Log("message", "transfer from spot to swap", "amount", newRealTf)
// }

// op.AddDecimal("spot_to_swap_amount", newRealTf)
// op.AddDecimal("spot_keep", newTf.Sub(newRealTf))
// op.AddField("spot_to_swap_duration", duration.String())

// op.AddDepth("swap", swap)

// real := swapOrder.AvgPrice.Div(spotOrder.AvgPrice).Sub(decimal.NewFromInt(1))
// // realUsdt := spotOrder.Filled.Mul(spotOrder.AvgPrice).Mul(newTf.Div(spotOrder.Filled))
// ccy := swapOrder.Filled.Mul(swapOrder.AvgPrice).Mul(one.Add(swapC.FeeRate().OpenTaker)).Div(spotOrder.AvgPrice.Mul(one.Sub(spotC.FeeRate().OpenMaker)))
// openExposure := spotOrder.Filled.Mul(spotOrder.AvgPrice).Mul(one.Sub(spotC.FeeRate().OpenMaker)).Sub(newTf.Mul(one.Add(swapC.FeeRate().OpenTaker)))

// op.AddDecimal("sr_open_real", real)
// op.AddDecimal("open_real_usdt", ccy)
// op.AddDecimal("open_exposure", openExposure) // 开仓敞口
// op.AddField("mode", "spot_maker")

// op.End()

// r, _ := real.Float64()
// li.Poss = append(li.Poss, Pos{
// 	ID:         id,
// 	SrOpen:     r,
// 	Unit:       amt.Div(cv),
// 	SpotToSwap: newRealTf,
// 	Fee:        swapOrder.Fee,
// 	Ccy:        ccy,
// 	OpenPrice:  swapOrder.AvgPrice,
// 	SpotKeep:   newTf.Sub(newRealTf),
// 	Mode:       rate.SpotMSwapT,
// })

// li.SpotKeep = li.SpotKeep.Add(newTf.Sub(newRealTf))
// li.TotalExposure = li.TotalExposure.Add(openExposure)

// if err := li.updateAccountInfo(ctx, spotC, swapC, logger); err != nil {
// 	level.Warn(logger).Log("message", "update account info failed", "err", err.Error(), "sleep", "80s")
// 	time.Sleep(80 * time.Second) // 休眠80s
// }

// op.SetCcy(ccy)
// 	return op, nil
// }

func (li *listImpl) CloseSwapMaker(ctx context.Context, swapC exchange.Client,
	srClose float64, closeTradeVolume float64, closeTickNum float64, swap *exchange.Depth,
	op *OrderResult, side ccexgo.OrderSide, makerType int, preTaker, needFrozen bool) (*OrderResult, *exchange.CfOrder, string, error) {
	// if li.isversed {
	return li.closeSwap1(ctx, swapC, srClose, closeTradeVolume, closeTickNum, swap, op, side, makerType, preTaker, needFrozen)
	// }
	// return li.closeSpotShort(ctx, spotC, srClose, closeTradeVolume, closeTickNum, spot, op)
}

// Close 平仓由于平仓数量与开仓数量不一致，一次平仓可能涉及多个仓位
func (li *listImpl) closeSwap1(ctx context.Context, swapC exchange.Client,
	srClose float64, closeTradeVolume float64, closeTickNum float64, swap *exchange.Depth,
	op *OrderResult, side ccexgo.OrderSide, makerType int, preTaker, needFrozen bool) (*OrderResult, *exchange.CfOrder, string, error) {

	id := fmt.Sprintf("%d_%d", time.Now().UnixNano(), li.PosID)
	if op != nil {
		id = op.GetOpID()
	}
	logger := log.With(li.logger, "id", id)

	cv := li.swap1.ContractVal()
	actsSymbol := li.swap1ActsSymbol
	if swapC.GetExchangeName() == li.swap2C.GetExchangeName() {
		if (li.swap1C.GetExchangeName() != li.swap2C.GetExchangeName()) ||
			(li.swap1C.GetExchangeName() == li.swap2C.GetExchangeName() && swap.Symbol.String() == li.swap2.String()) {
			cv = li.swap2.ContractVal()
			actsSymbol = li.swap2ActsSymbol
		}
	}
	exp := 0
	if swapC.GetExchangeName() == exchange.Okex5 { // 可能只有okx才有小数张，取决于acts接口转换导致？
		exp = actsSpot.CalcExponent(decimal.NewFromFloat(actsSymbol.AmountTickSize))
	}
	amt := decimal.NewFromFloat(closeTradeVolume).Div(cv).Round(int32(exp)) // 张
	swap1Side := side
	pricePrecision := li.swap1.PricePrecision()

	// 判断是swap1还是swap2，并设置相关参数
	// swapName := "swap1"
	if swapC.GetExchangeName() == li.swap2C.GetExchangeName() {
		if (li.swap1C.GetExchangeName() != li.swap2C.GetExchangeName()) ||
			(li.swap1C.GetExchangeName() == li.swap2C.GetExchangeName() && swap.Symbol.String() == li.swap2.String()) {
			swap1Side = reverseSide(side)
			pricePrecision = li.swap2.PricePrecision()
			// swapName = "swap2"
		}
	}
	if err := li.CheckPosSideAmount(swap1Side, reverseSide(swap1Side)); err != nil {
		return nil, nil, err.Error(), err
	}

	var success bool
	var frozenAmount decimal.Decimal

	if needFrozen {
		// 冻结数量机制 - 防止超平, 暂时只支持u本位
		frozenAmount, success = li.TryFreezeCloseAmount(amt.Mul(cv), true)
		if !success {
			err := fmt.Errorf("剩余本地可平数量不足, 无法平仓, 请求数量: %s", amt.String())
			return nil, nil, err.Error(), err
		}
	} else {
		frozenAmount = amt.Mul(cv)
	}

	priceExp := actsSpot.CalcExponent(pricePrecision)

	// price := decimal.NewFromFloat((spot.Asks[0].Price + spot.Bids[0].Price) / 2).Add(li.spot.PricePrecision().Mul(decimal.NewFromFloat(closeTickNum))).Round(int32(priceExp)).InexactFloat64()

	// amt := decimal.NewFromFloat(math.Min(closeTradeVolume/price, li.SpotKeep.Truncate(int32(exp)).InexactFloat64())) // 实际想卖的币

	var (
		price     float64
		bookPrice decimal.Decimal
	)
	usdt := amt.Mul(cv)
	if side == ccexgo.OrderSideCloseLong { // 平多 sell
		bookPrice = decimal.NewFromFloat(swap.Bids[0].Price)
		price = bookPrice.Add(pricePrecision.Mul(decimal.NewFromFloat(closeTickNum))).Round(int32(priceExp)).InexactFloat64()
		if makerType != 0 {
			price = swap.Asks[0].Price
		}
	} else { // 平空 buy
		bookPrice = decimal.NewFromFloat(swap.Asks[0].Price)
		price = bookPrice.Sub(pricePrecision.Mul(decimal.NewFromFloat(closeTickNum))).Round(int32(priceExp)).InexactFloat64()
		if makerType != 0 {
			price = swap.Bids[0].Price
		}
	}

	if !li.inverse {
		amt = frozenAmount
		usdt = frozenAmount.Mul(decimal.NewFromFloat(price))
	}
	ret := NewOrderResult()
	if op == nil {
		ret.Start()
		ret.AddAny("sr_close", srClose)
		ret.SetOpID(id)
	} else {
		ret = op
	}

	swapOrder, err := swapC.LimitOrder(ctxlog.SetLog(ctx, logger), side, amt, price)

	// 无论下单成功失败，都解冻数量
	// li.UnfreezeCloseAmount(swapName, frozenAmount)

	if err != nil {
		// 下单失败，直接解冻数量
		if needFrozen {
			li.UnfreezeCloseAmount(frozenAmount)
		}
		errType := swapC.HandError(err)
		li.UpdateBannedTime(errType, false)
		li.CheckErrShareBan(errType, swapC.GetExchangeName())
		level.Error(logger).Log("message", "create swap order failed", "side", side, "err", err.Error())
		if preTaker {
			processType := errToProcessType(errType)
			if processType == SystemProcess {
				return ret, nil, fmt.Sprintf("创建swap(%s)%s单失败 系统自动处理 usdt=%s, amount=%s, price=%f\n错误原因:%s", swapC.GetExchangeName(), side, usdt.String(), amt.String(), price, errType.String()), err
			}
		}
		return ret, nil, fmt.Sprintf("创建swap(%s)%s单失败 usdt=%s, amount=%s, price=%f\n错误原因:%s", swapC.GetExchangeName(), side, usdt.String(), amt.String(), price, errType.String()), err
	}

	level.Info(logger).Log("message", "create swap close order", "swapC", swapC.GetExchangeName(), "side", side, "pricePrecision", pricePrecision, "closeTickNum", closeTickNum, "bookPrice", bookPrice, "order", fmt.Sprintf("%+v", swapOrder))

	return ret, swapOrder, "", nil
}

// func (li *listImpl) closeSpotShort(ctx context.Context, spotC exchange.Client,
// 	srClose float64, closeTradeVolume float64, closeTickNum float64, spot *exchange.Depth,
// 	op *OrderResult) (*OrderResult, *exchange.CfOrder, string, error) {

// 	id := fmt.Sprintf("%d_%d", time.Now().Unix(), li.PosID)

// 	logger := log.With(li.logger, "id", id)

// 	exp := actsSpot.CalcExponent(li.spot.AmountPrecision())
// 	priceExp := actsSpot.CalcExponent(li.spot.PricePrecision())

// 	price := decimal.NewFromFloat((spot.Asks[0].Price + spot.Bids[0].Price) / 2).Sub(li.spot.PricePrecision().Mul(decimal.NewFromFloat(closeTickNum))).Round(int32(priceExp)).InexactFloat64()

// 	amt := decimal.NewFromFloat(closeTradeVolume) // 实际想买的币

// 	placeCurrency := amt.Round(int32(exp)) // 约到最小下单精度
// 	usdt := placeCurrency.Mul(decimal.NewFromFloat(price))
// 	ret := NewOrderResult()
// 	if op == nil {
// 		ret.Start()
// 		ret.AddFloat("sr_close", srClose)
// 	} else {
// 		ret = op
// 	}
// 	if usdt.LessThan(decimal.NewFromFloat(10)) { //小于10U
// 		return ret, nil, "下单价值小于10U,跳过现货挂买单", fmt.Errorf("create spot limit buy order fail usdt=%s, amount=%s, price=%f", usdt.String(), placeCurrency.String(), price)
// 	}

// 	spotOrder, err := spotC.LimitOrder(ctxlog.SetLog(ctx, logger), ccexgo.OrderSideBuy, placeCurrency, price)
// 	if err != nil {
// 		errType := spotC.HandError(err)
// 		level.Error(logger).Log("message", "create spot buy order failed", err.Error())
// 		return ret, nil, fmt.Sprintf("创建现货买单失败 usdt=%s, amount=%s, price=%f\n错误原因:%s", usdt.String(), placeCurrency.String(), price, errType.String()),
// 			errors.WithMessagef(err, "create spot limit buy order fail usdt=%s, amount=%s, price=%f", usdt.String(), placeCurrency.String(), price)
// 	}

// 	level.Info(logger).Log("message", "create spot buy order", "order", fmt.Sprintf("%+v", spotOrder))

// 	return ret, spotOrder, "", nil
// }

func (li *listImpl) CloseHedgeSwap2(ctx context.Context, spotC exchange.Client, swapC1 exchange.SwapClient, swapC2 exchange.SwapClient,
	spotOrder *exchange.CfOrder, swap *exchange.Depth, op *OrderResult, totalLimit float64, frozenAmount decimal.Decimal) (or *OrderResult, err error) {
	start := time.Now()
	defer func() {
		if e := recover(); e != nil {
			err = utilCommon.RecoverWithLog(li.logger, "CloseHedgeSwap2", start, e)
		}
	}()
	// if li.isversed {
	return li.closeHedgeSwap2(ctx, spotC, swapC1, swapC2, spotOrder, swap, op, totalLimit, frozenAmount)
	// }
	// return li.closeHedgeSwapLong(ctx, spotC, swapC, spotOrder, swap, op, totalLimit)
}

func (li *listImpl) closeHedgeSwap2(ctx context.Context, spotC exchange.Client, swapC1 exchange.SwapClient,
	swapC2 exchange.SwapClient, swap1Order *exchange.CfOrder, swap *exchange.Depth, op *OrderResult, totalLimit float64, frozenAmount decimal.Decimal) (*OrderResult, error) {
	defer li.UnfreezeCloseAmount(frozenAmount)

	id := fmt.Sprintf("%d_%d", time.Now().UnixNano(), li.PosID)
	logger := log.With(li.logger, "id", id, "mode", rate.Swap1MSwap2T)

	cv1 := li.swap1.ContractVal()
	cv2 := li.swap2.ContractVal()
	var (
		swap2Side ccexgo.OrderSide
		// price     float64
	)
	if swap1Order.Side == ccexgo.OrderSideCloseLong {
		swap2Side = ccexgo.OrderSideCloseShort
		// price = swap.Asks[0].Price
	} else {
		swap2Side = ccexgo.OrderSideCloseLong
		// price = swap.Bids[0].Price
	}
	// swap1Ccy := swap1Order.Filled.Mul(cv1).Div(swap1Order.AvgPrice)
	exp := 0
	if swapC2.GetExchangeName() == exchange.Okex5 { // 可能只有okx才有小数张，取决于acts接口转换导致？
		exp = actsSpot.CalcExponent(decimal.NewFromFloat(li.swap2ActsSymbol.AmountTickSize))
	}

	swap1Order.Filled = swap1Order.Filled.Round(int32(li.amount1Exp))
	realAmount := swap1Order.Filled
	if !li.inverse { // 重算个数
		realAmount = swap1Order.Filled.RoundFloor(int32(li.amountMinExp))
		if realAmount.IsZero() {
			// 直接计入swap1敞口
			li.AddSwapExposure(ctx, swap1Order.Filled.Neg(), "swap1")
			return nil, fmt.Errorf("平仓swap1成交, 数量对冲swap2时, 精度将%s->%s, 导致数量为0, 直接计入swap1敞口, 跳过仓位操作", swap1Order.Filled.String(), realAmount.String())
		}
	}

	// swap1Order Fee 可能因为unit缩减需要重算, 所以需要重算fee1Real
	fee1Real := swap1Order.Fee.Mul(realAmount).Div(swap1Order.Filled)

	// 直接基于swap1Order的数据计算swap2的下单数量
	// 对于对冲操作，unit2应该等于realAmount（经过精度调整后的数量）
	unit2 := realAmount
	// if unit2.IsZero() {
	// 	level.Warn(logger).Log("message", "unit2 is zero, return")
	// 	return nil, fmt.Errorf("【人工处理】 closeHedgeSwap2 平仓计算swap2数量为0, 请检查！！！, swap1Order=%+v", swap1Order)
	// }

	// level.Info(logger).Log("message", "loop for pos", "traverse", cp.tradverse, "deal_count", cp.count,
	// 	"order_unit", cp.Unit2, "funding_earn", cp.Funding,
	// 	"fee_earn", cp.Fee, "swap_to_spot", cp.SpotToSwap, "spot_keep", cp.SpotKeep, "usdt_used", cp.Ccy)

	one := decimal.NewFromInt(1)

	swap2Order, err := swapC2.MarketOrder(ctxlog.SetLog(ctx, logger), swap2Side, unit2, decimal.Zero, nil)
	if err != nil {
		// 下单失败，在锁内进行splitPos2计算并更新仓位
		li.mu.Lock()
		cp, _, newPos := li.splitPos2(cv2, swapC2.FeeRate().OpenTaker, realAmount.InexactFloat64(), swap1Order.AvgPrice.InexactFloat64(), nil, swap, totalLimit, swap2Side, rate.Swap1MSwap2T, exp)
		if cp != nil {
			li.Poss = newPos
			li.updateLocalTotalPos()
		}
		li.mu.Unlock()

		errType := swapC2.HandError(err)
		li.UpdateBannedTime(errType, false)
		li.CheckErrShareBan(errType, swapC2.GetExchangeName())
		if cp != nil {
			li.calcAvgOpenSpread(cp.Unit1, decimal.Zero, false)
		}
		processType := errToProcessType(errType)
		if processType == SystemProcess {
			li.AddSwapExposure(ctx, unit2, "swap2")
			return op, fmt.Errorf("【%s】[%s]swap2(%s)平仓失败 amount=%s, side=%s, swap1成交, swap1Order=%+v \n错误原因:%s, 已计入swap2敞口", processType, rate.Swap1MSwap2T, swapC2.GetExchangeName(), unit2, swap2Side, swap1Order, swapC2.HandError(err))
		} else {
			return op, fmt.Errorf("【人工处理】[%s]swap2(%s)平仓失败 amount=%s, side=%s, swap1成交, swap1Order=%+v \n错误原因:%s", rate.Swap1MSwap2T, swapC2.GetExchangeName(), unit2, swap2Side, swap1Order, swapC2.HandError(err))
		}
	}

	if !swap1Order.Filled.Equal(realAmount) {
		li.AddSwapExposure(ctx, swap1Order.Filled.Sub(realAmount).Neg(), "swap1")
	}

	// 下单成功，在锁内进行splitPos2计算并更新仓位
	li.mu.Lock()
	cp, closed, newPos := li.splitPos2(cv2, swapC2.FeeRate().OpenTaker, realAmount.InexactFloat64(), swap1Order.AvgPrice.InexactFloat64(), nil, swap, totalLimit, swap2Side, rate.Swap1MSwap2T, exp)
	li.Poss = newPos
	li.updateLocalTotalPos()
	li.mu.Unlock()

	if cp == nil {
		return nil, fmt.Errorf("两边都已成交, 选不出仓位, 跳过仓位操作及盈亏计算, swap1Order=%+v, swap2Order=%+v", swap1Order, swap2Order)
	}

	op.SetCcy(cp.CcyDeposit1, cp.CcyDeposit2)

	level.Info(logger).Log("message", "loop for pos", "traverse", cp.traverse, "deal_count", cp.count,
		"order_unit", cp.Unit2, "funding1", cp.Funding1, "funding_real", cp.Funding1Real, "funding2", cp.Funding2, "finding2_real", cp.Funding2Real, "fee1", cp.Fee1, "fee1_real", cp.Fee1Real,
		"fee2", cp.Fee2, "fee2_real", cp.Fee2Real, "deposit1", cp.CcyDeposit1, "deposit1_real", cp.CcyDeposit1Real, "deposit2", cp.CcyDeposit2Real)

	// 币安taker可能是bnb抵扣费, 要计算出对应的usdt费
	var bnbEqualFee decimal.Decimal
	if strings.Contains(swapC2.GetExchangeName(), exchange.Binance) && swap2Order.FeeCurrency == `BNB` && li.swap2.String() != `BNBUSDT` {
		bnbEqualFee = swap2Order.Filled.Mul(swap2Order.AvgPrice).Mul(swapC2.FeeRate().OpenTaker.Mul(decimal.NewFromFloat(0.9))).Neg()
		swap2Order.Fee = bnbEqualFee
	}

	updatePosIDs := []string{}
	var (
		swap1Profit     decimal.Decimal
		swap2Profit     decimal.Decimal
		swap2ProfitReal decimal.Decimal
		// targetRealCcy decimal.Decimal
	)

	// swap2Ccy := swap2Order.Filled.Mul(cv2).Div(swap2Order.AvgPrice)

	for _, pos := range closed {
		profit1 := pos.Unit1.Mul(cv1).Mul(one.Div(swap1Order.AvgPrice).Sub(one.Div(pos.OpenPrice1)))
		profit2 := pos.Unit2.Mul(cv2).Mul(one.Div(pos.OpenPrice2).Sub(one.Div(swap2Order.AvgPrice)))
		profitReal2 := pos.Unit2Real.Mul(cv2).Mul(one.Div(pos.OpenPrice2).Sub(one.Div(swap2Order.AvgPrice)))
		if !li.inverse {
			profit1 = pos.Unit1.Mul(pos.OpenPrice1.Sub(swap1Order.AvgPrice))
			profit2 = pos.Unit2.Mul(swap2Order.AvgPrice.Sub(pos.OpenPrice2))
			profitReal2 = pos.Unit2Real.Mul(swap2Order.AvgPrice.Sub(pos.OpenPrice2))
		}
		if swap2Side == ccexgo.OrderSideCloseShort { //平空,buy
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
	swap2Earn := swap2Profit.Add(cp.Funding2).Add(cp.Fee2Real).Add(swap2Order.Fee)
	swap2EarnReal := swap2ProfitReal.Add(cp.Funding2Real).Add(cp.Fee2Real).Add(swap2Order.Fee)

	level.Info(logger).Log("message", "close swap2 order", "id", swap2Order.ID.String(), "unit2", unit2,
		"swap2_avg_price", swap2Order.AvgPrice, "swap2_profit", swap2Profit, "swap2_earn", swap2Earn, "swap2_order_fee", swap2Order.Fee,
		"swap1_profit", swap1Profit, "swap1_earn", swap1Earn, "swap2_earn_real", swap2EarnReal, "bnb_equal_fee", bnbEqualFee)

	// realAmount := tf.Add(cp.SpotKeep)
	// closeExposure := realAmount.Sub(spotOrder.Filled)

	// ts := time.Now()
	// if err := li.transfer.Transfer(ctx, exchange.AccontTypeSwap, exchange.AccontTypeSpot, li.spot.Base(), tf); err != nil {
	// 	balance, e := swapC.FetchBalance(ctx)
	// 	if e != nil {
	// 		level.Warn(logger).Log("message", "fetch balance fail", "error", e.Error())
	// 		balance = []ccexgo.Balance{}
	// 	} else {
	// 		li.WithdrawlAvailable = balance[0].Free
	// 	}
	// 	newErr := fmt.Errorf("[%s]划转合约到现货失败 amount=%s balance=%+v\n错误原因:%s", rate.SpotMSwapT, tf, balance, swapC.HandError(err))
	// 	if err = message.SendImportant(ctx, message.NewOperateFail(fmt.Sprintf("%s 平仓失败", li.swap.String()), newErr.Error())); err != nil {
	// 		level.Warn(logger).Log("message", "send lark failed", "err", err.Error())
	// 	}
	// }
	// duration = time.Since(ts)
	// level.Info(logger).Log("message", "transfer from swap to spot", "amount", tf)

	level.Info(logger).Log("message", "close order done", "swap2_order_id", swap2Order.ID.String(), "swap_avg_price", swap2Order.AvgPrice,
		"swap2_order_unit", unit2, "swap1_order_id", swap1Order.ID.String(),
		"swap1_avg_price", swap1Order.AvgPrice, "swap1_order_fee", swap1Order.Fee, "swap1_fee_real", fee1Real)

	// li.TotalExposure = li.TotalExposure.Add(closeExposure)
	li.calcAvgOpenSpread(cp.Unit1, decimal.Zero, false)
	if err := li.updateAccountInfo(ctx, spotC, swapC1, swapC2, logger); err != nil {
		level.Warn(logger).Log("message", "update account info failed", "err", err.Error(), "sleep", "80s")
		li.UpdateBannedTime(nil, true)
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
	op.AddAny("mode", "swap1_maker")
	op.AddAny("pos_ids", strings.Join(updatePosIDs, ","))
	op.AddAny("maker_info", op.GetMakerInfo())
	op.End()
	return op, nil

}

// func (li *listImpl) closeHedgeSwapLong(ctx context.Context, spotC exchange.Client, swapC exchange.SwapClient,
// 	spotOrder *exchange.CfOrder, swap *exchange.Depth, op *OrderResult, totalLimit float64) (*OrderResult, error) {

// 	id := fmt.Sprintf("%d_%d", time.Now().Unix(), li.PosID)
// 	logger := log.With(li.logger, "id", id, "mode", rate.SpotMSwapT)
// 	cv := li.swap.ContractVal()

// 	cp, closed, newPos := li.splitPos2(cv, swapC.FeeRate().OpenTaker, spotOrder.Filled.InexactFloat64(), spotOrder.AvgPrice.InexactFloat64(), nil, swap, totalLimit)
// 	if cp == nil {
// 		//no available pos to close
// 		return nil, nil
// 	}

// 	level.Info(logger).Log("message", "loop for pos", "traverse", cp.tradverse, "deal_count", cp.count,
// 		"order_unit", cp.Unit, "funding_earn", cp.Funding,
// 		"fee_earn", cp.Fee, "swap_to_spot", cp.SpotToSwap, "spot_keep", cp.SpotKeep, "usdt_used", cp.Ccy)

// 	op.SetCcy(cp.Ccy)

// 	swapOrder, err := swapC.MarketOrder(ctxlog.SetLog(ctx, logger), ccexgo.OrderSideCloseLong, cp.Unit.Mul(cv), decimal.Zero)
// 	if err != nil {
// 		var (
// 			preProfit decimal.Decimal
// 		)
// 		price := decimal.NewFromFloat(swap.Bids[0].Price)
// 		for _, pos := range closed {
// 			profit := pos.Unit.Mul(cv).Mul(price.Sub(pos.OpenPrice))
// 			preProfit = preProfit.Add(profit)
// 		}
// 		preSwapEarn := preProfit.Add(cp.Funding).Add(cp.Fee).Sub(cp.Unit.Mul(cv).Mul(price).Mul(swapC.FeeRate().CloseTaker))
// 		preAmt := cp.SpotToSwap.Add(preSwapEarn)

// 		preTf := preAmt.Truncate(8)

// 		li.Poss = newPos
// 		li.SpotKeep = li.SpotKeep.Sub(cp.SpotKeep)
// 		return op, fmt.Errorf("[%s]合约平仓失败 unit=%s pre_trans=%s\n错误原因:%s", rate.SpotMSwapT, cp.Unit, preTf, swapC.HandError(err))
// 	}

// 	updatePosIDs := []string{}
// 	var swapProfit decimal.Decimal
// 	var cost decimal.Decimal
// 	realUsed := spotOrder.Filled.Mul(spotOrder.AvgPrice)

// 	for _, pos := range closed {
// 		profit := pos.Unit.Mul(cv).Mul(swapOrder.AvgPrice.Sub(pos.OpenPrice))
// 		swapProfit = swapProfit.Add(profit)
// 		closeFee := pos.Unit.Mul(swapOrder.AvgPrice).Mul(swapC.FeeRate().OpenTaker).Neg()
// 		total := pos.Funding.Add(pos.Fee).Add(pos.SpotToSwap).Add(pos.SpotKeep).Add(swapProfit).Add(closeFee)
// 		if realUsed.LessThan(total) {
// 			x := realUsed.Div(pos.Funding.Add(pos.Fee).Add(pos.SpotToSwap).Add(pos.SpotKeep).Div(pos.Unit).Add(cv.Mul(swapOrder.AvgPrice.Sub(pos.OpenPrice).Sub(swapOrder.AvgPrice.Mul(swapC.FeeRate().OpenTaker)))))
// 			cost = cost.Add(x.Div(pos.Unit).Mul(pos.Ccy))
// 			// realUsed = decimal.Zero
// 		} else {
// 			realUsed = realUsed.Sub(total)
// 			cost = cost.Add(pos.Ccy)
// 		}

// 		updatePosIDs = append(updatePosIDs, pos.ID)
// 	}

// 	swapEarn := swapProfit.Add(cp.Funding).Add(cp.Fee).Add(swapOrder.Fee)
// 	amt := cp.SpotToSwap.Add(swapEarn)

// 	level.Info(logger).Log("message", "close swap order", "id", swapOrder.ID.String(), "unit", cp.Unit,
// 		"avg_price", swapOrder.AvgPrice, "profit", swapProfit, "swap_earn", swapEarn, "swap_order", swapOrder.Fee)

// 	var duration time.Duration
// 	tf := amt.Truncate(8)
// 	realAmount := tf.Add(cp.SpotKeep)
// 	closeExposure := realAmount.Sub(spotOrder.Filled.Mul(spotOrder.AvgPrice))

// 	ts := time.Now()
// 	if err := li.transfer.Transfer(ctx, exchange.AccontTypeSwap, exchange.AccontTypeSpot, li.spot.Quote(), tf); err != nil {
// 		balance, e := swapC.FetchBalance(ctx)
// 		if e != nil {
// 			level.Warn(logger).Log("message", "fetch balance fail", "error", e.Error())
// 			balance = []ccexgo.Balance{}
// 		} else {
// 			li.WithdrawlAvailable = balance[0].Free
// 		}

// 		newErr := fmt.Errorf("[%s]划转合约到现货失败 amount=%s balance=%+v\n错误原因:%s", rate.SpotMSwapT, tf, balance, swapC.HandError(err))
// 		if err = message.SendImportant(ctx, message.NewOperateFail(fmt.Sprintf("%s 平仓失败", li.swap.String()), newErr.Error())); err != nil {
// 			level.Warn(logger).Log("message", "send lark failed", "err", err.Error())
// 		}

// 		// return op, fmt.Errorf("[%s]划转合约到现货失败 amount=%s balance=%+v\n错误原因:%s", rate.SpotMSwapT, tf, balance, swapC.HandError(err))
// 	}
// 	duration = time.Since(ts)
// 	level.Info(logger).Log("message", "transfer from swap to spot", "amount", tf)

// 	level.Info(logger).Log("message", "close order done", "swap_order_id", swapOrder.ID.String(), "swap_avg_price", swapOrder.AvgPrice,
// 		"swap_order_unit", cp.Unit, "swap_to_spot_amount", tf, "swap_to_spot_duration", duration, "spot_order_id", spotOrder.ID.String(),
// 		"spot_order_avg_price", spotOrder.AvgPrice, "spot_order_fee", spotOrder.Fee)

// 	op.End()

// 	li.Poss = newPos
// 	li.SpotKeep = li.SpotKeep.Sub(cp.SpotKeep)
// 	li.TotalExposure = li.TotalExposure.Add(closeExposure)

// 	if err := li.updateAccountInfo(ctx, spotC, swapC, logger); err != nil {
// 		level.Warn(logger).Log("message", "update account info failed", "err", err.Error(), "sleep", "80s")
// 		time.Sleep(80 * time.Second) // 休眠80s
// 	}

// 	//计算盈亏
// 	// var realUsdt = cp.targetUsdtUsed.Mul(spotOrder.Filled.Div(targetRealCcy))
// 	earn := spotOrder.Filled.Sub(spotOrder.Fee).Sub(cost)

// 	realEarn := spotOrder.Filled.Sub(spotOrder.Fee).Sub(cost)

// 	op.AddDecimal("sr_close_real", swapOrder.AvgPrice.Div(spotOrder.AvgPrice).Sub(decimal.NewFromInt(1)))
// 	op.AddOrder("spot", spotOrder)
// 	op.AddOrder("swap", swapOrder)
// 	op.AddDepth("swap", swap)
// 	op.AddDecimal("spot_keep", cp.SpotKeep)
// 	op.AddDecimal("swap_profit", swapProfit)
// 	op.AddDecimal("funding", cp.Funding)
// 	op.AddDecimal("usdt_used", cp.Ccy)
// 	op.AddDecimal("target_usdt_used", cp.targetUsdtUsed)
// 	op.AddDecimal("earn", earn)
// 	op.AddDecimal("earn_rate", earn.Div(cost).Mul(decimal.NewFromFloat(100)))
// 	op.AddDecimal("real_earn", realEarn)
// 	op.AddDecimal("real_earn_rate", realEarn.Div(cost).Mul(decimal.NewFromFloat(100)))
// 	op.AddDecimal("close_exposure", closeExposure)
// 	op.AddDecimal(`total_spot_amount`, realAmount)
// 	op.AddField("swap_to_spot_duration", duration.String())
// 	op.AddField("mode", "spot_maker")

// 	op.AddField("pos_ids", strings.Join(updatePosIDs, ","))
// 	op.AddDecimal("spot_order_fee", spotOrder.Fee.Neg())
// 	op.AddDecimal("swap_order_fee", swapOrder.Fee)
// 	return op, nil
// }

// splitPos 根据平仓数量以及totalLimit对仓位进行划分调整
func (li *listImpl) splitPos2(cv decimal.Decimal, takerFeeRate decimal.Decimal, closeTradeAmount float64, closeAvgPrice float64, spot *exchange.Depth,
	swap *exchange.Depth, totalLimit float64, swapSide ccexgo.OrderSide, mode rate.RunMode, exp int) (cp *closeParam, closed []Pos, new []Pos) {
	// if spot != nil {
	// return li.splitPosWithSwap(cv, takerFeeRate, closeTradeAmount, closeAvgPrice, spot, totalLimit)
	// }
	// if !li.isversed { //传U
	// 	closeTradeAmount = closeTradeAmount * closeAvgPrice
	// }
	if mode == rate.Swap1TSwap2M {
		return li.splitPosWithSwap2(cv, takerFeeRate, closeTradeAmount, closeAvgPrice, swap, totalLimit, swapSide, mode, exp)
	}
	return li.splitPosWithSwap1(cv, takerFeeRate, closeTradeAmount, closeAvgPrice, swap, totalLimit, swapSide, mode, exp)
}

func (li *listImpl) splitPosWithSwap1(cv decimal.Decimal, takerFeeRate decimal.Decimal, closeTradeAmount, closeAvgPrice float64, swap *exchange.Depth,
	totalLimit float64, swap2Side ccexgo.OrderSide, mode rate.RunMode, exp int) (cp *closeParam, closed []Pos, new []Pos) {
	amount := decimal.NewFromFloat(closeTradeAmount) //U本位是币，币本位是张
	remainAmount := amount
	var (
		orderUnit        decimal.Decimal
		funding1Earn     decimal.Decimal
		funding1RealEarn decimal.Decimal
		funding2Earn     decimal.Decimal
		funding2RealEarn decimal.Decimal
		cost1            decimal.Decimal
		cost1Real        decimal.Decimal
		cost2            decimal.Decimal
		cost2Real        decimal.Decimal
		fee1Earn         decimal.Decimal
		fee1RealEarn     decimal.Decimal
		fee2Earn         decimal.Decimal
		fee2RealEarn     decimal.Decimal

		dealCount int
		traverse  int
	)
	for idx, p := range li.Poss {
		if remainAmount.IsZero() { //将剩余的pos补齐
			new = append(new, li.Poss[idx:]...)
			break
		}
		traverse += 1

		// 自动去掉一边0仓位
		if p.Unit1.IsZero() {
			level.Warn(li.logger).Log("message", "splitPosWithSwap1", "p.Unit1", p.Unit1)
			if !p.Unit2.IsZero() {
				li.TotalSwap2Exposure = li.TotalSwap2Exposure.Add(p.Unit2)
			}
			continue
		}

		if p.Unit2.IsZero() {
			level.Warn(li.logger).Log("message", "splitPosWithSwap1", "p.Unit2", p.Unit2)
			if !p.Unit1.IsZero() { // 只支持u本位
				li.TotalSwap1Exposure = li.TotalSwap1Exposure.Add(p.Unit1)
			}
			continue
		}

		var (
			dealUnitX  decimal.Decimal
			dealUnit2  decimal.Decimal
			dealAmount decimal.Decimal
		)

		sr := swap.Bids[0].Price/closeAvgPrice - 1
		if mode != rate.Swap1MSwap2M {
			if (swap2Side == ccexgo.OrderSideCloseShort && ((p.SrOpen-sr)/(1+sr)*(p.OpenPrice1.InexactFloat64()/closeAvgPrice) < totalLimit)) || // 买
				(swap2Side == ccexgo.OrderSideCloseLong && ((sr-p.SrOpen)/(1+sr)*(p.OpenPrice1.InexactFloat64()/closeAvgPrice) < totalLimit)) { // 卖
				new = append(new, p)
				continue
			}
		}

		if p.Unit1.GreaterThan(remainAmount) {
			if li.inverse {
				dealUnitX = remainAmount.Div(p.Unit1).Mul(p.Unit2Real) //小数张
				dealUnit2 = decimal.Min(dealUnitX.Round(0), p.Unit2)
				dealAmount = remainAmount
			} else {
				dealUnitX = remainAmount.Div(p.Unit1).Mul(p.Unit2Real) //小数个
				dealUnit2 = decimal.Min(dealUnitX.Div(cv).Round(int32(exp)).Mul(cv), p.Unit2)
				// dealUnit = remainAmount.Div(p.Funding.Add(p.SpotKeep).Add(p.SpotToSwap).Add(p.Fee).Div(p.Unit).Add(cv.Mul(
				// 	decimal.NewFromFloat(swap.Bids[0].Price).Sub(p.OpenPrice).Sub(decimal.NewFromFloat(swap.Bids[0].Price).Mul(takerFeeRate))))).Ceil()
				dealAmount = remainAmount
			}
		} else {
			// dealUnitX = remainAmount
			dealUnit2 = p.Unit2
			dealAmount = p.Unit1
		}

		//根据成交数量计算对应的funding, rate
		dealRate1 := dealAmount.Div(p.Unit1).Truncate(8)
		var dealRate2 decimal.Decimal
		if p.Unit2.IsZero() {
			dealRate2 = decimal.Zero
		} else {
			dealRate2 = dealUnit2.Div(p.Unit2).Truncate(8) //整数比例
		}

		dealFunding1 := p.Funding1.Mul(dealRate1).Truncate(8)
		dealFunding1Real := p.Funding1Real.Mul(dealRate1).Truncate(8)
		dealFunding2Real := p.Funding2Real.Mul(dealRate1).Truncate(8)
		dealFee1 := p.Fee1.Mul(dealRate1).Truncate(8)
		dealFee1Real := p.Fee1Real.Mul(dealRate1).Truncate(8)
		dealCcy1 := p.Ccy1.Mul(dealRate1).Truncate(8)
		dealUnit1Real := p.Unit1Real.Mul(dealRate1).Truncate(8)
		dealCcyDeposit1 := p.CcyDeposit1.Mul(dealRate1).Truncate(8)
		dealCcyDeposit1Real := p.CcyDeposit1Real.Mul(dealRate1).Truncate(8)
		dealCcyDeposit2Real := p.CcyDeposit2Real.Mul(dealRate1).Truncate(8)

		dealUnit1 := dealAmount
		dealUnit2Real := p.Unit2Real.Mul(dealRate1).Truncate(8)
		dealFee2Real := p.Fee2Real.Mul(dealRate1).Truncate(8)
		dealCcyDeposit := p.CcyDeposit.Mul(dealRate1).Truncate(8)
		dealCcyDepositReal := p.CcyDepositReal.Mul(dealRate1).Truncate(8)
		dealOpenExposure := p.OpenExposure.Mul(dealRate1).Truncate(8)

		dealFunding2 := p.Funding2.Mul(dealRate2).Truncate(8)
		dealFee2 := p.Fee2.Mul(dealRate2).Truncate(8)
		dealCcy2 := p.Ccy2.Mul(dealRate2).Truncate(8)
		dealCcyDeposit2 := p.CcyDeposit2.Mul(dealRate2).Truncate(8)

		orderUnit = orderUnit.Add(dealUnit2)
		funding1Earn = funding1Earn.Add(dealFunding1)
		funding1RealEarn = funding1RealEarn.Add(dealFunding1Real)
		funding2Earn = funding2Earn.Add(dealFunding2)
		funding2RealEarn = funding2RealEarn.Add(dealFunding2Real)
		fee1Earn = fee1Earn.Add(dealFee1)
		fee1RealEarn = fee1RealEarn.Add(dealFee1Real)
		fee2Earn = fee2Earn.Add(dealFee2)
		fee2RealEarn = fee2RealEarn.Add(dealFee2Real)
		cost1 = cost1.Add(dealCcyDeposit1)
		cost1Real = cost1Real.Add(dealCcyDeposit1Real)
		cost2 = cost2.Add(dealCcyDeposit2)
		cost2Real = cost2Real.Add(dealCcyDeposit2Real)

		remainAmount = remainAmount.Sub(dealAmount)
		dealCount += 1

		closed = append(closed, Pos{
			ID:              p.ID,
			SrOpen:          p.SrOpen,
			Funding1:        dealFunding1,
			Funding1Real:    dealFunding1Real,
			Funding2:        dealFunding2,
			Funding2Real:    dealFunding2Real,
			Unit1:           dealUnit1,
			Unit1Real:       dealUnit1Real,
			Unit2:           dealUnit2,
			Unit2Real:       dealUnit2Real,
			Fee1:            dealFee1,
			Fee1Real:        dealFee1Real,
			Fee2:            dealFee2,
			Fee2Real:        dealFee2Real,
			Ccy1:            dealCcy1,
			Ccy2:            dealCcy2,
			OpenPrice1:      p.OpenPrice1,
			OpenPrice2:      p.OpenPrice2,
			OpenExposure:    dealOpenExposure,
			CcyDeposit1:     dealCcyDeposit1,
			CcyDeposit1Real: dealCcyDeposit1Real,
			CcyDeposit2:     dealCcyDeposit2,
			CcyDeposit2Real: dealCcyDeposit2Real,
			CcyDeposit:      dealCcyDeposit,
			CcyDepositReal:  dealCcyDepositReal,
			Mode:            p.Mode,
		})

		if amt := p.Unit1.Sub(dealUnit1); amt.IsPositive() {
			new = append(new, Pos{
				ID:              p.ID,
				SrOpen:          p.SrOpen,
				Direction:       p.Direction,
				Funding1:        p.Funding1.Sub(dealFunding1),
				Funding1Real:    p.Funding1Real.Sub(dealFunding1Real),
				Funding2:        p.Funding2.Sub(dealFunding2),
				Funding2Real:    p.Funding2Real.Sub(dealFunding2Real),
				Unit1:           p.Unit1.Sub(dealUnit1),
				Unit1Real:       p.Unit1Real.Sub(dealUnit1Real),
				Unit2:           p.Unit2.Sub(dealUnit2),
				Unit2Real:       p.Unit2Real.Sub(dealUnit2Real),
				Fee1:            p.Fee1.Sub(dealFee1),
				Fee1Real:        p.Fee1Real.Sub(dealFee1Real),
				Fee2:            p.Fee2.Sub(dealFee2),
				Fee2Real:        p.Fee2Real.Sub(dealFee2Real),
				Ccy1:            p.Ccy1.Sub(dealCcy1),
				Ccy2:            p.Ccy2.Sub(dealCcy2),
				OpenPrice1:      p.OpenPrice1,
				OpenPrice2:      p.OpenPrice2,
				OpenExposure:    p.OpenExposure.Sub(dealOpenExposure),
				CcyDeposit1:     p.CcyDeposit1.Sub(dealCcyDeposit1),
				CcyDeposit1Real: p.CcyDeposit1Real.Sub(dealCcyDeposit1Real),
				CcyDeposit2:     p.CcyDeposit2.Sub(dealCcyDeposit2),
				CcyDeposit2Real: p.CcyDeposit2Real.Sub(dealCcyDeposit2Real),
				CcyDeposit:      p.CcyDeposit.Sub(dealCcyDeposit),
				CcyDepositReal:  p.CcyDepositReal.Sub(dealCcyDepositReal),
				Mode:            p.Mode,
			})
		}
	}

	if orderUnit.IsZero() {
		return
	}

	cp = &closeParam{
		Pos: Pos{
			Unit2:           orderUnit,
			Funding1:        funding1Earn,
			Funding1Real:    funding1RealEarn,
			Funding2:        funding2Earn,
			Funding2Real:    funding2RealEarn,
			Fee1:            fee1Earn,
			Fee1Real:        fee1RealEarn,
			Fee2:            fee2Earn,
			Fee2Real:        fee2RealEarn,
			CcyDeposit1:     cost1,
			CcyDeposit1Real: cost1Real,
			CcyDeposit2:     cost2,
			CcyDeposit2Real: cost2Real,
		},
		traverse: traverse,
		count:    dealCount,
	}
	return
}

// func (li *listImpl) splitPosWithSwap(cv decimal.Decimal, takerFeeRate decimal.Decimal, closeTradeAmount float64, closeAvgPrice float64, spot *exchange.Depth, totalLimit float64) (cp *closeParam, closed []ClosePos, new []Pos) {
// 	amount := decimal.NewFromFloat(closeTradeAmount)
// 	if !li.isversed { //U本位也变成张数
// 		amount = amount.Div(cv)
// 	}
// 	remainAmount := amount
// 	var (
// 		orderUnit      decimal.Decimal
// 		fundingEarn    decimal.Decimal
// 		feeEarn        decimal.Decimal
// 		swapToSpot     decimal.Decimal
// 		spotKeep       decimal.Decimal
// 		ccyUsed        decimal.Decimal
// 		targetUsdtUsed decimal.Decimal
// 		dealCount      int
// 		traverse       int
// 	)

// 	for idx, p := range li.Poss {
// 		if remainAmount.IsZero() { //将剩余的pos补齐
// 			new = append(new, li.Poss[idx:]...)
// 			break
// 		}
// 		traverse += 1

// 		var (
// 			dealUnit decimal.Decimal
// 		)
// 		// if li.isversed {
// 		// } else {
// 		// 	srClose := closeAvgPrice/spot.Asks[0].Price - 1
// 		// 	if (srClose-p.SrOpen)/(1+p.SrOpen)+p.Funding.Div(decimal.NewFromFloat(closeAvgPrice)).Div(p.Ccy).InexactFloat64() <= totalLimit {
// 		// 		new = append(new, p)
// 		// 		continue
// 		// 	}
// 		// }

// 		if p.Unit.GreaterThan(remainAmount) {
// 			dealUnit = remainAmount
// 		} else {
// 			dealUnit = p.Unit
// 		}

// 		//根据成交数量计算对应的funding, rate
// 		dealRate := dealUnit.Div(p.Unit)

// 		dealFunding := p.Funding.Mul(dealRate)
// 		dealSpotToSwap := p.SpotToSwap.Mul(dealRate)
// 		dealFee := p.Fee.Mul(dealRate)
// 		dealCcy := p.Ccy.Mul(dealRate)
// 		dealSpotKeep := p.SpotKeep.Mul(dealRate).Truncate(8)

// 		orderUnit = orderUnit.Add(dealUnit)
// 		fundingEarn = fundingEarn.Add(dealFunding)
// 		swapToSpot = swapToSpot.Add(dealSpotToSwap)
// 		feeEarn = feeEarn.Add(dealFee)
// 		ccyUsed = ccyUsed.Add(dealCcy)
// 		spotKeep = spotKeep.Add(dealSpotKeep)
// 		remainAmount = remainAmount.Sub(dealUnit)
// 		dealCount += 1

// 		closed = append(closed, ClosePos{
// 			Pos: Pos{
// 				ID:         p.ID,
// 				SrOpen:     p.SrOpen,
// 				Funding:    dealFunding,
// 				Fee:        dealFee,
// 				SpotToSwap: dealSpotToSwap,
// 				Ccy:        dealCcy,
// 				Unit:       dealUnit,
// 				OpenPrice:  p.OpenPrice,
// 				SpotKeep:   dealSpotKeep,
// 				Mode:       p.Mode,
// 			},
// 		})

// 		if amt := p.Unit.Sub(dealUnit); amt.IsPositive() {
// 			new = append(new, Pos{
// 				ID:         p.ID,
// 				SrOpen:     p.SrOpen,
// 				Funding:    p.Funding.Sub(dealFunding),
// 				Fee:        p.Fee.Sub(dealFee),
// 				SpotToSwap: p.SpotToSwap.Sub(dealSpotToSwap),
// 				Ccy:        p.Ccy.Sub(dealCcy),
// 				Unit:       amt,
// 				OpenPrice:  p.OpenPrice,
// 				SpotKeep:   p.SpotKeep.Sub(dealSpotKeep),
// 				Mode:       p.Mode,
// 			})
// 		}
// 	}

// 	if orderUnit.IsZero() {
// 		return
// 	}

// 	cp = &closeParam{
// 		Pos: Pos{
// 			Funding:    fundingEarn,
// 			Fee:        feeEarn,
// 			Unit:       orderUnit,
// 			SpotToSwap: swapToSpot,
// 			Ccy:        ccyUsed,
// 			SpotKeep:   spotKeep,
// 		},
// 		tradverse:      traverse,
// 		count:          dealCount,
// 		targetUsdtUsed: targetUsdtUsed,
// 	}
// 	return
// }
