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

// OpenSwap limit开仓
func (li *listImpl) OpenSwap(ctx context.Context, swapC exchange.SwapClient,
	srOpen float64, openTradeUnit float64, openTickNum float64,
	swap *exchange.Depth, op *OrderResult) (*OrderResult, *exchange.CfOrder, string, error) {
	// if li.isversed {
	// 	return li.openSwapShort(ctx, swapC, srOpen, openTradeUnit, openTickNum, swap, op)
	// }
	// return li.openSwapLong(ctx, swapC, srOpen, openTradeUnit, openTickNum, swap, op)
	return nil, nil, ``, fmt.Errorf(`not supported`)
}

func (li *listImpl) OpenHedgeSwap1(ctx context.Context, spotC exchange.Client, swapC1 exchange.SwapClient,
	swapC2 exchange.SwapClient, swap2Order *exchange.CfOrder, swap *exchange.Depth, op *OrderResult) (or *OrderResult, err error) {
	start := time.Now()
	defer func() {
		if e := recover(); e != nil {
			err = utilCommon.RecoverWithLog(li.logger, "OpenHedgeSwap1", start, e)
		}
	}()
	return li.openHedgeSwap1(ctx, spotC, swapC1, swapC2, swap2Order, swap, op)
}

// swap2开空或开多
func (li *listImpl) openHedgeSwap1(ctx context.Context, spotC exchange.Client, swapC1 exchange.SwapClient,
	swapC2 exchange.SwapClient, swap2Order *exchange.CfOrder, swap *exchange.Depth, op *OrderResult) (*OrderResult, error) {

	id := op.GetOpID()
	logger := log.With(li.logger, "id", id, "mode", rate.Swap1TSwap2M)
	cv1 := li.swap1.ContractVal()
	cv2 := li.swap2.ContractVal()
	one := decimal.NewFromInt(1)

	// tf := swap1Order.Filled.Mul(one.Sub(spotC.FeeRate().OpenMaker)) // 实际不转敞口个数，对冲敞口
	swap2Ccy := swap2Order.Filled.Mul(cv1).Div(swap2Order.AvgPrice)
	if !li.inverse {
		swap2Ccy = swap2Order.Filled
	}

	// shouldAmt := tf.Mul(decimal.NewFromFloat(swap.Bids[0].Price)).Div(one.Add(swapC2.FeeRate().OpenTaker)).Div(cv2)
	// amt := shouldAmt.Truncate(0)        // 截断
	var (
		swap1Side ccexgo.OrderSide
		price     float64
	)
	if swap2Order.Side == ccexgo.OrderSideBuy {
		swap1Side = ccexgo.OrderSideSell
		price = swap.Bids[0].Price
	} else {
		swap1Side = ccexgo.OrderSideBuy
		price = swap.Asks[0].Price
	}
	shouldAmt := swap2Ccy.Mul(decimal.NewFromFloat(price)).Div(cv1) // 小数张
	amt := shouldAmt.Round(0)
	if !li.inverse {
		shouldAmt = swap2Ccy.Div(cv1)
		exp := 0
		if swapC1.GetExchangeName() == exchange.Okex5 { // 可能只有okx才有小数张，取决于acts接口转换导致？
			exp = actsSpot.CalcExponent(decimal.NewFromFloat(li.swap1ActsSymbol.AmountTickSize))
		}
		// 向下约, 把敞口转移到精度更好的合约
		amt = shouldAmt.RoundFloor(int32(exp)).Mul(cv1)
	}

	if amt.Equal(decimal.Zero) { // 直接计入swap2敞口
		li.AddSwapExposure(ctx, swap2Order.Filled, "swap2")
		level.Warn(logger).Log("message", "amt is zero", "filled", swap2Order.Filled, "fee", swap2Order.Fee, "price", price, "total_swap2_exposure", li.TotalSwap2Exposure)
		// li.TotalExposure = li.TotalExposure.Add(swap1Order.Filled.Sub(spotOrder.Fee))
		return nil, fmt.Errorf("swap2成交价值算出swap1开0张,swap2成交数量=%s,price=%f, 当前swap2敞口=%s", swap2Ccy, price, li.TotalSwap2Exposure.String())
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

	swap1Order, err := swapC1.MarketOrder(ctx, swap1Side, amt, decimal.Zero, nil)
	if err != nil {
		errType := swapC1.HandError(err)
		li.UpdateBannedTime(errType, false)
		li.CheckErrShareBan(errType, swapC1.GetExchangeName())
		processType := errToProcessType(errType)
		if processType == SystemProcess {
			li.AddSwapExposure(ctx, swap2Order.Filled, "swap2")
			if errType.Code == exchange.AccountInsufficientBalance {
				return nil, fmt.Errorf("【人工处理】[%s]swap1(%s)开仓失败 amount=%s, side=%s, swap2成交, swap2Order=%+v \n错误原因:%s, 已计入swap2敞口, 需要手动处理余额问题", rate.Swap1TSwap2M, swapC2.GetExchangeName(), amt.String(), swap1Side, swap2Order, errType)
			}
			return nil, fmt.Errorf("【%s】[%s]swap1(%s)开仓失败 amount=%s, side=%s, swap2成交, swap2Order=%+v \n错误原因:%s, 已计入swap2敞口", processType, rate.Swap1TSwap2M, swapC2.GetExchangeName(), amt.String(), swap1Side, swap2Order, errType)
		} else {
			return nil, fmt.Errorf("【人工处理】[%s]swap1(%s)开仓失败 amount=%s, side=%s, swap2成交, swap2Order=%+v \n错误原因:%s", rate.Swap1TSwap2M, swapC2.GetExchangeName(), amt.String(), swap1Side, swap2Order, swapC1.HandError(err))
		}
	}

	// 币安taker可能是bnb抵扣费, 要计算出对应的usdt费
	var bnbEqualFee decimal.Decimal
	if strings.Contains(swapC1.GetExchangeName(), exchange.Binance) && swap1Order.FeeCurrency == `BNB` && li.swap1.String() != `BNBUSDT` {
		bnbEqualFee = swap1Order.Filled.Mul(swap1Order.AvgPrice).Mul(swapC1.FeeRate().OpenTaker.Mul(decimal.NewFromFloat(0.9))).Neg()
		swap1Order.Fee = bnbEqualFee
	}

	level.Info(logger).Log("message", "create swap1 market sell order", "id", swap1Order.ID.String(), "side", swap1Side, "bnb_equal_fee", bnbEqualFee)

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

	// 重算filled1及filled2, 以小的为标准, 实际上swap2挂单全部成交或者部分成交，一定是swap1 <= swap2
	// 所以其实是给swap2加敞口
	if filled1.GreaterThan(filled2) {
		filled1 = filled2
		li.AddSwapExposure(ctx, swap1Order.Filled.Sub(filled1), "swap1")
	} else if filled1.LessThan(filled2) {
		filled2 = filled1
		li.AddSwapExposure(ctx, swap2Order.Filled.Sub(filled2), "swap2")
	}

	swap1Ccy := filled1.Mul(cv1).Div(swap1Order.AvgPrice)

	realSrOpen := swap2Order.AvgPrice.Div(swap1Order.AvgPrice).Sub(one)
	// realUsdt := swap2Order.Filled.Mul(cv).Div(swapOrder.AvgPrice).Mul(one.Add(swapC2.FeeRate().OpenTaker)).Div(one.Sub(spotC.FeeRate().OpenMaker)).Mul(spotOrder.AvgPrice)
	unit1 := filled1
	unit2 := filled2
	ccy1 := swap1Ccy
	ccy2 := swap2Ccy
	unit1Real := swap2Ccy.Mul(swap1Order.AvgPrice).Div(cv2)
	unit2Real := swap2Ccy
	fee1Real := swap1Order.Fee.Mul(unit1Real).Div(filled1)
	fee2Real := swap2Order.Fee
	ccyDeposit1 := swap1Ccy.Div(li.level1)
	ccyDeposit2 := swap2Ccy.Div(li.level2)
	ccyDeposit1Real := ccyDeposit1.Mul(unit1Real).Div(filled1)
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
		ccyDeposit1Real = ccyDeposit1.Mul(unit1Real).Div(swap1Order.Filled)
	}
	ccyDeposit := ccyDeposit1.Add(ccyDeposit2)
	openExposure := ccy2.Sub(ccy1) // 币本位是币，u本位是u

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
	op.AddAny("deposit1_real", ccyDeposit1Real)
	op.AddAny("deposit2", ccyDeposit2)
	op.AddAny("deposit2_real", ccyDeposit2)
	op.AddAny("deposit", ccyDeposit)
	op.AddAny("deposit_real", ccyDeposit2.Add(ccyDeposit1Real))
	op.AddAny("open_exposure", openExposure) // 开仓敞口
	op.AddAny("mode", rate.Swap1TSwap2M)
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
		CcyDeposit1Real: ccyDeposit1Real,
		CcyDeposit2:     ccyDeposit2,
		CcyDeposit2Real: ccyDeposit2,
		CcyDeposit:      ccyDeposit,
		CcyDepositReal:  ccyDeposit2.Add(ccyDeposit1Real),
		Mode:            rate.Swap1TSwap2M,
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

// CloseSwap 平仓由于平仓数量与开仓数量不一致，一次平仓可能涉及多个仓位
func (li *listImpl) CloseSwap(ctx context.Context, swapC exchange.SwapClient,
	srClose float64, closeTradeVolume float64, closeTickNum float64, swap *exchange.Depth,
	op *OrderResult) (*OrderResult, *exchange.CfOrder, string, error) {
	// if li.isversed {
	// 	return li.closeSwapShort(ctx, swapC, srClose, closeTradeVolume, closeTickNum, swap, op)
	// }
	// return li.closeSwapLong(ctx, swapC, srClose, closeTradeVolume, closeTickNum, swap, op)
	return nil, nil, ``, fmt.Errorf(`not supported`)

}

func (li *listImpl) CloseHedgeSwap1(ctx context.Context, spotC exchange.Client, swapC1 exchange.SwapClient, swapC2 exchange.SwapClient,
	spotOrder *exchange.CfOrder, swap *exchange.Depth, op *OrderResult, totalLimit float64, frozenAmount decimal.Decimal) (or *OrderResult, err error) {
	start := time.Now()
	defer func() {
		if e := recover(); e != nil {
			err = utilCommon.RecoverWithLog(li.logger, "CloseHedgeSwap1", start, e)
		}
	}()
	return li.closeHedgeSwap1(ctx, spotC, swapC1, swapC2, spotOrder, swap, op, totalLimit, frozenAmount)
}

func (li *listImpl) closeHedgeSwap1(ctx context.Context, spotC exchange.Client, swapC1 exchange.SwapClient,
	swapC2 exchange.SwapClient, swap2Order *exchange.CfOrder, swap *exchange.Depth, op *OrderResult, totalLimit float64, frozenAmount decimal.Decimal) (*OrderResult, error) {
	defer li.UnfreezeCloseAmount(frozenAmount)

	id := fmt.Sprintf("%d_%d", time.Now().UnixNano(), li.PosID)
	logger := log.With(li.logger, "id", id, "mode", rate.Swap1TSwap2M)

	cv1 := li.swap1.ContractVal()
	cv2 := li.swap2.ContractVal()
	var (
		swap1Side ccexgo.OrderSide
		// price     float64
	)
	if swap2Order.Side == ccexgo.OrderSideCloseLong {
		swap1Side = ccexgo.OrderSideCloseShort
		// price = swap.Asks[0].Price
	} else {
		swap1Side = ccexgo.OrderSideCloseLong
		// price = swap.Bids[0].Price
	}
	// swap1Ccy := swap1Order.Filled.Mul(cv1).Div(swap1Order.AvgPrice)
	exp := 0
	if swapC1.GetExchangeName() == exchange.Okex5 { // 可能只有okx才有小数张，取决于acts接口转换导致？
		exp = actsSpot.CalcExponent(decimal.NewFromFloat(li.swap1ActsSymbol.AmountTickSize))
	}

	swap2Order.Filled = swap2Order.Filled.Round(int32(li.amount2Exp))
	realAmount := swap2Order.Filled
	if !li.inverse { // 重算个数
		realAmount = swap2Order.Filled.RoundFloor(int32(li.amountMinExp))
		if realAmount.IsZero() {
			// 直接计入swap2敞口
			li.AddSwapExposure(ctx, swap2Order.Filled.Neg(), "swap2")
			return nil, fmt.Errorf("平仓swap2成交, 数量对冲swap1时, 精度将%s->%s, 导致数量为0, 直接计入swap2敞口, 跳过仓位操作", swap2Order.Filled.String(), realAmount.String())
		}
	}

	// swap2Order Fee 可能因为unit缩减需要重算, 所以需要重算fee2Real
	fee2Real := swap2Order.Fee.Mul(realAmount).Div(swap2Order.Filled)

	// 直接基于swap2Order的数据计算swap1的下单数量
	// 对于对冲操作，unit1应该等于realAmount（经过精度调整后的数量）
	unit1 := realAmount
	// if unit1.IsZero() {
	// 	level.Warn(logger).Log("message", "unit1 is zero, return")
	// 	return nil, fmt.Errorf("【人工处理】 closeHedgeSwap1 平仓计算swap1数量为0, 请检查！！！, swap2Order=%+v", swap2Order)
	// }

	// level.Info(logger).Log("message", "loop for pos", "traverse", cp.tradverse, "deal_count", cp.count,
	// 	"order_unit", cp.Unit2, "funding_earn", cp.Funding,
	// 	"fee_earn", cp.Fee, "swap_to_spot", cp.SpotToSwap, "spot_keep", cp.SpotKeep, "usdt_used", cp.Ccy)

	one := decimal.NewFromInt(1)

	swap1Order, err := swapC1.MarketOrder(ctxlog.SetLog(ctx, logger), swap1Side, unit1, decimal.Zero, nil)
	if err != nil {
		// 下单失败，在锁内进行splitPos2计算并更新仓位
		li.mu.Lock()
		cp, _, newPos := li.splitPos2(cv1, swapC1.FeeRate().OpenTaker, realAmount.InexactFloat64(), swap2Order.AvgPrice.InexactFloat64(), nil, swap, totalLimit, swap1Side, rate.Swap1TSwap2M, exp)
		if cp != nil {
			li.Poss = newPos
			li.updateLocalTotalPos()
		}
		li.mu.Unlock()

		errType := swapC1.HandError(err)
		li.UpdateBannedTime(errType, false)
		li.CheckErrShareBan(errType, swapC1.GetExchangeName())
		if cp != nil {
			li.calcAvgOpenSpread(cp.Unit1, decimal.Zero, false)
		}
		processType := errToProcessType(errType)
		if processType == SystemProcess {
			li.AddSwapExposure(ctx, unit1, "swap1")
			return op, fmt.Errorf("【%s】[%s]swap1(%s)平仓失败 amount=%s, side=%s, swap2成交, swap2Order=%+v \n错误原因:%s, 已计入swap1敞口", processType, rate.Swap1TSwap2M, swapC1.GetExchangeName(), unit1, swap1Side, swap2Order, swapC1.HandError(err))
		} else {
			return op, fmt.Errorf("【人工处理】[%s]swap1(%s)平仓失败 amount=%s, side=%s, swap2成交, swap2Order=%+v \n错误原因:%s", rate.Swap1TSwap2M, swapC1.GetExchangeName(), unit1, swap1Side, swap2Order, swapC1.HandError(err))
		}
	}

	if !swap2Order.Filled.Equal(realAmount) { // swap2 平多了要开回来
		li.AddSwapExposure(ctx, swap2Order.Filled.Sub(realAmount).Neg(), "swap2")
	}

	// 下单成功，在锁内进行splitPos2计算并更新仓位
	li.mu.Lock()
	cp, closed, newPos := li.splitPos2(cv1, swapC1.FeeRate().OpenTaker, realAmount.InexactFloat64(), swap2Order.AvgPrice.InexactFloat64(), nil, swap, totalLimit, swap1Side, rate.Swap1TSwap2M, exp)
	li.Poss = newPos
	li.updateLocalTotalPos()
	li.mu.Unlock()

	if cp == nil {
		return nil, fmt.Errorf("两边都已成交, 选不出仓位, 跳过仓位操作及盈亏计算, swap1Order=%+v, swap2Order=%+v", swap1Order, swap2Order)
	}

	op.SetCcy(cp.CcyDeposit1, cp.CcyDeposit2)

	level.Info(logger).Log("message", "loop for pos", "traverse", cp.traverse, "deal_count", cp.count,
		"order_unit", cp.Unit1, "funding1", cp.Funding1, "funding_real", cp.Funding1Real, "funding2", cp.Funding2, "finding2_real", cp.Funding2Real, "fee1", cp.Fee1, "fee1_real", cp.Fee1Real,
		"fee2", cp.Fee2, "fee2_real", cp.Fee2Real, "deposit1", cp.CcyDeposit1, "deposit1_real", cp.CcyDeposit1Real, "deposit2", cp.CcyDeposit2Real)

	// 币安taker可能是bnb抵扣费, 要计算出对应的usdt费
	var bnbEqualFee decimal.Decimal
	if strings.Contains(swapC1.GetExchangeName(), exchange.Binance) && swap1Order.FeeCurrency == `BNB` && li.swap1.String() != `BNBUSDT` {
		bnbEqualFee = swap1Order.Filled.Mul(swap1Order.AvgPrice).Mul(swapC1.FeeRate().OpenTaker.Mul(decimal.NewFromFloat(0.9))).Neg()
		swap1Order.Fee = bnbEqualFee
	}

	updatePosIDs := []string{}
	var (
		swap1Profit     decimal.Decimal
		swap2Profit     decimal.Decimal
		swap1ProfitReal decimal.Decimal
		// targetRealCcy decimal.Decimal
	)

	// swap2Ccy := swap2Order.Filled.Mul(cv2).Div(swap2Order.AvgPrice)

	for _, pos := range closed {
		profit1 := pos.Unit1.Mul(cv1).Mul(one.Div(swap1Order.AvgPrice).Sub(one.Div(pos.OpenPrice1)))
		profit2 := pos.Unit2.Mul(cv2).Mul(one.Div(pos.OpenPrice2).Sub(one.Div(swap2Order.AvgPrice)))
		profitReal1 := pos.Unit1Real.Mul(cv1).Mul(one.Div(swap1Order.AvgPrice).Sub(one.Div(pos.OpenPrice1)))
		if !li.inverse {
			profit1 = pos.Unit1.Mul(pos.OpenPrice1.Sub(swap1Order.AvgPrice))
			profit2 = pos.Unit2.Mul(swap2Order.AvgPrice.Sub(pos.OpenPrice2))
			profitReal1 = pos.Unit1Real.Mul(pos.OpenPrice1.Sub(swap1Order.AvgPrice))
		}
		if swap1Side == ccexgo.OrderSideCloseLong { //平多,buy
			profit1 = profit1.Neg()
			profit2 = profit2.Neg()
			profitReal1 = profitReal1.Neg()
		}
		swap1Profit = swap1Profit.Add(profit1)
		swap2Profit = swap2Profit.Add(profit2)
		swap1ProfitReal = swap1ProfitReal.Add(profitReal1)
		updatePosIDs = append(updatePosIDs, pos.ID)
	}
	swap1Earn := swap1Profit.Add(cp.Funding1).Add(cp.Fee1Real).Add(swap1Order.Fee) // 币本位是币，u本位是u
	swap2Earn := swap2Profit.Add(cp.Funding2).Add(cp.Fee2Real).Add(fee2Real)
	swap1EarnReal := swap1ProfitReal.Add(cp.Funding1Real).Add(cp.Fee1Real).Add(swap1Order.Fee)

	level.Info(logger).Log("message", "close swap1 order", "id", swap1Order.ID.String(), "unit1", cp.Unit1,
		"swap1_avg_price", swap1Order.AvgPrice, "swap1_profit", swap1Profit, "swap1_earn", swap1Earn, "swap1_order_fee", swap1Order.Fee,
		"swap2_profit", swap2Profit, "swap2_earn", swap2Earn, "swap1_earn_real", swap1EarnReal, "bnb_equal_fee", bnbEqualFee)

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

	level.Info(logger).Log("message", "close order done", "swap1_order_id", swap1Order.ID.String(), "swap_avg_price", swap1Order.AvgPrice,
		"swap1_order_unit", unit1, "swap2_order_id", swap2Order.ID.String(),
		"swap2_avg_price", swap2Order.AvgPrice, "swap2_order_fee", swap2Order.Fee, "swap2_fee_real", fee2Real)

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
	realEarn := swap2Earn.Add(swap1EarnReal)

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
	op.AddAny("swap2_earn_real", swap1EarnReal)
	op.AddAny("earn", earn)
	op.AddAny("earn_rate", earn.Div(cost).Mul(decimal.NewFromFloat(100)))
	op.AddAny("real_earn", realEarn)
	op.AddAny("real_earn_rate", realEarn.Div(costReal).Mul(decimal.NewFromFloat(100)))
	// op.AddDecimal("close_exposure", closeExposure)
	op.AddAny("mode", rate.Swap1TSwap2M)
	op.AddAny("pos_ids", strings.Join(updatePosIDs, ","))
	op.AddAny("maker_info", op.GetMakerInfo())
	op.End()
	return op, nil
}

func (li *listImpl) splitPosWithSwap2(cv decimal.Decimal, takerFeeRate decimal.Decimal, closeTradeAmount, closeAvgPrice float64, swap *exchange.Depth,
	totalLimit float64, swap1Side ccexgo.OrderSide, mode rate.RunMode, exp int) (cp *closeParam, closed []Pos, new []Pos) {
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
			level.Warn(li.logger).Log("message", "splitPosWithSwap2", "p.Unit1", p.Unit1)
			if !p.Unit2.IsZero() {
				li.TotalSwap2Exposure = li.TotalSwap2Exposure.Add(p.Unit2)
			}
			continue
		}

		if p.Unit2.IsZero() {
			level.Warn(li.logger).Log("message", "splitPosWithSwap2", "p.Unit2", p.Unit2)
			if !p.Unit1.IsZero() { // 只支持u本位
				li.TotalSwap1Exposure = li.TotalSwap1Exposure.Add(p.Unit1)
			}
			continue
		}

		var (
			dealUnitX  decimal.Decimal
			dealUnit1  decimal.Decimal
			dealAmount decimal.Decimal
		)

		// sr := closeAvgPrice/swap.Bids[0].Price - 1
		// if mode != rate.Swap1MSwap2M {
		// 	if (swap1Side == ccexgo.OrderSideCloseLong && ((p.SrOpen-sr)/(1+sr)*(p.OpenPrice1.InexactFloat64()/closeAvgPrice) < totalLimit)) || // 买
		// 		(swap1Side == ccexgo.OrderSideCloseShort && ((sr-p.SrOpen)/(1+sr)*(p.OpenPrice1.InexactFloat64()/closeAvgPrice) < totalLimit)) { // 卖
		// 		new = append(new, p)
		// 		continue
		// 	}
		// }

		if p.Unit2.GreaterThan(remainAmount) {
			if li.inverse {
				dealUnitX = remainAmount.Div(p.Unit2).Mul(p.Unit1Real) //小数张
				dealUnit1 = decimal.Min(dealUnitX.Round(0), p.Unit1)
				dealAmount = remainAmount
			} else {
				dealUnitX = remainAmount.Div(p.Unit2).Mul(p.Unit1Real) //小数个
				dealUnit1 = decimal.Min(dealUnitX.Div(cv).Round(int32(exp)).Mul(cv), p.Unit1)
				// dealUnit = remainAmount.Div(p.Funding.Add(p.SpotKeep).Add(p.SpotToSwap).Add(p.Fee).Div(p.Unit).Add(cv.Mul(
				// 	decimal.NewFromFloat(swap.Bids[0].Price).Sub(p.OpenPrice).Sub(decimal.NewFromFloat(swap.Bids[0].Price).Mul(takerFeeRate))))).Ceil()
				dealAmount = remainAmount
			}
		} else {
			// dealUnitX = remainAmount
			dealUnit1 = p.Unit1
			dealAmount = p.Unit2
		}

		//根据成交数量计算对应的funding, rate
		dealRate2 := dealAmount.Div(p.Unit2).Truncate(8)
		var dealRate1 decimal.Decimal
		if p.Unit1.IsZero() {
			dealRate1 = decimal.Zero
		} else {
			dealRate1 = dealUnit1.Div(p.Unit1).Truncate(8) //整数比例
		}

		dealFunding2 := p.Funding2.Mul(dealRate2).Truncate(8)
		dealFunding1Real := p.Funding1Real.Mul(dealRate2).Truncate(8)
		dealFunding2Real := p.Funding2Real.Mul(dealRate2).Truncate(8)
		dealFee2 := p.Fee2.Mul(dealRate2).Truncate(8)
		dealFee2Real := p.Fee2Real.Mul(dealRate2).Truncate(8)
		dealCcy2 := p.Ccy2.Mul(dealRate2).Truncate(8)
		dealUnit2Real := p.Unit2Real.Mul(dealRate2).Truncate(8)
		dealCcyDeposit2 := p.CcyDeposit2.Mul(dealRate2).Truncate(8)
		dealCcyDeposit1Real := p.CcyDeposit1Real.Mul(dealRate2).Truncate(8)
		dealCcyDeposit2Real := p.CcyDeposit2Real.Mul(dealRate2).Truncate(8)

		dealUnit2 := dealAmount
		dealUnit1Real := p.Unit1Real.Mul(dealRate2).Truncate(8)
		dealFee1Real := p.Fee1Real.Mul(dealRate2).Truncate(8)
		dealCcyDeposit := p.CcyDeposit.Mul(dealRate2).Truncate(8)
		dealCcyDepositReal := p.CcyDepositReal.Mul(dealRate2).Truncate(8)
		dealOpenExposure := p.OpenExposure.Mul(dealRate2).Truncate(8)

		dealFunding1 := p.Funding1.Mul(dealRate1).Truncate(8)
		dealFee1 := p.Fee1.Mul(dealRate1).Truncate(8)
		dealCcy1 := p.Ccy1.Mul(dealRate1).Truncate(8)
		dealCcyDeposit1 := p.CcyDeposit1.Mul(dealRate1).Truncate(8)

		orderUnit = orderUnit.Add(dealUnit1)
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

		if amt := p.Unit2.Sub(dealUnit2); amt.IsPositive() {
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
			Unit1:           orderUnit,
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
