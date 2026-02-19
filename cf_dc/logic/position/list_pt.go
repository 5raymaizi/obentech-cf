package position

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cf_arbitrage/exchange"
	actsSpot "cf_arbitrage/exchange/acts/spot"
	"cf_arbitrage/logic/config"
	"cf_arbitrage/logic/rate"
	utilCommon "cf_arbitrage/util/common"

	ccexgo "github.com/NadiaSama/ccexgo/exchange"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/log/level"
	"github.com/shopspring/decimal"
)

// tm pre taker模式 https://digifinex.sg.larksuite.com/docx/HdYWd2UTNoICItxmjfSlouNNg4e

// OpenSpot taker开仓
func (li *listImpl) OpenSwapTakerOrder(ctx context.Context, config *config.Config, swapC exchange.SwapClient,
	srOpen float64, openTradeUnit float64, side ccexgo.OrderSide, mode rate.RunMode) (*OrderResult, *exchange.CfOrder, error) {
	// if li.putInTradeCurrency {
	return li.openSwapTaker(ctx, config, swapC, srOpen, openTradeUnit, side, mode)
	// }
	// return li.openSwapLong(ctx, swapC, srOpen, openTradeUnit, openTickNum, swap, op)
}

// OpenSpot taker开仓
func (li *listImpl) openSwapTaker(ctx context.Context, config *config.Config, swapC exchange.SwapClient,
	srOpen float64, openTradeUnit float64, side ccexgo.OrderSide, mode rate.RunMode) (*OrderResult, *exchange.CfOrder, error) {
	li.mu.Lock()
	id := fmt.Sprintf("%s_%d", time.Now().Format("060102_150405"), li.PosID)
	li.PosID += 1
	li.mu.Unlock()
	logger := log.With(li.logger, "id", id, "mode", mode, "mode2", "preTaker")
	swapName := "swap1"
	if mode == rate.Swap1MSwap2T {
		swapName = "swap2"
	}

	amt := decimal.NewFromFloat(openTradeUnit) // 想买的个数
	// if amt.LessThan(decimal.NewFromFloat(li.swapActsSymbol.MinimumOrderAmount)) {
	// 	return nil, nil, fmt.Sprintf("合约下单数量小于%f张,跳过挂单", li.swapActsSymbol.MinimumOrderAmount), fmt.Errorf("open swap size less than one")
	// }

	ret := NewOrderResult()
	ret.AddAny("pos_id", id)
	ret.Start()
	ret.AddAny("sr_open", srOpen)
	ret.AddAny("open_trade_unit", openTradeUnit)
	ret.SetOpID(id)

	spotRestStart := time.Now()
	swapOrderT, err := swapC.MarketOrder(ctx, side, amt, decimal.Zero, nil)
	spotRestEnd := time.Now()
	if err != nil {
		level.Error(logger).Log("message", "create swap order failed", "amt", amt, "side", side, "err", err.Error())
		errType := swapC.HandError(err)
		li.UpdateBannedTime(errType, false)
		li.CheckErrShareBan(errType, swapC.GetExchangeName())
		processType := errToProcessType(errType)
		newErr := fmt.Errorf("【人工处理】preTaker 无敞口产生(根据错误处理),创建%s %s单失败\n错误原因:%s", swapName, side, errType)
		if processType == SystemProcess {
			newErr = fmt.Errorf("【%s】preTaker 无敞口产生,创建%s %s单失败\n错误原因:%s", processType, swapName, side, errType)
		}
		return nil, nil, newErr
	}

	swapOrderT.RestStart = spotRestStart
	swapOrderT.RestEnd = spotRestEnd

	if swapOrderT.Filled.IsZero() && swapOrderT.Status == ccexgo.OrderStatusCancel {
		return nil, nil, fmt.Errorf("【系统自动处理】preTaker %s %s失败\n错误原因:订单可能因滑点问题被交易所取消", swapName, side)
	}

	level.Info(logger).Log("message", "create swap order", "order", fmt.Sprintf("%+v", swapOrderT))

	usdt := decimal.NewFromFloat(openTradeUnit).Mul(swapOrderT.AvgPrice)
	ret.AddAny("open_usdt", usdt)
	return ret, swapOrderT, nil
}

// CloseSpot taker开仓
func (li *listImpl) CloseSwapTakerOrder(ctx context.Context, config *config.Config, swapC exchange.SwapClient,
	srClose float64, closeTradeUnit float64, side ccexgo.OrderSide, mode rate.RunMode) (*OrderResult, *exchange.CfOrder, error) {
	// if li.putInTradeCurrency {
	return li.closeSwapTaker(ctx, config, swapC, srClose, closeTradeUnit, side, mode)
	// }
	// return li.openSwapLong(ctx, swapC, srOpen, openTradeUnit, openTickNum, swap, op)
}

// CloseSpot taker开仓
func (li *listImpl) closeSwapTaker(ctx context.Context, config *config.Config, swapC exchange.SwapClient,
	srClose float64, closeTradeUnit float64, side ccexgo.OrderSide, mode rate.RunMode) (*OrderResult, *exchange.CfOrder, error) {
	id := fmt.Sprintf("%s_%d", time.Now().Format("060102_150405"), li.PosID)
	logger := log.With(li.logger, "id", id, "mode", mode, "mode2", "preTaker")
	swapName := "swap1"
	if mode == rate.Swap1MSwap2T {
		swapName = "swap2"
	}

	amt := decimal.NewFromFloat(closeTradeUnit) // 想买的个数
	// if amt.LessThan(decimal.NewFromFloat(li.swapActsSymbol.MinimumOrderAmount)) {
	// 	return nil, nil, fmt.Sprintf("合约下单数量小于%f张,跳过挂单", li.swapActsSymbol.MinimumOrderAmount), fmt.Errorf("open swap size less than one")
	// }

	ret := NewOrderResult()
	ret.Start()
	ret.AddAny("sr_close", srClose)
	ret.SetOpID(id)

	if mode == rate.Swap1MSwap2T {
		amt = decimal.Min(amt, li.LocalPos2Amount)
	} else {
		amt = decimal.Min(amt, li.LocalPos1Amount)
	}

	spotRestStart := time.Now()
	swapOrderT, err := swapC.MarketOrder(ctx, side, amt, decimal.Zero, nil)
	spotRestEnd := time.Now()
	if err != nil {
		level.Error(logger).Log("message", "create swap order failed", "amt", amt, "side", side, "err", err.Error())
		errType := swapC.HandError(err)
		li.UpdateBannedTime(errType, false)
		li.CheckErrShareBan(errType, swapC.GetExchangeName())
		processType := errToProcessType(errType)
		newErr := fmt.Errorf("【人工处理】preTaker 无敞口产生(根据错误处理),创建%s %s单失败\n错误原因:%s", swapName, side, errType)
		if processType == SystemProcess {
			newErr = fmt.Errorf("【%s】preTaker 无敞口产生,创建%s %s单失败\n错误原因:%s", processType, swapName, side, errType)
		}
		return nil, nil, newErr
	}

	swapOrderT.RestStart = spotRestStart
	swapOrderT.RestEnd = spotRestEnd

	if swapOrderT.Filled.IsZero() && swapOrderT.Status == ccexgo.OrderStatusCancel {
		return nil, nil, fmt.Errorf("【系统自动处理】preTaker %s %s失败\n错误原因:订单可能因滑点问题被交易所取消", swapName, side)
	}

	level.Info(logger).Log("message", "create swap order", "order", fmt.Sprintf("%+v", swapOrderT))
	return ret, swapOrderT, nil
}

func (li *listImpl) PreTakerOpenFinished(ctx context.Context, swapC1, swapC2 exchange.SwapClient, srOpen float64,
	swap1Order, swap2Order *exchange.CfOrder, swap1, swap2 *exchange.Depth, op *OrderResult, mode rate.RunMode) (or *OrderResult, err error) {
	start := time.Now()
	defer func() {
		if e := recover(); e != nil {
			err = utilCommon.RecoverWithLog(li.logger, "PreTakerOpenFinished", start, e)
		}
	}()
	return li.preTakerOpenFinished(ctx, swapC1, swapC2, srOpen, swap1Order, swap2Order, swap1, swap2, op, mode)
}

func (li *listImpl) preTakerOpenFinished(ctx context.Context, swapC1, swapC2 exchange.SwapClient, srOpen float64,
	swap1Order, swap2Order *exchange.CfOrder, swap1, swap2 *exchange.Depth, op *OrderResult, mode rate.RunMode) (*OrderResult, error) {
	id := op.GetOpID()
	logger := log.With(li.logger, "id", id, "mode", mode)

	// 对于u本位来说都是币
	filled1 := swap1Order.Filled
	filled2 := swap2Order.Filled

	// 重算filled1及filled2, 以小的为标准, 实际上taker单全部成交
	if filled1.GreaterThan(filled2) {
		filled1 = filled2
		li.AddSwapExposure(ctx, swap1Order.Filled.Sub(filled1), "swap1")
	} else if filled1.LessThan(filled2) {
		filled2 = filled1
		li.AddSwapExposure(ctx, swap2Order.Filled.Sub(filled2), "swap2")
	}

	realSr := swap2Order.AvgPrice.Div(swap1Order.AvgPrice).Sub(decimal.NewFromInt(1))

	unit1 := filled1
	unit2 := filled2
	swap1Ccy := filled1
	swap2Ccy := filled2
	ccy1 := swap1Ccy.Mul(swap1Order.AvgPrice)
	ccy2 := swap2Ccy.Mul(swap2Order.AvgPrice)
	unit1Real := swap1Ccy
	unit2Real := swap2Ccy
	unit1Real = swap1Ccy
	unit2Real = swap2Ccy
	fee1Real := swap1Order.Fee.Mul(unit1Real).Div(swap1Order.Filled)
	fee2Real := swap2Order.Fee.Mul(unit2Real).Div(swap2Order.Filled)
	ccyDeposit1 := ccy1.Div(li.level1)
	ccyDeposit2 := ccy2.Div(li.level2)
	ccyDeposit1Real := ccyDeposit1.Mul(unit1Real).Div(swap1Order.Filled)
	ccyDeposit2Real := ccyDeposit2.Mul(unit2Real).Div(swap2Order.Filled)
	ccyDeposit := ccyDeposit1.Add(ccyDeposit2)
	openExposure := ccy1.Sub(ccy2) // 币本位是币，u本位是u

	op.AddAny("sr_open_real", realSr)
	op.AddOrder("swap1", swap1Order)
	op.AddOrder("swap2", swap2Order)
	op.AddDepth("swap2", swap2)
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
	op.AddAny("deposit2_real", ccyDeposit2Real)
	op.AddAny("deposit", ccyDeposit)
	op.AddAny("deposit_real", ccyDeposit1Real.Add(ccyDeposit2Real))
	op.AddAny("open_exposure", openExposure) // 开仓敞口
	op.AddAny("mode", mode+"_pt")
	op.AddAny("maker_info", op.GetMakerInfo())
	op.End()

	r, _ := realSr.Float64()
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
		CcyDeposit2Real: ccyDeposit2Real,
		CcyDeposit:      ccyDeposit,
		CcyDepositReal:  ccyDeposit1Real.Add(ccyDeposit2Real),
		Mode:            mode,
	})

	li.TotalExposure = li.TotalExposure.Add(openExposure)
	li.updateLocalTotalPos()
	li.mu.Unlock()

	li.calcAvgOpenSpread(unit1, realSr, true)
	if err := li.updateAccountInfo(ctx, nil, swapC1, swapC2, logger); err != nil {
		level.Warn(logger).Log("message", "update account info failed", "err", err.Error(), "sleep", "80s")
		// time.Sleep(80 * time.Second) // 休眠80s
		li.UpdateBannedTime(nil, true)
	}

	op.SetCcy(ccyDeposit1, ccyDeposit2)
	return op, nil
}

func (li *listImpl) PreTakerCloseFinished(ctx context.Context, swapC1, swapC2 exchange.SwapClient, srClose float64,
	swap1Order, swap2Order *exchange.CfOrder, swap1, swap2 *exchange.Depth, op *OrderResult, mode rate.RunMode, totalLimit float64, frozenAmount decimal.Decimal) (or *OrderResult, err error) {
	start := time.Now()
	defer func() {
		if e := recover(); e != nil {
			err = utilCommon.RecoverWithLog(li.logger, "PreTakerCloseFinished", start, e)
		}
	}()
	return li.preTakerCloseFinished(ctx, swapC1, swapC2, srClose, swap1Order, swap2Order, swap1, swap2, op, mode, totalLimit, frozenAmount)
}

func (li *listImpl) preTakerCloseFinished(ctx context.Context, swapC1, swapC2 exchange.SwapClient, srClose float64,
	swap1Order, swap2Order *exchange.CfOrder, swap1, swap2 *exchange.Depth, op *OrderResult, mode rate.RunMode, totalLimit float64, frozenAmount decimal.Decimal) (*OrderResult, error) {
	id := fmt.Sprintf("%d_%d", time.Now().UnixNano(), li.PosID)
	logger := log.With(li.logger, "id", id, "mode", mode)

	cv1 := li.swap2.ContractVal()
	cv2 := li.swap1.ContractVal()
	swap1Side := swap1Order.Side

	swapTakerC := swapC1
	tSwapActsSymbol := li.swap1ActsSymbol
	swapTakerSide := swap1Order.Side
	swapTakerDepth := swap1
	swapOrderM := swap2Order
	if mode == rate.Swap1MSwap2T {
		swapTakerC = swapC2
		tSwapActsSymbol = li.swap2ActsSymbol
		swapTakerSide = swap2Order.Side
		swapTakerDepth = swap2
		swapOrderM = swap1Order
	}
	exp := 0
	if swapTakerC.GetExchangeName() == exchange.Okex5 { // 可能只有okx才有小数张，取决于acts接口转换导致？
		exp = actsSpot.CalcExponent(decimal.NewFromFloat(tSwapActsSymbol.AmountTickSize))
	}

	// 对于u本位来说都是币
	filled1 := swap1Order.Filled
	filled2 := swap2Order.Filled

	// 重算filled1及filled2, 以大的为标准, 实际上taker单全部成交, 将敞口变成平仓敞口
	if filled1.GreaterThan(filled2) {
		filled2 = filled1
	} else if filled1.LessThan(filled2) {
		filled1 = filled2
	}

	li.mu.Lock()
	cp, closed, newPos := li.splitPos2(cv1, swapTakerC.FeeRate().OpenTaker, filled2.InexactFloat64(), swapOrderM.AvgPrice.InexactFloat64(), nil, swapTakerDepth, totalLimit, swapTakerSide, mode, exp)
	if cp == nil {
		li.mu.Unlock()
		li.UnfreezeCloseAmount(frozenAmount)
		level.Warn(logger).Log("message", "no available pos to close", "handle", "PreTakerCloseFinished")
		return nil, fmt.Errorf("【人工处理】 PreTakerCloseFinished 平仓选不出仓位,请检查！！！两边都已成交\nswap1Order=%+v\nswap2Order=%+v", swap1Order, swap2Order)
	}

	li.Poss = newPos
	li.updateLocalTotalPos()
	li.mu.Unlock()

	li.UnfreezeCloseAmount(frozenAmount)

	if !filled1.Equal(swap1Order.Filled) {
		li.AddSwapExposure(ctx, filled1.Sub(swap1Order.Filled), "swap1")
	}
	if !filled2.Equal(swap2Order.Filled) {
		li.AddSwapExposure(ctx, filled2.Sub(swap2Order.Filled), "swap2")
	}

	level.Info(logger).Log("message", "loop for pos", "traverse", cp.traverse, "deal_count", cp.count,
		"order_unit", cp.Unit1, "funding1", cp.Funding1, "funding_real", cp.Funding1Real, "funding2", cp.Funding2, "finding2_real", cp.Funding2Real, "fee1", cp.Fee1, "fee1_real", cp.Fee1Real,
		"fee2", cp.Fee2, "fee2_real", cp.Fee2Real, "deposit1", cp.CcyDeposit1, "deposit1_real", cp.CcyDeposit1Real, "deposit2", cp.CcyDeposit2Real)

	one := decimal.NewFromInt(1)
	op.SetCcy(cp.CcyDeposit1, cp.CcyDeposit2)

	fee1Real := swap1Order.Fee.Mul(filled1.Div(swap1Order.Filled))
	fee2Real := swap2Order.Fee.Mul(filled2.Div(swap2Order.Filled))

	updatePosIDs := []string{}
	var (
		swap1Profit     decimal.Decimal
		swap2Profit     decimal.Decimal
		swap1ProfitReal decimal.Decimal
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
	swap1Earn := swap1Profit.Add(cp.Funding1).Add(cp.Fee1Real).Add(fee1Real) // 币本位是币，u本位是u
	swap2Earn := swap2Profit.Add(cp.Funding2).Add(cp.Fee2Real).Add(fee2Real)
	swap1EarnReal := swap1ProfitReal.Add(cp.Funding1Real).Add(cp.Fee1Real).Add(fee1Real)

	level.Info(logger).Log("message", "close swap1 order", "id", swap1Order.ID.String(), "unit1", cp.Unit1,
		"swap1_avg_price", swap1Order.AvgPrice, "swap1_profit", swap1Profit, "swap1_earn", swap1Earn, "swap1_order_fee", swap1Order.Fee,
		"swap2_profit", swap2Profit, "swap2_earn", swap2Earn, "swap1_earn_real", swap1EarnReal, "mode_ptm", mode)

	level.Info(logger).Log("message", "close order done", "swap1_order_id", swap1Order.ID.String(), "swap_avg_price", swap1Order.AvgPrice,
		"swap1_order_unit", cp.Unit1, "swap2_order_id", swap2Order.ID.String(),
		"swap2_avg_price", swap2Order.AvgPrice, "swap2_order_fee", swap2Order.Fee, "swap2_fee_real", fee2Real)

	// li.TotalExposure = li.TotalExposure.Add(closeExposure)
	li.calcAvgOpenSpread(cp.Unit1, decimal.Zero, false)
	if err := li.updateAccountInfo(ctx, nil, swapC1, swapC2, logger); err != nil {
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
	op.AddDepth("swap2", swap1)
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
	op.AddAny("mode", mode+"_pt")
	op.AddAny("pos_ids", strings.Join(updatePosIDs, ","))
	op.AddAny("maker_info", op.GetMakerInfo())
	op.End()

	return op, nil
}
