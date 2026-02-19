package position

import (
	"cf_arbitrage/exchange"
	"cf_arbitrage/mock/mexchange"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	ccexgo "github.com/NadiaSama/ccexgo/exchange"
	"github.com/go-kit/kit/log"
	"github.com/golang/mock/gomock"
	"github.com/shopspring/decimal"
)

type (
	spotSymbol struct {
		*ccexgo.BaseSpotSymbol
	}

	swapSymbol struct {
		*ccexgo.BaseSwapSymbol
	}

	decimalMatcher struct {
		expect decimal.Decimal
	}

	orderMatcher struct {
		expect []*ccexgo.Order
	}
)

func newOM(expect ...*ccexgo.Order) *orderMatcher {
	ex := make([]*ccexgo.Order, len(expect))
	copy(ex, expect)

	return &orderMatcher{
		expect: ex,
	}
}

func (om *orderMatcher) Matches(x interface{}) bool {
	var os []*ccexgo.Order
	switch t := x.(type) {
	case *ccexgo.Order:
		os = append(os, t)

	case []*ccexgo.Order:
		os = t
	}

	if len(os) != len(om.expect) {
		return false
	}

	for i := 0; i < len(os); i++ {
		po := os[i]
		eo := om.expect[i]

		if po.ID.String() != eo.ID.String() {
			return false
		}
	}
	return true
}

func (om *orderMatcher) String() string {
	fields := []string{}
	for _, o := range om.expect {
		fields = append(fields, fmt.Sprintf("order=%s", o.ID.String()))
	}

	return fmt.Sprintf("orders match %s", strings.Join(fields, ","))
}

func newDM(expect decimal.Decimal) *decimalMatcher {
	return &decimalMatcher{
		expect: expect,
	}
}

func (dm *decimalMatcher) Matches(x interface{}) bool {
	val := x.(decimal.Decimal)

	return val.Equal(dm.expect)
}

func (dm *decimalMatcher) String() string {
	return fmt.Sprintf("matcher of %s", dm.expect.String())
}

func NewSpotSymbol(base, quote string) ccexgo.SpotSymbol {
	return &spotSymbol{
		ccexgo.NewBaseSpotSymbol(base, quote, ccexgo.SymbolConfig{
			AmountMin: decimal.NewFromFloat(10.0),
			ValueMin:  decimal.NewFromFloat(100.0),
		}, nil),
	}
}

func (ss *spotSymbol) String() string {
	return fmt.Sprintf("%s%s", ss.Base(), ss.Quote())
}

func NewSwapSymbol(sym string, cv decimal.Decimal) ccexgo.SwapSymbol {
	return &swapSymbol{
		ccexgo.NewBaseSwapSymbolWithCfg(sym, cv, ccexgo.SymbolConfig{}, nil),
	}
}
func (ss *swapSymbol) String() string {
	return ss.Index()
}

func TestOpen(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	transfer := mexchange.NewMockFundTransfer(ctrl)
	spotC := mexchange.NewMockClient(ctrl)
	swapC := mexchange.NewMockSwapClient(ctrl)
	cv := decimal.NewFromFloat(10.0)
	swap := NewSwapSymbol("BTC-USDT", cv)
	spot := NewSpotSymbol("BTC", "USDT")

	li := NewList(spot, swap, transfer, log.NewNopLogger()).(*listImpl)
	li.Poss = append(li.Poss, Pos{ID: "1"})

	openAmount := 10
	spotC.EXPECT().MarketOrder(gomock.Any(), ccexgo.OrderSideBuy,
		newDM(decimal.NewFromFloat(float64(openAmount)).Mul(cv))).Return(&ccexgo.Order{
		ID:       ccexgo.NewIntID(100),
		Filled:   decimal.NewFromFloat(10.0),
		AvgPrice: decimal.NewFromFloat(5.5),
		Fee:      decimal.NewFromFloat(1.0),
	}, nil)
	spotC.EXPECT().FetchBalance(gomock.Any()).Return([]ccexgo.Balance{}, nil)
	spotC.EXPECT().FeeRate().Return(exchange.FeeRate{
		OpenTaker: decimal.NewFromFloat(0.1),
	})

	transfer.EXPECT().Transfer(gomock.Any(), exchange.AccontTypeSpot, exchange.AccontTypeSwap, "BTC", newDM(decimal.NewFromFloat(9.0))).Return(nil)

	swapC.EXPECT().MarketOrder(gomock.Any(), ccexgo.OrderSideSell, newDM(decimal.NewFromFloat(float64(openAmount)))).Return(&ccexgo.Order{
		ID: ccexgo.NewIntID(101),
	}, nil)
	swapC.EXPECT().Position(gomock.Any()).Return(&ccexgo.Position{}, nil)
	swapC.EXPECT().FetchBalance(gomock.Any()).Return([]ccexgo.Balance{{}}, nil)
	swapC.EXPECT().FeeRate().Return(exchange.FeeRate{
		OpenTaker: decimal.NewFromFloat(-0.1),
	})

	depth := &exchange.Depth{
		OrderBook: &ccexgo.OrderBook{
			Bids: []ccexgo.OrderElem{{Price: 10.0, Amount: 1.0}, {Price: 10.0, Amount: 2.0}},
			Asks: []ccexgo.OrderElem{{Price: 10.0, Amount: 1.0}, {Price: 20.0, Amount: 2.0}},
		},
		ID: 1,
	}
	result, err := li.Open(context.Background(), spotC, swapC, 1.0, openAmount, depth, depth)
	if err != nil {
		t.Fatalf("unexpect error happend error=%s", err.Error())
	}

	expect := map[string]string{
		"spot_order_id": "100",
		"swap_order_id": "101",
	}
	for _, fs := range result.fields {
		ex, ok := expect[fs[0]]
		if ok {
			if ex != fs[1] {
				t.Errorf("bad '%s' expect '%s' got '%s'", fs[0], ex, fs[1])
			}
		}
	}

	if p := li.Poss[1]; !p.SpotToSwap.Equal(decimal.NewFromFloat(9.0)) || p.Fee.Equal(decimal.NewFromFloat(1.0)) {
		t.Errorf("invalid pos=%+v", p)
	}
}

func TestClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	transfer := mexchange.NewMockFundTransfer(ctrl)
	spotC := mexchange.NewMockClient(ctrl)
	swapC := mexchange.NewMockSwapClient(ctrl)
	cv := decimal.NewFromFloat(10.0)
	swap := NewSwapSymbol("BTC-USDT", cv)
	spot := NewSpotSymbol("BTC", "USDT")

	li := NewList(spot, swap, transfer, log.NewNopLogger()).(*listImpl)
	ctx := context.Background()
	depth := &exchange.Depth{
		ID: 1,
		OrderBook: &ccexgo.OrderBook{
			Bids: []ccexgo.OrderElem{
				{Price: 10.0},
				{Price: 8.0001},
			},
			Asks: []ccexgo.OrderElem{
				{Price: 9.0},
				{Price: 8.0},
			},
		},
	}

	t.Run("test close list pos all", func(t *testing.T) {
		li.Poss = []Pos{
			{ID: "1", SrOpen: 0.01, Funding: decimal.NewFromFloat(0.01), Fee: decimal.NewFromFloat(-0.005),
				Unit: decimal.NewFromFloat(10.0), SpotToSwap: decimal.NewFromFloat(10.0), Usdt: decimal.NewFromFloat(300),
				OpenPrice: decimal.NewFromFloat(10.0)},
			{ID: "2", SrOpen: 0.01, Funding: decimal.NewFromFloat(0.001), Fee: decimal.NewFromFloat(-0.001),
				Unit: decimal.NewFromFloat(1.0), SpotToSwap: decimal.NewFromFloat(8.0), Usdt: decimal.NewFromFloat(100),
				OpenPrice: decimal.NewFromFloat(10.0)},
		}

		swapC.EXPECT().MarketOrder(gomock.Any(), ccexgo.OrderSideCloseShort, decimal.NewFromFloat(11.0)).Return(&ccexgo.Order{
			ID:       ccexgo.NewIntID(1),
			AvgPrice: decimal.NewFromFloat(5.0),
			Fee:      decimal.NewFromFloat(-0.002),
		}, nil)
		//swapEarn = 11 * 10 * (1 / 5 - 1 / 10) = 11
		//swapFunding  + fee = 0.01 + 0.001 - 0.005 - 0.001  - 0.002 = 0.003
		//total = spotToSwap + swapEarn + swapFunding = 18 + 11 + 0.003
		transfer.EXPECT().Transfer(ctx, exchange.AccontTypeSwap, exchange.AccontTypeSpot, "BTC", newDM(decimal.NewFromFloat(29.003))).Return(nil)

		spotC.EXPECT().MarketOrder(gomock.Any(), ccexgo.OrderSideSell, newDM(decimal.NewFromFloat(29.003))).Return(&ccexgo.Order{
			ID: ccexgo.NewIntID(10),
		}, nil)
		spotC.EXPECT().FetchOrder(gomock.Any(), &ccexgo.Order{
			ID: ccexgo.NewIntID(10),
		}).Return(&ccexgo.Order{
			ID:       ccexgo.NewIntID(10),
			AvgPrice: decimal.NewFromFloat(100.0),
			Filled:   decimal.NewFromFloat(5.0),
			Fee:      decimal.NewFromFloat(1.0),
		}, nil)
		spotC.EXPECT().FetchBalance(gomock.Any()).Return([]ccexgo.Balance{}, nil)

		swapC.EXPECT().Position(gomock.Any()).Return(&ccexgo.Position{}, nil)
		swapC.EXPECT().FetchBalance(gomock.Any()).Return([]ccexgo.Balance{{}}, nil)

		r, err := li.Close(ctx, spotC, swapC, 0.001, 150, 0.0, 1, depth, depth)
		if err != nil {
			t.Fatalf("close fail error=%s", err.Error())
		}
		if len(li.Poss) != 0 {
			t.Errorf("expect no poss %+v", li.Poss)
		}

		expect := map[string]string{
			"earn": "99.0",
		}
		for _, s := range r.fields {
			val, ok := expect[s[0]]
			if ok {
				exEarn, _ := decimal.NewFromString(val)
				if exEarn.String() != s[1] {
					t.Errorf("bad expect got '%s' expect '%s'", s[1], exEarn.String())
				}
			}
		}
	})

	t.Run("test close list empty due to totalLimit", func(t *testing.T) {
		li.Poss = []Pos{
			{ID: "1", SrOpen: 0.01, Funding: decimal.NewFromFloat(0.01), Fee: decimal.NewFromFloat(-0.005), Unit: decimal.NewFromFloat(10.0)},
			{ID: "2", SrOpen: 0.01, Funding: decimal.NewFromFloat(0.001), Fee: decimal.NewFromFloat(-0.001), Unit: decimal.NewFromFloat(1.0)},
		}

		_, err := li.Close(ctx, spotC, swapC, 0.001, 150, 100.0, 1, depth, depth)
		if err != nil {
			t.Fatalf("close fail error=%s", err.Error())
		}
		if len(li.Poss) != 2 || li.Poss[0].ID != "1" || li.Poss[1].ID != "2" {
			t.Errorf("expect no poss %+v", li.Poss)
		}
	})

	t.Run("test close partial due to amount limit", func(t *testing.T) {
		li.Poss = []Pos{
			{ID: "1", SrOpen: -2.0, Funding: decimal.NewFromFloat(-0.1), Fee: decimal.NewFromFloat(-0.5), Unit: decimal.NewFromFloat(10.0), OpenPrice: decimal.NewFromFloat(1.0)},
			{ID: "2", SrOpen: 0.01, Funding: decimal.NewFromFloat(0.001), Fee: decimal.NewFromFloat(-0.001), Unit: decimal.NewFromFloat(1.0), Usdt: decimal.NewFromFloat(2.0),
				OpenPrice: decimal.NewFromFloat(1.0)}, //pos 2 is dealed
			{ID: "3", SrOpen: -2.0, Funding: decimal.NewFromFloat(-0.1), Fee: decimal.NewFromFloat(-0.1), Unit: decimal.NewFromFloat(1.0),
				OpenPrice: decimal.NewFromFloat(2.0)},
			{ID: "4", SrOpen: 0.01, Funding: decimal.NewFromFloat(0.006), Fee: decimal.NewFromFloat(-0.003), Unit: decimal.NewFromFloat(9.0),
				SpotToSwap: decimal.NewFromFloat(9), Usdt: decimal.NewFromFloat(9.0),
				OpenPrice: decimal.NewFromFloat(3.0)}, //pos 4 is partially dealed
			{ID: "5", SrOpen: -2.0, Funding: decimal.NewFromFloat(-0.1), Fee: decimal.NewFromFloat(-0.1), Unit: decimal.NewFromFloat(1.0),
				OpenPrice: decimal.NewFromFloat(4.0)},
		}

		li.PosAvgOpenPrice = decimal.NewFromFloat(10.0)

		order := &ccexgo.Order{ID: ccexgo.NewIntID(1), AvgPrice: decimal.NewFromFloat(1.0)}
		swapC.EXPECT().MarketOrder(gomock.Any(), ccexgo.OrderSideCloseShort, decimal.NewFromFloat(4.0)).Return(order, nil)
		transfer.EXPECT().Transfer(gomock.Any(), exchange.AccontTypeSwap, exchange.AccontTypeSpot, "BTC", gomock.Any()).Return(nil)
		spotC.EXPECT().MarketOrder(gomock.Any(), ccexgo.OrderSideSell, gomock.Any()).Return(order, nil)
		spotC.EXPECT().FetchOrder(ctx, gomock.Any()).Return(order, nil)
		spotC.EXPECT().FetchBalance(gomock.Any()).Return([]ccexgo.Balance{}, nil)
		swapC.EXPECT().Position(ctx).Return(&ccexgo.Position{}, nil)
		swapC.EXPECT().FetchBalance(gomock.Any()).Return([]ccexgo.Balance{{}}, nil)

		//设置WithdrawlLimit 来限制平仓
		li.WithdrawlAvailable = cv.Mul(decimal.NewFromFloat(4)).Div(decimal.NewFromFloat(depth.Asks[0].Price)).Sub(decimal.NewFromFloat(0.001))
		a, b := li.Close(ctx, spotC, swapC, 0, 4, 0, 1, depth, depth)
		if a != nil || b != nil {
			t.Fatalf("exepect no operate")
		}

		li.WithdrawlAvailable = cv.Mul(decimal.NewFromFloat(4)).Div(decimal.NewFromFloat(depth.Asks[0].Price)).Add(decimal.NewFromFloat(0.001))
		if _, err := li.Close(ctx, spotC, swapC, 0.001, 4, 0.0001, 1, depth, depth); err != nil {
			t.Errorf("close fail error=%s", err.Error())
		}

		prec := decimal.NewFromFloat(1e-8)
		if len(li.Poss) != 4 || li.Poss[0].ID != "1" || li.Poss[1].ID != "3" ||
			li.Poss[2].ID != "4" || !li.Poss[2].Unit.Equal(decimal.NewFromFloat(6.0)) ||
			!li.Poss[2].SpotToSwap.Sub(decimal.NewFromFloat(6.0)).LessThan(prec) ||
			!li.Poss[2].Funding.Sub(decimal.NewFromFloat(0.004)).Abs().LessThan(prec) ||
			!li.Poss[2].Fee.Sub(decimal.NewFromFloat(-0.002)).Abs().LessThan(prec) ||
			!li.Poss[2].Usdt.Sub(decimal.NewFromFloat(6.0)).Abs().LessThan(prec) ||
			!li.Poss[2].OpenPrice.Equal(decimal.NewFromFloat(3.0)) ||
			li.Poss[3].ID != "5" || !li.Poss[3].OpenPrice.Equal(decimal.NewFromFloat(4.0)) ||
			!li.Poss[0].OpenPrice.Equal(decimal.NewFromFloat(1.0)) {
			t.Errorf("invalid poss %+v %+v %+v\n", li.Poss, li.Poss[2].Funding.Equal(decimal.NewFromFloat(0.004)), li.Poss[2].Fee.Equal(decimal.NewFromFloat(-0.002)))
		}
	})

	t.Run("test close fetch fail", func(t *testing.T) {
		li.Poss = []Pos{
			{ID: "1", SrOpen: 0.01, Funding: decimal.NewFromFloat(0.01), Fee: decimal.NewFromFloat(-0.005),
				Unit: decimal.NewFromFloat(10.0), SpotToSwap: decimal.NewFromFloat(10.0), Usdt: decimal.NewFromFloat(300),
				OpenPrice: decimal.NewFromFloat(10.0)},
		}
		swapC.EXPECT().MarketOrder(gomock.Any(), ccexgo.OrderSideCloseShort, gomock.Any()).Return(&ccexgo.Order{
			ID:       ccexgo.NewIntID(1),
			AvgPrice: decimal.NewFromFloat(5.0),
			Fee:      decimal.NewFromFloat(-0.002),
		}, nil)
		transfer.EXPECT().Transfer(ctx, exchange.AccontTypeSwap, exchange.AccontTypeSpot, "BTC", gomock.Any()).Return(nil)

		spotC.EXPECT().MarketOrder(gomock.Any(), ccexgo.OrderSideSell, gomock.Any()).Return(&ccexgo.Order{
			ID: ccexgo.NewIntID(10),
		}, nil)
		spotC.EXPECT().FetchOrder(gomock.Any(), &ccexgo.Order{
			ID: ccexgo.NewIntID(10),
		}).Return(nil, fmt.Errorf("test error")).Times(3)

		if _, err := li.Close(ctx, spotC, swapC, 0.001, 150, 0.0, 1, depth, depth); err == nil {
			t.Fatalf("expect close fail")
		}
	})
}

// func TestUpdateFunding(t *testing.T) {
// 	ctrl := gomock.NewController(t)
// 	defer ctrl.Finish()

// 	now := time.Now()
// 	swapC := mexchange.NewMockSwapClient(ctrl)
// 	swapC.EXPECT().Funding(gomock.Any()).Return([]ccexgo.Finance{
// 		{Amount: decimal.NewFromFloat(1.0), Time: now.Add(time.Second * -1)},
// 		{Amount: decimal.NewFromFloat(2.0), Time: now.Add(time.Second * 1)},
// 		{Amount: decimal.NewFromFloat(2.0), Time: now.Add(time.Second * -2)},
// 		{Amount: decimal.NewFromFloat(2.0), Time: now.Add(time.Second * 2)},
// 	}, nil).Times(2)

// 	li := listImpl{
// 		exportField: exportField{
// 			LastFundingTime: now,
// 			Poss: []Pos{
// 				{Unit: decimal.NewFromFloat(1)},
// 				{Unit: decimal.NewFromFloat(1)},
// 				{Unit: decimal.NewFromFloat(3)},
// 			},
// 		},
// 		logger: log.NewNopLogger(),
// 	}

// 	for i := 0; i < 2; i++ { //尝试更新两次
// 		if err := li.UpdateFunding(context.Background(), swapC); err != nil {
// 			t.Fatalf("update funding fail")
// 		}

// 		if !li.LastFundingTime.Equal(now.Add(time.Second * 2)) {
// 			t.Errorf("invalid last funding time %s, expect=%s", li.LastFundingTime, now.Add(time.Second*2))
// 		}
// 		if !li.Poss[0].Funding.Equal(decimal.NewFromFloat(0.8)) || !li.Poss[1].Funding.Equal(decimal.NewFromFloat(0.8)) ||
// 			!li.Poss[2].Funding.Equal(decimal.NewFromFloat(2.4)) {
// 			t.Errorf("invalid poss %+v", li.Poss)
// 		}
// 	}
// }
