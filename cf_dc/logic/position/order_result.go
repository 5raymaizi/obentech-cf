package position

import (
	"cf_arbitrage/exchange"
	"encoding/json"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
)

type (
	OrderResult struct {
		OpID      string
		ccy1      decimal.Decimal //开平仓对应的usdt
		ccy2      decimal.Decimal //开平仓对应的usdt
		fields    [][2]string
		makerInfo map[string]any // 储存maker信息
	}
)

func NewOrderResult() *OrderResult {
	return &OrderResult{
		fields: make([][2]string, 0),
	}
}

func (or *OrderResult) SetOpID(opID string) {
	or.OpID = opID
}

func (or *OrderResult) GetOpID() string {
	return or.OpID
}

func (or *OrderResult) SetCcy(val1, val2 decimal.Decimal) {
	or.ccy1 = val1
	or.ccy2 = val2
}
func (or *OrderResult) Ccy() (decimal.Decimal, decimal.Decimal) {
	return or.ccy1, or.ccy2
}

func (or *OrderResult) SetMakerInfo(val map[string]any) {
	or.makerInfo = val
}

func (or *OrderResult) GetMakerInfo() map[string]any {
	return or.makerInfo
}

func (or *OrderResult) AddAny(key string, val any) {
	switch v := val.(type) {
	case float64:
		or.fields = append(or.fields, [2]string{key, fmt.Sprintf("%f", v)})
	case string:
		or.fields = append(or.fields, [2]string{key, v})
	case decimal.Decimal:
		or.fields = append(or.fields, [2]string{key, v.String()})
	case map[string]any:
		b, _ := json.Marshal(v)
		or.fields = append(or.fields, [2]string{key, string(b)})
	default:
		or.fields = append(or.fields, [2]string{key, fmt.Sprintf("%v", v)})
	}
}

func (or *OrderResult) Start() {
	or.fields = append(or.fields, [2]string{"start", time.Now().String()})
}

func (or *OrderResult) End() {
	or.fields = append(or.fields, [2]string{"end", time.Now().String()})
}

func (or *OrderResult) AddOrder(kind string, order *exchange.CfOrder) {
	var fee decimal.Decimal
	if kind == "spot" {
		fee = order.Fee.Neg()
	} else {
		fee = order.Fee
	}
	or.fields = append(or.fields,
		[2]string{fmt.Sprintf("%s_order_id", kind), order.ID.String()},
		[2]string{fmt.Sprintf("%s_avg_price", kind), order.AvgPrice.String()},
		[2]string{fmt.Sprintf("%s_deal_amount", kind), order.Filled.String()},
		[2]string{fmt.Sprintf("%s_side", kind), order.Side.String()},
		[2]string{fmt.Sprintf("%s_fee", kind), fee.String()},
		[2]string{fmt.Sprintf("%s_fee_currency", kind), order.FeeCurrency},
		[2]string{fmt.Sprintf("%s_is_maker", kind), fmt.Sprintf("%v", order.IsMaker)},
		[2]string{fmt.Sprintf("%s_open_time", kind), order.Created.String()},
		[2]string{fmt.Sprintf("%s_done_time", kind), order.Updated.String()},
	)
}

func (or *OrderResult) AddDepth(kind string, depth *exchange.Depth) {
	or.fields = append(or.fields,
		[2]string{fmt.Sprintf("%s_depth_id", kind), fmt.Sprintf("%d", depth.ID)},
		[2]string{fmt.Sprintf("%s_depth_bid0_price", kind), decimal.NewFromFloat(depth.Bids[0].Price).String()},
		[2]string{fmt.Sprintf("%s_depth_ask0_price", kind), decimal.NewFromFloat(depth.Asks[0].Price).String()},
		[2]string{fmt.Sprintf("%s_depth_bid1_price", kind), decimal.NewFromFloat(depth.Bids[1].Price).String()},
		[2]string{fmt.Sprintf("%s_depth_ask1_price", kind), decimal.NewFromFloat(depth.Asks[1].Price).String()},
		[2]string{fmt.Sprintf("%s_depth_timestamp", kind), depth.Created.String()},
	)
}

func (or *OrderResult) Fields() [][2]string {
	return or.fields
}
