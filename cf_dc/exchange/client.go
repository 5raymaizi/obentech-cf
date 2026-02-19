package exchange

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"cf_arbitrage/util/logger"

	common "go.common"
	"go.common/apis"
	"go.common/helper"

	"github.com/NadiaSama/ccexgo/exchange"
	"github.com/go-kit/log/level"
	"github.com/shopspring/decimal"
	"github.com/szmcdull/glinq/gmap"
)

const (
	Success                          = iota
	DisConnected                     // 网络问题
	NewOrderRejected                 // 创建订单失败
	NewOrderLimit                    // 创建订单频繁
	CloseOrderRejected               // 平仓订单被拒绝
	CancelRejected                   // 取消订单失败,找不到订单
	NoSuchOrder                      // 找不到订单
	ErrorMsgReceived                 // 收到错误信息
	AccountInsufficientBalance       // 账户余额不足或保证金不足
	MinNotional                      // 不满足最小下单量
	WeightLimitPerMinute             // 一分钟权重访问限制
	IpLimitPerMinute                 // 一分钟ip访问限制
	IpBannedUntil                    // ip被禁止访问
	ExceededTheMaximum               // 挂单或持仓超出当前初始杠杆下的最大值
	ExceededMaxPosition              // 挂单和持仓超出平台仓位限制
	Forbidden                        // 403 Forbidden
	InBorrowOrRepay                  // 有借款或还款在处理中
	MarketInsufficientLoanableAssets // 市场可借贷资产不足
	ContextDeadlineExceeded          // 网络请求超时
	ExchangePriceLimit               // 交易所价格限制
	ExchangeSystemBusy               // 交易所系统繁忙
	OtherReason                      // 其他原因

	Okex5            = `okex5`
	Binance          = `binance`
	Bybit5           = `bybit5`
	HuobiPro         = `huobipro`
	BinancePortfolio = `binancecross`  // 币安统一账户
	BinanceMargin    = `binancecross1` // 币安统一账户pm1.5-经典统一账户
	GateIO           = `gateio`
	HyperLiquid      = `hyperliquid`
)

var ExchangeList = []string{Okex5, Binance, Bybit5, HuobiPro, BinancePortfolio, BinanceMargin, GateIO, HyperLiquid}

type ErrorType struct {
	Code    int
	Message string
}

var errInfo = [...]string{"成功", "网络问题", "订单会立即成交", "创建订单频繁", "平仓订单被拒绝, 可能仓位数量不够了", "取消订单失败,找不到订单", "找不到订单", "收到错误信息",
	"账户余额不足", "不满足最小下单量", "一分钟权重访问限制1200", "一分钟ip访问限制2400", "ip被禁止访问", "挂单或持仓超出当前初始杠杆下的最大值", "挂单和持仓超出平台仓位限制", "403 Forbidden, 禁止访问",
	"有借款或还款在处理中", "市场可借贷资产不足,请调整借款金额或明天再试", "网络请求超时", "交易所价格限制", "交易所系统繁忙,请稍后重试", "其他原因,部分情况需检查是否出现敞口"}

func NewErrorType(code int, errStr string) *ErrorType {
	return &ErrorType{Code: code, Message: errStr}
}

func (e ErrorType) String() string {
	ei := errInfo[e.Code]
	switch e.Code {
	case OtherReason, ErrorMsgReceived, DisConnected, ContextDeadlineExceeded, ExceededTheMaximum, ExchangePriceLimit, AccountInsufficientBalance: // 返回具体原因，便于排查
		return fmt.Sprintf("%s\n%s", ei, e.Message)
	case IpBannedUntil: //匹配出具体时间
		endTime := strings.Split(strings.Split(e.Message, "until ")[1], `.`)[0]
		return fmt.Sprintf("%s直到%s(%s)", ei, endTime, time.UnixMilli(helper.MustInt64(endTime)).Format("2006-01-02 15:04:05.000"))
	case NewOrderLimit:
		limit := strings.Split(strings.Split(e.Message, `current limit is`)[1], `orders per`)
		return fmt.Sprintf("%s,当前限制为每%s %s个订单", ei, strings.TrimSpace(limit[1]), strings.TrimSpace(limit[0]))
	case MarketInsufficientLoanableAssets: //
		if strings.Contains(e.Message, "in loan pool to borrow") { // 匹配出不足的资产
			currency := strings.Split(strings.Split(e.Message, `available in loan`)[0], `Insufficient`)[1]
			return fmt.Sprintf("%s市场可借贷资产不足", currency)
		}
		return ei
	case ExceededMaxPosition:
		// Order failed. Your total value of same-direction AVAAI-USDT-SWAP open positions and orders can't exceed 250000 USD or 20% of the platform's open interest
		// 提取出250000 USD 或 20%
		if strings.Contains(e.Message, `open positions and orders can't exceed`) {
			split1Str := strings.Split(strings.Split(e.Message, `open positions and orders can't exceed`)[1], `of`)[0]
			value1 := strings.Split(split1Str, `or`)[0]
			value2 := strings.Split(split1Str, `or`)[1]
			return fmt.Sprintf("%s,当前最大仓位限制为%s或平台总持仓的%s", ei, value1, value2)
		}
		if strings.Contains(e.Message, `risk limit is`) {
			splitStr := strings.Split(strings.Split(e.Message, `would reach to`)[1], `while risk limit is`)
			if len(splitStr) != 2 {
				return ei
			}
			value1 := splitStr[0]
			value2 := splitStr[1]
			return fmt.Sprintf("%s,目前持仓为%s,当前杠杆下最大仓位限制为%s", ei, value1, value2)
		}
		return ei
	case MinNotional:
		if strings.Contains(e.Message, `notional must be no smaller than`) {
			minNotional := strings.TrimSpace(strings.Split(strings.Split(e.Message, "than")[1], "(")[0])
			return fmt.Sprintf("订单名义价值必须不小于%s(除非选择只减仓)", minNotional)
		}
		return ei
	default:
		return ei
	}
}

type (
	//Depth 增加了ID字段可以快速查找
	Depth struct {
		*exchange.OrderBook
		ID       uint64
		Exchange string
	}

	CfOrder struct {
		IDs            []string
		exchange.Order // 避免空指针
		IsMaker        bool
		WsPush         bool
		RestStart      time.Time
		RestEnd        time.Time
	}

	CfOrderChan struct {
		Orders    chan *CfOrder
		Status    apis.OrderStatus
		UpdatedAt time.Time
	}

	Position struct {
		Long  exchange.Position
		Short exchange.Position
	}

	BookTicker struct {
		ID         int64 //标识
		TimeStamp  time.Time
		Ask1Price  float64
		Ask1Amount float64
		Bid1Price  float64
		Bid1Amount float64
	}

	TaskCompletionSourceT[T any] struct {
		CreateTime time.Time
		common.TaskCompletionSourceT[T]
	}

	MySyncMap[K comparable, V any] struct {
		*gmap.SyncMap[K, V]
	}

	//Client 封装了针对一个market symbol的websocket,rest接口.
	//用于获取盘口深度, 创建市价/限价单. 不支持并发调用MarketOrder;支持并发调用LimitOrder
	Client interface {
		MarketOrder(ctx context.Context, side exchange.OrderSide, amount decimal.Decimal, quoteAmount decimal.Decimal, extraParams map[string]string) (*CfOrder, error)
		LimitOrder(ctx context.Context, side exchange.OrderSide, amount decimal.Decimal, price float64) (*CfOrder, error)
		LimitOrders(ctx context.Context, orderType apis.OrderType, side exchange.OrderSide, amount []decimal.Decimal, price []float64, extraParams map[string]string) ([]*CfOrder, error)
		// 撤单返回成交信息
		CancelOrder(ctx context.Context, order *CfOrder) (*CfOrder, error)
		CancelOrders(ctx context.Context, order []*CfOrder) ([]*CfOrder, error)
		FetchOrder(ctx context.Context, order *CfOrder) (*CfOrder, error)
		FetchBalance(ctx context.Context) ([]exchange.Balance, error)
		Depth() <-chan *Depth
		BookTicker() *BookTicker
		BalanceNotify() *common.Event
		BookTickerNotify() *common.Event
		Balance() *MySyncMap[string, *apis.Balance]
		Error() error
		MakerOrder(orderID string) <-chan *CfOrder

		HandError(err error) *ErrorType
		Done() <-chan struct{}
		Close() error
		FeeRate() FeeRate
		LevelInfo() LevelInfo

		InitFeeRate(ctx context.Context) error
		Start(ctx context.Context, subscribeTrade bool) error
		GetExchangeName() string

		ReconnectWs() error
	}

	SwapClient interface {
		Client
		Position(ctx context.Context) (*Position, error)    // ws优先+rest
		GetPosition(ctx context.Context) (*Position, error) // rest数据
		Funding(ctx context.Context) ([]exchange.Finance, error)
		FundingRate(ctx context.Context) (*apis.FundingRate, error)
		Index() <-chan *apis.IndexEvent
		PosNotify() *common.Event
		ADLOrder() <-chan *apis.Order
		// 获取ws订单, 订单的fee是到目前总手续费
		GetWsOrder(orderID string) (*CfOrder, error)
	}

	//AccountType 账号类型不同账号资金划转使用
	AccountType int

	//FundTransfer 资金划转现货，永续
	FundTransfer interface {
		Transfer(ctx context.Context, from AccountType, to AccountType, currency string, amount decimal.Decimal) error
	}

	//手续费率计算手续费用现货不区分spot, close, 负手续费代表有返佣
	FeeRate struct {
		OpenMaker  decimal.Decimal
		OpenTaker  decimal.Decimal
		CloseMaker decimal.Decimal
		CloseTaker decimal.Decimal
	}

	LevelInfo struct {
		Level            float64
		MaxNotionalValue decimal.Decimal
	}
)

const (
	AccountTypeSpot AccountType = iota
	AccountTypeSwap
)

func (d *Depth) String() string {
	return fmt.Sprintf("id=%d ts=%s", d.ID, d.Created)
}

const epsilonStr = 1e-12 // 定义误差范围

// 比较两个 decimal.Decimal 是否在允许的误差范围内
func AlmostEqual(a, b decimal.Decimal) bool {
	epsilon := decimal.NewFromFloat(epsilonStr)
	return a.Sub(b).Abs().LessThanOrEqual(epsilon)
}

func CalcExponent(precision decimal.Decimal) int {
	precisionFloat, _ := precision.Float64()
	exponent := -math.Log10(precisionFloat)
	return int(math.Round(exponent))
}

func DeleteT[K comparable, V any](m *gmap.SyncMap[K, V], key K, mapName string) {
	m.Delete(key)
	level.Info(logger.GetLogger()).Log("message", fmt.Sprintf("%s delete key", mapName), "key", key)
}

func LoadOrNewOrderChan(orderCh *gmap.SyncMap[string, *CfOrderChan], orderId string) *CfOrderChan {
	ch, _ := orderCh.LoadOrNew(orderId, func() *CfOrderChan {
		return &CfOrderChan{
			Orders:    make(chan *CfOrder, 16),
			UpdatedAt: time.Now(),
		}
	})
	return ch
}

func (bt *BookTicker) GetID() int64 {
	return bt.ID
}

func (bt *BookTicker) GetTimestamp() int64 {
	return bt.TimeStamp.UnixMilli()
}

func (bt *BookTicker) GetBid1Price() float64 {
	return bt.Bid1Price
}

func (bt *BookTicker) GetBid1Amount() float64 {
	return bt.Bid1Amount
}

func (bt *BookTicker) GetAsk1Price() float64 {
	return bt.Ask1Price
}

func (bt *BookTicker) GetAsk1Amount() float64 {
	return bt.Ask1Amount
}

func (m *MySyncMap[K, V]) Get(key K) V {
	actual, _ := m.LoadOrNew(key, func() V {
		var zero V
		// 使用反射检查V是否为指针类型
		rt := reflect.TypeOf(zero)
		if rt != nil && rt.Kind() == reflect.Pointer && rt.Elem().Kind() != reflect.Invalid {
			// 创建一个非nil的V类型实例
			return reflect.New(rt.Elem()).Interface().(V)
		}
		return *new(V)
	})
	return actual
}
