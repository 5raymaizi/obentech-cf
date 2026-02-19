package spot

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cf_arbitrage/exchange"
	"cf_arbitrage/exchange/acts"
	"cf_arbitrage/message"
	"cf_arbitrage/util"
	utilCommon "cf_arbitrage/util/common"
	"cf_arbitrage/util/logger"
	"cf_arbitrage/util/ws"

	common "go.common"
	"go.common/apis"
	"go.common/apis/actsApi"
	"go.common/apis/apiFactory"
	"go.common/cache_service"
	"go.common/log"
	"go.common/uuid"

	ccexgo "github.com/NadiaSama/ccexgo/exchange"
	"github.com/NadiaSama/ccexgo/misc/ctxlog"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"github.com/szmcdull/glinq/gmap"
	"github.com/tidwall/gjson"
)

type (
	Client struct {
		ctx                 context.Context
		symbol              ccexgo.SpotSymbol
		actsApi             apis.Interface
		binanceActsApi      apis.Interface // 币安是pm1
		exit                *common.Event
		done                chan struct{}
		balanceChangeNotify *common.Event
		bookTickerNotify    *common.Event
		feeRate             *ccexgo.TradeFee
		orderbookCH         chan *exchange.Depth
		balances            *exchange.MySyncMap[string, *apis.Balance]
		bookTicker          *exchange.BookTicker
		err                 atomic.Value
		depthID             uint64
		limitOrderTasks     *gmap.SyncMap[string, *common.TaskCompletionSourceT[*exchange.CfOrder]]
		finishOrderTasks    *gmap.SyncMap[string, *exchange.TaskCompletionSourceT[*exchange.CfOrder]]
		wsOrders            *gmap.SyncMap[string, *exchange.CfOrder]     // 存储更新order信息
		takerOrderCh        *gmap.SyncMap[string, *exchange.CfOrderChan] // taker订单channel
		makerOrderCh        *gmap.SyncMap[string, *exchange.CfOrderChan]
	}
)

func NewClientCB(symbol ccexgo.SpotSymbol, exchangeName, key, secret string, api apis.Interface, vault bool) func(ctx context.Context) (exchange.Client, error) {
	return func(ctx context.Context) (exchange.Client, error) {
		client, err := NewClient(symbol, exchangeName, key, secret, api, vault)
		if err != nil {
			return nil, err
		}
		// err = client.Start(ctx)
		// if err != nil {
		// 	client.Close()
		// 	client.actsApi.Close()
		// 	return nil, errors.WithMessage(err, "client start fail")
		// }
		return client, nil
	}
}

func NewClient(symbol ccexgo.SpotSymbol, exchangeName, key, secret string, api2 apis.Interface, vault bool) (*Client, error) {
	if exchangeName == exchange.BinanceMargin {
		exchangeName = exchange.Binance
	}
	key, _ = utilCommon.DecryptKey(key)
	var logStr string
	if vault {
		logStr = fmt.Sprintf(`acts,%s,vault:%s`, exchangeName, key)
	} else {
		logStr = fmt.Sprintf(`acts,%s,%s`, exchangeName, key)
		apiSecret, apiPassphrase := utilCommon.DecryptKey(secret)
		if exchangeName == exchange.Okex5 {
			if apiPassphrase == `` {
				return &Client{}, fmt.Errorf("acts spot %s api init fail", exchangeName)
			}
			logStr = fmt.Sprintf(`%s,%s,password=%s`, logStr, apiSecret, apiPassphrase)
		} else {
			logStr = fmt.Sprintf(`%s,%s`, logStr, apiSecret)
		}
	}

	api, err := apiFactory.GetApiAndLogin(logStr)
	if err != nil {
		return &Client{}, errors.WithMessage(err, "acts spot api init fail")
	}
	ret := Client{
		symbol:              symbol,
		actsApi:             api,
		binanceActsApi:      api2,
		exit:                common.NewEvent(),
		done:                make(chan struct{}),
		orderbookCH:         make(chan *exchange.Depth, 128),
		balanceChangeNotify: common.NewEvent(),
		bookTickerNotify:    common.NewEvent(),
		balances:            &exchange.MySyncMap[string, *apis.Balance]{SyncMap: gmap.NewSyncMap[string, *apis.Balance]()},
		limitOrderTasks:     gmap.NewSyncMap[string, *common.TaskCompletionSourceT[*exchange.CfOrder]](),
		finishOrderTasks:    gmap.NewSyncMap[string, *exchange.TaskCompletionSourceT[*exchange.CfOrder]](),
		wsOrders:            gmap.NewSyncMap[string, *exchange.CfOrder](),
		takerOrderCh:        gmap.NewSyncMap[string, *exchange.CfOrderChan](),
		makerOrderCh:        gmap.NewSyncMap[string, *exchange.CfOrderChan](),
	}

	return &ret, nil
}

func (cl *Client) InitFeeRate(ctx context.Context) error {
	if cl.feeRate != nil {
		return nil
	}

	api := cl.actsApi
	if cl.binanceActsApi != nil {
		api = cl.binanceActsApi
	}
	fr, err := api.GetTradeFee(apis.ProductTypeSpot, cl.symbol.String())
	if err != nil {
		return errors.WithMessage(err, fmt.Sprintf("fetch %s spot fee rate fail", cl.actsApi.GetExchangeName()))
	}
	if len(fr) == 0 {
		return fmt.Errorf("%s fetch spot trade fee is null", cl.actsApi.GetExchangeName())
	}
	cl.feeRate = &ccexgo.TradeFee{}
	cl.feeRate.Symbol = cl.symbol
	cl.feeRate.Maker = decimal.NewFromFloat(fr[0].OpenMakerFee)
	cl.feeRate.Taker = decimal.NewFromFloat(fr[0].OpenTakerFee)
	cl.ctx = ctx
	ctxlog.Info(ctx, "message", "spot feeRate", "maker", cl.feeRate.Maker, "taker", cl.feeRate.Taker, "exchange", cl.actsApi.GetExchangeName())
	return nil
}

// Start 订阅相关websocket 启动Loop
func (cl *Client) Start(ctx context.Context, subscribeTrade bool) error {
	if err := cl.InitFeeRate(ctx); err != nil {
		return err
	}

	go cl.loop(ctx)

	if cl.actsApi.GetExchangeName() != exchange.Binance || cl.binanceActsApi != nil { // TODO 暂时先这样处理吧
		if err := cl.actsApi.SubscribeDepths(cl.OnDepths, apis.ProductTypeSpot, 5, cl.symbol.String()); err != nil {
			return errors.WithMessage(err, "subscribe depth fail")
		}
		if cl.GetExchangeName() == exchange.Okex5 || strings.Contains(cl.GetExchangeName(), exchange.Binance) ||
			cl.GetExchangeName() == exchange.Bybit5 {
			cl.SubscribeBookTicker()
		}
	}

	if strings.Contains(cl.actsApi.GetExchangeName(), exchange.Binance) {
		if strings.ToUpper(cl.symbol.String()) == `BNB_USDT` {
			cl.feeRate.Maker = cl.feeRate.Maker.Mul(decimal.NewFromInt(3).Div(decimal.NewFromInt(4)))
			cl.feeRate.Taker = cl.feeRate.Taker.Mul(decimal.NewFromInt(3).Div(decimal.NewFromInt(4)))

			if cl.actsApi.GetExchangeName() != exchange.Binance || cl.binanceActsApi != nil {
				common.SetInterval(1*time.Minute, func() {
					if b, err := cl.actsApi.GetBalance(apis.ProductTypeSpot, ``); err != nil {
						log.Error("cron get balance failed,err:%+v", err)
					} else {
						cl.OnBalances(b)
					}
				}).Run()
			}
		}
	}

	if err := cl.actsApi.SubscribeBalance(cl.OnBalances, apis.ProductTypeSpot, cl.symbol.Base(), cl.symbol.Quote()); err != nil {
		return errors.WithMessage(err, "subscribe balances fail")
	}

	if err := cl.actsApi.SubscribeOrders(cl.OnOrders, apis.ProductTypeSpot, cl.symbol.String()); err != nil {
		return errors.WithMessage(err, "subscribe orders fail")
	}

	if cl.actsApi.GetExchangeName() == exchange.Binance && cl.binanceActsApi == nil {
		cl.actsApi.SetWsPingInterval(apis.ProductTypeSpot, time.Second*20)
	}

	return nil
}

func (cl *Client) SubscribeBookTicker() {
	var subscribeMsg []byte
	symbol := strings.ToLower(strings.Replace(cl.symbol.String(), "_", "", -1))
	url := fmt.Sprintf(`wss://stream.binance.com:9443/ws/%s@bookTicker`, symbol)
	if cl.actsApi.GetExchangeName() == exchange.Okex5 {
		symbol = strings.Replace(cl.symbol.String(), "_", "-", -1)
		url = acts.GetOkxWsUrl()
		subscribeMsg = fmt.Appendf(nil, `{"op": "subscribe","args": [{"channel": "bbo-tbt","instId": "%s"}]}`, symbol)
	} else if cl.GetExchangeName() == exchange.Bybit5 {
		url = `wss://stream.bybit.com/v5/public/spot`
		subscribeMsg = fmt.Appendf(nil, `{"op": "subscribe","args": ["orderbook.1.%s"]}`, symbol)
	}
	wsClient := ws.NewWebSocketClient(cl.ctx, url, subscribeMsg, cl.OnBookTicker)
	go wsClient.Run(cl.exit)
}

// FetchBalance rest获取余额，顺便更新ws余额？
func (cl *Client) FetchBalance(ctx context.Context) ([]ccexgo.Balance, error) {
	balances, err := cl.actsApi.GetBalance(apis.ProductTypeSpot, ``)

	if err != nil {
		return nil, errors.WithMessage(err, "fetch balance fail")
	}

	level.Info(logger.GetLogger()).Log("message", "spot fetch balance ", "balances", fmt.Sprintf("%+v", balances), "exchange", cl.actsApi.GetExchangeName())
	cl.OnBalances(balances)

	base := balances.Get(strings.ToUpper(cl.symbol.Base()))
	base1 := &ccexgo.Balance{
		Currency: strings.ToUpper(cl.symbol.Base()),
		Total:    base.Total,
		Free:     base.Free,
		Frozen:   base.Used,
	}

	quote := balances.Get(strings.ToUpper(cl.symbol.Quote()))
	quote1 := &ccexgo.Balance{
		Currency: strings.ToUpper(cl.symbol.Quote()),
		Total:    quote.Total,
		Free:     quote.Free,
		Frozen:   quote.Used,
	}
	ret := []ccexgo.Balance{*base1, *quote1}
	return ret, nil
}

// MarketOrder 为了保证速率目前通过 websocket 推送获取order结果，但websocket推送缺少均价
func (cl *Client) MarketOrder(ctx context.Context, side ccexgo.OrderSide, amount, quoteAmount decimal.Decimal, extraParams map[string]string) (*exchange.CfOrder, error) {
	var (
		sid apis.OrderSide
		amt float64
	)

	if side == ccexgo.OrderSideBuy {
		sid = apis.OrderSideBuy
	} else if side == ccexgo.OrderSideSell {
		sid = apis.OrderSideSell
	} else {
		return nil, errors.Errorf("unkown orderside '%s'", side)
	}

	exp := CalcExponent(cl.symbol.AmountPrecision())
	amt, _ = amount.Truncate(int32(exp)).Float64()

	if cl.actsApi.GetExchangeName() == exchange.Binance {
		extraParams = nil
	}

	order := &apis.Order{
		Symbol:      cl.symbol.String(),
		SymbolType:  apis.ProductTypeSpot,
		Type:        apis.OrderTypeMarket,
		Side:        sid,
		ExtraParams: extraParams,
	}
	if acts.GetManual() {
		order.ClientOrderID = uuid.NewUniqueString()[:28] + acts.ManualTakerOpSuffix
	}
	if !quoteAmount.IsZero() {
		order.QuoteAmount = quoteAmount.Truncate(5).InexactFloat64()
	} else {
		order.Amount = amt
	}
	createTime := time.Now()
	if err := cl.actsApi.AddOrders(order); err != nil {
		return nil, err
	}

	orderCh := exchange.LoadOrNewOrderChan(cl.takerOrderCh, order.OrderID)
	defer func() { // 如果退出了市价函数, 说明拿到结果了, channel就没有必要存在了
		exchange.DeleteT(cl.takerOrderCh, order.OrderID, "spot takerOrderCh")
		close(orderCh.Orders)
	}()

	ret := exchange.CfOrder{}
	if order.Status == apis.OrderStatusFilled {
		cache_service.LoadOrStore2("spot_rest_"+order.OrderID, 5*time.Minute, 0, func() (string, error) { return order.OrderID, nil })
		// 读取已有数据，避免堵塞
		if len(orderCh.Orders) > 0 {
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				t := time.NewTimer(time.Second * 30)
				defer func() {
					t.Stop()
					wg.Done()
				}()

				for {
					select {
					case o, ok := <-orderCh.Orders:
						if ok {
							if o.ID.String() == order.OrderID {
								ctxlog.Warn(ctx, "message", "spot get order from orderCH", "order", fmt.Sprintf("%+v", o), "len", len(orderCh.Orders))
							}
						} else {
							ctxlog.Warn(ctx, "message", "spot get order from orderCH", "order", "orderCH closed")
							return
						}
					case <-t.C:
						ctxlog.Warn(ctx, "message", "spot get order from orderCH", "read", "read timeout")
						return
					default:
						ctxlog.Warn(ctx, "message", "spot get order from orderCH", "read", "read end")
						return
					}
				}
			}()
			wg.Wait()
		}
		ret.ID = ccexgo.NewStrID(order.OrderID)
		ret.Symbol = cl.symbol
		ret.Created = createTime
		ret.Updated = order.Timestamp
		ret.AvgPrice = decimal.NewFromFloat(order.AveragePrice)
		ret.Filled = decimal.NewFromFloat(order.AmountFilled)
		ret.Side = side
		ret.Status = acts.ActsStatus2ccex(order.Status)
		ret.Type = acts.ActsType2ccex(order.Type)

		ret.Fee = decimal.NewFromFloat(order.TotalFee)
		ret.FeeCurrency = order.FeeCurrency
		ret.WsPush = true
		return &ret, nil
	}
	done := make(chan struct{})
	defer close(done)
	restCH := make(chan *exchange.CfOrder, 1)
	go func() {
		//启动rest 获取order
		//避免websocket推送失败无法获取订单成交状态

		or := &exchange.CfOrder{
			Order: ccexgo.Order{
				Symbol: cl.symbol,
				ID:     ccexgo.NewStrID(order.OrderID),
			},
		}
		ecount := 0
		ticker := time.NewTicker(time.Millisecond * 20)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				o, err := cl.FetchOrder(ctx, or)

				if err != nil {
					ctxlog.Error(ctx, "message", "fetch rest order fail", "error", err.Error())
					ecount += 1
					if ecount > 3 {
						return
					}
					break
				}

				if o.Status == ccexgo.OrderStatusCancel || o.Status == ccexgo.OrderStatusDone {
					select {
					case restCH <- o:
					default:
					}
					return
				}

			case <-done:
				select {
				case <-restCH:
				default:
				}
				return
			}
		}
	}()

	timeOut := time.After(time.Second)
	for {
		select {
		case o := <-restCH:
			ctxlog.Warn(ctx, "message", "spot get order from rest interface", "id", o.ID.String(), "amount", o.Amount, "filled", o.Filled, "avg_price", o.AvgPrice, "status", o.Status)
			if o.Status == ccexgo.OrderStatusDone || o.Status == ccexgo.OrderStatusCancel {
				return o, nil
			}

		case <-timeOut:
			o, err := cl.FetchOrder(ctx, &exchange.CfOrder{
				Order: ccexgo.Order{
					Symbol: cl.symbol,
					ID:     ccexgo.NewStrID(order.OrderID),
				},
			})
			if err != nil || o.Status == ccexgo.OrderStatusUnknown {
				return nil, common.NewErrorf(err, "获取订单结果超时,主动查询失败,%+v", o)
			}
			// 市价只要挂上去了默认当作完全成交？
			return o, nil

		case o := <-orderCh.Orders:
			ctxlog.Info(ctx, "message", "spot get ws order notify", "amount", o.Amount, "filled", o.Filled, "fee", o.Fee, "avg_price", o.AvgPrice, "status", o.Status, "order", o.ID.String())
			ret.ID = ccexgo.NewStrID(order.OrderID)
			ret.Symbol = cl.symbol
			if !o.Created.IsZero() {
				ret.Created = o.Created
			}

			if !o.Updated.Before(ret.Updated) {
				ret.Updated = o.Updated
			}
			ret.Filled = o.Filled
			ret.Side = side
			if cl.actsApi.GetExchangeName() == exchange.Bybit5 {
				ret.Fee = o.Fee
			} else {
				ret.Fee = ret.Fee.Add(o.Fee)
			}

			if o.Status == ccexgo.OrderStatusDone || o.Status == ccexgo.OrderStatusCancel {
				ret.Status = o.Status
				ret.Type = o.Type

				// if side == ccexgo.OrderSideBuy { //因为下市价单所以手续费用taker算。
				// ret.FeeCurrency = cl.symbol.Base()
				// 	ret.AvgPrice = o.AvgPrice
				// } else {
				ret.FeeCurrency = o.FeeCurrency
				ret.AvgPrice = o.AvgPrice
				// }
				ret.WsPush = true
				return &ret, nil
			}
		}
	}
}

// LimitOrder 返回订单
func (cl *Client) LimitOrder(ctx context.Context, side ccexgo.OrderSide, amount decimal.Decimal, price float64) (*exchange.CfOrder, error) {
	var (
		sid apis.OrderSide
		amt float64
	)

	if side == ccexgo.OrderSideBuy {
		sid = apis.OrderSideBuy
	} else if side == ccexgo.OrderSideSell {
		sid = apis.OrderSideSell
	} else {
		return nil, errors.Errorf("unkown orderside '%s'", side)
	}

	exp := CalcExponent(cl.symbol.AmountPrecision())
	amt, _ = amount.Truncate(int32(exp)).Float64()

	order := &apis.Order{
		Symbol:     cl.symbol.String(),
		SymbolType: apis.ProductTypeSpot,
		Type:       apis.OrderTypeMakerOnly,
		Side:       sid,
		Amount:     amt,
		Price:      price,
	}

	if err := cl.actsApi.AddOrders(order); err != nil {
		return nil, err
	}

	return &exchange.CfOrder{
		Order: ccexgo.Order{
			ID:      ccexgo.NewStrID(order.OrderID),
			Price:   decimal.NewFromFloat(price),
			Amount:  decimal.NewFromFloat(amt),
			Symbol:  cl.symbol,
			Created: order.Createtime,
			Side:    side,
			Type:    acts.ActsType2ccex(order.Type),
			Status:  acts.ActsStatus2ccex(order.Status),
		},
	}, nil
}

// LimitOrders 同时挂多个单并返回订单
func (cl *Client) LimitOrders(ctx context.Context, orderType apis.OrderType, side ccexgo.OrderSide, amount []decimal.Decimal, price []float64, extraParams map[string]string) ([]*exchange.CfOrder, error) {
	var (
		sid apis.OrderSide
	)

	if side == ccexgo.OrderSideBuy {
		sid = apis.OrderSideBuy
	} else if side == ccexgo.OrderSideSell {
		sid = apis.OrderSideSell
	} else {
		return nil, errors.Errorf("unkown orderside '%s'", side)
	}

	exp := CalcExponent(cl.symbol.AmountPrecision())
	var orders []*apis.Order
	for i := range amount {
		amt, _ := amount[i].Truncate(int32(exp)).Float64()

		order := &apis.Order{
			ClientOrderID: uuid.NewUniqueString(),
			Symbol:        cl.symbol.String(),
			SymbolType:    apis.ProductTypeSpot,
			Type:          orderType,
			Side:          sid,
			Amount:        amt,
			Price:         price[i],
			ExtraParams:   extraParams,
		}
		orders = append(orders, order)
	}

	result := utilCommon.WorkData{}
	tasks := []*common.TaskCompletionSourceT[*exchange.CfOrder]{}
	start := time.Now()
	for i := range orders { // 只发单，不等响应
		task := common.NewTaskCompletionSourceT[*exchange.CfOrder]()
		tasks = append(tasks, task)
		cl.limitOrderTasks.Store(orders[i].ClientOrderID, task)

		go func(order *apis.Order) {
			if err := cl.actsApi.AddOrders(order); err != nil {
				ctxlog.Warn(ctx, "message", "add order failed", "id", order.OrderID, "error", err.Error())
				task.SetError(err)
			} else {
				task.SetResult(&exchange.CfOrder{Order: ccexgo.Order{
					ID:      ccexgo.NewStrID(order.OrderID),
					Created: order.Timestamp, // 下单响应里把订单更新时间当做创建时间
					Updated: order.Timestamp,
					Status:  acts.ActsStatus2ccex(order.Status),
				}})
			}
		}(orders[i])
	}

	var (
		loaded      bool
		resultOrder []*exchange.CfOrder
	)
	for i, task := range tasks { // 等new推送
		defer exchange.DeleteT(cl.limitOrderTasks, orders[i].ClientOrderID, "spot limitOrderTasks")

		if err := task.WaitTimeout(time.Second * 3); err != nil {
			result.AddData(err, nil)
			continue
		}
		o, err := task.GetResult()
		if err != nil {
			result.AddData(err, nil)
			continue
		}

		order := &exchange.CfOrder{
			Order: ccexgo.Order{
				ID:       o.ID,
				ClientID: ccexgo.StrID(orders[i].ClientOrderID),
				Price:    decimal.NewFromFloat(orders[i].Price),
				Amount:   decimal.NewFromFloat(orders[i].Amount),
				Symbol:   cl.symbol,
				Created:  o.Created,
				Updated:  o.Updated,
				Side:     side,
				Type:     acts.ActsType2ccex(orders[i].Type),
				Status:   o.Status,
			},
		}
		// 如果推送慢到，先把task压入
		_, loaded = cl.finishOrderTasks.LoadOrNew(o.ID.String(), func() *exchange.TaskCompletionSourceT[*exchange.CfOrder] {
			return &exchange.TaskCompletionSourceT[*exchange.CfOrder]{
				CreateTime:            time.Now(),
				TaskCompletionSourceT: *common.NewTaskCompletionSourceT[*exchange.CfOrder](),
			}
		})

		resultOrder = append(resultOrder, order)
	}
	ctxlog.Info(ctx, "message", "spot limit orders", "time_delay", time.Since(start), "start", start, "loaded", loaded)

	return resultOrder, result.Err
}

func (cl *Client) CancelOrder(ctx context.Context, check *exchange.CfOrder) (*exchange.CfOrder, error) {
	order := &apis.Order{
		Symbol:     cl.symbol.String(),
		SymbolType: apis.ProductTypeSpot,
		OrderID:    check.ID.String(),
	}
	if err := cl.actsApi.CancelOrders(order); err != nil { // 查看是什么类型报错，直接去查询订单
		ctxlog.Warn(ctx, "message", "spot cancel order failed", "id", order.OrderID, "error", err.Error())

		errType := cl.HandError(err)
		if errType.Code == exchange.DisConnected || errType.Code == exchange.ContextDeadlineExceeded || errType.Code == exchange.OtherReason { // 网络问题，再撤销一次
			if err := cl.actsApi.CancelOrders(order); err != nil {
				errType = cl.HandError(err)
				ctxlog.Warn(ctx, "message", "spot second cancel order failed", "id", order.OrderID, "error", err.Error())
				return nil, err
			}
		}
	}

	or, err := cl.FetchOrder(ctx, check)

	if err != nil { // 查询订单报错,再查一次
		ctxlog.Warn(ctx, "message", "spot fetch order failed", "id", order.OrderID, "error", err.Error())
		or, err = cl.FetchOrder(ctx, check)
		if err != nil {
			// errType := cl.HandError(err)
			ctxlog.Warn(ctx, "message", "spot second fetch order failed", "id", order.OrderID, "error", err.Error())
			return nil, err
		}
	}
	for or.Status == ccexgo.OrderStatusOpen {
		time.Sleep(50 * time.Millisecond)
		or, err = cl.FetchOrder(ctx, check)

		if err != nil { // 查询订单报错,再查一次
			ctxlog.Warn(ctx, "message", "spot three fetch order failed", "id", order.OrderID, "error", err.Error())
			or, err = cl.FetchOrder(ctx, check)
			if err != nil {
				// errType := cl.HandError(err)
				ctxlog.Warn(ctx, "message", "spot four fetch order failed", "id", order.OrderID, "error", err.Error())
				return nil, err
			}
		}
	}
	return or, nil
}

// 批量撤单
func (cl *Client) CancelOrders(ctx context.Context, checks []*exchange.CfOrder) ([]*exchange.CfOrder, error) {
	var orders []*exchange.CfOrder
	if len(checks) == 0 {
		return orders, nil
	}
	groups := sync.WaitGroup{}
	groups.Add(len(checks))
	wd := utilCommon.WorkData{}
	for i := range checks {
		check := checks[i]
		order := &apis.Order{
			Symbol:     cl.symbol.String(),
			SymbolType: apis.ProductTypeSpot,
			// OrderID:    check.ID.String(),
		}
		if check.ID.String() != "" {
			order.OrderID = check.ID.String()
		} else {
			order.ClientOrderID = check.ClientID.String()
		}
		// 并撤单并查单
		go func(wd *utilCommon.WorkData, group *sync.WaitGroup, order *apis.Order) {
			defer group.Done()

			// 刚好成交了，概率较小
			wsTask, loaded := cl.finishOrderTasks.LoadOrNew(check.ID.String(), func() *exchange.TaskCompletionSourceT[*exchange.CfOrder] {
				return &exchange.TaskCompletionSourceT[*exchange.CfOrder]{
					CreateTime:            time.Now(),
					TaskCompletionSourceT: *common.NewTaskCompletionSourceT[*exchange.CfOrder](),
				}
			})
			if loaded {
				if wsTask.IsDone() { // 直接丢出结果,理论上没有err
					result, _ := wsTask.GetResult()
					time.AfterFunc(time.Second*5, func() {
						exchange.DeleteT(cl.finishOrderTasks, check.ID.String(), "spot CancelOrders finishOrderTasks loaded")
					})
					wd.AddData(nil, result)
					return
				}
			}

			restTask := common.NewTaskCompletionSourceT[*exchange.CfOrder]()
			go func() {
				if err := cl.actsApi.CancelOrders(order); err != nil { // 查看是什么类型报错，直接去查询订单
					ctxlog.Warn(ctx, "message", "spot cancel order failed", "id", order.OrderID, "error", err.Error())
					errType := cl.HandError(err)
					if errType.Code == exchange.DisConnected || errType.Code == exchange.ContextDeadlineExceeded || errType.Code == exchange.OtherReason { // 网络问题，再撤销一次
						if err := cl.actsApi.CancelOrders(order); err != nil {
							ctxlog.Warn(ctx, "message", "spot second cancel order failed", "id", order.OrderID, "error", err.Error())
							errType = cl.HandError(err)
							if errType.Code == exchange.DisConnected || errType.Code == exchange.ContextDeadlineExceeded || errType.Code == exchange.OtherReason { // 网络问题
								// wd.AddData(fmt.Errorf("订单id:%s,再次撤销现货订单失败,错误:%s", order.OrderID, errType), nil)
								restTask.SetError(fmt.Errorf("订单id:%s,再次撤销现货订单失败,错误:%s", order.OrderID, errType))
								return
							}
						}
					}
				}
				restTask.SetResult(&exchange.CfOrder{Order: ccexgo.Order{ID: check.ID, Status: acts.ActsStatus2ccex(order.Status), Filled: decimal.NewFromFloat(order.AmountFilled), Updated: order.Timestamp}})
			}()

			var (
				result *exchange.CfOrder
				err    error
			)
			select {
			case <-wsTask.Done():
				result, _ := wsTask.GetResult()
				wd.AddData(nil, result)
				return
			case <-restTask.Done():
				result, err = restTask.GetResult()
				if err != nil {
					wd.AddData(err, nil)
					return
				}
			}

			// 这里检测下订单状态,币安支持撤单响应
			if result.Status == ccexgo.OrderStatusCancel {
				ctxlog.Info(ctx, "message", "spot cancel order success", "id", order.OrderID, "response", fmt.Sprintf("%+v", order))
				if result.Filled.IsZero() { // 没成交直接返回撤单成功
					wd.AddData(nil, result)
					return
				}
			}

			// ws 推送慢才会走到这里
			or, err := cl.FetchOrder(ctx, check)

			if err != nil { // 查询订单报错,再查一次
				ctxlog.Warn(ctx, "message", "spot fetch order failed", "id", order.OrderID, "client_id", order, "error", err.Error())

				errType := cl.HandError(err)

				if strings.Contains(cl.GetExchangeName(), exchange.Binance) { //TODO 后面和上面的逻辑一起优化吧
					if errType.Code == exchange.NoSuchOrder { // 这种情况大概率就应该是成交了,但是还查不到成交
						// !使用maker的处理方式
						done := make(chan struct{})
						defer close(done)
						restCH := make(chan *exchange.CfOrder, 1)
						go func() {
							or := &exchange.CfOrder{
								Order: ccexgo.Order{
									Symbol: cl.symbol,
									ID:     ccexgo.NewStrID(order.OrderID),
								},
							}

							ticker := time.NewTicker(50 * time.Millisecond)
							defer ticker.Stop()

							for {
								select {
								case <-ticker.C:
									if o, err := cl.FetchOrder(ctx, or); err != nil {
										ctxlog.Warn(ctx, "message", "fetch order fail", "error", err.Error())
									} else {
										if o.Status == ccexgo.OrderStatusDone || o.Status == ccexgo.OrderStatusCancel {
											restCH <- o
											return
										}
									}

								case <-done:
									select {
									case <-restCH:
									default:
									}
									return
								}
							}
						}()

						orderCh := exchange.LoadOrNewOrderChan(cl.makerOrderCh, order.OrderID)
						ret := exchange.CfOrder{}
						timeOut := time.After(5 * time.Second)
						var wsFee decimal.Decimal
						for {
							//?进行超时判断，永续可能需要很久才能获得成交结果
							select {
							case o := <-orderCh.Orders: // ?好像会造成外部结果不可靠
								/*if o.ID.String() != order.OrderID {
									// !不是匹配的id再塞回去,因为可能并发
									// ctxlog.Warn(ctx, "message", "cancel spot skip unmatched order_id", "want", order.OrderID, "got", o.ID.String(), "amount", o.Amount, "fee", o.Fee.Neg(), "filled", o.Filled)
									// cl.makerOrderCH <- o
									continue
								}*/

								ctxlog.Info(ctx, "message", "cancel spot get ws order notify", "amount", o.Amount, "filled", o.Filled, "fee", o.Fee.Neg(), "avg_price", o.AvgPrice, "status", o.Status, "order", o.ID.String())
								wsFee = wsFee.Add(o.Fee)
								if o.Status == ccexgo.OrderStatusDone || o.Status == ccexgo.OrderStatusCancel {
									ret = *o
									ret.Fee = wsFee
									wd.AddData(nil, &ret)
									return
								}

							case <-timeOut:
								wd.AddData(common.NewError("获取现货订单结果超时,主动查询失败,%+v", order), nil)
								return

							case o := <-restCH:
								ctxlog.Info(ctx, "message", "cancel spot get order from rest api", "amount", o.Amount, "filled", o.Filled, "fee", o.Fee.Neg(), "avg_price", o.AvgPrice, "status", o.Status, "order", o.ID.String())
								if o.Status == ccexgo.OrderStatusDone || o.Status == ccexgo.OrderStatusCancel {
									ret = *o
									ret.Fee = o.Fee
									wd.AddData(nil, &ret)
									return
								}
							}
						}
					}
				} else {
					time.Sleep(50 * time.Millisecond)
				}

				or, err = cl.FetchOrder(ctx, check)
				if err != nil {
					ctxlog.Warn(ctx, "message", "spot second fetch order failed", "id", order.OrderID, "error", err.Error())
					wd.AddData(fmt.Errorf("订单id:%s,再次查询现货订单失败,错误:%s", order.OrderID, cl.HandError(err)), nil)
					return
				}
			}
			for or.Status == ccexgo.OrderStatusOpen {
				time.Sleep(50 * time.Millisecond)
				or, err = cl.FetchOrder(ctx, check)

				if err != nil { // 查询订单报错,再查一次
					ctxlog.Warn(ctx, "message", "spot three fetch order failed", "id", order.OrderID, "error", err.Error())
					time.Sleep(50 * time.Millisecond)
					or, err = cl.FetchOrder(ctx, check)
					if err != nil {
						ctxlog.Warn(ctx, "message", "spot four fetch order failed", "id", order.OrderID, "error", err.Error())
						wd.AddData(fmt.Errorf("订单id:%s,四次查询现货订单失败,错误:%s", order.OrderID, cl.HandError(err)), nil)
						return
					}
				}
			}
			wd.AddData(nil, or)
		}(&wd, &groups, order)
	}
	groups.Wait()
	for i := range wd.Data {
		orders = append(orders, wd.Data[i].(*exchange.CfOrder))
	}

	return orders, wd.Err
}

// TODO develop
func (cl *Client) FetchOrder(ctx context.Context, check *exchange.CfOrder) (result *exchange.CfOrder, err error) {
	defer func() {
		if err == nil {
			ctxlog.Info(ctx, "message", "spot fetch order", "result", fmt.Sprintf("%+v", result))
		}
	}()
	wsTask, loaded := cl.finishOrderTasks.LoadOrNew(check.ID.String(), func() *exchange.TaskCompletionSourceT[*exchange.CfOrder] {
		return &exchange.TaskCompletionSourceT[*exchange.CfOrder]{
			CreateTime:            time.Now(),
			TaskCompletionSourceT: *common.NewTaskCompletionSourceT[*exchange.CfOrder](),
		}
	})
	if loaded {
		if wsTask.IsDone() { // 直接丢出结果,理论上没有err
			result, err = wsTask.GetResult()
			time.AfterFunc(time.Second*5, func() {
				exchange.DeleteT(cl.finishOrderTasks, check.ID.String(), "spot finishOrderTasks loaded")
			})
			return
		}
	}

	var order *apis.Order
	restTask := common.NewTaskCompletionSourceT[*exchange.CfOrder]()
	typ := apis.ProductTypeSpot

	// 开协程并等待task，任一返回
	go func() {
		order, err = cl.actsApi.GetOrder(typ, check.ID.String(), cl.symbol.String())
		if err != nil {
			ctxlog.Warn(ctx, "message", "fetch spot order fail", "orderID", check.ID.String(), "err", err)
			restTask.SetError(err)
			return
		}

		var (
			feeCurrency string
		)
		if order.Side == apis.OrderSideBuy {
			feeCurrency = cl.symbol.Base()
		} else {
			feeCurrency = cl.symbol.Quote()
		}

		ret := exchange.CfOrder{
			Order: ccexgo.Order{
				ID:          ccexgo.NewStrID(order.OrderID),
				ClientID:    ccexgo.NewStrID(order.ClientOrderID),
				Symbol:      cl.symbol,
				Price:       decimal.NewFromFloat(order.Price),
				AvgPrice:    decimal.NewFromFloat(order.AveragePrice),
				Amount:      decimal.NewFromFloat(order.Amount),
				Filled:      decimal.NewFromFloat(order.AmountFilled),
				Fee:         decimal.NewFromFloat(order.TotalFee),
				FeeCurrency: feeCurrency,
				Created:     order.Createtime,
				Updated:     order.Timestamp,
				Side:        acts.ActsSide2ccex(order.Side),
				Status:      acts.ActsStatus2ccex(order.Status),
				Type:        acts.ActsType2ccex(order.Type),
			},
		}
		if cl.actsApi.GetExchangeName() == exchange.Okex5 {
			ret.FeeCurrency = order.FeeCurrency
		}
		restTask.SetResult(&ret)
	}()

	select {
	case <-wsTask.Done():
		result, err = wsTask.GetResult()
		time.AfterFunc(time.Second*5, func() {
			exchange.DeleteT(cl.finishOrderTasks, check.ID.String(), "spot finishOrderTasks")
		})
		return result, err
	case <-restTask.Done():
		result, err = restTask.GetResult()
		if err != nil {
			return nil, err
		}
	}

	// rest才会走下来
	if strings.Contains(cl.GetExchangeName(), exchange.Binance) || cl.GetExchangeName() == exchange.Okex5 {
		var feeRate decimal.Decimal
		if result.Type == ccexgo.OrderTypeLimit {
			feeRate = cl.feeRate.Maker
		} else { //下市价单手续费用taker算。
			feeRate = cl.feeRate.Taker
		}
		if strings.Contains(cl.GetExchangeName(), exchange.Binance) {
			if result.Side == ccexgo.OrderSideBuy {
				result.Fee = result.Filled.Mul(feeRate)
			} else {
				result.Fee = result.Filled.Mul(result.AvgPrice).Mul(feeRate)
			}
		}
		if order.Type == apis.OrderTypeLimit && (result.Status == ccexgo.OrderStatusDone || (result.Status == ccexgo.OrderStatusCancel && !result.Filled.IsZero())) { // 限价，有成交再调成交明细
			order2, err := cl.actsApi.GetMyTrades(apis.GetOrderArgs{SymbolType: typ, Symbol: cl.symbol.String(), Id: order.OrderID})
			if err != nil {
				ctxlog.Warn(ctx, "message", "spot getMyTrades err", "orderID", check.ID.String(), "err", err)
				if result.Price.Equal(result.AvgPrice) {
					result.IsMaker = true
				}
			} else {
				var fee decimal.Decimal
				for _, o := range order2 {
					result.FeeCurrency = o.FeeCurrency
					fee = fee.Add(decimal.NewFromFloat(o.Fee))
				}
				if len(order2) != 0 {
					if order2[0].MakerUid > 0 {
						result.IsMaker = true
					}
					result.Fee = fee
				}
			}
		} else if order.Type == apis.OrderTypeMakerOnly {
			result.IsMaker = true
		}
	}
	return result, nil
}

func (cl *Client) HandError(err error) *exchange.ErrorType {
	var errStr string
	switch err := err.(type) {
	case apis.Error:
		errStr = err.Msg
	case common.BaseError:
		errStr = err.Msg
	default:
		errStr = err.Error()
	}
	ctxlog.Info(cl.ctx, "message", "spot HandError", "errStr", errStr)
	switch {
	case strings.Contains(errStr, "unable to process your"):
		return exchange.NewErrorType(exchange.DisConnected, errStr)
	case strings.Contains(errStr, `Too many new orders`):
		return exchange.NewErrorType(exchange.NewOrderLimit, errStr)
	case strings.Contains(errStr, "Unknown order sent"):
		return exchange.NewErrorType(exchange.CancelRejected, errStr)
	case strings.Contains(errStr, "Order does not exist"), strings.Contains(errStr, "order not exists"):
		return exchange.NewErrorType(exchange.NoSuchOrder, errStr)
	case strings.Contains(errStr, "Order would immediately match"), strings.Contains(errStr, "Due to the order could not be"):
		return exchange.NewErrorType(exchange.NewOrderRejected, errStr)
	case strings.Contains(errStr, "insufficient"), strings.Contains(errStr, "available in loan pool to borrow"), strings.Contains(errStr, "Insufficient"):
		if strings.Contains(errStr, "insufficient loanable assets") || strings.Contains(errStr, "loan pool to borrow") {
			return exchange.NewErrorType(exchange.MarketInsufficientLoanableAssets, errStr)
		}
		return exchange.NewErrorType(exchange.AccountInsufficientBalance, errStr)
	case strings.Contains(errStr, `NOTIONAL`), strings.Contains(errStr, `minimum order amount`):
		return exchange.NewErrorType(exchange.MinNotional, errStr)
	case strings.Contains(errStr, `current limit is`), strings.Contains(errStr, `current limit of IP`):
		return exchange.NewErrorType(exchange.WeightLimitPerMinute, errStr)
	case strings.Contains(errStr, `2400 requests per minute`):
		return exchange.NewErrorType(exchange.IpLimitPerMinute, errStr)
	case strings.Contains(errStr, `banned until`):
		return exchange.NewErrorType(exchange.IpBannedUntil, errStr)
	case strings.Contains(errStr, `403 Forbidden`):
		return exchange.NewErrorType(exchange.Forbidden, errStr)
	case strings.Contains(errStr, `pending borrow or repayment`):
		return exchange.NewErrorType(exchange.InBorrowOrRepay, errStr)
	case strings.Contains(errStr, `context deadline exceeded`):
		return exchange.NewErrorType(exchange.ContextDeadlineExceeded, errStr)
	default:
		return exchange.NewErrorType(exchange.OtherReason, errStr)
	}
}

func (cl *Client) Error() error {
	val := cl.err.Load()
	if val != nil {
		return val.(error)
	}
	return nil
}

func (cl *Client) Done() <-chan struct{} {
	return cl.done
}

func (cl *Client) Close() error {
	defer func() {
		time.Sleep(10 * time.Millisecond)
		cl.takerOrderCh.RangeNonReentrant(func(s string, coc *exchange.CfOrderChan) bool {
			close(coc.Orders)
			return true
		})
		cl.takerOrderCh.Clear()
		cl.makerOrderCh.RangeNonReentrant(func(s string, coc *exchange.CfOrderChan) bool {
			close(coc.Orders)
			return true
		})
		cl.makerOrderCh.Clear()
		close(cl.orderbookCH)
	}()
	cl.actsApi.Close()
	cl.exit.Set()
	log := logger.GetLogger()
	level.Info(log).Log("message", "spot closed")
	select {
	case <-cl.done:
		return nil
	case <-time.After(time.Second * 2):
		close(cl.done)
		return fmt.Errorf("quit timeout after 2secs")
	}
}

func (cl *Client) ReconnectWs() error {
	err := cl.actsApi.ReconnectWS()
	if err != nil {
		return errors.WithMessage(err, "reconnect ws fail")
	}

	deadline := time.After(time.Second * 10)
	for {
		if cl.actsApi.CheckWSAlive(apis.ProductTypeSpot) { // todo ActsApi没有检查订阅状态
			return nil
		}
		select {
		case <-time.After(time.Second):
			continue
		case <-deadline:
			return common.NewErrorf(nil, `reconnect websocket timeout`)
		}
	}
}

func (cl *Client) Depth() <-chan *exchange.Depth {
	return cl.orderbookCH
}

func (cl *Client) BalanceNotify() *common.Event {
	return cl.balanceChangeNotify
}

func (cl *Client) BookTickerNotify() *common.Event {
	return cl.bookTickerNotify
}

func (cl *Client) BookTicker() *exchange.BookTicker {
	return cl.bookTicker
}

func (cl *Client) Balance() *exchange.MySyncMap[string, *apis.Balance] {
	if cl.balances.Len() == 0 {
		if cl.actsApi.GetExchangeName() == exchange.Okex5 {
			return cl.balances
		} else { // 初始化获取一遍
			bal, err := cl.actsApi.GetBalance(apis.ProductTypeSpot, ``)
			if err != nil {
				return cl.balances
			}
			for currency, b := range bal.Values {
				if currency != strings.ToUpper(cl.symbol.Base()) && currency != strings.ToUpper(cl.symbol.Quote()) {
					continue
				}
				cl.balances.Store(currency, b)
			}
		}
	}

	return cl.balances
}

func (cl *Client) MakerOrder(orderID string) <-chan *exchange.CfOrder {
	orderCh := exchange.LoadOrNewOrderChan(cl.makerOrderCh, orderID)
	return orderCh.Orders
}

func (cl *Client) FeeRate() exchange.FeeRate {
	return exchange.FeeRate{
		OpenMaker:  cl.feeRate.Maker,
		OpenTaker:  cl.feeRate.Taker,
		CloseMaker: cl.feeRate.Maker,
		CloseTaker: cl.feeRate.Taker,
	}
}

func (cl *Client) LevelInfo() exchange.LevelInfo {
	levelInfo := exchange.LevelInfo{
		Level: 1,
	}
	if cl.actsApi.GetExchangeName() == exchange.Okex5 {
		levelInfo, _, _ = cache_service.LoadOrStore2("spot_margin_level", 5*time.Minute, 0, func() (exchange.LevelInfo, error) {
			l := exchange.LevelInfo{
				Level: 1,
			}
			params := &actsApi.RawRequestParams{
				Method:  http.MethodGet,
				Private: true,
				Headers: make(map[string]string),
			}
			params.Headers["Content-Type"] = "application/json"
			params.Url = "/account/leverage-info"
			params.Params = make(map[string]any)
			params.Params[`mgnMode`] = "cross"
			params.Params[`instId`] = cl.symbol.Base() + "-" + cl.symbol.Quote()
			rep, err := cl.actsApi.(*actsApi.ActsAPI).RawRequestJSON(params)
			if err != nil || rep.Get(`code`).Int() != 0 {
				if err == nil {
					err = fmt.Errorf("%s", rep.String())
				}
				level.Warn(logger.GetLogger()).Log("message", "spot level info error", "err", err.Error())
				return l, err
			}

			for _, js := range rep.Get(`data`).Array() {
				if js.Get(`instId`).String() == cl.symbol.Base() {
					l.Level = js.Get(`lever`).Float()
					break
				}
			}
			return l, nil
		})
	}
	return levelInfo
}

func (cl *Client) loop(ctx context.Context) {
	defer close(cl.done)
	clearTimer := time.NewTimer(15 * time.Second)
	for {
		select {
		case <-ctx.Done(): // 外部context结束
			return
		case <-cl.exit.Done(): // 主动销毁实例
			return
		case <-clearTimer.C:
			cl.wsOrders.DeleteIf(func(k string, v *exchange.CfOrder) bool {
				var delete bool
				if v.Status == ccexgo.OrderStatusDone || v.Status == ccexgo.OrderStatusCancel { // 正常马上就会消费
					if time.Since(v.Updated).Seconds() > 10 {
						delete = true
					}
				} else if time.Since(v.Updated).Seconds() > 600 { // 其余状态的订单缓存10min
					delete = true
				}
				if delete {
					ctxlog.Info(cl.ctx, "message", "spot delete ws orders", "id", v.ID, "order_update", v.Updated, "since_update", time.Since(v.Updated))
					return true
				}
				return false
			})
			cl.finishOrderTasks.DeleteIf(func(k string, v *exchange.TaskCompletionSourceT[*exchange.CfOrder]) bool {
				delete := false
				if v.IsDone() {
					fo, _ := v.GetResult()
					if time.Since(fo.Updated).Seconds() > 15 { // 超过15s没获取task结果
						ctxlog.Info(cl.ctx, "message", "spot delete finish orders task", "id", k, "order_update", fo.Updated, "task_create", v.CreateTime)
						delete = true
					}
				} else {
					if time.Since(v.CreateTime).Seconds() > 600 { // ! 600s无成交？无推送
						ctxlog.Info(cl.ctx, "message", "spot delete finish orders task", "id", k, "task_create", v.CreateTime)
						delete = true
					}
				}
				return delete
			})

			cl.takerOrderCh.DeleteIf(func(k string, v *exchange.CfOrderChan) bool {
				delete := false
				if time.Since(v.UpdatedAt).Seconds() > 60 { // 60s无推送
					close(v.Orders)
					ctxlog.Info(cl.ctx, "message", "spot delete taker orders", "id", k, "update", v.UpdatedAt, "len", len(v.Orders))
					delete = true
				}
				return delete
			})

			cl.makerOrderCh.DeleteIf(func(k string, v *exchange.CfOrderChan) bool {
				delete := false
				if v.Status == apis.OrderStatusFilled || v.Status == apis.OrderStatusCanceled {
					if time.Since(v.UpdatedAt).Seconds() > 10 {
						delete = true
					}
				} else if time.Since(v.UpdatedAt).Seconds() > 600 { // 600s无推送
					delete = true
				}
				if delete {
					close(v.Orders)
					ctxlog.Info(cl.ctx, "message", "spot delete maker orders", "id", k, "update", v.UpdatedAt, "len", len(v.Orders))
				}
				return delete
			})

			clearTimer = time.NewTimer(15 * time.Second)
		}
	}
}

func CalcExponent(precision decimal.Decimal) int {
	return exchange.CalcExponent(precision)
}

func (cl *Client) OnOrders(orders *apis.OrderEvent) {
	for _, o := range orders.Orders {
		fee := decimal.NewFromFloat(o.TradeFee)              // websocket使用订单逐笔成交手续费
		if cl.actsApi.GetExchangeName() == exchange.Bybit5 { // 总成交fee
			fee = decimal.NewFromFloat(o.TotalFee)
		}
		pushO := &exchange.CfOrder{
			Order: ccexgo.Order{
				ID:          ccexgo.NewStrID(o.OrderID),
				ClientID:    ccexgo.NewStrID(o.ClientOrderID),
				Symbol:      cl.symbol,
				AvgPrice:    decimal.NewFromFloat(o.AveragePrice),
				Amount:      decimal.NewFromFloat(o.Amount),
				Filled:      decimal.NewFromFloat(o.AmountFilled),
				Fee:         fee,
				FeeCurrency: o.FeeCurrency,
				Created:     o.Createtime,
				Updated:     o.Timestamp,
				Side:        acts.ActsSide2ccex(o.Side),
				Status:      acts.ActsStatus2ccex(o.Status),
				Type:        acts.ActsType2ccex(o.Type),
			},
			IsMaker: o.IsMaker,
			WsPush:  true,
		}

		if o.Type == apis.OrderTypeMakerOnly { // 强制变成maker,避免交易所推送问题
			pushO.IsMaker = true // cancel状态的话, 如果部分成交取消后, maker态会变成false
		}

		cl.wsOrders.LoadAndUpdate(o.OrderID, func(old *exchange.CfOrder) (new *exchange.CfOrder, updated bool) {
			if old == nil {
				new = pushO
				updated = true
				return
			}
			new = old
			if new.Filled.LessThan(pushO.Filled) || new.Updated.Before(pushO.Updated) || new.Status < pushO.Status {
				new.AvgPrice = pushO.AvgPrice
				new.Filled = pushO.Filled
				new.Updated = pushO.Updated
				new.Status = pushO.Status
				if pushO.Status != ccexgo.OrderStatusCancel { // cancel状态的话，如果部分成交，前面肯定有一条部分成交推送？这样就能更新maker标记
					new.IsMaker = pushO.IsMaker // 部分成交取消后,maker态会变成false
				}
				if pushO.FeeCurrency != `` {
					new.FeeCurrency = pushO.FeeCurrency
				}

				if cl.actsApi.GetExchangeName() == exchange.Bybit5 {
					new.Fee = pushO.Fee
				} else {
					new.Fee = new.Fee.Add(pushO.Fee)
				}

				updated = true
			}

			if o.Status == apis.OrderStatusFilled || o.Status == apis.OrderStatusCanceled || o.Status == apis.OrderStatusFilledPartiallyAndCanceled {
				task, _ := cl.finishOrderTasks.LoadOrNew(o.OrderID, func() *exchange.TaskCompletionSourceT[*exchange.CfOrder] {
					return &exchange.TaskCompletionSourceT[*exchange.CfOrder]{
						CreateTime:            time.Now(),
						TaskCompletionSourceT: *common.NewTaskCompletionSourceT[*exchange.CfOrder](),
					}
				})
				task.SetResult(new)
			}

			return new, updated
		})

		if o.Type == apis.OrderTypeMakerOnly || o.Type == apis.OrderTypeLimit { //挂单
			if pushO.Status == ccexgo.OrderStatusOpen && pushO.Filled.IsZero() {
				task, ok := cl.limitOrderTasks.Load(pushO.ClientID.String())
				if !ok {
					level.Warn(logger.GetLogger()).Log("message", "spot maker order open but limits order not exits", "order", fmt.Sprintf("%+v", o))
				} else {
					task.SetResult(pushO)
				}
				continue
			}

			orderCh := exchange.LoadOrNewOrderChan(cl.makerOrderCh, o.OrderID)
			if err := util.ChanSafeSend(orderCh.Orders, pushO, func() {
				/*log := logger.GetLogger()
				for len(cl.makerOrderCH) > 2*cap(cl.makerOrderCH)/3 { // 写不进，丢弃最前面的1/3
					removeO := <-cl.makerOrderCH
					level.Warn(log).Log("message", "spot maker order channel full", "removeOrder", fmt.Sprintf("%+v", removeO), "len", len(cl.makerOrderCH), "ex", cl.actsApi.GetExchangeName())
				}
				cl.makerOrderCH <- newO*/
				common.PanicWithTime(fmt.Sprintf("%s maker order channel full", o.OrderID))
			}); err != nil {
				go message.Send(context.Background(), message.NewCommonMsg(fmt.Sprintf("%s channel异常", cl.symbol.String()), fmt.Sprintf("makerOrderCH 偶现异常,联系开发查看日志确定问题 %s", err)))
				return
			}
			orderCh.UpdatedAt = time.Now()
			orderCh.Status = o.Status
		} else if o.Type == apis.OrderTypeMarket {
			if strings.HasSuffix(o.ClientOrderID, acts.ManualTakerOpSuffix) {
				level.Info(logger.GetLogger()).Log("message", "spot get manual order", "order", fmt.Sprintf("%+v", o))
				continue
			}
			orderCh := exchange.LoadOrNewOrderChan(cl.takerOrderCh, o.OrderID)
			_, loaded := cache_service.Instance().Get("spot_rest_" + o.OrderID)
			if !loaded {
				if err := util.ChanSafeSend(orderCh.Orders, pushO, func() {
					common.PanicWithTime(fmt.Sprintf("%s taker order channel full", o.OrderID))
				}); err != nil {
					go message.Send(context.Background(), message.NewCommonMsg(fmt.Sprintf("%s channel异常", cl.symbol.String()), fmt.Sprintf("orderCH 偶现异常,联系开发查看日志确定问题 %s", err)))
					return
				}
				orderCh.UpdatedAt = time.Now()
			} else { // 说明rest下单响应拿到结果了，避免channel堵塞
				level.Warn(logger.GetLogger()).Log("message", "spot order ignore", "order", fmt.Sprintf("%+v", pushO), "len", len(orderCh.Orders), "ex", cl.actsApi.GetExchangeName())
			}
		}
	}
}

func (cl *Client) OnBalances(e *apis.BalanceEvent) {
	if len(e.Values) == 0 {
		return
	}
	// 只推变动的资产过来
	change := false
	for _, currency := range []string{strings.ToUpper(cl.symbol.Base()), strings.ToUpper(cl.symbol.Quote())} {
		b := e.Get(currency)
		// 更新内存快照
		cl.balances.LoadAndUpdate(currency, func(old *apis.Balance) (new *apis.Balance, updated bool) {
			if old == nil {
				new = b
				updated = true
				change = true
				return
			}
			if b.Time.IsZero() {
				return
			}
			if !old.Free.Equal(b.Free) {
				if cl.actsApi.GetExchangeName() != exchange.Okex5 || (cl.actsApi.GetExchangeName() == exchange.Okex5 && !exchange.AlmostEqual(old.Total.Sub(old.UnrealizedProfit), b.Total.Sub(b.UnrealizedProfit))) {
					level.Info(logger.GetLogger()).Log("message", "spot OnBalances", "currency", currency, "old", fmt.Sprintf("%+v", old), "new", fmt.Sprintf("%+v", b))
				}
			} else {
				log.Info("[Spot] OnBalances currency=%s old=%+v new=%+v ", currency, old, b)
			}
			if !old.Time.After(b.Time) {
				new = b
				updated = true
				if currency == strings.ToUpper(cl.symbol.Base()) {
					if !old.Total.Equal(b.Total) {
						if cl.actsApi.GetExchangeName() == exchange.Okex5 {
							if !exchange.AlmostEqual(old.Total.Sub(old.UnrealizedProfit), b.Total.Sub(b.UnrealizedProfit)) {
								change = true
							}
						} else {
							change = true
						}
					}
				}
			}
			return
		})
	}
	if change && (cl.actsApi.GetExchangeName() != exchange.Binance || cl.binanceActsApi != nil) {
		cl.balanceChangeNotify.Set()
	}
}

func (cl *Client) OnDepths(depths *apis.DepthEvent) {
	newDs := &ccexgo.OrderBook{
		Symbol:  cl.symbol,
		Bids:    make([]ccexgo.OrderElem, 0),
		Asks:    make([]ccexgo.OrderElem, 0),
		Created: depths.Time,
	}
	for _, ask := range depths.Asks {
		newDs.Asks = append(newDs.Asks, ccexgo.OrderElem{
			Price:  ask.Price,
			Amount: ask.Amount,
		})
	}
	for _, bid := range depths.Bids {
		newDs.Bids = append(newDs.Bids, ccexgo.OrderElem{
			Price:  bid.Price,
			Amount: bid.Amount,
		})
	}
	// b, _ := json.Marshal(depths)
	// level.Info(logger.GetLogger()).Log("message", "spot depth", "depthId", cl.depthID, "depths.Asks0", fmt.Sprintf("%+v", depths.Asks[0]), "depths.Bids0", fmt.Sprintf("%+v", depths.Bids[0]), "depthCreated", depths.Time, "depthEvent", depths.EventTime)

	depthCh := &exchange.Depth{OrderBook: newDs, ID: cl.depthID}

	if err := util.ChanSafeSend(cl.orderbookCH, depthCh, func() {
		for len(cl.orderbookCH) > 1*cap(cl.orderbookCH)/2 { // 写不进，丢弃最前面的1/3,前面的过时没处理就不处理了
			removeDepth := <-cl.orderbookCH
			level.Warn(logger.GetLogger()).Log("message", "spot depth channel full", "removeDepth", removeDepth.ID, "len", len(cl.orderbookCH))
		}
		cl.orderbookCH <- depthCh
	}); err != nil {
		go message.SendP3Important(context.Background(), message.NewCommonMsgWithImport(fmt.Sprintf("%s channel异常", cl.symbol.String()), "orderbookCH 偶现异常,联系开发查看日志确定问题"))
	}

	cl.depthID++
}

func (cl *Client) OnBookTicker(raw []byte) {
	/*
		{
		  "u":400900217,     // order book updateId
		  "s":"BNBUSDT",     // 交易对
		  "b":"25.35190000", // 买单最优挂单价格
		  "B":"31.21000000", // 买单最优挂单数量
		  "a":"25.36520000", // 卖单最优挂单价格
		  "A":"40.66000000"  // 卖单最优挂单数量
		}
	*/
	data := gjson.ParseBytes(raw)
	bt := exchange.BookTicker{}
	if cl.actsApi.GetExchangeName() == exchange.Okex5 {
		if data.Get(`event`).String() == `subscribe` {
			return
		}
		d := data.Get(`data`).Array()
		if len(d) == 0 {
			return
		}
		first := d[0]
		bt.ID = first.Get(`seqId`).Int()
		bt.TimeStamp = time.UnixMilli(first.Get(`ts`).Int())
		bt.Ask1Price = first.Get(`asks.0.0`).Float()
		bt.Ask1Amount = first.Get(`asks.0.1`).Float()
		bt.Bid1Price = first.Get(`bids.0.0`).Float()
		bt.Bid1Amount = first.Get(`bids.0.1`).Float()
	} else if cl.GetExchangeName() == exchange.Bybit5 {
		if data.Get(`op`).String() == `pong` || data.Get(`op`).String() == `subscribe` {
			return
		}
		book := data.Get(`data`)
		bt.ID = book.Get(`u`).Int()
		bt.TimeStamp = time.UnixMilli(data.Get(`cts`).Int())
		bt.Ask1Price = book.Get(`a.0.0`).Float()
		bt.Ask1Amount = book.Get(`a.0.1`).Float()
		bt.Bid1Price = book.Get(`b.0.0`).Float()
		bt.Bid1Amount = book.Get(`b.0.1`).Float()
	} else {
		bt.ID = data.Get(`u`).Int()
		bt.TimeStamp = time.Now()
		bt.Ask1Price = data.Get(`a`).Float()
		bt.Ask1Amount = data.Get(`A`).Float()
		bt.Bid1Price = data.Get(`b`).Float()
		bt.Bid1Amount = data.Get(`B`).Float()
	}

	cl.bookTicker = &bt
	cl.bookTickerNotify.Set()
	// ctxlog.Info(cl.ctx, "message", "spot OnBookTicker", "data", data.String())
}

func (cl *Client) GetExchangeName() string {
	return cl.actsApi.GetExchangeName()
}
