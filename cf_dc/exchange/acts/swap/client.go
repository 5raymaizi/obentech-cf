package swap

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
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
	"go.common/helper"
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
		symbol              ccexgo.Symbol
		actsSymbol          *apis.Symbol
		inverse             bool // 是否反向合约
		putInTradeCurrency  bool // 用现货交易币做本金,即BTC
		actsApi             apis.Interface
		exit                *common.Event
		done                chan struct{}
		balanceChangeNotify *common.Event
		bookTickerNotify    *common.Event
		posChangeNotify     *common.Event
		orderbookCH         chan *exchange.Depth
		indexCH             chan *apis.IndexEvent
		balances            *exchange.MySyncMap[string, *apis.Balance]
		fundingRate         *apis.FundingRate
		levelInfo           *exchange.LevelInfo
		bookTicker          *exchange.BookTicker
		err                 atomic.Value
		feeRate             *exchange.FeeRate
		feeCurrency         string
		depthID             uint64
		posInfo             struct {
			pos *exchange.Position // ws缓存pos
			mu  sync.Mutex
		}
		limitOrderTasks  *gmap.SyncMap[string, *common.TaskCompletionSourceT[*exchange.CfOrder]]
		finishOrderTasks *gmap.SyncMap[string, *exchange.TaskCompletionSourceT[*exchange.CfOrder]]
		wsOrders         *gmap.SyncMap[string, *exchange.CfOrder] // 存储orders
		takerOrderCh     *gmap.SyncMap[string, *exchange.CfOrderChan]
		makerOrderCh     *gmap.SyncMap[string, *exchange.CfOrderChan]
		adlOrderCh       chan *apis.Order

		balanceCurrencies []string
		ask1Price         float64
		bid1Price         float64
	}
)

func NewClientCB(symbol ccexgo.Symbol, actsSymbol *apis.Symbol, exchangeName, key, secret string, reverseOperation, vault bool) func(ctx context.Context) (exchange.SwapClient, error) {
	return func(ctx context.Context) (exchange.SwapClient, error) {
		client, err := NewClient(symbol, actsSymbol, exchangeName, key, secret, reverseOperation, vault)
		if err != nil {
			return nil, err
		}
		// if err := client.Start(ctx); err != nil {
		// 	client.Close()
		// 	client.actsApi.Close()
		// 	return nil, errors.WithMessage(err, "start client fail")
		// }
		return client, nil
	}
}

func NewClient(symbol ccexgo.Symbol, actsSymbol *apis.Symbol, exchangeName, key, secret string, reverseOperation, vault bool) (*Client, error) {
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
				return &Client{}, fmt.Errorf("acts swap %s api init fail", exchangeName)
			}
			logStr = fmt.Sprintf(`%s,%s,password=%s`, logStr, apiSecret, apiPassphrase)
		} else {
			logStr = fmt.Sprintf(`%s,%s`, logStr, apiSecret)
		}
	}

	api, err := apiFactory.GetApiAndLogin(logStr)
	if err != nil {
		return &Client{}, errors.WithMessage(err, "acts swap api init fail")
	}
	inverse := actsSymbol.ContractValueIsInQuoteCurrency
	ret := &Client{
		symbol:              symbol,
		actsSymbol:          actsSymbol,
		inverse:             inverse,
		putInTradeCurrency:  !inverse == !reverseOperation,
		actsApi:             api,
		exit:                common.NewEvent(),
		done:                make(chan struct{}),
		orderbookCH:         make(chan *exchange.Depth, 128),
		indexCH:             make(chan *apis.IndexEvent, 10),
		balanceChangeNotify: common.NewEvent(),
		bookTickerNotify:    common.NewEvent(),
		posChangeNotify:     common.NewEvent(),
		feeCurrency:         actsSymbol.BalanceCurrency,
		balances:            &exchange.MySyncMap[string, *apis.Balance]{SyncMap: gmap.NewSyncMap[string, *apis.Balance]()},
		limitOrderTasks:     gmap.NewSyncMap[string, *common.TaskCompletionSourceT[*exchange.CfOrder]](),
		finishOrderTasks:    gmap.NewSyncMap[string, *exchange.TaskCompletionSourceT[*exchange.CfOrder]](),
		wsOrders:            gmap.NewSyncMap[string, *exchange.CfOrder](),
		takerOrderCh:        gmap.NewSyncMap[string, *exchange.CfOrderChan](),
		makerOrderCh:        gmap.NewSyncMap[string, *exchange.CfOrderChan](),
		adlOrderCh:          make(chan *apis.Order, 32),
	}

	ret.balanceCurrencies = append(ret.balanceCurrencies, ret.feeCurrency)
	if strings.Contains(ret.actsApi.GetExchangeName(), exchange.Binance) && ret.symbol.String() == `BNBUSDT` {
		ret.balanceCurrencies = append(ret.balanceCurrencies, `BNB`)
	}

	return ret, nil
}

func (cl *Client) Position(ctx context.Context) (*exchange.Position, error) {
	if cl.posInfo.pos == nil || cl.GetExchangeName() == exchange.HyperLiquid { // ws没有数据从rest拿
		return cl.GetPosition(ctx)
	}
	return cl.posInfo.pos, nil
}

func (cl *Client) GetPosition(ctx context.Context) (*exchange.Position, error) {
	pos, err := cl.actsApi.GetPositions(cl.actsSymbol.ProductType, cl.symbol.String())
	if err != nil {
		return nil, errors.WithMessage(err, "get position info fail")
	}
	if len(pos.Value) == 0 {
		return &exchange.Position{Long: ccexgo.Position{
			Symbol: cl.symbol,
			Side:   ccexgo.PositionSideLong,
		}, Short: ccexgo.Position{
			Symbol: cl.symbol,
			Side:   ccexgo.PositionSideShort,
		}}, nil
	}
	// rest 更新ws缓存
	ePos, err := cl.processPosition(pos, "rest")
	if err != nil {
		return nil, err
	}
	cl.posInfo.mu.Lock()
	defer cl.posInfo.mu.Unlock()
	lastPos := cl.posInfo.pos
	if lastPos != nil {
		if lastPos.Long.CreateTime.Before(ePos.Long.CreateTime) || lastPos.Short.CreateTime.Before(ePos.Short.CreateTime) {
			cl.posInfo.pos = ePos
		}
	} else {
		cl.posInfo.pos = ePos
	}
	return ePos, nil
}

func (cl *Client) processPosition(pos *apis.Positions, source string) (*exchange.Position, error) {
	var (
		long, short *apis.Position
	)
	ePos := &exchange.Position{}

	for i := range pos.Value {
		p := pos.Value[i]
		if source == "rest" {
			ctxlog.Info(cl.ctx, "message", "processPosition", "pos", fmt.Sprintf("%+v", p), "exchange", cl.actsApi.GetExchangeName())
		} else { // 打到另一个日志里去
			log.Info("processPosition pos=%+v exchange=%s", p, cl.actsApi.GetExchangeName())
		}
		switch p.Direction {
		case apis.OrderSideSell: // 币本位和反向u本位
			short = p
		case apis.OrderSideBuy:
			long = p
		case apis.OrderSideBoth:
			if cl.GetExchangeName() == exchange.HyperLiquid { // 暂时只给hyperliquid处理单向持仓
				if p.Amount > 0 {
					p.Direction = apis.OrderSideBuy
					long = p
				} else {
					p.Direction = apis.OrderSideSell
					p.Amount = math.Abs(p.Amount)
					short = p
				}
			}
		}
	}

	// 构造空仓
	if short == nil {
		short = &apis.Position{Direction: apis.OrderSideSell, Leverage: 1}
	}
	if long == nil {
		long = &apis.Position{Direction: apis.OrderSideBuy, Leverage: 1}
	}
	processP := func(position *apis.Position) {
		if position != nil {
			ret := &ccexgo.Position{
				Symbol:        cl.symbol,
				Mode:          ccexgo.PositionModeCross,
				Position:      decimal.NewFromFloat(position.Amount),
				AvailPosition: decimal.NewFromFloat(position.Available),
				AvgOpenPrice:  decimal.NewFromFloat(position.AverageOpenPrice),
				UNRealizedPNL: decimal.NewFromFloat(position.UnrealizedPnL),
				Margin:        decimal.NewFromFloat(position.PositionMargin),
				CreateTime:    position.Time,
			}
			if cl.needTransform() { // 币安u本位是个数，都兼容用个数
				exp := exchange.CalcExponent(decimal.NewFromFloat(cl.actsSymbol.AmountTickSize * cl.actsSymbol.ContractValue))
				ret.Position = decimal.NewFromFloat(position.Amount * cl.actsSymbol.ContractValue).Round(int32(exp))
				ret.AvailPosition = decimal.NewFromFloat(position.Available * cl.actsSymbol.ContractValue).Round(int32(exp))
			}

			if position.Direction.IsLong() {
				ret.Side = ccexgo.PositionSideLong
				ePos.Long = *ret
				if (source == "rest" && strings.Contains(cl.actsApi.GetExchangeName(), exchange.Binance)) || cl.actsApi.GetExchangeName() == exchange.Okex5 { // 币安的ws推送里没有倍数信息
					cl.levelInfo = &exchange.LevelInfo{
						Level:            position.Leverage,
						MaxNotionalValue: decimal.NewFromFloat(position.MaxNotionValue),
					}
				}
			} else {
				ret.Side = ccexgo.PositionSideShort
				ePos.Short = *ret
				if (source == "rest" && strings.Contains(cl.actsApi.GetExchangeName(), exchange.Binance)) || cl.actsApi.GetExchangeName() == exchange.Okex5 {
					cl.levelInfo = &exchange.LevelInfo{
						Level:            position.Leverage,
						MaxNotionalValue: decimal.NewFromFloat(position.MaxNotionValue),
					}
				}
			}
			r, err := json.Marshal(position)
			if err == nil {
				ret.Raw = string(r)
			}
		}
	}

	processP(long)
	processP(short)

	// if cl.actsSymbol.ProductType == apis.ProductTypeSwap {
	// 	if (cl.inverse && !cl.putInTradeCurrency) || (!cl.inverse && !cl.putInTradeCurrency) {
	// 		if !ePos.Long.Position.IsZero() {
	// 			return nil, fmt.Errorf("存在多仓与实际要开方向仓位不符 long:%+v", ePos.Long)
	// 		}
	// 	} else {
	// 		if !ePos.Short.Position.IsZero() { // 应持有多仓，但持有空仓就报错
	// 			return nil, fmt.Errorf("存在空仓与实际要开方向仓位不符 short:%+v", ePos.Short)
	// 		}
	// 	}
	// }

	return ePos, nil
}

func (cl *Client) FetchBalance(ctx context.Context) ([]ccexgo.Balance, error) {
	balances, err := cl.actsApi.GetBalance(cl.actsSymbol.ProductType, cl.feeCurrency)
	if err != nil {
		return nil, errors.WithMessage(err, "fetch balance failed")
	}

	level.Info(logger.GetLogger()).Log("message", "swap fetch balance ", "balances", fmt.Sprintf("%+v", balances))

	ret := []ccexgo.Balance{}
	bal := balances.Get(cl.feeCurrency)
	ret = append(ret, ccexgo.Balance{
		Currency: cl.feeCurrency,
		Total:    bal.Total,
		Frozen:   bal.Used,
		Free:     bal.Free, //free位可提款数量,用于查问题
	})

	cl.OnBalances(balances)

	return ret, nil
}

func (cl *Client) InitFeeRate(ctx context.Context) error {
	if cl.feeRate != nil {
		return nil
	}

	if cl.GetExchangeName() == exchange.HyperLiquid {
		cl.ctx = ctx
		cl.feeRate = &exchange.FeeRate{}
		cl.feeRate.CloseMaker = decimal.NewFromFloat(0.00015)
		cl.feeRate.CloseTaker = decimal.NewFromFloat(0.00045)
		cl.feeRate.OpenMaker = decimal.NewFromFloat(0.00015)
		cl.feeRate.OpenTaker = decimal.NewFromFloat(0.00045)
		return nil
	}

	fr, err := cl.actsApi.GetTradeFee(cl.actsSymbol.ProductType, cl.symbol.String()) // 接入acts

	if err != nil {
		return errors.WithMessage(err, fmt.Sprintf("fetch %s swap fee rate fail ", cl.actsApi.GetExchangeName()))
	}
	if len(fr) == 0 {
		return fmt.Errorf("%s fetch swap trade fee is null", cl.actsApi.GetExchangeName())
	}

	cl.ctx = ctx
	cl.feeRate = &exchange.FeeRate{}
	cl.feeRate.CloseMaker = decimal.NewFromFloat(fr[0].CloseMakerFee)
	cl.feeRate.CloseTaker = decimal.NewFromFloat(fr[0].CloseTakerFee)
	cl.feeRate.OpenMaker = decimal.NewFromFloat(fr[0].OpenMakerFee)
	cl.feeRate.OpenTaker = decimal.NewFromFloat(fr[0].OpenTakerFee)
	ctxlog.Info(ctx, "message", "swap feeRate", "maker", cl.feeRate.CloseMaker, "taker", cl.feeRate.CloseTaker, "exchange", cl.actsApi.GetExchangeName())
	return nil
}

func (cl *Client) Start(ctx context.Context, subscribeTrade bool) error {
	if err := cl.InitFeeRate(ctx); err != nil {
		return err
	}

	go cl.loop(ctx)

	// indexSymbol := cl.symbol.String()
	// if cl.actsApi.GetExchangeName() == exchange.Okex5 {
	// 	indexSymbol = cl.actsSymbol.BaseCurrency + `-USDT`
	// }

	// if err := cl.actsApi.SubscribeIndex(cl.OnIndex, cl.actsSymbol.ProductType, indexSymbol); err != nil {
	// 	return errors.WithMessage(err, "subscribe index fail")
	// }
	if err := cl.actsApi.SubscribeDepths(cl.OnDepths, cl.actsSymbol.ProductType, 5, cl.symbol.String()); err != nil {
		return errors.WithMessage(err, "subscribe depth fail")
	}

	if cl.actsSymbol.ProductType == apis.ProductTypeSwap {
		if cl.GetExchangeName() != exchange.HyperLiquid {
			if err := cl.actsApi.SubscribeFundingRate(cl.OnFundingRate, cl.actsSymbol.ProductType, cl.symbol.String()); err != nil {
				return errors.WithMessage(err, "subscribe funding rate fail")
			}
		}

		if cl.actsApi.GetExchangeName() == exchange.Okex5 || strings.Contains(cl.actsApi.GetExchangeName(), exchange.Binance) ||
			cl.GetExchangeName() == exchange.Bybit5 || cl.GetExchangeName() == exchange.GateIO || cl.GetExchangeName() == exchange.HyperLiquid {
			cl.SubscribeBookTicker(subscribeTrade)
		}
	}

	if err := cl.actsApi.SubscribeOrders(cl.OnOrders, cl.actsSymbol.ProductType, cl.symbol.String()); err != nil {
		return errors.WithMessage(err, "subscribe orders fail")
	}

	cl.actsApi.(*actsApi.ActsAPI).SetIgnoreResubscribeError(true)

	if cl.GetExchangeName() == exchange.HyperLiquid {
		return nil
	}

	if err := cl.actsApi.SubscribeBalance(cl.OnBalances, cl.actsSymbol.ProductType, cl.feeCurrency); err != nil {
		return errors.WithMessage(err, "subscribe balances fail")
	}

	if err := cl.actsApi.SubscribePositions(cl.OnPositions, cl.actsSymbol.ProductType, cl.symbol.String()); err != nil {
		return errors.WithMessage(err, "subscribe position fail")
	}

	return nil
}

func (cl *Client) SubscribeBookTicker(subscribeTrade bool) {
	var subscribeMsg []byte
	symbol := strings.ToLower(strings.ReplaceAll(cl.symbol.String(), "_", ""))
	url := `wss://dstream.binance.com/ws/%s@bookTicker`
	if !cl.inverse {
		url = `wss://fstream.binance.com/ws/%s@bookTicker`
	} else {
		symbol = fmt.Sprintf("%s_perp", symbol)
	}
	url = fmt.Sprintf(url, symbol)
	if cl.actsApi.GetExchangeName() == exchange.Okex5 {
		symbol = strings.ReplaceAll(cl.symbol.String(), "_", "-")
		url = acts.GetOkxWsUrl()
		if subscribeTrade {
			subscribeMsg = fmt.Appendf(nil, `{"op": "subscribe","args": [{"channel": "bbo-tbt","instId": "%s"}, {"channel": "trades","instId": "%s"}]}`, symbol, symbol)
		} else {
			subscribeMsg = fmt.Appendf(nil, `{"op": "subscribe","args": [{"channel": "bbo-tbt","instId": "%s"}]}`, symbol)
		}
	} else if cl.GetExchangeName() == exchange.Bybit5 {
		symbol = strings.ToUpper(cl.symbol.String())
		url = `wss://stream.bybit.com/v5/public/linear`
		if cl.inverse {
			url = `wss://stream.bybit.com/v5/public/inverse`
		}
		subscribeMsg = fmt.Appendf(nil, `{"op": "subscribe","args": ["orderbook.1.%s"]}`, symbol)
	} else if cl.GetExchangeName() == exchange.GateIO {
		symbol = strings.ToUpper(cl.symbol.String())
		url = `wss://fx-ws.gateio.ws/v4/ws/usdt`
		subscribeMsg = fmt.Appendf(nil, `{"event": "subscribe","channel":"futures.book_ticker","payload": ["%s"]}`, symbol)
	} else if cl.GetExchangeName() == exchange.HyperLiquid {
		url = `wss://api.hyperliquid.xyz/ws`
		symbol = strings.TrimSuffix(strings.ToUpper(cl.symbol.String()), "USDT")
		subscribeMsg = fmt.Appendf(nil, `{"method":"subscribe","subscription":{"type":"bbo","coin":"%s"}}`, symbol)
	}
	wsClient := ws.NewWebSocketClient(cl.ctx, url, subscribeMsg, cl.OnBookTicker)
	go wsClient.Run(cl.exit)
}

func (cl *Client) FetchOrder(ctx context.Context, order *exchange.CfOrder) (result *exchange.CfOrder, err error) {
	defer func() {
		if err == nil {
			ctxlog.Info(ctx, "message", "swap fetch order", "result", fmt.Sprintf("%+v", result))
		} else {
			ctxlog.Warn(ctx, "message", "swap fetch order fail", "orderID", order.ID.String(), "error", err.Error())
		}
	}()
	wsTask, loaded := cl.finishOrderTasks.LoadOrNew(order.ID.String(), func() *exchange.TaskCompletionSourceT[*exchange.CfOrder] {
		return &exchange.TaskCompletionSourceT[*exchange.CfOrder]{
			CreateTime:            time.Now(),
			TaskCompletionSourceT: *common.NewTaskCompletionSourceT[*exchange.CfOrder](),
		}
	})
	if loaded {
		if wsTask.IsDone() { // 直接丢出结果,理论上没有err
			result, err = wsTask.GetResult()
			time.AfterFunc(time.Second*5, func() {
				exchange.DeleteT(cl.finishOrderTasks, order.ID.String(), "swap finishOrderTasks loaded")
			})
			return
		}
	}

	var or *apis.Order
	restTask := common.NewTaskCompletionSourceT[*exchange.CfOrder]()

	go func() {
		or, err = cl.actsApi.GetOrder(cl.actsSymbol.ProductType, order.ID.String(), cl.symbol.String())
		if err != nil {
			restTask.SetError(err)
			return
		}

		ret := exchange.CfOrder{
			Order: ccexgo.Order{
				ID:          ccexgo.NewStrID(or.OrderID),
				ClientID:    ccexgo.NewStrID(or.ClientOrderID),
				Symbol:      cl.symbol,
				Amount:      decimal.NewFromFloat(or.Amount),
				Filled:      decimal.NewFromFloat(or.AmountFilled),
				Price:       decimal.NewFromFloat(or.Price),
				AvgPrice:    decimal.NewFromFloat(or.AveragePrice),
				Fee:         decimal.NewFromFloat(or.TotalFee),
				FeeCurrency: cl.feeCurrency,
				Created:     or.Createtime,
				Updated:     or.Timestamp,
				Status:      acts.ActsStatus2ccex(or.Status),
				Type:        acts.ActsType2ccex(or.Type),
				Side:        acts.ActsSide2ccex(or.Side),
			},
		}
		if cl.needTransform() {
			ret.Amount = decimal.NewFromFloat(or.Amount * cl.actsSymbol.ContractValue)
			ret.Filled = decimal.NewFromFloat(or.AmountFilled * cl.actsSymbol.ContractValue)
		}
		restTask.SetResult(&ret)
	}()

	select {
	case <-wsTask.Done():
		result, err = wsTask.GetResult()
		time.AfterFunc(time.Second*5, func() {
			exchange.DeleteT(cl.finishOrderTasks, order.ID.String(), "swap finishOrderTasks")
		})
		return result, err
	case <-restTask.Done():
		result, err = restTask.GetResult()
		if err != nil {
			return nil, err
		}
	}

	if (strings.Contains(cl.GetExchangeName(), exchange.Binance) || cl.GetExchangeName() == exchange.Okex5) && !result.Filled.IsZero() { // 币安计算好手续费返回
		feeRate := cl.feeRate.CloseMaker
		if result.Type == ccexgo.OrderTypeMarket {
			feeRate = cl.feeRate.CloseTaker
		}
		if strings.Contains(cl.GetExchangeName(), exchange.Binance) {
			if cl.inverse {
				result.Fee = result.Filled.Mul(cl.symbol.(ccexgo.SwapSymbol).ContractVal()).Div(result.AvgPrice).Mul(feeRate)
			} else {
				result.Fee = result.Filled.Mul(cl.symbol.(ccexgo.SwapSymbol).ContractVal()).Mul(result.AvgPrice).Mul(feeRate)
			}
		}
		if or.Type == apis.OrderTypeLimit && (result.Status == ccexgo.OrderStatusDone || (result.Status == ccexgo.OrderStatusCancel && !result.Filled.IsZero())) { // 限价，有成交再调成交明细
			order2, err := cl.actsApi.GetMyTrades(apis.GetOrderArgs{SymbolType: cl.actsSymbol.ProductType, Symbol: cl.symbol.String(), Id: or.OrderID})
			if err != nil {
				ctxlog.Warn(ctx, "message", "swap getMyTrades err", "orderID", order.ID.String(), "err", err)
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
		} else if or.Type == apis.OrderTypeMakerOnly {
			result.IsMaker = true
		}
	}
	// r, err := json.Marshal(or)
	// if err == nil {
	// 	result.Raw = string(r)
	// }
	return result, nil
}

// MarketOrder quoteAmount在此处无用
func (cl *Client) MarketOrder(ctx context.Context, side ccexgo.OrderSide, amount, quoteAmount decimal.Decimal, extraParams map[string]string) (*exchange.CfOrder, error) {
	var (
		dir apis.OrderSide
	)

	if side == ccexgo.OrderSideBuy {
		dir = apis.OrderSideBuy
	} else if side == ccexgo.OrderSideCloseShort {
		dir = apis.OrderSideCloseShort
	} else if side == ccexgo.OrderSideSell {
		dir = apis.OrderSideSell
	} else {
		dir = apis.OrderSideCloseLong
	}

	exp := 0
	if cl.inverse {
		if cl.GetExchangeName() == exchange.Okex5 {
			exp = exchange.CalcExponent(decimal.NewFromFloat(cl.actsSymbol.AmountTickSize))
		}
	} else {
		exp = exchange.CalcExponent(decimal.NewFromFloat(cl.actsSymbol.AmountTickSize))
		if cl.GetExchangeName() == exchange.Okex5 || cl.GetExchangeName() == exchange.GateIO { // 个数精度
			exp = exchange.CalcExponent(decimal.NewFromFloat(cl.actsSymbol.AmountTickSize * cl.actsSymbol.ContractValue))
		}
	}

	amount = amount.RoundFloor(int32(exp))
	amt, _ := amount.Float64()

	order := &apis.Order{
		Symbol:      cl.symbol.String(),
		SymbolType:  cl.actsSymbol.ProductType,
		Type:        apis.OrderTypeMarket,
		Side:        dir,
		Amount:      amt,
		Leverage:    `2`,
		ExtraParams: extraParams,
	}
	if cl.needTransform() { // 不是binance，bybit要转成张数下单
		order.Amount = cl.actsSymbol.RoundAmount(amt / cl.actsSymbol.ContractValue)
	}

	if acts.GetManual() {
		UUID := uuid.NewUniqueString()
		order.ClientOrderID = UUID[:28] + acts.ManualTakerOpSuffix
		if cl.GetExchangeName() == exchange.GateIO {
			order.ClientOrderID = UUID[:20] + acts.ManualTakerOpSuffix
		}
		if cl.GetExchangeName() == exchange.HyperLiquid {
			order.ClientOrderID = `0x` + order.ClientOrderID
		}
	}

	if cl.GetExchangeName() == exchange.HyperLiquid {
		var price float64
		if side == ccexgo.OrderSideBuy || side == ccexgo.OrderSideCloseShort {
			price = cl.ask1Price
			if price == 0 {
				ticker, err := cl.actsApi.GetTicker(apis.ProductTypeSwap, cl.symbol.String())
				if err != nil {
					return nil, err
				}
				price = ticker.Last
			}
			price = price * 1.003
		} else {
			price = cl.bid1Price
			if price == 0 {
				ticker, err := cl.actsApi.GetTicker(apis.ProductTypeSwap, cl.symbol.String())
				if err != nil {
					return nil, err
				}
				price = ticker.Last
			}
			price = price * 0.997
		}
		order.Price = cl.actsSymbol.RoundPrice(price)
	}

	if err := cl.actsApi.AddOrders(order); err != nil {
		return nil, err
	}

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

		// 退避机制：等待时间指数增长（每次翻倍），最大为1s
		waitDuration := 30 * time.Millisecond
		maxInterval := 1 * time.Second

		timer := time.NewTimer(waitDuration)
		defer timer.Stop()

		for {
			select {
			case <-timer.C:
				if o, err := cl.FetchOrder(ctx, or); err != nil {
					ctxlog.Warn(ctx, "message", "fetch order fail", "error", err.Error())
				} else {
					if o.Status == ccexgo.OrderStatusDone || o.Status == ccexgo.OrderStatusCancel {
						restCH <- o
						return
					}
				}

				// 指数增长：30ms -> 60ms -> 120ms -> 240ms -> 480ms -> 960ms -> 1s
				waitDuration *= 2
				if waitDuration > maxInterval {
					waitDuration = maxInterval
				}
				timer.Reset(waitDuration)

			case <-done:
				select {
				case <-restCH:
				default:
				}
				return
			}
		}
	}()

	orderCh := exchange.LoadOrNewOrderChan(cl.takerOrderCh, order.OrderID)
	defer func() { // 如果退出了市价函数, 说明拿到结果了, channel就没有必要存在了
		exchange.DeleteT(cl.takerOrderCh, order.OrderID, "swap takerOrderCh")
		close(orderCh.Orders)
	}()
	timeOut := time.After(time.Second * 30)
	ret := exchange.CfOrder{}
	var wsFee decimal.Decimal
	for {
		select {
		case o := <-orderCh.Orders:
			wsFee = wsFee.Add(o.Fee.Neg()) // 取反为了保持公式统一。。
			if cl.actsApi.GetExchangeName() == exchange.Bybit5 {
				wsFee = o.Fee.Neg()
			}
			ctxlog.Info(ctx, "message", "swap get ws order notify", "amount", o.Amount, "filled", o.Filled, "fee", o.Fee.Neg(), "fee_currency", o.FeeCurrency, "ws_fee", wsFee, "avg_price", o.AvgPrice, "status", o.Status, "order", o.ID.String())

			if o.Status == ccexgo.OrderStatusDone || o.Status == ccexgo.OrderStatusCancel {
				ret = *o
				ret.Fee = wsFee
				return &ret, nil
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

		case o := <-restCH:
			ctxlog.Info(ctx, "message", "swap get order from rest api", "amount", o.Amount, "filled", o.Filled, "fee", o.Fee.Neg(), "avg_price", o.AvgPrice, "status", o.Status, "order", o.ID.String())
			if o.Status == ccexgo.OrderStatusDone || o.Status == ccexgo.OrderStatusCancel {
				ret = *o
				ret.Fee = o.Fee.Neg()
				return &ret, nil
			}
		}
	}
}

func (cl *Client) LimitOrder(ctx context.Context, side ccexgo.OrderSide, amount decimal.Decimal, price float64) (*exchange.CfOrder, error) {
	var (
		dir apis.OrderSide
	)

	if side == ccexgo.OrderSideBuy {
		dir = apis.OrderSideBuy
	} else if side == ccexgo.OrderSideCloseShort {
		dir = apis.OrderSideCloseShort
	} else if side == ccexgo.OrderSideSell {
		dir = apis.OrderSideSell
	} else {
		dir = apis.OrderSideCloseLong
	}
	amt, _ := amount.Float64()

	order := &apis.Order{
		ClientOrderID: uuid.NewUniqueString(),
		Symbol:        cl.symbol.String(),
		SymbolType:    cl.actsSymbol.ProductType,
		Type:          apis.OrderTypeMakerOnly,
		Side:          dir,
		Amount:        amt,
		Price:         price,
		Leverage:      `2`,
	}
	if cl.GetExchangeName() == exchange.GateIO {
		order.ClientOrderID = order.ClientOrderID[:24]
	}
	if cl.GetExchangeName() == exchange.HyperLiquid {
		order.ClientOrderID = `0x` + order.ClientOrderID
	}
	if cl.needTransform() { // 不是binance,bybit要转成张数下单
		order.Amount = cl.actsSymbol.RoundAmount(amt / cl.actsSymbol.ContractValue)
	}
	task := common.NewTaskCompletionSourceT[*exchange.CfOrder]()
	cl.limitOrderTasks.Store(order.ClientOrderID, task)

	start := time.Now()
	go func() {
		if err := cl.actsApi.AddOrders(order); err != nil {
			ctxlog.Warn(ctx, "message", "add order failed", "id", order.OrderID, "error", err.Error())
			task.SetError(err)
		} else {
			amount := decimal.NewFromFloat(order.Amount)
			if cl.needTransform() {
				amount = amount.Mul(decimal.NewFromFloat(cl.actsSymbol.ContractValue))
			}
			task.SetResult(&exchange.CfOrder{Order: ccexgo.Order{
				ID:      ccexgo.NewStrID(order.OrderID),
				Amount:  amount,
				Created: order.Timestamp,
				Updated: order.Timestamp,
				Status:  acts.ActsStatus2ccex(order.Status),
			}})
		}
	}()

	defer exchange.DeleteT(cl.limitOrderTasks, order.ClientOrderID, "swap limitOrderTasks")
	if err := task.WaitTimeout(time.Second * 3); err != nil {
		return nil, err
	}
	o, err := task.GetResult()
	if err != nil {
		return nil, err
	}

	// 这里有概率是从ws拿到的
	order2 := &exchange.CfOrder{
		Order: ccexgo.Order{
			ID:          o.ID,
			ClientID:    ccexgo.NewStrID(order.ClientOrderID),
			Price:       decimal.NewFromFloat(order.Price),
			AvgPrice:    o.AvgPrice,
			Amount:      o.Amount,
			Filled:      o.Filled,
			Fee:         o.Fee,
			FeeCurrency: o.FeeCurrency,
			Symbol:      cl.symbol,
			Created:     o.Created,
			Updated:     o.Updated,
			Side:        side,
			Type:        acts.ActsType2ccex(order.Type),
			Status:      o.Status,
		},
	}
	// if cl.needTransform() {
	// 	order2.Amount = order2.Amount.Mul(decimal.NewFromFloat(cl.actsSymbol.ContractValue))
	// }
	// 如果推送慢到，先把task压入
	_, loaded := cl.finishOrderTasks.LoadOrNew(o.ID.String(), func() *exchange.TaskCompletionSourceT[*exchange.CfOrder] {
		return &exchange.TaskCompletionSourceT[*exchange.CfOrder]{
			CreateTime:            time.Now(),
			TaskCompletionSourceT: *common.NewTaskCompletionSourceT[*exchange.CfOrder](),
		}
	})
	ctxlog.Info(ctx, "message", "swap limit order", "time_delay", time.Since(start), "start", start, "loaded", loaded, "id", order2.ID)

	return order2, nil
}

// LimitOrders 同时挂多个单并返回订单
func (cl *Client) LimitOrders(ctx context.Context, orderType apis.OrderType, side ccexgo.OrderSide, amount []decimal.Decimal, price []float64, extraParams map[string]string) ([]*exchange.CfOrder, error) {
	var (
		dir apis.OrderSide
	)

	if side == ccexgo.OrderSideBuy {
		dir = apis.OrderSideBuy
	} else if side == ccexgo.OrderSideCloseShort {
		dir = apis.OrderSideCloseShort
	} else if side == ccexgo.OrderSideSell {
		dir = apis.OrderSideSell
	} else {
		dir = apis.OrderSideCloseLong
	}

	var orders []*apis.Order
	for i := range amount {
		amt, _ := amount[i].Float64()
		order := &apis.Order{
			ClientOrderID: uuid.NewUniqueString(),
			Symbol:        cl.symbol.String(),
			SymbolType:    cl.actsSymbol.ProductType,
			Type:          orderType,
			Side:          dir,
			Amount:        amt,
			Price:         price[i],
			Leverage:      `2`,
		}
		if cl.GetExchangeName() == exchange.GateIO {
			order.ClientOrderID = order.ClientOrderID[:24]
		}
		if cl.GetExchangeName() == exchange.HyperLiquid {
			order.ClientOrderID = `0x` + order.ClientOrderID
		}
		if cl.needTransform() { // 不是binance,bybit要转成张数下单
			order.Amount = cl.actsSymbol.RoundAmount(amt / cl.actsSymbol.ContractValue)
		}
		orders = append(orders, order)
	}
	result := utilCommon.WorkData{}
	tasks := []*common.TaskCompletionSourceT[*exchange.CfOrder]{}
	start := time.Now()
	for i := range orders {
		task := common.NewTaskCompletionSourceT[*exchange.CfOrder]()
		tasks = append(tasks, task)
		cl.limitOrderTasks.Store(orders[i].ClientOrderID, task)

		go func(order *apis.Order) {
			if err := cl.actsApi.AddOrders(order); err != nil {
				ctxlog.Warn(ctx, "message", "add order failed", "id", order.OrderID, "error", err.Error())
				task.SetError(err)
			} else {
				amount := decimal.NewFromFloat(order.Amount)
				if cl.needTransform() {
					amount = amount.Mul(decimal.NewFromFloat(cl.actsSymbol.ContractValue))
				}
				task.SetResult(&exchange.CfOrder{Order: ccexgo.Order{
					ID:      ccexgo.NewStrID(order.OrderID),
					Amount:  amount,
					Created: order.Timestamp,
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
		defer exchange.DeleteT(cl.limitOrderTasks, orders[i].ClientOrderID, "swap limitOrderTasks")

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
				ID:          o.ID,
				ClientID:    ccexgo.NewStrID(orders[i].ClientOrderID),
				Price:       decimal.NewFromFloat(orders[i].Price),
				AvgPrice:    o.AvgPrice,
				Amount:      o.Amount,
				Filled:      o.Filled,
				Fee:         o.Fee,
				FeeCurrency: o.FeeCurrency,
				Symbol:      cl.symbol,
				Created:     o.Created,
				Updated:     o.Updated,
				Side:        side,
				Type:        acts.ActsType2ccex(orders[i].Type),
				Status:      o.Status,
			},
		}
		// if cl.needTransform() {
		// 	order.Amount = order.Amount.Mul(decimal.NewFromFloat(cl.actsSymbol.ContractValue))
		// }
		// 如果推送慢到，先把task压入
		_, loaded = cl.finishOrderTasks.LoadOrNew(o.ID.String(), func() *exchange.TaskCompletionSourceT[*exchange.CfOrder] {
			return &exchange.TaskCompletionSourceT[*exchange.CfOrder]{
				CreateTime:            time.Now(),
				TaskCompletionSourceT: *common.NewTaskCompletionSourceT[*exchange.CfOrder](),
			}
		})
		resultOrder = append(resultOrder, order)
	}
	ctxlog.Info(ctx, "message", "swap limit orders", "time_delay", time.Since(start), "start", start, "loaded", loaded)

	return resultOrder, result.Err
}

func (cl *Client) CancelOrder(ctx context.Context, check *exchange.CfOrder) (*exchange.CfOrder, error) {
	ctxlog.Info(ctx, "message", "swap cancel order", "id", check.ID.String(), "exchange", cl.GetExchangeName())
	order := &apis.Order{
		Symbol:     cl.symbol.String(),
		SymbolType: cl.actsSymbol.ProductType,
		OrderID:    check.ID.String(),
	}

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
				exchange.DeleteT(cl.finishOrderTasks, check.ID.String(), "swap CancelOrder finishOrderTasks loaded")
			})
			return result, nil
		}
	}

	restTask := common.NewTaskCompletionSourceT[*exchange.CfOrder]()
	go func() {
		if err := cl.actsApi.CancelOrders(order); err != nil { // 查看是什么类型报错，直接去查询订单
			ctxlog.Warn(ctx, "message", "swap cancel order failed", "id", order.OrderID, "error", err.Error())

			errType := cl.HandError(err)
			if errType.Code == exchange.DisConnected || errType.Code == exchange.ContextDeadlineExceeded || errType.Code == exchange.OtherReason { // 网络问题，再撤销一次
				if err := cl.actsApi.CancelOrders(order); err != nil {
					ctxlog.Warn(ctx, "message", "swap second cancel order failed", "id", order.OrderID, "error", err.Error())
					errType = cl.HandError(err)
					if errType.Code == exchange.DisConnected || errType.Code == exchange.ContextDeadlineExceeded || errType.Code == exchange.OtherReason { // 网络问题
						restTask.SetError(err)
						return
					} else if errType.Code != exchange.NoSuchOrder { // 找不到订单可能是成交了
						restTask.SetError(err)
						return
					}
				}
			} else if errType.Code != exchange.NoSuchOrder { // 找不到订单可能是成交了
				restTask.SetError(err)
				return
			}
		}
		if cl.needTransform() {
			order.Amount = order.Amount * cl.actsSymbol.ContractValue
			order.AmountFilled = order.AmountFilled * cl.actsSymbol.ContractValue
		}
		restTask.SetResult(&exchange.CfOrder{Order: ccexgo.Order{ID: check.ID, Status: acts.ActsStatus2ccex(order.Status), Filled: decimal.NewFromFloat(order.AmountFilled), Updated: order.Timestamp}})
	}()

	var (
		result *exchange.CfOrder
		err    error
	)
	select {
	case <-wsTask.Done():
		result, _ = wsTask.GetResult()
		return result, nil
	case <-restTask.Done():
		result, err = restTask.GetResult()
		if err != nil {
			return nil, err
		}
	}

	// 这里检测下订单状态,币安支持撤单响应
	if result.Status == ccexgo.OrderStatusCancel {
		ctxlog.Info(ctx, "message", "swap cancel order success", "id", order.OrderID, "response", fmt.Sprintf("%+v", order))
		if result.Filled.IsZero() { // 没成交直接返回撤单成功
			return result, nil
		}
	}

	or, err := cl.FetchOrder(ctx, check)

	if err != nil { // 查询订单报错,再查一次
		ctxlog.Warn(ctx, "message", "swap fetch order failed", "id", order.OrderID, "error", err.Error())

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
							Symbol:   cl.symbol,
							ID:       ccexgo.NewStrID(order.OrderID),
							ClientID: ccexgo.NewStrID(order.ClientOrderID),
						},
					}

					// 退避机制：等待时间指数增长（每次翻倍），最大为1s
					waitDuration := 50 * time.Millisecond
					maxInterval := 1 * time.Second

					timer := time.NewTimer(waitDuration)
					defer timer.Stop()

					for {
						select {
						case <-timer.C:
							if o, err := cl.FetchOrder(ctx, or); err != nil {
								ctxlog.Warn(ctx, "message", "fetch order fail", "error", err.Error())
							} else {
								if o.Status == ccexgo.OrderStatusDone || o.Status == ccexgo.OrderStatusCancel {
									restCH <- o
									return
								}
							}

							waitDuration *= 2
							if waitDuration > maxInterval {
								waitDuration = maxInterval
							}
							timer.Reset(waitDuration)

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
				timeOut := time.After(30 * time.Second)
				var wsFee decimal.Decimal
				for {
					//?进行超时判断，永续可能需要很久才能获得成交结果
					select {
					case o := <-orderCh.Orders: // ?好像会造成外部结果不可靠
						/*if o.ID.String() != order.OrderID {
							// !不是匹配的id再塞回去,因为可能并发
							// ctxlog.Warn(ctx, "message", "swap cancel skip unmatched order_id", "want", order.OrderID, "got", o.ID.String(), "amount", o.Amount, "fee", o.Fee.Neg(), "filled", o.Filled)
							// cl.makerOrderCH <- o
							continue
						}*/

						ctxlog.Info(ctx, "message", "cancel swap get ws order notify", "amount", o.Amount, "filled", o.Filled, "fee", o.Fee.Neg(), "avg_price", o.AvgPrice, "status", o.Status, "order", o.ID.String())
						wsFee = wsFee.Add(o.Fee) // !合约外部取反
						if o.Status == ccexgo.OrderStatusDone || o.Status == ccexgo.OrderStatusCancel {
							ret = *o
							ret.Fee = wsFee
							return &ret, nil
						}

					case <-timeOut:
						return nil, fmt.Errorf("获取合约订单结果超时,主动查询失败,%+v, 请人工处理", order)

					case o := <-restCH:
						ctxlog.Info(ctx, "message", "cancel swap get order from rest api", "amount", o.Amount, "filled", o.Filled, "fee", o.Fee.Neg(), "avg_price", o.AvgPrice, "status", o.Status, "order", o.ID.String())
						if o.Status == ccexgo.OrderStatusDone || o.Status == ccexgo.OrderStatusCancel {
							ret = *o
							ret.Fee = o.Fee
							return &ret, nil
						}
					}
				}
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}

		or, err = cl.FetchOrder(ctx, check)
		if err != nil {
			ctxlog.Warn(ctx, "message", "swap second fetch order failed", "id", order.OrderID, "error", err.Error())
			return nil, err
		}
	}
	start := time.Now()
	for or.Status == ccexgo.OrderStatusOpen { // 火币撤单中状态延后再查询
		if time.Since(start) > 100*time.Second {
			return nil, fmt.Errorf("获取合约订单结果超时100+s,主动查询失败,%+v, 请人工处理", order)
		}
		time.Sleep(50 * time.Millisecond)
		or, err = cl.FetchOrder(ctx, check)

		if err != nil { // 查询订单报错,再查一次
			ctxlog.Warn(ctx, "message", "swap three fetch order failed", "id", order.OrderID, "error", err.Error())
			time.Sleep(50 * time.Millisecond)
			or, err = cl.FetchOrder(ctx, check)
			if err != nil {
				ctxlog.Warn(ctx, "message", "swap four fetch order failed", "id", order.OrderID, "error", err.Error())
				return nil, err
			}
		}
	}
	return or, nil
}

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
			SymbolType: cl.actsSymbol.ProductType,
			// OrderID:    check.ID.String(),
		}
		if check.ID.String() != "" {
			order.OrderID = check.ID.String()
		} else {
			order.ClientOrderID = check.ClientID.String()
		}

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
						exchange.DeleteT(cl.finishOrderTasks, check.ID.String(), "swap CancelOrders finishOrderTasks loaded")
					})
					wd.AddData(nil, result)
					return
				}
			}

			restTask := common.NewTaskCompletionSourceT[*exchange.CfOrder]()
			go func() {
				if err := cl.actsApi.CancelOrders(order); err != nil { // 查看是什么类型报错，直接去查询订单
					ctxlog.Warn(ctx, "message", "swap cancel order failed", "id", order.OrderID, "error", err.Error())

					errType := cl.HandError(err)
					if errType.Code == exchange.DisConnected || errType.Code == exchange.ContextDeadlineExceeded || errType.Code == exchange.OtherReason { // 网络问题，再撤销一次
						if err := cl.actsApi.CancelOrders(order); err != nil {
							ctxlog.Warn(ctx, "message", "swap second cancel order failed", "id", order.OrderID, "error", err.Error())
							errType = cl.HandError(err)
							if errType.Code == exchange.DisConnected || errType.Code == exchange.ContextDeadlineExceeded || errType.Code == exchange.OtherReason { // 网络问题
								restTask.SetError(err)
								return
							} else if errType.Code != exchange.NoSuchOrder { // 找不到订单可能是成交了
								restTask.SetError(err)
								return
							}
						}
					} else if errType.Code != exchange.NoSuchOrder { // 找不到订单可能是成交了
						restTask.SetError(err)
						return
					}
				}
				if cl.needTransform() {
					order.Amount = order.Amount * cl.actsSymbol.ContractValue
					order.AmountFilled = order.AmountFilled * cl.actsSymbol.ContractValue
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
				ctxlog.Info(ctx, "message", "swap cancel order success", "id", order.OrderID, "response", fmt.Sprintf("%+v", order))
				if result.Filled.IsZero() { // 没成交直接返回撤单成功
					wd.AddData(nil, result)
					return
				}
			}

			or, err := cl.FetchOrder(ctx, check)

			if err != nil { // 查询订单报错,再查一次
				ctxlog.Warn(ctx, "message", "swap fetch order failed", "id", order.OrderID, "error", err.Error())

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
									Symbol:   cl.symbol,
									ID:       ccexgo.NewStrID(order.OrderID),
									ClientID: ccexgo.NewStrID(order.ClientOrderID),
								},
							}

							// 退避机制：等待时间指数增长（每次翻倍），最大为1s
							waitDuration := 50 * time.Millisecond
							maxInterval := 1 * time.Second

							timer := time.NewTimer(waitDuration)
							defer timer.Stop()

							for {
								select {
								case <-timer.C:
									if o, err := cl.FetchOrder(ctx, or); err != nil {
										ctxlog.Warn(ctx, "message", "fetch order fail", "error", err.Error())
									} else {
										if o.Status == ccexgo.OrderStatusDone || o.Status == ccexgo.OrderStatusCancel {
											restCH <- o
											return
										}
									}

									waitDuration *= 2
									if waitDuration > maxInterval {
										waitDuration = maxInterval
									}
									timer.Reset(waitDuration)

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
						timeOut := time.After(30 * time.Second)
						var wsFee decimal.Decimal
						for {
							//?进行超时判断，永续可能需要很久才能获得成交结果
							select {
							case o := <-orderCh.Orders: // ?好像会造成外部结果不可靠
								/*if o.ID.String() != order.OrderID {
									// !不是匹配的id再塞回去,因为可能并发
									// ctxlog.Warn(ctx, "message", "swap cancel skip unmatched order_id", "want", order.OrderID, "got", o.ID.String(), "amount", o.Amount, "fee", o.Fee.Neg(), "filled", o.Filled)
									// cl.makerOrderCH <- o
									continue
								}*/

								ctxlog.Info(ctx, "message", "cancel swap get ws order notify", "amount", o.Amount, "filled", o.Filled, "fee", o.Fee.Neg(), "avg_price", o.AvgPrice, "status", o.Status, "order", o.ID.String())
								wsFee = wsFee.Add(o.Fee) // !合约外部取反
								if o.Status == ccexgo.OrderStatusDone || o.Status == ccexgo.OrderStatusCancel {
									ret = *o
									ret.Fee = wsFee
									wd.AddData(nil, &ret)
									return
								}

							case <-timeOut:
								wd.AddData(fmt.Errorf("获取合约订单结果超时,主动查询失败,%+v", order), nil)
								return

							case o := <-restCH:
								ctxlog.Info(ctx, "message", "cancel swap get order from rest api", "amount", o.Amount, "filled", o.Filled, "fee", o.Fee.Neg(), "avg_price", o.AvgPrice, "status", o.Status, "order", o.ID.String())
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
					ctxlog.Warn(ctx, "message", "swap second fetch order failed", "id", order.OrderID, "error", err.Error())
					wd.AddData(err, nil)
					return
				}
			}
			for or.Status == ccexgo.OrderStatusOpen { // 火币撤单中状态延后再查询
				time.Sleep(50 * time.Millisecond)
				or, err = cl.FetchOrder(ctx, check)

				if err != nil { // 查询订单报错,再查一次
					ctxlog.Warn(ctx, "message", "swap three fetch order failed", "id", order.OrderID, "error", err.Error())
					time.Sleep(50 * time.Millisecond)
					or, err = cl.FetchOrder(ctx, check)
					if err != nil {
						ctxlog.Warn(ctx, "message", "swap four fetch order failed", "id", order.OrderID, "error", err.Error())
						wd.AddData(err, nil)
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

// TODO分交易所, 改成可配置的？
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
	ctxlog.Info(cl.ctx, "message", "swap HandError", "errStr", errStr)
	switch {
	case strings.Contains(errStr, "unable to process your"):
		return exchange.NewErrorType(exchange.DisConnected, errStr)
	case strings.Contains(errStr, "Unknown order sent"): // apis api不会抛出这个错误
		return exchange.NewErrorType(exchange.CancelRejected, errStr)
	case strings.Contains(errStr, "Order does not exist"), strings.Contains(errStr, "order not exists"):
		return exchange.NewErrorType(exchange.NoSuchOrder, errStr)
	case strings.Contains(errStr, "Order would immediately match"), strings.Contains(errStr, "Due to the order could not be"), strings.Contains(errStr, "have immediately matched"),
		strings.Contains(errStr, "ORDER_POC_IMMEDIATE"):
		return exchange.NewErrorType(exchange.NewOrderRejected, errStr)
	case strings.Contains(errStr, "insufficient"), strings.Contains(errStr, "Insufficient"), strings.Contains(errStr, "available margin"):
		if strings.Contains(errStr, "insufficient loanable assets") {
			return exchange.NewErrorType(exchange.MarketInsufficientLoanableAssets, errStr)
		}
		return exchange.NewErrorType(exchange.AccountInsufficientBalance, errStr)
	case strings.Contains(errStr, `MIN_NOTIONAL`), strings.Contains(errStr, `notional must be no smaller than`), strings.Contains(errStr, `meet minimum order`):
		return exchange.NewErrorType(exchange.MinNotional, errStr)
	case strings.Contains(errStr, `current limit is`), strings.Contains(errStr, `current limit of IP`), strings.Contains(errStr, `Rate Limit`):
		return exchange.NewErrorType(exchange.WeightLimitPerMinute, errStr)
	case strings.Contains(errStr, `2400 requests per minute`):
		return exchange.NewErrorType(exchange.IpLimitPerMinute, errStr)
	case strings.Contains(errStr, `banned until`):
		return exchange.NewErrorType(exchange.IpBannedUntil, errStr)
	case strings.Contains(errStr, `Exceeded the maximum`), strings.Contains(errStr, `maximum position amount`):
		return exchange.NewErrorType(exchange.ExceededTheMaximum, errStr)
	case strings.Contains(errStr, `open positions and orders can't exceed`), strings.Contains(errStr, `position value`):
		return exchange.NewErrorType(exchange.ExceededMaxPosition, errStr)
	case strings.Contains(errStr, `ReduceOnly Order is rejected`), strings.Contains(errStr, `reduce or close`):
		return exchange.NewErrorType(exchange.CloseOrderRejected, errStr)
	case strings.Contains(errStr, `403 Forbidden`):
		return exchange.NewErrorType(exchange.Forbidden, errStr)
	case strings.Contains(errStr, `context deadline exceeded`):
		return exchange.NewErrorType(exchange.ContextDeadlineExceeded, errStr)
	case strings.Contains(errStr, `not meet the PERCENT_PRICE`), strings.Contains(errStr, `not within the price limit`):
		return exchange.NewErrorType(exchange.ExchangePriceLimit, errStr)
	case strings.Contains(errStr, `try again`):
		return exchange.NewErrorType(exchange.ExchangeSystemBusy, errStr)
	default:
		return exchange.NewErrorType(exchange.OtherReason, errStr)

	}
}

func (cl *Client) Error() error {
	err := cl.err.Load()
	if err != nil {
		return err.(error)
	}
	return nil
}

func (cl *Client) Done() <-chan struct{} {
	return cl.done
}

func (cl *Client) Depth() <-chan *exchange.Depth {
	return cl.orderbookCH
}

func (cl *Client) Index() <-chan *apis.IndexEvent {
	return cl.indexCH
}

func (cl *Client) BalanceNotify() *common.Event {
	return cl.balanceChangeNotify
}

func (cl *Client) ADLOrder() <-chan *apis.Order {
	return cl.adlOrderCh
}

func (cl *Client) PosNotify() *common.Event {
	return cl.posChangeNotify
}

func (cl *Client) BookTickerNotify() *common.Event {
	return cl.bookTickerNotify
}

func (cl *Client) BookTicker() *exchange.BookTicker {
	return cl.bookTicker
}

func (cl *Client) Balance() *exchange.MySyncMap[string, *apis.Balance] {
	if cl.GetExchangeName() == exchange.HyperLiquid {
		cl.FetchBalance(cl.ctx)
	}
	return cl.balances
}

func (cl *Client) MakerOrder(orderID string) <-chan *exchange.CfOrder {
	orderCh := exchange.LoadOrNewOrderChan(cl.makerOrderCh, orderID)
	return orderCh.Orders
}

func (cl *Client) GetWsOrder(orderId string) (*exchange.CfOrder, error) {
	order, ok := cl.wsOrders.Load(orderId)
	if !ok {
		return nil, errors.New("order not found")
	}
	// 浅拷贝,避免外部拿到中间状态的订单
	orderCopy := *order // 复制订单对象
	return &orderCopy, nil
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
	level.Info(log).Log("message", "swap closed", "exchange", cl.GetExchangeName())
	select {
	case <-cl.done:
		return nil
	case <-time.After(time.Second * 2):
		close(cl.done)
		return fmt.Errorf("close timeout after 2secs")
	}
}

func (cl *Client) ReconnectWs() error {
	err := cl.actsApi.ReconnectWS()
	if err != nil {
		return errors.WithMessage(err, "reconnect ws fail")
	}

	deadline := time.After(time.Second * 10)
	for {
		if cl.actsApi.CheckWSAlive(cl.actsSymbol.ProductType) { // todo ActsApi没有检查订阅状态
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

func (cl *Client) FeeRate() exchange.FeeRate {
	return *cl.feeRate
}

func (cl *Client) LevelInfo() exchange.LevelInfo {
	switch cl.actsApi.GetExchangeName() {
	case exchange.BinancePortfolio, exchange.Okex5, exchange.BinanceMargin, exchange.Binance:
		if cl.levelInfo != nil {
			return *cl.levelInfo
		}
	}
	return exchange.LevelInfo{
		Level: 1,
	}
}

// FundingRate 获取资金费率信息
func (cl *Client) FundingRate(ctx context.Context) (*apis.FundingRate, error) {
	if cl.fundingRate == nil || cl.GetExchangeName() == exchange.HyperLiquid { // ws 没推前用rest
		ret, err := cl.actsApi.SwapFundingRate(cl.symbol.String())
		if err != nil {
			return nil, err
		}
		return ret[0], nil
	}
	return cl.fundingRate, nil
}

// Funding 获取资金费信息
func (cl *Client) Funding(ctx context.Context) ([]ccexgo.Finance, error) {
	if cl.GetExchangeName() == exchange.GateIO || cl.GetExchangeName() == exchange.HyperLiquid {
		return cl.funding2(ctx)
	}
	requestParams := apis.SwapFinanceLogRequest{
		Symbol: cl.symbol.String(),
		Limit:  100,
		Type:   apis.SwapFinanceLogFunding,
	}
	if cl.actsApi.GetExchangeName() == exchange.Bybit5 {
		requestParams.Limit = 50
	}
	finance, err := cl.actsApi.SwapFinanceLog(requestParams)

	if err != nil {
		return nil, errors.WithMessage(err, "fetch funding fail")
	}

	ccexgoFs := make([]ccexgo.Finance, 0)

	for _, fr := range finance {
		ccexgoFs = append(ccexgoFs, ccexgo.Finance{
			ID:       fmt.Sprintf("%d", fr.ID),
			Symbol:   cl.symbol,
			Currency: fr.Currency,
			Amount:   fr.Amount,
			Type:     ccexgo.FinanceTypeFunding,
			Time:     fr.Time,
			Raw:      fr,
		})
	}

	return ccexgoFs, nil
}

func (cl *Client) funding2(ctx context.Context) ([]ccexgo.Finance, error) {
	args := apis.FetchBillsArgs{
		Symbol: cl.symbol.String(),
		Type:   apis.ProductTypeSwap,
		Status: apis.BillStatusFunding,
	}
	bills, err := cl.actsApi.GetBills(args)
	if err != nil {
		return nil, errors.WithMessage(err, "fetch funding fail")
	}
	ccexgoFs := make([]ccexgo.Finance, 0)
	for _, bill := range bills {
		ccexgoFs = append(ccexgoFs, ccexgo.Finance{
			ID:       bill.Id,
			Symbol:   cl.symbol,
			Currency: bill.Currency,
			Amount:   decimal.NewFromFloat(bill.Amount),
			Type:     ccexgo.FinanceTypeFunding,
			Time:     bill.Timestamp,
			Raw:      bill,
		})
	}
	return ccexgoFs, nil
}

func (cl *Client) loop(ctx context.Context) {
	defer close(cl.done)
	clearTimer := time.NewTimer(15 * time.Second)

	for {
		select {
		case <-ctx.Done(): // 外部context结束
			return
		case wsEvent := <-cl.actsApi.WSEvent():
			if wsEvent.Type == apis.WSEventResubscribe {
				if strings.Contains(wsEvent.Error.Error(), `doesn't exist`) {
					common.PanicWithTime(fmt.Sprintf("%s ws重连失败, 通道不存在: %s", cl.GetExchangeName(), wsEvent.Error.Error()))
				}
			}
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
					ctxlog.Info(cl.ctx, "message", "swap delete ws orders", "id", v.ID, "order_update", v.Updated, "since_update", time.Since(v.Updated), "ex", cl.GetExchangeName())
					return true
				}
				return false
			})
			cl.finishOrderTasks.DeleteIf(func(k string, v *exchange.TaskCompletionSourceT[*exchange.CfOrder]) bool {
				delete := false
				if v.IsDone() {
					fo, _ := v.GetResult()
					if time.Since(fo.Updated).Seconds() > 15 { // 超过15s没获取task结果
						ctxlog.Info(cl.ctx, "message", "swap delete finish orders task", "id", k, "order_update", fo.Updated, "task_create", v.CreateTime)
						delete = true
					}
				} else {
					if time.Since(v.CreateTime).Seconds() > 600 { // ! 600s无成交？无推送
						ctxlog.Info(cl.ctx, "message", "swap delete finish orders task", "id", k, "task_create", v.CreateTime)
						delete = true
					}
				}
				return delete
			})

			cl.takerOrderCh.DeleteIf(func(k string, v *exchange.CfOrderChan) bool {
				delete := false
				if time.Since(v.UpdatedAt).Seconds() > 60 { // 60s无推送
					close(v.Orders)
					ctxlog.Info(cl.ctx, "message", "swap delete taker orders", "id", k, "update", v.UpdatedAt, "len", len(v.Orders))
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
					ctxlog.Info(cl.ctx, "message", "swap delete maker orders", "id", k, "update", v.UpdatedAt, "len", len(v.Orders))
				}
				return delete
			})

			clearTimer = time.NewTimer(15 * time.Second)
		}
	}
}

func (cl *Client) OnOrders(orders *apis.OrderEvent) {
	for _, o := range orders.Orders {
		if (cl.GetExchangeName() == exchange.GateIO || cl.GetExchangeName() == exchange.Bybit5) &&
			o.Status == apis.OrderStatusUnknown {
			log.Info("%s swap trade %+v", cl.GetExchangeName(), o)
			continue
		}
		fee := decimal.NewFromFloat(o.TradeFee)              // websocket使用订单逐笔成交手续费,币安问题
		if cl.actsApi.GetExchangeName() == exchange.Bybit5 { // 目前只有总成交fee
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
				FeeCurrency: cl.feeCurrency,
				Created:     o.Createtime,
				Updated:     o.Timestamp,
				Side:        acts.ActsSide2ccex(o.Side),
				Status:      acts.ActsStatus2ccex(o.Status),
				Type:        acts.ActsType2ccex(o.Type),
			},
			IsMaker: o.IsMaker,
			WsPush:  true,
		}
		if cl.needTransform() {
			contractValue := decimal.NewFromFloat(cl.actsSymbol.ContractValue)
			pushO.Filled = decimal.NewFromFloat(o.AmountFilled).Mul(contractValue)
			pushO.Amount = decimal.NewFromFloat(o.Amount).Mul(contractValue)
		}
		if o.Type == apis.OrderTypeMakerOnly { // 强制变成maker,避免交易所推送问题
			pushO.IsMaker = true // cancel状态的话, 如果部分成交取消后, maker态会变成false
		}

		if strings.Contains(cl.GetExchangeName(), exchange.Binance) {
			pushO.FeeCurrency = o.FeeCurrency
		}

		log.Info("%s swap OnOrders %+v", cl.GetExchangeName(), pushO)

		if (strings.Contains(cl.actsApi.GetExchangeName(), exchange.Binance) && strings.Contains(o.ClientOrderID, `autoclose`)) ||
			(cl.actsApi.GetExchangeName() == exchange.Okex5 && helper.PartInArray(o.ClientOrderID, []string{`adl`, `liquidation`})) {
			closeType := "强平"
			if strings.Contains(o.ClientOrderID, `adl`) {
				closeType = "ADL"
				// 进行上报
				if cl.needTransform() { // 改一下原始数量吧
					o.TradeAmount = decimal.NewFromFloat(o.TradeAmount * cl.actsSymbol.ContractValue).InexactFloat64()
				}
				if err := util.ChanSafeSend(cl.adlOrderCh, o, func() {
					common.PanicWithTime(fmt.Sprintf("%s adl order channel full", o.OrderID))
				}); err != nil {
					go message.SendP3Important(context.Background(), message.NewCommonMsgWithAt(fmt.Sprintf("%s channel异常", cl.symbol.String()), fmt.Sprintf("adl队列 偶现异常,联系开发查看日志确定问题 %s", err)))
				}
			}
			msg := fmt.Sprintf("订单号:%s,订单类型:%s,数量:%s,方向:%s,价格:%s", pushO.ID, closeType, pushO.Filled, pushO.Side, pushO.AvgPrice)
			go message.SendImportant(context.Background(), message.NewCommonMsgWithImport(fmt.Sprintf("%s 收到ADL或强平订单", cl.symbol.String()), msg))
			continue
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
				if pushO.Status != ccexgo.OrderStatusCancel {
					new.IsMaker = pushO.IsMaker
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

		if o.Type == apis.OrderTypeMakerOnly || o.Type == apis.OrderTypeLimit {
			if pushO.Status == ccexgo.OrderStatusOpen && pushO.Filled.IsZero() {
				task, ok := cl.limitOrderTasks.Load(pushO.ClientID.String())
				if !ok {
					level.Warn(logger.GetLogger()).Log("message", "swap maker order open but limits order not exits", "order", fmt.Sprintf("%+v", o))
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
					level.Warn(log).Log("message", "swap maker order channel full", "removeOrder", fmt.Sprintf("%+v", removeO), "len", len(cl.makerOrderCH))
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
				level.Info(logger.GetLogger()).Log("message", "swap get manual order", "order", fmt.Sprintf("%+v", o))
				continue
			}
			orderCh := exchange.LoadOrNewOrderChan(cl.takerOrderCh, o.OrderID)
			if err := util.ChanSafeSend(orderCh.Orders, pushO, func() {
				common.PanicWithTime(fmt.Sprintf("%s taker order channel full", o.OrderID))
			}); err != nil {
				go message.Send(context.Background(), message.NewCommonMsg(fmt.Sprintf("%s channel异常", cl.symbol.String()), fmt.Sprintf("orderCH 偶现异常,联系开发查看日志确定问题 %s", err)))
				return
			}
			orderCh.UpdatedAt = time.Now()
		}
	}
}

func (cl *Client) OnDepths(depths *apis.DepthEvent) {
	newDs := &ccexgo.OrderBook{
		Symbol:  cl.symbol,
		Bids:    make([]ccexgo.OrderElem, 0, len(depths.Bids)),
		Asks:    make([]ccexgo.OrderElem, 0, len(depths.Asks)),
		Created: depths.Time,
	}
	cv := cl.actsSymbol.ContractValue
	for _, ask := range depths.Asks {
		cca := ccexgo.OrderElem{
			Price:  ask.Price,
			Amount: ask.Amount,
		}
		if cl.needTransform() {
			cca.Amount = ask.Amount * cv
		}
		newDs.Asks = append(newDs.Asks, cca)
	}
	for _, bid := range depths.Bids {
		ccb := ccexgo.OrderElem{
			Price:  bid.Price,
			Amount: bid.Amount,
		}
		if cl.needTransform() {
			ccb.Amount = bid.Amount * cv
		}
		newDs.Bids = append(newDs.Bids, ccb)
	}

	// b, _ := json.Marshal(depths)
	// level.Info(logger.GetLogger()).Log("message", "swap depth", "depthId", cl.depthID, "depths.Asks0", fmt.Sprintf("%+v", depths.Asks[0]), "depths.Bids0", fmt.Sprintf("%+v", depths.Bids[0]), "depthCreated", depths.Time, "depthEvent", depths.EventTime)

	depthCh := &exchange.Depth{OrderBook: newDs, ID: cl.depthID, Exchange: cl.actsApi.GetExchangeName()}

	if err := util.ChanSafeSend(cl.orderbookCH, depthCh, func() {
		for len(cl.orderbookCH) > 1*cap(cl.orderbookCH)/2 { // 写不进，丢弃最前面的1/3,前面的过时没处理就不处理了
			removeDepth := <-cl.orderbookCH
			level.Warn(logger.GetLogger()).Log("message", "swap depth channel full", "removeDepth", removeDepth.ID, "len", len(cl.orderbookCH))
		}
		cl.orderbookCH <- depthCh
	}); err != nil {
		go message.SendP3Important(context.Background(), message.NewCommonMsgWithImport(fmt.Sprintf("%s channel异常", cl.symbol.String()), "orderbookCH 偶现异常,联系开发查看日志确定问题"))
	}

	if len(depthCh.Asks) > 0 {
		cl.ask1Price = depths.Asks[0].Price
	}
	if len(depthCh.Bids) > 0 {
		cl.bid1Price = depths.Bids[0].Price
	}

	cl.depthID++
}

func (cl *Client) OnIndex(index *apis.IndexEvent) {
	cl.indexCH <- index
}

func (cl *Client) OnFundingRate(fundingRate *apis.FundingRateEvent) {
	cl.fundingRate = fundingRate
}

func (cl *Client) OnPositions(poss *apis.PositionEvent) {
	if len(poss.Value) == 0 {
		level.Error(logger.GetLogger()).Log("message", "OnPositions push null")
		return
	}
	ePos, err := cl.processPosition(poss, "ws")
	if err != nil {
		level.Warn(logger.GetLogger()).Log("message", "OnPositions process pos failed", "err", err.Error())
	} else {
		cl.posInfo.mu.Lock()
		defer cl.posInfo.mu.Unlock()
		lastPos := cl.posInfo.pos
		cl.posInfo.pos = ePos
		if lastPos == nil {
			return
		}

		if !lastPos.Long.Position.Equal(ePos.Long.Position) || !lastPos.Short.Position.Equal(ePos.Short.Position) {
			ctxlog.Info(cl.ctx, "message", "OnPositions", "Long", fmt.Sprintf("%+v", ePos.Long), "Short", fmt.Sprintf("%+v", ePos.Short))
			cl.posChangeNotify.Set()
		}
	}
}

func (cl *Client) OnBalances(e *apis.BalanceEvent) {
	if len(e.Values) == 0 {
		return
	}
	// 只推变动的资产过来
	change := false
	for _, currency := range cl.balanceCurrencies {
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
					level.Info(logger.GetLogger()).Log("message", "swap OnBalances", "currency", currency, "old", fmt.Sprintf("%+v", old), "new", fmt.Sprintf("%+v", b))
					change = true
				}
			} else {
				log.Info("[Swap] OnBalances currency=%s old=%+v new=%+v", currency, old, b)
			}
			if !old.Time.After(b.Time) {
				new = b
				updated = true
			}
			return
		})
	}

	if change {
		cl.balanceChangeNotify.Set()
	}
}

func (cl *Client) OnBookTicker(raw []byte) {
	/*
		{
		  "e":"bookTicker",         // 事件类型
		  "u":17242169,             // 更新ID
		  "s":"BTCUSD_200626",      // 交易对
		  "ps":"BTCUSD",            // 标的交易对
		  "b":"9548.1",             // 买单最优挂单价格
		  "B":"52",                 // 买单最优挂单数量
		  "a":"9548.5",             // 卖单最优挂单价格
		  "A":"11",                 // 卖单最优挂单数量
		  "T":1591268628155,        // 撮合时间
		  "E":1591268628166         // 事件时间
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
		// 如果tradeId存在,说明是trade事件
		if first.Get(`tradeId`).Exists() {
			if cl.bookTicker == nil {
				return
			}
			bt.ID = first.Get(`seqId`).Int()
			bt.TimeStamp = time.UnixMilli(first.Get(`ts`).Int())
			bt.Ask1Price = cl.bookTicker.Ask1Price
			bt.Ask1Amount = cl.bookTicker.Ask1Amount
			bt.Bid1Price = cl.bookTicker.Bid1Price
			bt.Bid1Amount = cl.bookTicker.Bid1Amount
			price := first.Get(`px`).Float()
			if first.Get(`side`).String() == `sell` {
				if price < cl.bookTicker.Bid1Price {
					// 是不是还要更新ask价格？
					cl.bookTicker.Bid1Price = price
					cl.bookTicker = &bt
					cl.bookTickerNotify.Set()
				}
			} else {
				if price > cl.bookTicker.Ask1Price {
					cl.bookTicker.Ask1Price = price
					cl.bookTicker = &bt
					cl.bookTickerNotify.Set()
				}
			}
			return
		}
		bt.ID = first.Get(`seqId`).Int()
		bt.TimeStamp = time.UnixMilli(first.Get(`ts`).Int())
		bt.Ask1Price = first.Get(`asks.0.0`).Float()
		bt.Ask1Amount = first.Get(`asks.0.1`).Float()
		bt.Bid1Price = first.Get(`bids.0.0`).Float()
		bt.Bid1Amount = first.Get(`bids.0.1`).Float()
		cv := cl.actsSymbol.ContractValue
		if cl.needTransform() {
			bt.Ask1Amount = bt.Ask1Amount * cv
			bt.Bid1Amount = bt.Bid1Amount * cv
		}
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
	} else if cl.GetExchangeName() == exchange.GateIO {
		if data.Get(`event`).String() == `subscribe` {
			return
		}
		result := data.Get(`result`)
		bt.ID = result.Get(`u`).Int()
		bt.TimeStamp = time.UnixMilli(result.Get(`t`).Int())
		bt.Ask1Price = result.Get(`a`).Float()
		bt.Ask1Amount = result.Get(`A`).Float()
		bt.Bid1Price = result.Get(`b`).Float()
		bt.Bid1Amount = result.Get(`B`).Float()
	} else if cl.GetExchangeName() == exchange.HyperLiquid {
		if data.Get(`method`).String() == `subscribe` {
			return
		}
		result := data.Get(`data.bbo`)
		bt.ID = data.Get(`data.time`).Int()
		bt.TimeStamp = time.UnixMilli(data.Get(`data.time`).Int())
		bt.Ask1Price = result.Get(`1.px`).Float()
		bt.Ask1Amount = result.Get(`1.sz`).Float()
		bt.Bid1Price = result.Get(`0.px`).Float()
		bt.Bid1Amount = result.Get(`0.sz`).Float()
		cl.bid1Price = bt.Bid1Price
		cl.ask1Price = bt.Ask1Price
	} else {
		bt.ID = data.Get(`u`).Int()
		bt.TimeStamp = time.UnixMilli(data.Get(`E`).Int())
		bt.Ask1Price = data.Get(`a`).Float()
		bt.Ask1Amount = data.Get(`A`).Float()
		bt.Bid1Price = data.Get(`b`).Float()
		bt.Bid1Amount = data.Get(`B`).Float()
	}

	cl.bookTicker = &bt
	cl.bookTickerNotify.Set()
	// ctxlog.Info(cl.ctx, "message", "swap OnBookTicker", "data", data.String(), "bt", fmt.Sprintf("%+v", bt))
}

// 目前只有ok、gate要转成张数
func (cl *Client) needTransform() bool {
	if !cl.inverse && !helper.InStringArray(cl.actsApi.GetExchangeName(), []string{exchange.Binance, exchange.BinancePortfolio, exchange.Bybit5, exchange.HyperLiquid}) {
		return true
	}
	return false
}

func (cl *Client) GetExchangeName() string {
	return cl.actsApi.GetExchangeName()
}
