package acts

import (
	"context"
	"fmt"
	"strings"

	"cf_arbitrage/exchange"
	utilCommon "cf_arbitrage/util/common"

	"go.common/apis"
	"go.common/apis/actsApi"
	"go.common/apis/apiFactory"
	"go.common/helper"
	"go.common/sliceX"

	ccexgo "github.com/NadiaSama/ccexgo/exchange"
	"github.com/NadiaSama/ccexgo/exchange/binance/spot"
	"github.com/NadiaSama/ccexgo/exchange/binance/swap"

	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
)

type (
	FundTransfer struct {
		client *actsApi.ActsAPI
	}
)

var (
	Symbols             = make(map[string][]*apis.Symbol)
	okxWsUrl            = `wss://ws.okx.com:8443/ws/v5/public`
	manual              = false  // 标记是手动操作生成的client
	ManualTakerOpSuffix = `d0af` // 手动taker订单后缀
	ManualMakerOpSuffix = `f0af` // 手动maker订单后缀
)

func NewActsApi(exchangeName, key, secret string, vault bool) (apis.Interface, error) {
	ex := exchangeName
	if exchangeName == exchange.BinanceMargin {
		ex = exchange.Binance
	}

	key, _ = utilCommon.DecryptKey(key)
	var logStr string
	if vault {
		logStr = fmt.Sprintf(`acts,%s,vault:%s`, ex, key)
	} else {
		logStr = fmt.Sprintf(`acts,%s,%s`, ex, key)
		apiSecret, apiPassphrase := utilCommon.DecryptKey(secret)
		if ex == exchange.Okex5 {
			if apiPassphrase == `` {
				return nil, fmt.Errorf("acts %s api init fail", ex)
			}
			logStr = fmt.Sprintf(`%s,%s,password=%s`, logStr, apiSecret, apiPassphrase)
		} else {
			logStr = fmt.Sprintf(`%s,%s`, logStr, apiSecret)
		}
	}

	api, err := apiFactory.GetApiAndLogin(logStr)
	if err != nil {
		return nil, errors.WithMessage(err, "acts api init fail")
	}
	symbols, err := api.GetSymbols()
	if err != nil {
		return nil, errors.WithMessage(err, "get symbols fail")
	}
	Symbols[exchangeName] = symbols
	return api, nil
}

func ParseSpotSymbol(symbol, exchangeName string) (ccexgo.SpotSymbol, *apis.Symbol, error) {
	index := sliceX.FindIf(len(Symbols[exchangeName]), func(i int) bool {
		return Symbols[exchangeName][i].Name == symbol && Symbols[exchangeName][i].ProductType == apis.ProductTypeSpot
	})
	if index < 0 {
		return nil, nil, fmt.Errorf(`未找到现货交易对 %s,请检查配置`, symbol)
	}
	return spot2ccexgo(Symbols[exchangeName][index]), Symbols[exchangeName][index], nil
}

func spot2ccexgo(aSymbol *apis.Symbol) ccexgo.SpotSymbol {
	cfg := ccexgo.SymbolConfig{
		AmountMin:       decimal.NewFromFloat(aSymbol.MinimumOrderAmount),
		AmountMax:       decimal.Zero,
		AmountPrecision: decimal.NewFromFloat(aSymbol.AmountTickSize),
		PricePrecision:  decimal.NewFromFloat(aSymbol.PriceTickSize),
		ValuePrecision:  decimal.NewFromFloat(aSymbol.MinimumOrderMoneyInQuoteCurrency),
		ValueMin:        decimal.NewFromFloat(aSymbol.MinimumOrderMoneyInQuoteCurrency),
	}
	ret := &spot.SpotSymbol{
		BaseSpotSymbol: ccexgo.NewBaseSpotSymbol(aSymbol.BaseCurrency, aSymbol.QuoteCurrency, cfg, aSymbol),
		Symbol:         aSymbol.Name,
	}
	return ret
}

func swap2ccexgo(aSymbol *apis.Symbol, exchangeName string) ccexgo.SwapSymbol {
	var contractValue float64
	cfg := ccexgo.SymbolConfig{
		PricePrecision: decimal.NewFromFloat(aSymbol.PriceTickSize),
		AmountMax:      decimal.Zero,
	}
	if aSymbol.ContractValueIsInQuoteCurrency {
		cfg.AmountMin = decimal.NewFromInt(1)
		cfg.AmountPrecision = decimal.NewFromInt(1)
		contractValue = aSymbol.ContractValue
	} else {
		cfg.AmountMin = decimal.NewFromFloat(aSymbol.MinimumOrderAmount)
		cfg.AmountPrecision = decimal.NewFromFloat(aSymbol.AmountTickSize)
		contractValue = aSymbol.ContractValue
		if strings.Contains(exchangeName, exchange.Binance) || exchangeName == exchange.Bybit5 || exchangeName == exchange.HyperLiquid { // 币安u本位用精度当面值
			contractValue = aSymbol.MinimumOrderAmount
		}
	}
	ret := &swap.SwapSymbol{
		BaseSwapSymbol: ccexgo.NewBaseSwapSymbolWithCfg(aSymbol.Name, decimal.NewFromFloat(contractValue), cfg, aSymbol),
		Symbol:         aSymbol.Name,
	}
	return ret
}

func ParseSwapSymbol(symbol, exchangeName string, typ apis.ProductType) (ccexgo.SwapSymbol, *apis.Symbol, error) {
	index := sliceX.FindIf(len(Symbols[exchangeName]), func(i int) bool {
		return Symbols[exchangeName][i].Name == symbol && Symbols[exchangeName][i].ProductType == typ
	})
	if index < 0 {
		return nil, nil, fmt.Errorf(`未找到合约交易对 %s,请检查配置`, symbol)
	}
	return swap2ccexgo(Symbols[exchangeName][index], exchangeName), Symbols[exchangeName][index], nil
}

func NewFundTransfer(api apis.Interface) *FundTransfer {
	return &FundTransfer{
		client: api.(*actsApi.ActsAPI),
	}
}

func (ft *FundTransfer) Transfer(ctx context.Context, from, to exchange.AccountType, currency string, amount decimal.Decimal) error {
	if helper.InStringArray(ft.client.GetExchangeName(), []string{exchange.Okex5, exchange.BinancePortfolio, exchange.Bybit5, exchange.BinanceMargin, exchange.GateIO}) { //不需要转账
		return nil
	}

	var (
		typ string
	)

	if from == exchange.AccountTypeSpot {
		typ = `MAIN_CMFUTURE`
	} else {
		typ = `CMFUTURE_MAIN`
	}
	if strings.ToUpper(currency) == `USDT` { //U本位转账
		typ = strings.ReplaceAll(typ, `CMF`, `UMF`)
	}

	amt := amount.String()
	args := &actsApi.TransferArgs{
		Type:     typ,
		Currency: currency,
		Amount:   amt,
	}
	err := ft.client.Transfer(args)
	return err
}

// Getter
func GetOkxWsUrl() string {
	return okxWsUrl
}

// Setter
func SetColoOkxWs() {
	okxWsUrl = `wss://colows-d.okx.com/ws/v5/public`
}

// Getter
func GetManual() bool {
	return manual
}

// Setter
func SetManual() {
	manual = true
}

func ActsType2ccex(typ apis.OrderType) ccexgo.OrderType {
	switch typ {
	case apis.OrderTypeLimit:
		fallthrough
	case apis.OrderTypeMakerOnly:
		return ccexgo.OrderTypeLimit
	default:
		return ccexgo.OrderTypeMarket
	}
}

func ActsSide2ccex(side apis.OrderSide) ccexgo.OrderSide {
	switch side {
	case apis.OrderSideBuy:
		return ccexgo.OrderSideBuy
	case apis.OrderSideSell:
		return ccexgo.OrderSideSell
	case apis.OrderSideCloseLong:
		return ccexgo.OrderSideCloseLong
	default:
		return ccexgo.OrderSideCloseShort
	}
}

func ActsStatus2ccex(status apis.OrderStatus) ccexgo.OrderStatus {
	switch status {
	case apis.OrderStatusOpen:
		return ccexgo.OrderStatusOpen
	case apis.OrderStatusFilled:
		return ccexgo.OrderStatusDone
	case apis.OrderStatusCanceled:
		return ccexgo.OrderStatusCancel
	case apis.OrderStatusFailed:
		return ccexgo.OrderStatusFailed
	default:
		return ccexgo.OrderStatusUnknown
	}
}

func CcexSide2Acts(side ccexgo.OrderSide) apis.OrderSide {
	switch side {
	case ccexgo.OrderSideBuy:
		return apis.OrderSideBuy
	case ccexgo.OrderSideSell:
		return apis.OrderSideSell
	case ccexgo.OrderSideCloseLong:
		return apis.OrderSideCloseLong
	default:
		return apis.OrderSideCloseShort
	}
}
