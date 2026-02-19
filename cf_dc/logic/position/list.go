package position

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"cf_arbitrage/exchange"
	actsSpot "cf_arbitrage/exchange/acts/spot"
	"cf_arbitrage/logic/config"
	"cf_arbitrage/logic/rate"
	"cf_arbitrage/message"
	utilCommon "cf_arbitrage/util/common"

	common "go.common"
	"go.common/apis"
	"go.common/helper"
	"go.common/redis_service"

	ccexgo "github.com/NadiaSama/ccexgo/exchange"
	"github.com/NadiaSama/ccexgo/misc/ctxlog"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"github.com/tidwall/gjson"
)

type (
	AutoExceptionProcessType string
	shareBanType             int8
)

const (
	SystemProcess AutoExceptionProcessType = `系统自动处理`
	ManualProcess AutoExceptionProcessType = `人工处理`
)

const (
	netWorkBan shareBanType = iota + 1 // 网络问题
	assetBan                           // 资产问题
	limitBan                           // 限频问题
	riskBan                            // 风险问题（保证金率）
)

type (
	//Pos 一个仓位
	Pos struct {
		ID              string
		SrOpen          float64
		Direction       string
		Funding1        decimal.Decimal // 币本位是币,U本位是U
		Funding1Real    decimal.Decimal // 币本位是币,U本位是U
		Funding2        decimal.Decimal // 币本位是币,U本位是U
		Funding2Real    decimal.Decimal // 币本位是币,U本位是U
		Unit1           decimal.Decimal
		Unit1Real       decimal.Decimal
		Unit2           decimal.Decimal
		Unit2Real       decimal.Decimal
		Fee1            decimal.Decimal
		Fee1Real        decimal.Decimal
		Fee2            decimal.Decimal
		Fee2Real        decimal.Decimal
		Ccy1            decimal.Decimal // 币本位是币,U本位是u
		Ccy2            decimal.Decimal // 币本位是币,U本位是u
		OpenPrice1      decimal.Decimal
		OpenPrice2      decimal.Decimal
		OpenExposure    decimal.Decimal
		CcyDeposit1     decimal.Decimal // 币本位是币,U本位是u
		CcyDeposit1Real decimal.Decimal // 币本位是币,U本位是u
		CcyDeposit2     decimal.Decimal // 币本位是币,U本位是u
		CcyDeposit2Real decimal.Decimal // 币本位是币,U本位是u
		CcyDeposit      decimal.Decimal // 币本位是币,U本位是u
		CcyDepositReal  decimal.Decimal // 币本位是币,U本位是u
		Mode            rate.RunMode
	}

	PositionInfo struct {
		// Amount             decimal.Decimal // 仓位数量
		// AvgPrice           decimal.Decimal // 平均价格
		Swap1LongPos  Position
		Swap1ShortPos Position
		Swap1Amount   decimal.Decimal // 合约数量(保证金),币本位是币,U本位是U

		Swap2LongPos  Position
		Swap2ShortPos Position
		Swap2Amount   decimal.Decimal // 合约数量(保证金),币本位是币,U本位是U

		// SpotAmount         decimal.Decimal // 现货数量,币本位是币,U本位是U
		TotalExposure      decimal.Decimal // 总敞口
		TotalSwap1Exposure decimal.Decimal // swap1敞口
		TotalSwap2Exposure decimal.Decimal // swap2敞口
		AutoSpotExExposure decimal.Decimal // 自动异常敞口
		AutoSwapExExposure decimal.Decimal // 合约待转账
	}
	//List 仓位列表集合,具体开仓平仓流程参看代码, list并不支持并发同时操作
	List interface {
		Open(ctx context.Context, swapC1, swapC2 exchange.SwapClient, srOpen float64, openTradeUnit float64,
			swap1 *exchange.Depth, swap2 *exchange.Depth, side1 ccexgo.OrderSide) (*OrderResult, error)
		Close(ctx context.Context, swapC1, swapC2 exchange.SwapClient, crClose float64, closeTradeUnit float64,
			swap1 *exchange.Depth, swap2 *exchange.Depth, side1 ccexgo.OrderSide, cfg *config.Config) (*OrderResult, error)

		OpenSwapMaker(ctx context.Context, spotC exchange.Client,
			srOpen float64, openTradeUnit float64, openTickNum float64,
			swap *exchange.Depth, op *OrderResult, side ccexgo.OrderSide, makerType int, preTaker bool) (*OrderResult, *exchange.CfOrder, string, error)
		OpenSwap(ctx context.Context, swapC exchange.SwapClient,
			srOpen float64, openTradeUnit float64, openTickNum float64,
			swap *exchange.Depth, op *OrderResult) (*OrderResult, *exchange.CfOrder, string, error)
		CloseSwapMaker(ctx context.Context, spotC exchange.Client, crClose float64, closeTradeUnit float64, closeTickNum float64,
			swap *exchange.Depth, op *OrderResult, side ccexgo.OrderSide, makerType int, preTaker bool, needFrozen bool) (*OrderResult, *exchange.CfOrder, string, error)
		CloseSwap(ctx context.Context, swapC exchange.SwapClient,
			crClose float64, closeTradeUnit float64, closeTickNum float64,
			swap *exchange.Depth, op *OrderResult) (*OrderResult, *exchange.CfOrder, string, error)

		// maker-maker 挂单
		OpenAllLimit(ctx context.Context, swapC1, swapC2 exchange.SwapClient,
			srOpen float64, openTradeUnit float64, openTickNum []float64, spot *exchange.Depth,
			swap *exchange.Depth, op *OrderResult, side ccexgo.OrderSide, cfg *config.Config) (*OrderResult, *exchange.CfOrder, *exchange.CfOrder, string, int, rate.RunMode)

		CloseAllLimit(ctx context.Context, swapC1, swapC2 exchange.SwapClient,
			srClose float64, closeTradeUnit float64, closeTickNum []float64, spot *exchange.Depth,
			swap *exchange.Depth, op *OrderResult, side ccexgo.OrderSide, cfg *config.Config) (*OrderResult, *exchange.CfOrder, *exchange.CfOrder, string, int, rate.RunMode)

		OpenHedgeSwap2(ctx context.Context, spotC exchange.Client, swapC1 exchange.SwapClient,
			swapC2 exchange.SwapClient, swap1Order *exchange.CfOrder, swap *exchange.Depth, op *OrderResult) (*OrderResult, error)
		OpenHedgeSwap1(ctx context.Context, spotC exchange.Client, swapC1 exchange.SwapClient,
			swapC2 exchange.SwapClient, swap2Order *exchange.CfOrder, swap *exchange.Depth, op *OrderResult) (*OrderResult, error)

		CloseHedgeSwap2(ctx context.Context, spotC exchange.Client, swapC1 exchange.SwapClient,
			swapC2 exchange.SwapClient, spotOrder *exchange.CfOrder, swap *exchange.Depth, op *OrderResult,
			totalLimit float64, frozenAmount decimal.Decimal) (*OrderResult, error)
		CloseHedgeSwap1(ctx context.Context, spotC exchange.Client, swapC1 exchange.SwapClient,
			swapC2 exchange.SwapClient, swapOrder *exchange.CfOrder, swap *exchange.Depth, op *OrderResult,
			totalLimit float64, frozenAmount decimal.Decimal) (*OrderResult, error)

		// maker-maker 对冲
		OpenAllLimitHedge(ctx context.Context, swapC1, swapC2 exchange.SwapClient,
			swap1Order *exchange.CfOrder, swap2Order *exchange.CfOrder, swap *exchange.Depth, spot *exchange.Depth, op *OrderResult) (*OrderResult, error)

		// maker-maker 对冲
		CloseAllLimitHedge(ctx context.Context, swapC1, swapC2 exchange.SwapClient, swap1Order *exchange.CfOrder, swap2Order *exchange.CfOrder,
			swap *exchange.Depth, spot *exchange.Depth, op *OrderResult, totalLimit float64, frozenAmount decimal.Decimal) (*OrderResult, error)

		OpenSwapTakerOrder(ctx context.Context, cfg *config.Config, swapC exchange.SwapClient,
			srOpen float64, openTradeUnit float64, side ccexgo.OrderSide, mode rate.RunMode) (*OrderResult, *exchange.CfOrder, error)

		CloseSwapTakerOrder(ctx context.Context, cfg *config.Config, swapC exchange.SwapClient,
			srClose float64, closeTradeUnit float64, side ccexgo.OrderSide, mode rate.RunMode) (*OrderResult, *exchange.CfOrder, error)

		PreTakerOpenFinished(ctx context.Context, swapC1, swapC2 exchange.SwapClient, srOpen float64,
			swap1Order, swap2Order *exchange.CfOrder, swap1, swap2 *exchange.Depth, op *OrderResult, mode rate.RunMode) (*OrderResult, error)

		PreTakerCloseFinished(ctx context.Context, swapC1, swapC2 exchange.SwapClient, srClose float64,
			swap1Order, swap2Order *exchange.CfOrder, swap1, swap2 *exchange.Depth, op *OrderResult, mode rate.RunMode, totalLimit float64, frozenAmount decimal.Decimal) (*OrderResult, error)

		//更新spot, swap账号信息
		UpdateAccount(ctx context.Context, spotC exchange.Client, swapC1 exchange.SwapClient, swapC2 exchange.SwapClient, cnt int64) error
		// 更新资金费
		UpdateFunding(ctx context.Context, swapC1 exchange.SwapClient, typ string) ([][2]string, error)

		// UpdateEstimateExposure()

		// 处理正常敞口
		// ProcessExposure(ctx context.Context, spotC exchange.Client) error
		// 自动处理异常敞口
		// ProcessExExposure(ctx context.Context, spotC exchange.Client, swapC exchange.SwapClient, processSpot bool) error

		// 外部调用, 增加合约敞口
		AddSwapExposure(ctx context.Context, exposure decimal.Decimal, swapName string)

		// 处理合约敞口
		ProcessSwapExposure(ctx context.Context, swapC1 exchange.SwapClient, swapC2 exchange.SwapClient, swap1 *exchange.Depth, swap2 *exchange.Depth, cfg *config.Config) (bool, error)

		AddExposure(ctx context.Context, exposure decimal.Decimal)

		// 更新本地记录的总仓位
		UpdateLocalTotalPos() (decimal.Decimal, decimal.Decimal)

		// 更新合约仓位
		UpdateSwap(swapPos1, swapPos2 *exchange.Position)

		//外部调用，判断现货是否超过最小下单量和最小下单价值
		OverMinOrderValue(closeTradeVolume, midPrice, closeTickNum float64) bool

		//外部调用，判断现在下单的方向
		DealSide(open bool) ccexgo.OrderSide

		Position() *PositionInfo

		Banned() bool //是否禁止操作

		// 初始化平均开仓价差
		InitAvgOpenSpread()

		CanOrder(swap1Side, swap2Side apis.OrderSide) bool

		// 风控停止
		UpdateBannedTime(errType *exchange.ErrorType, isBan bool)

		// 带原因和分钟数的风控停止
		UpdateBannedTime2(errType *exchange.ErrorType, isBan bool, reason string, minutes int)

		// 设置环境共享ban
		CheckErrShareBan(errType *exchange.ErrorType, exchangeName string)

		SetSwap1Swap2(swap1C, swap2C exchange.SwapClient)
		// 处理adl
		ProcessADL(ctx context.Context, swap1Amount, swap2Amount, price decimal.Decimal, side ccexgo.OrderSide) (swap1Order, swap2Order *exchange.CfOrder, err error)

		// 检查仓位方向是否正确
		CheckPosSideAmount(side1, side2 ccexgo.OrderSide) error

		// 清仓
		ClearPos(ctx context.Context, swapC1 exchange.SwapClient, swapC2 exchange.SwapClient, side1 ccexgo.OrderSide, clearNoOrder bool) error

		// 尝试冻结平仓数量，防止超平
		TryFreezeCloseAmount(requestAmount decimal.Decimal, frozen bool) (decimal.Decimal, bool)

		// 解冻平仓数量
		UnfreezeCloseAmount(amount decimal.Decimal)
	}

	exportField struct {
		AutoExceptionInfo       Exception       `json:"auto_exception_info"` // 自动处理的异常
		BannedUntil             int64           `json:"banned_until"`        // 被ban到的时间戳
		AvgOpenSpread           decimal.Decimal `json:"avg_open_spread"`     // 平均开仓价差
		TotalUnit               decimal.Decimal `json:"total_unit"`          // 计算平均开仓价差用
		Swap1LongPos            Position        `json:"swap1_long_pos"`
		Swap1ShortPos           Position        `json:"swap1_short_pos"`
		Swap1Amount             decimal.Decimal `json:"swap1_amount"` // 币本位是币,U本位是U
		Swap1Exchange           string          `json:"swap1_ex"`
		Swap1WithdrawlAvailable decimal.Decimal `json:"swap1_withdraw_available"`
		Swap2LongPos            Position        `json:"swap2_long_pos"`
		Swap2ShortPos           Position        `json:"swap2_short_pos"`
		Swap2Exchange           string          `json:"swap2_ex"`
		Swap2Amount             decimal.Decimal `json:"swap2_amount"` // 币本位是币,U本位是U
		Swap2WithdrawlAvailable decimal.Decimal `json:"swap2_withdraw_available"`
		LocalPos1Amount         decimal.Decimal `json:"local_pos1_amount"` //本地记录持仓
		LocalPos2Amount         decimal.Decimal `json:"local_pos2_amount"` //本地记录持仓
		TotalExposure           decimal.Decimal `json:"total_exposure"`
		SwapCcy                 decimal.Decimal `json:"swap_ccy"`
		Funding1                decimal.Decimal `json:"funding1"` // 币本位是币,U本位是U
		Funding2                decimal.Decimal `json:"funding2"` // 币本位是币,U本位是U
		Poss                    []Pos           `json:"poss"`
		PosID                   int             `json:"pos_id"`
		LastSwap1FundingTime    time.Time       `json:"last_swap1_funding_time"`
		LastSwap2FundingTime    time.Time       `json:"last_swap2_funding_time"`

		TotalSwap1Exposure decimal.Decimal `json:"total_swap1_exposure"` // swap1 合约敞口, 正数表示多了要平仓, 负数表示少了要开仓
		TotalSwap2Exposure decimal.Decimal `json:"total_swap2_exposure"` // swap2 合约敞口, 正数表示多了要平仓, 负数表示少了要开仓
	}

	listImpl struct {
		exportField
		inverse         bool
		transfer        exchange.FundTransfer
		logger          log.Logger
		envName         string
		shareBan        shareBan
		spot            ccexgo.SpotSymbol
		swap1           ccexgo.SwapSymbol
		swap2           ccexgo.SwapSymbol
		swap1ActsSymbol *apis.Symbol
		swap2ActsSymbol *apis.Symbol
		level1          decimal.Decimal
		level2          decimal.Decimal
		swap1C          exchange.SwapClient
		swap2C          exchange.SwapClient
		amount1Exp      int
		amount2Exp      int
		amountMinExp    int

		// 并发控制
		mu sync.RWMutex // 保护exportField中的字段

		// 冻结数量机制 - 防止超平
		frozenCloseAmount1 decimal.Decimal // swap1冻结的平仓数量（已下单但未成交）
		frozenCloseAmount2 decimal.Decimal // swap2冻结的平仓数量（已下单但未成交）
		minOrderAmount1    decimal.Decimal // swap1最小下单数量
		minOrderAmount2    decimal.Decimal // swap2最小下单数量

		p0LastWarnTime time.Time // p0级别上次告警时间
	}

	closeParam struct {
		Pos
		traverse int
		count    int
		// targetUsdtUsed decimal.Decimal
	}

	Exception struct {
		SpotExposure decimal.Decimal `json:"spot_exposure"` // 现货需要处理的数量，由现货和合约共同确定， 正敞口(多了)
		SwapExposure decimal.Decimal `json:"swap_exposure"` // 合约只转账 正敞口(多了):转到现货， 负敞口:现货转到合约
		Logs         []string        `json:"logs"`
	}

	Position struct {
		PosAmount       decimal.Decimal `json:"pos_amount"`
		PosAvgOpenPrice decimal.Decimal `json:"pos_avg_open_price"` // 开仓均价
		SwapOpenFees    decimal.Decimal `json:"swap_open_fees"`
	}

	shareBan struct {
		typ   shareBanType
		until int64
		mu    sync.Mutex
	}
)

// func (e *Exception) addExposure(spotEx, swapEx decimal.Decimal, logs string) {
// 	e.SpotExposure = e.SpotExposure.Add(spotEx)
// 	e.SwapExposure = e.SwapExposure.Add(swapEx)
// 	e.Logs = append(e.Logs, fmt.Sprintf("%s %s", time.Now().Format(`2006-01-02 15:04:05.000`), logs))
// }

// 增加合约敞口, 内部有锁
func (li *listImpl) AddSwapExposure(ctx context.Context, exposure decimal.Decimal, swapName string) {
	li.mu.Lock()
	defer li.mu.Unlock()

	if swapName == "swap1" {
		li.TotalSwap1Exposure = li.TotalSwap1Exposure.Add(exposure)
		level.Info(li.logger).Log("message", "add swap1 exposure", "exposure", exposure, "total_swap1_exposure", li.TotalSwap1Exposure)
	} else {
		li.TotalSwap2Exposure = li.TotalSwap2Exposure.Add(exposure)
		level.Info(li.logger).Log("message", "add swap2 exposure", "exposure", exposure, "total_swap2_exposure", li.TotalSwap2Exposure)
	}
}

func NewList(spot ccexgo.SpotSymbol, swap1 ccexgo.SwapSymbol, swap2 ccexgo.SwapSymbol, redisUniqueName string, transfer exchange.FundTransfer, logger log.Logger, level1, level2 int64,
	swap1actsSymbol, swap2actsSymbol *apis.Symbol, swap1Exchange, swap2Exchange string) List {
	li := &listImpl{
		exportField: exportField{
			AutoExceptionInfo:    Exception{},
			Poss:                 make([]Pos, 0),
			LastSwap1FundingTime: time.Now(),
			LastSwap2FundingTime: time.Now(),
		},
		envName:         redisUniqueName,
		spot:            spot,
		swap1:           swap1,
		swap1ActsSymbol: swap1actsSymbol,
		level1:          decimal.NewFromInt(level1),
		swap2:           swap2,
		swap2ActsSymbol: swap2actsSymbol,
		level2:          decimal.NewFromInt(level2),
		transfer:        transfer,
		inverse:         swap1actsSymbol.ContractValueIsInQuoteCurrency,
		logger:          log.With(logger, "component", "pos_list"),
	}
	if !li.inverse {
		exp1 := actsSpot.CalcExponent(li.swap1.ContractVal())
		minOrderAmount1 := decimal.NewFromFloat(swap1actsSymbol.MinimumOrderAmount)
		if swap1Exchange == exchange.Okex5 || swap1Exchange == exchange.GateIO {
			exp1 = actsSpot.CalcExponent(decimal.NewFromFloat(li.swap1ActsSymbol.AmountTickSize * li.swap1ActsSymbol.ContractValue))
			minOrderAmount1 = decimal.NewFromFloat(swap1actsSymbol.MinimumOrderAmount * swap1actsSymbol.ContractValue)
		}
		exp2 := actsSpot.CalcExponent(li.swap2.ContractVal())
		minOrderAmount2 := decimal.NewFromFloat(swap2actsSymbol.MinimumOrderAmount)
		if swap2Exchange == exchange.Okex5 || swap2Exchange == exchange.GateIO {
			exp2 = actsSpot.CalcExponent(decimal.NewFromFloat(li.swap2ActsSymbol.AmountTickSize * li.swap2ActsSymbol.ContractValue))
			minOrderAmount2 = decimal.NewFromFloat(swap2actsSymbol.MinimumOrderAmount * swap2actsSymbol.ContractValue)
		}
		li.amount1Exp = exp1
		li.amount2Exp = exp2
		amountExp := min(exp1, exp2)
		li.amountMinExp = amountExp
		li.minOrderAmount1 = minOrderAmount1
		li.minOrderAmount2 = minOrderAmount2
	}

	return li
}

func (li *listImpl) SetSwap1Swap2(swap1C, swap2C exchange.SwapClient) {
	li.swap1C = swap1C
	li.swap2C = swap2C
	li.Swap1Exchange = swap1C.GetExchangeName()
	li.Swap2Exchange = swap2C.GetExchangeName()
}

func (li *listImpl) UnmarshalJSON(raw []byte) error {
	var ex exportField
	if err := json.Unmarshal(raw, &ex); err != nil {
		return errors.WithMessage(err, "unmarshl expotField failed")
	}
	li.exportField = ex

	level.Info(li.logger).Log("message", "unmarshal done", "Swap1LongPos", fmt.Sprintf(`%+v`, ex.Swap1LongPos), "Swap1ShortPos", fmt.Sprintf(`%+v`, ex.Swap1ShortPos),
		"Swap2LongPos", fmt.Sprintf(`%+v`, ex.Swap2LongPos), "Swap2ShortPos", fmt.Sprintf(`%+v`, ex.Swap2ShortPos), "pos_len", len(ex.Poss),
		"last_swap1_funding_time", ex.LastSwap1FundingTime, "last_swap2_funding_time", ex.LastSwap2FundingTime, "id", ex.PosID,
		"spot", li.spot.String(), "swap", li.swap1.String(), "swap2", li.swap2.String())
	// TODO 暂时启动使用当前时间，大部分程度避免单所跨所一起开多算funding, 后面如果不依赖自己记录的funding可以不用考虑了
	// 只是一个参考值
	li.LastSwap1FundingTime = time.Now()
	li.LastSwap2FundingTime = time.Now()
	// 初始化冻结数量机制
	li.UpdateLocalTotalPos()
	li.initializeFrozenAmounts()
	return nil
}

// 初始化平均开仓价差
func (li *listImpl) InitAvgOpenSpread() {
	if len(li.Poss) > 0 {
		var (
			total     decimal.Decimal
			totalUnit decimal.Decimal
		)
		for _, p := range li.Poss {
			total = total.Add(decimal.NewFromFloat(p.SrOpen).Mul(p.Unit1))
			totalUnit = totalUnit.Add(p.Unit1)
		}
		if !totalUnit.IsZero() {
			li.AvgOpenSpread = total.Div(totalUnit)
		}
		li.TotalUnit = totalUnit
	} else if !li.AvgOpenSpread.IsZero() && len(li.Poss) == 0 {
		li.AvgOpenSpread = decimal.Zero
		li.TotalUnit = decimal.Zero
	}
}

// 计算平均开仓价差
func (li *listImpl) calcAvgOpenSpread(unit, srOpen decimal.Decimal, open bool) {
	li.mu.Lock()
	defer li.mu.Unlock()
	li.InitAvgOpenSpread()
	// if open {
	// 	li.AvgOpenSpread = (li.AvgOpenSpread.Mul(li.TotalUnit).Add(unit.Mul(srOpen))).Div(li.TotalUnit.Add(unit))
	// 	li.TotalUnit = li.TotalUnit.Add(unit)
	// } else {
	// 	li.TotalUnit = li.TotalUnit.Sub(unit)
	// 	if !li.TotalUnit.IsPositive() {
	// 		li.TotalUnit = decimal.Zero
	// 		li.AvgOpenSpread = decimal.Zero
	// 	}
	// }
}

func (li *listImpl) Position() *PositionInfo {
	li.mu.RLock()
	defer li.mu.RUnlock()

	return &PositionInfo{
		Swap1LongPos:  li.Swap1LongPos,
		Swap1ShortPos: li.Swap1ShortPos,
		Swap1Amount:   li.Swap1Amount,

		Swap2LongPos:  li.Swap2LongPos,
		Swap2ShortPos: li.Swap2ShortPos,
		Swap2Amount:   li.Swap2Amount,
		// SpotAmount:    li.SpotAmount,
		// Amount:             li.PosAmount,
		// AvgPrice:           li.PosAvgOpenPrice,
		TotalExposure:      li.TotalExposure,
		TotalSwap1Exposure: li.TotalSwap1Exposure,
		TotalSwap2Exposure: li.TotalSwap2Exposure,
		AutoSpotExExposure: li.AutoExceptionInfo.SpotExposure,
		AutoSwapExExposure: li.AutoExceptionInfo.SwapExposure,
	}
}

func (li *listImpl) Banned() bool {
	return time.Now().UnixMilli() <= li.BannedUntil
}

func (li *listImpl) Open(ctx context.Context,
	swapC1, swapC2 exchange.SwapClient,
	srOpen float64, openTradeUnit float64,
	swap1 *exchange.Depth, swap2 *exchange.Depth, side1 ccexgo.OrderSide) (op *OrderResult, err error) {
	start := time.Now()
	defer func() {
		if e := recover(); e != nil {
			err = utilCommon.RecoverWithLog(li.logger, "Open", start, e)
		}
	}()
	// if li.inverse {
	return li.openSwap1Swap2(ctx, swapC1, swapC2, srOpen, openTradeUnit, swap1, swap2, side1)
	// }
	// return li.openSpotShortSwapLong(ctx, spotC, swapC, srOpen, openTradeUnit, spot, swap)
}

// openSwap1Swap2 开多现货，开空合约
func (li *listImpl) openSwap1Swap2(ctx context.Context,
	swapC1, swapC2 exchange.SwapClient,
	srOpen float64, openTradeUnit float64,
	swap1 *exchange.Depth, swap2 *exchange.Depth, side1 ccexgo.OrderSide) (*OrderResult, error) {

	li.mu.Lock()
	id := fmt.Sprintf("%s_%d", time.Now().Format("060102_150405"), li.PosID)
	li.PosID += 1
	li.mu.Unlock()
	logger := log.With(li.logger, "id", id)
	cv1 := li.swap1.ContractVal()
	swap1Side := side1
	swap2Side := reverseSide(side1)
	amountExp := li.amountMinExp

	amt := decimal.NewFromFloat(openTradeUnit) // 个数
	placeCurrency := amt.Round(int32(amountExp))

	placeSize1 := placeCurrency.Div(li.swap1.ContractVal())
	placeSize2 := placeCurrency.Div(li.swap2.ContractVal())
	if placeSize1.LessThan(decimal.NewFromFloat(li.swap1ActsSymbol.MinimumOrderAmount)) || placeSize2.LessThan(decimal.NewFromFloat(li.swap2ActsSymbol.MinimumOrderAmount)) {
		return nil, fmt.Errorf("下单数量小于最小下单数量 swap1=%s(min=%f) swap2=%s(min=%f)", placeSize1.String(), li.swap1ActsSymbol.MinimumOrderAmount, placeSize2.String(), li.swap2ActsSymbol.MinimumOrderAmount)
	}

	usdt := placeCurrency.Mul(decimal.NewFromFloat(swap1.Asks[0].Price))

	ret := NewOrderResult()
	ret.AddAny("pos_id", id)
	ret.Start()
	ret.AddAny("sr_open", srOpen)
	ret.AddAny("open_trade_unit", amt)
	ret.AddAny("open_usdt", usdt)

	timer := utilCommon.NewTimeOutTimer(ctx, li.swap1.String(), 6, fmt.Sprintf("%s TT开仓", id))
	defer timer.Stop()

	swap1Order, err := swapC1.MarketOrder(ctxlog.SetLog(ctx, logger), swap1Side, placeCurrency, decimal.Zero, nil)
	if err != nil {
		errType := swapC1.HandError(err)
		li.UpdateBannedTime(errType, false)
		li.CheckErrShareBan(errType, swapC2.GetExchangeName())
		processType := errToProcessType(errType)
		if processType == SystemProcess {
			return nil, fmt.Errorf("【%s】swap1(%s)开仓失败 amount=%s, side=%s \n错误原因:%s", processType, swapC1.GetExchangeName(), placeCurrency, side1, swapC1.HandError(err))
		}
		return nil, fmt.Errorf("【人工处理】无敞口产生(根据错误处理),创建swap1%s单失败\n错误原因:%s", swap1Side, errType)
	}
	level.Info(logger).Log("message", "create swap1 order", "id", swap1Order.ID.String(), "filled",
		swap1Order.Filled, "fee", swap1Order.Fee, "side", swap1Side)

	swap2Order, err := swapC2.MarketOrder(ctxlog.SetLog(ctx, logger), swap2Side, placeCurrency, decimal.Zero, nil)
	if err != nil {
		errType := swapC2.HandError(err)
		li.UpdateBannedTime(errType, false)
		li.CheckErrShareBan(errType, swapC2.GetExchangeName())
		processType := errToProcessType(errType)

		if processType == SystemProcess {
			li.AddSwapExposure(ctx, swap1Order.Filled, "swap1")
			if errType.Code == exchange.AccountInsufficientBalance {
				return nil, fmt.Errorf("【人工处理】[%s]swap2(%s)开仓失败 amount=%s, side=%s, swap1成交, swap1Order=%+v \n错误原因:%s, 已计入swap1敞口, 需要手动处理余额问题", rate.Swap1TSwap2T, swapC2.GetExchangeName(), amt.String(), swap1Side, swap1Order, errType)
			}
			return nil, fmt.Errorf("【%s】[%s]swap2(%s)开仓失败 amount=%s, side=%s, swap1成交, swap1Order=%+v \n错误原因:%s, 已计入swap1敞口", processType, rate.Swap1TSwap2T, swapC2.GetExchangeName(), amt.String(), swap1Side, swap1Order, errType)
		} else {
			return nil, fmt.Errorf("【人工处理】[%s]swap2(%s)开仓失败 amount=%s, side=%s, swap1成交, swap1Order=%+v \n错误原因:%s", rate.Swap1TSwap2T, swapC2.GetExchangeName(), amt.String(), swap1Side, swap1Order, swapC1.HandError(err))
		}
	}
	level.Info(logger).Log("message", "create swap sell order", "id", swap2Order.ID.String())

	// 币安taker可能是bnb抵扣费, 要计算出对应的usdt费
	var (
		bnbEqualFee1 decimal.Decimal
		bnbEqualFee2 decimal.Decimal
	)

	if strings.Contains(swapC1.GetExchangeName(), exchange.Binance) && swap1Order.FeeCurrency == `BNB` && li.swap1.String() != `BNBUSDT` {
		bnbEqualFee1 = swap1Order.Filled.Mul(swap1Order.AvgPrice).Mul(swapC1.FeeRate().OpenTaker.Mul(decimal.NewFromFloat(0.9))).Neg()
		swap1Order.Fee = bnbEqualFee1
	}
	if strings.Contains(swapC2.GetExchangeName(), exchange.Binance) && swap2Order.FeeCurrency == `BNB` && li.swap2.String() != `BNBUSDT` {
		bnbEqualFee2 = swap2Order.Filled.Mul(swap2Order.AvgPrice).Mul(swapC2.FeeRate().OpenTaker.Mul(decimal.NewFromFloat(0.9))).Neg()
		swap2Order.Fee = bnbEqualFee2
	}

	realSr := swap2Order.AvgPrice.Div(swap1Order.AvgPrice).Sub(decimal.NewFromInt(1))

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

	swap1Ccy := filled1
	swap2Ccy := filled2

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

	ret.AddAny("sr_open_real", realSr)
	ret.AddOrder("swap1", swap1Order)
	ret.AddOrder("swap2", swap2Order)
	ret.AddDepth("swap2", swap2)
	ret.AddAny("unit1_real", unit1Real)
	ret.AddAny("unit2_real", unit2Real)
	ret.AddAny("fee1", swap1Order.Fee)
	ret.AddAny("fee1_real", fee1Real)
	ret.AddAny("fee2", swap2Order.Fee)
	ret.AddAny("fee2_real", fee2Real)
	ret.AddAny("ccy1", ccy1)
	ret.AddAny("ccy2", ccy2)
	ret.AddAny("deposit1", ccyDeposit1)
	ret.AddAny("deposit1_real", ccyDeposit1)
	ret.AddAny("deposit2", ccyDeposit2)
	ret.AddAny("deposit2_real", ccyDeposit2Real)
	ret.AddAny("deposit", ccyDeposit)
	ret.AddAny("deposit_real", ccyDeposit1.Add(ccyDeposit2Real))
	ret.AddAny("open_exposure", openExposure) // 开仓敞口
	ret.AddAny("mode", rate.Swap1TSwap2T)
	ret.AddAny("maker_info", "")
	ret.End()

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
		CcyDeposit1Real: ccyDeposit1,
		CcyDeposit2:     ccyDeposit2,
		CcyDeposit2Real: ccyDeposit2Real,
		CcyDeposit:      ccyDeposit,
		CcyDepositReal:  ccyDeposit1.Add(ccyDeposit2Real),
		Mode:            rate.Swap1TSwap2T,
	})

	li.TotalExposure = li.TotalExposure.Add(openExposure)
	li.updateLocalTotalPos()
	li.mu.Unlock()

	li.calcAvgOpenSpread(unit1, realSr, true)

	if err := li.updateAccountInfo(ctx, nil, swapC1, swapC2, logger); err != nil {
		level.Warn(logger).Log("message", "update account info failed", "err", err.Error(), "sleep", li.BannedUntil)
		li.UpdateBannedTime(nil, true)
	}

	ret.SetCcy(ccyDeposit1, ccyDeposit2)
	return ret, nil
}

// openSpotShortSwapLong 开空现货开多合约
func (li *listImpl) openSpotShortSwapLong(ctx context.Context,
	spotC exchange.Client, swapC exchange.SwapClient,
	srOpen float64, openTradeUnit int,
	spot *exchange.Depth, swap *exchange.Depth) (*OrderResult, error) {

	// id := fmt.Sprintf("%s_%d", time.Now().Format("20060102"), li.PosID)
	// li.PosID += 1
	// logger := log.With(li.logger, "id", id)

	// spotFR := spotC.FeeRate()
	// swapFR := swapC.FeeRate()
	// cv := li.swap.ContractVal()

	// rate := swapFR.OpenTaker.Add(decimal.NewFromFloat(1)).Div(decimal.NewFromFloat(1).Sub(spotFR.OpenTaker))

	// amt := decimal.NewFromInt(int64(openTradeUnit))
	// exp := actsSpot.CalcExponent(li.spot.AmountPrecision())
	// currency := amt.Mul(cv).Mul(decimal.NewFromFloat(swap.Asks[0].Price)).Mul(rate) // 实际想卖的U
	// placeCurrency := currency.Round(int32(exp))
	// usdt := placeCurrency

	// ret := NewOrderResult()
	// ret.AddField("pos_id", id)
	// ret.Start()
	// ret.AddFloat("sr_open", srOpen)
	// ret.AddDecimal("open_trade_unit", amt)
	// ret.AddDecimal("open_usdt", usdt)

	// timer := li.newTimeOutTimer(ctx, 6)
	// defer timer.Stop()

	// spotOrder, err := spotC.MarketOrder(ctxlog.SetLog(ctx, logger), ccexgo.OrderSideSell, decimal.Zero, placeCurrency)
	// if err != nil {
	// 	errType := spotC.HandError(err)
	// 	li.UpdateBannedTime(errType, false)
	// 	level.Error(logger).Log("message", "create spot sell order failed", "err", err.Error())
	// 	return nil, fmt.Errorf("【人工处理】无敞口产生(根据错误处理),创建现货卖单失败\n错误原因:%s", errType)
	// }
	// //现货market-buy order手续费通过手续费率(taker)计算，avgPrice通过 usdt/filled计算
	// //spotOrder, err = spotC.FetchOrder(ctx, spotOrder)
	// ret.AddOrder("spot", spotOrder)
	// level.Info(logger).Log("message", "create spot sell order", "id", spotOrder.ID.String(), "filled",
	// 	spotOrder.Filled, "fee", spotOrder.Fee, "fee_calc_rate", rate)

	// realGet := spotOrder.Filled.Mul(spotOrder.AvgPrice).Sub(spotOrder.Fee)
	// tf := realGet.Truncate(8)

	// realTf := tf.Mul(decimal.NewFromInt(4)).Div(decimal.NewFromInt(5)).Truncate(8)

	// firstAmount := li.SwapAmount
	// var (
	// 	duration   time.Duration
	// 	transInAdv bool
	// )
	// if firstAmount.LessThan(decimal.NewFromInt(3000)) {
	// 	transInAdv = true
	// 	ts := time.Now()
	// 	if err := li.transfer.Transfer(ctx, exchange.AccontTypeSpot, exchange.AccontTypeSwap, li.spot.Quote(), realTf); err != nil {
	// 		balance, e := spotC.FetchBalance(ctx)
	// 		if e != nil {
	// 			level.Warn(logger).Log("message", "fetch balance failed", "error", e.Error())
	// 			balance = []ccexgo.Balance{}
	// 		}
	// 		errType := swapC.HandError(err)
	// 		li.UpdateBannedTime(errType, false)
	// 		processType := errToProcessType(errType)
	// 		newErr := fmt.Errorf("【%s】(合约开多前)划转现货->合约失败,产生敞口:%s 目前余额:%+v\n错误原因:%s", processType, realGet, balance, errType)
	// 		if processType == SystemProcess {
	// 			li.AutoExceptionInfo.addExposure(realGet, decimal.Zero, newErr.Error())
	// 		}
	// 		return nil, newErr
	// 	}
	// 	duration = time.Since(ts)
	// 	level.Info(logger).Log("message", "transfer from spot to swap", "amount", realTf)
	// }

	// swapOrder, err := swapC.MarketOrder(ctx, ccexgo.OrderSideBuy, amt.Mul(cv), decimal.Zero) // 个数
	// if err != nil {
	// 	var newErr error
	// 	errType := swapC.HandError(err)
	// 	li.UpdateBannedTime(errType, false)
	// 	processType := errToProcessType(errType)
	// 	if transInAdv { // 提前转账了
	// 		newErr = fmt.Errorf("【%s】合约开多失败,合约产生敞口:%s,现货产生敞口:%s,总需买入:%s\n错误原因:%s", processType, realTf, realGet.Sub(realTf), realGet, errType)
	// 		if processType == SystemProcess {
	// 			li.AutoExceptionInfo.addExposure(realGet, realTf, newErr.Error())
	// 		}
	// 	} else {
	// 		newErr = fmt.Errorf("【%s】合约开多失败,现货产生敞口:%s,需买入\n错误原因:%s", processType, realGet, errType)
	// 		if processType == SystemProcess {
	// 			li.AutoExceptionInfo.addExposure(realGet, decimal.Zero, newErr.Error())
	// 		}
	// 	}
	// 	level.Error(logger).Log("message", "create swap open long order failed", "err", err.Error())
	// 	return nil, newErr
	// }
	// level.Info(logger).Log("message", "create swap sell order", "id", swapOrder.ID.String())
	// ret.AddOrder("swap", swapOrder)

	// if firstAmount.GreaterThanOrEqual(decimal.NewFromInt(3000)) {
	// 	ts := time.Now()
	// 	if err := li.transfer.Transfer(ctx, exchange.AccontTypeSpot, exchange.AccontTypeSwap, li.spot.Quote(), realTf); err != nil {
	// 		balance, e := spotC.FetchBalance(ctx)
	// 		if e != nil {
	// 			level.Warn(logger).Log("message", "fetch balance failed", "error", e.Error())
	// 			balance = []ccexgo.Balance{}
	// 		}
	// 		errType := swapC.HandError(err)
	// 		li.UpdateBannedTime(errType, false)
	// 		processType := errToProcessType(errType)
	// 		newErr := fmt.Errorf("【%s】(合约开多后)划转现货->合约失败,无敞口产生,需划转到合约:%s 目前余额:%+v\n错误原因:%s", processType, realTf, balance, errType)
	// 		if processType == SystemProcess {
	// 			li.AutoExceptionInfo.addExposure(decimal.Zero, realTf.Neg(), newErr.Error())
	// 		}
	// 		if err = message.SendImportant(ctx, message.NewOperateFail(fmt.Sprintf("%s 开仓失败", li.swap.String()), newErr.Error())); err != nil {
	// 			level.Warn(logger).Log("message", "send lark failed", "err", err.Error())
	// 		}
	// 		// return nil, newErr
	// 	}
	// 	duration = time.Since(ts)
	// 	level.Info(logger).Log("message", "transfer from spot to swap", "amount", realTf)
	// }

	// ret.AddDecimal("spot_to_swap_amount", realTf)
	// ret.AddDecimal("spot_keep", tf.Sub(realTf))
	// ret.AddField("spot_to_swap_duration", duration.String())

	// ret.AddDepth("spot", spot)
	// ret.AddDepth("swap", swap)

	// real := swapOrder.AvgPrice.Div(spotOrder.AvgPrice).Sub(decimal.NewFromInt(1))
	// realUsdt := spotOrder.Filled.Mul(spotOrder.AvgPrice)
	// ret.AddDecimal("sr_open_real", real)
	// ret.AddDecimal("open_real_usdt", realUsdt)
	// ret.AddDecimal("currency_diff", currency.Sub(placeCurrency)) // 算出的币-下单的币
	// ret.AddDecimal("real_currency_diff", currency.Sub(realUsdt)) // 算出的币-成交的币
	// ret.End()
	// ret.AddDecimal("spot_order_fee", spotOrder.Fee.Neg())
	// ret.AddDecimal("swap_order_fee", swapOrder.Fee)

	// r, _ := real.Float64()
	// swapCcy := swapOrder.Filled.Mul(cv).Mul(swapOrder.AvgPrice)
	// slipExposure := spotOrder.Filled.Mul(spotOrder.AvgPrice).Sub(swapCcy)
	// li.Poss = append(li.Poss, Pos{
	// 	ID:               id,
	// 	SrOpen:           r,
	// 	Unit:             amt,
	// 	SpotToSwap:       realTf,
	// 	Fee:              swapOrder.Fee,
	// 	Ccy:              spotOrder.Filled,
	// 	OpenPrice:        swapOrder.AvgPrice,
	// 	RealCurrencyDiff: currency.Sub(realUsdt),
	// 	SpotKeep:         tf.Sub(realTf),
	// 	SwapCcy:          swapCcy,
	// 	SlipExposure:     slipExposure,
	// })

	// // li.RealCurrencyDiff = li.RealCurrencyDiff.Add(currency.Sub(spotOrder.Filled))
	// li.SpotKeep = li.SpotKeep.Add(tf.Sub(realTf))
	// li.SwapCcy = li.SwapCcy.Add(swapCcy)
	// // li.SlipExposure = li.SlipExposure.Add(slipExposure)
	// li.TotalExposure = li.TotalExposure.Add(usdt.Sub(spotOrder.Filled.Mul(spotOrder.AvgPrice)).Mul(decimal.NewFromInt(1).Sub(spotFR.OpenTaker)))
	// li.UpdateLocalTotalPos()

	// if err := li.updateAccountInfo(ctx, spotC, swapC, logger); err != nil {
	// 	level.Warn(logger).Log("message", "update account info failed", "err", err.Error(), "sleep", "80s")
	// 	// time.Sleep(80 * time.Second) // 休眠80s
	// 	li.UpdateBannedTime(nil, true)
	// 	// return nil, err // 可忽略，最多超出一点设定仓位
	// }

	// ret.SetCcy(spotOrder.Filled)
	// return ret, nil
	return nil, nil
}

// splitPos 根据平仓数量以及totalLimit对仓位进行划分调整
// func (li *listImpl) splitPos(cv decimal.Decimal, closeTradeUnit int, spot *exchange.Depth, swap *exchange.Depth, totalLimit float64) (cp *closeParam, closed []Pos, new []Pos) {
// 	unit := decimal.NewFromInt(int64(closeTradeUnit))
// 	var (
// 		srClose00 float64
// 		price     decimal.Decimal
// 	)
// 	if li.isversed {
// 		srClose00 = swap.Asks[0].Price/spot.Bids[0].Price - 1
// 		price = decimal.NewFromFloat(spot.Bids[0].Price)
// 	} else {
// 		srClose00 = swap.Bids[0].Price/spot.Asks[0].Price - 1
// 		price = decimal.NewFromFloat(spot.Asks[0].Price)
// 	}

// 	remainUnit := unit
// 	var (
// 		orderUnit    decimal.Decimal
// 		fundingEarn  decimal.Decimal
// 		feeEarn      decimal.Decimal
// 		swapToSpot   decimal.Decimal
// 		spotKeep     decimal.Decimal
// 		ccyUsed      decimal.Decimal // 币本位是U,U本位是币
// 		swapCcy      decimal.Decimal // 币本位是币,U本位是U
// 		slipExposure decimal.Decimal
// 		dealCount    int
// 		traverse     int
// 	)
// 	for idx, p := range li.Poss {
// 		if remainUnit.IsZero() { //将剩余的pos补齐
// 			new = append(new, li.Poss[idx:]...)
// 			break
// 		}
// 		traverse += 1

// 		var dealUnit decimal.Decimal
// 		if p.Unit.GreaterThan(remainUnit) {
// 			dealUnit = remainUnit
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
// 		dealSwapCcy := p.SwapCcy.Mul(dealRate).Truncate(8)
// 		dealSlipExposure := p.SlipExposure.Mul(dealRate).Truncate(8)

// 		if li.isversed {
// 			fundingRate, _ := p.Funding.Mul(price).Div(p.Ccy).Float64()
// 			if (p.SrOpen-srClose00)/(1+srClose00)+fundingRate <= totalLimit {
// 				new = append(new, p)
// 				continue
// 			}
// 		} else {
// 			fundingRate, _ := p.Funding.Div(price).Div(p.Ccy).Float64()
// 			if (srClose00-p.SrOpen)/(1+p.SrOpen)+fundingRate <= totalLimit {
// 				new = append(new, p)
// 				continue
// 			}
// 		}

// 		orderUnit = orderUnit.Add(dealUnit)
// 		fundingEarn = fundingEarn.Add(dealFunding)
// 		swapToSpot = swapToSpot.Add(dealSpotToSwap)
// 		feeEarn = feeEarn.Add(dealFee)
// 		ccyUsed = ccyUsed.Add(dealCcy)
// 		spotKeep = spotKeep.Add(dealSpotKeep)
// 		remainUnit = remainUnit.Sub(dealUnit)
// 		swapCcy = swapCcy.Add(dealSwapCcy)
// 		slipExposure = slipExposure.Add(dealSlipExposure)

// 		dealCount += 1

// 		closed = append(closed, Pos{
// 			ID:         p.ID,
// 			SrOpen:     p.SrOpen,
// 			Funding:    dealFunding,
// 			Fee:        dealFee,
// 			SpotToSwap: dealSpotToSwap,
// 			Ccy:        dealCcy,
// 			Unit:       dealUnit,
// 			OpenPrice:  p.OpenPrice,
// 			SpotKeep:   dealSpotKeep,
// 		})

// 		if amt := p.Unit.Sub(dealUnit); amt.IsPositive() {
// 			new = append(new, Pos{
// 				ID:               p.ID,
// 				SrOpen:           p.SrOpen,
// 				Funding:          p.Funding.Sub(dealFunding),
// 				Fee:              p.Fee.Sub(dealFee),
// 				SpotToSwap:       p.SpotToSwap.Sub(dealSpotToSwap),
// 				Ccy:              p.Ccy.Sub(dealCcy),
// 				Unit:             amt,
// 				OpenPrice:        p.OpenPrice,
// 				RealCurrencyDiff: p.RealCurrencyDiff,
// 				SpotKeep:         p.SpotKeep.Sub(dealSpotKeep),
// 				SwapCcy:          p.SwapCcy.Sub(dealSwapCcy),
// 				SlipExposure:     p.SlipExposure.Sub(dealSlipExposure),
// 			})
// 		}
// 	}

// 	if orderUnit.IsZero() {
// 		return
// 	}

// 	cp = &closeParam{
// 		Pos: Pos{
// 			Funding:      fundingEarn,
// 			Fee:          feeEarn,
// 			Unit:         orderUnit,
// 			SpotToSwap:   swapToSpot,
// 			Ccy:          ccyUsed,
// 			SpotKeep:     spotKeep,
// 			SwapCcy:      swapCcy,
// 			SlipExposure: slipExposure,
// 		},
// 		tradverse: traverse,
// 		count:     dealCount,
// 	}
// 	return
// }

// Close 平仓由于平仓数量与开仓数量不一致，一次平仓可能涉及多个仓位
func (li *listImpl) Close(ctx context.Context, swapC1, swapC2 exchange.SwapClient,
	srClose float64, closeTradeUnit float64,
	swap1 *exchange.Depth, swap2 *exchange.Depth, side1 ccexgo.OrderSide, cfg *config.Config) (op *OrderResult, err error) {
	start := time.Now()
	defer func() {
		if e := recover(); e != nil {
			err = utilCommon.RecoverWithLog(li.logger, "Close", start, e)
		}
	}()
	// if li.inverse {
	return li.closeSwap1Swap2(ctx, swapC1, swapC2, srClose, closeTradeUnit, swap1, swap2, side1, cfg)
	// }
	// return li.closeSwapLongSpotShort(ctx, spotC, swapC, srClose, closeTradeUnit, totalLimit, volumeCloseMin, spot, swap)
}

// closeSwap1Swap2 永续平空现货平多
func (li *listImpl) closeSwap1Swap2(ctx context.Context, swapC1, swapC2 exchange.SwapClient,
	srClose float64, closeTradeUnit float64, swap1 *exchange.Depth, swap2 *exchange.Depth, side1 ccexgo.OrderSide, cfg *config.Config) (*OrderResult, error) {
	id := fmt.Sprintf("%d_%d", time.Now().UnixNano(), li.PosID)
	logger := log.With(li.logger, "id", id)
	amountExp := li.amountMinExp
	cv1 := li.swap1.ContractVal()
	cv2 := li.swap2.ContractVal()

	swap1Side := side1
	swap2Side := reverseSide(side1)

	li.mu.RLock()
	localPos2Amount := li.LocalPos2Amount
	swap1ShortPos := li.Swap1ShortPos.PosAmount
	swap2LongPos := li.Swap2LongPos.PosAmount
	swap1LongPos := li.Swap1LongPos.PosAmount
	swap2ShortPos := li.Swap2ShortPos.PosAmount
	li.mu.RUnlock()

	amount1 := decimal.Min(decimal.NewFromFloat(closeTradeUnit), localPos2Amount).Round(int32(amountExp)) // 平的个数

	if side1 == ccexgo.OrderSideCloseShort { // 平空
		amount1 = decimal.Min(amount1, swap1ShortPos, swap2LongPos) // 平的个数
	} else {
		amount1 = decimal.Min(amount1, swap1LongPos, swap2ShortPos) // 平的个数
	}

	placeSize1 := amount1.Div(cv1)
	placeSize2 := amount1.Div(cv2)
	if placeSize1.LessThan(decimal.NewFromFloat(li.swap1ActsSymbol.MinimumOrderAmount)) || placeSize2.LessThan(decimal.NewFromFloat(li.swap2ActsSymbol.MinimumOrderAmount)) {
		errMsg := fmt.Sprintf("下单数量小于最小下单数量 swap1=%s(min=%f) swap2=%s(min=%f)", placeSize1.String(), li.swap1ActsSymbol.MinimumOrderAmount, placeSize2.String(), li.swap2ActsSymbol.MinimumOrderAmount)
		// 给大的一边先处理，另一边依赖自动清理仓位
		// 用交易所的大的一边先处理, 可能本地仓位为0了
		if side1 == ccexgo.OrderSideCloseShort { // 平空
			amount1 = decimal.Min(swap1ShortPos, swap2LongPos) // 平的个数
		} else {
			amount1 = decimal.Min(swap1LongPos, swap2ShortPos) // 平的个数
		}
		placeSize1 = amount1.Div(li.swap1.ContractVal())
		placeSize2 = amount1.Div(li.swap2.ContractVal())
		if placeSize1.GreaterThanOrEqual(placeSize2) {
			swap1Order, err := swapC1.MarketOrder(ctxlog.SetLog(ctx, logger), swap1Side, amount1, decimal.Zero, nil)
			if err != nil {
				errType := swapC1.HandError(err)
				li.UpdateBannedTime(errType, false)
				li.CheckErrShareBan(errType, swapC1.GetExchangeName())
				errMsg += fmt.Sprintf("\n创建swap1%s单失败 amount=%s 错误原因:%s", swap1Side, amount1.String(), errType.String())
			} else {
				level.Info(logger).Log("message", "create swap1 market order", "side", swap1Side, "order", fmt.Sprintf("%+v", swap1Order))
				errMsg += fmt.Sprintf("\n创建swap1%s单成功 amount=%s", swap1Side, amount1.String())
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
		return nil, fmt.Errorf(errMsg)
	}

	timer := utilCommon.NewTimeOutTimer(ctx, li.swap1.String(), 5, fmt.Sprintf("%s TT平仓", id))
	defer timer.Stop()

	swap2Order, err := swapC2.MarketOrder(ctxlog.SetLog(ctx, logger), swap2Side, amount1, decimal.Zero, nil)
	if err != nil {
		errType := swapC2.HandError(err)
		li.UpdateBannedTime(errType, false)
		li.CheckErrShareBan(errType, swapC2.GetExchangeName())
		processType := errToProcessType(errType)
		if processType == SystemProcess {
			return nil, fmt.Errorf("【%s】swap2(%s)平仓失败 amount=%s, side=%s \n错误原因:%s", processType, swapC2.GetExchangeName(), amount1, swap2Side, errType.String())
		}
		return nil, fmt.Errorf("【人工处理】swap2(%s)平仓失败 amount=%s, side=%s \n错误原因:%s", swapC2.GetExchangeName(), amount1, swap2Side, errType.String())
	}

	exp := 0
	if swapC1.GetExchangeName() == exchange.Okex5 { // 可能只有okx才有小数张，取决于acts接口转换导致？
		exp = actsSpot.CalcExponent(decimal.NewFromFloat(li.swap1ActsSymbol.AmountTickSize))
	}

	swap1Order, err := swapC1.MarketOrder(ctx, swap1Side, amount1, decimal.Zero, nil)
	if err != nil {
		// 下单失败，在锁内进行splitPos2计算并更新仓位
		li.mu.Lock()
		cp, _, newPos := li.splitPos2(cv1, swapC1.FeeRate().OpenTaker, amount1.InexactFloat64(), swap2Order.AvgPrice.InexactFloat64(), nil, swap1, cfg.TotalLimit, swap1Side, rate.Swap1TSwap2T, exp)
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
			li.AddSwapExposure(ctx, amount1, "swap1")
			return nil, fmt.Errorf("【%s】swap1(%s)平仓失败 amount=%s, side=%s, swap2成交, swap2Order=%+v \n错误原因:%s, 已计入swap1敞口", processType, swapC1.GetExchangeName(), amount1, swap1Side, swap2Order, swapC1.HandError(err))
		} else {
			return nil, fmt.Errorf("【人工处理】swap1(%s)平仓失败 amount=%s, side=%s, swap2成交, swap2Order=%+v \n错误原因:%s", swapC1.GetExchangeName(), amount1, swap1Side, swap2Order, swapC1.HandError(err))
		}
	}

	op := NewOrderResult()
	op.Start()
	op.AddAny("sr_close", srClose)

	// 下单成功，在锁内进行splitPos2计算并更新仓位
	li.mu.Lock()
	cp, closed, newPos := li.splitPos2(cv1, swapC1.FeeRate().OpenTaker, amount1.InexactFloat64(), swap2Order.AvgPrice.InexactFloat64(), nil, swap1, cfg.TotalLimit, swap1Side, rate.Swap1TSwap2T, exp)
	li.Poss = newPos
	li.updateLocalTotalPos()
	li.mu.Unlock()

	if cp == nil {
		return nil, fmt.Errorf("两边都已成交, 选不出仓位, 跳过仓位操作及盈亏计算, swap1Order=%+v, swap2Order=%+v", swap1Order, swap2Order)
	}

	op.SetCcy(cp.CcyDeposit1, cp.CcyDeposit2)

	level.Info(logger).Log("message", "loop for pos", "traverse", cp.traverse, "deal_count", cp.count,
		"order_unit", cp.Unit1, "funding1", cp.Funding1, "funding_real", cp.Funding1Real, "funding2", cp.Funding2, "finding2_real", cp.Funding2Real, "fee1", cp.Fee1, "fee1_real", cp.Fee1Real,
		"fee2", cp.Fee2, "fee2_real", cp.Fee2Real, "deposit1", cp.CcyDeposit1, "deposit1_real", cp.CcyDeposit1Real, "deposit2", cp.CcyDeposit2Real, "mode", rate.Swap1TSwap2T)

	// 币安taker可能是bnb抵扣费, 要计算出对应的usdt费
	var (
		bnbEqualFee1 decimal.Decimal
		bnbEqualFee2 decimal.Decimal
	)
	one := decimal.NewFromInt(1)

	if strings.Contains(swapC1.GetExchangeName(), exchange.Binance) && swap1Order.FeeCurrency == `BNB` && li.swap1.String() != `BNBUSDT` {
		bnbEqualFee1 = swap1Order.Filled.Mul(swap1Order.AvgPrice).Mul(swapC1.FeeRate().OpenTaker.Mul(decimal.NewFromFloat(0.9))).Neg()
		swap1Order.Fee = bnbEqualFee1
	}
	if strings.Contains(swapC2.GetExchangeName(), exchange.Binance) && swap2Order.FeeCurrency == `BNB` && li.swap2.String() != `BNBUSDT` {
		bnbEqualFee2 = swap2Order.Filled.Mul(swap2Order.AvgPrice).Mul(swapC2.FeeRate().OpenTaker.Mul(decimal.NewFromFloat(0.9))).Neg()
		swap2Order.Fee = bnbEqualFee2
	}

	updatePosIDs := []string{}
	var (
		swap1Profit     decimal.Decimal
		swap2Profit     decimal.Decimal
		swap1ProfitReal decimal.Decimal
	)

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
	swap2Earn := swap2Profit.Add(cp.Funding2).Add(cp.Fee2Real).Add(swap2Order.Fee)
	swap1EarnReal := swap1ProfitReal.Add(cp.Funding1Real).Add(cp.Fee1Real).Add(swap1Order.Fee)

	level.Info(logger).Log("message", "close swap1 order", "id", swap1Order.ID.String(), "unit1", cp.Unit1,
		"swap1_avg_price", swap1Order.AvgPrice, "swap1_profit", swap1Profit, "swap1_earn", swap1Earn, "swap1_order_fee", swap1Order.Fee,
		"swap2_profit", swap2Profit, "swap2_earn", swap2Earn, "swap1_earn_real", swap1EarnReal, "bnb_equal_fee1", bnbEqualFee1, "bnb_equal_fee2", bnbEqualFee2)

	level.Info(logger).Log("message", "close order done", "swap1_order_id", swap1Order.ID.String(), "swap_avg_price", swap1Order.AvgPrice,
		"swap1_order_unit", amount1, "swap2_order_id", swap2Order.ID.String(),
		"swap2_avg_price", swap2Order.AvgPrice, "swap2_order_fee", swap2Order.Fee)

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
	op.AddDepth("swap2", swap2)
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
	op.AddAny("mode", rate.Swap1TSwap2T)
	op.AddAny("pos_ids", strings.Join(updatePosIDs, ","))
	op.AddAny("maker_info", "")
	op.End()
	return op, nil
}

// closeSwapLongSpotShort 永续平多现货平空
func (li *listImpl) closeSwapLongSpotShort(ctx context.Context, spotC exchange.Client, swapC exchange.SwapClient,
	srClose float64, closeTradeUnit int, totalLimit float64, volumeCloseMin int,
	spot *exchange.Depth, swap *exchange.Depth) (*OrderResult, error) {
	// id := fmt.Sprintf("%d_%d", time.Now().Unix(), li.PosID)
	// logger := log.With(li.logger, "id", id)
	// cv := li.swap.ContractVal()

	// cp, closed, newPos := li.splitPos(cv, closeTradeUnit, spot, swap, totalLimit)
	// if cp == nil {
	// 	//no available pos to close
	// 	return nil, nil
	// }

	// unitt, _ := cp.Unit.Float64()
	// if int(unitt) < volumeCloseMin {
	// 	level.Info(logger).Log("message", "unitt less than volume close min")
	// 	return nil, nil
	// }

	// level.Info(logger).Log("message", "loop for pos", "traverse", cp.tradverse, "deal_count", cp.count,
	// 	"order_unit", cp.Unit, "funding_earn", cp.Funding,
	// 	"fee_earn", cp.Fee, "swap_to_spot", cp.SpotToSwap, "spot_keep", cp.SpotKeep, "usdt_used", cp.Ccy)
	// var preProfit decimal.Decimal
	// price := decimal.NewFromFloat(swap.Bids[0].Price)

	// for _, pos := range closed {
	// 	profit := pos.Unit.Mul(cv).Mul(price.Sub(pos.OpenPrice))
	// 	preProfit = preProfit.Add(profit)
	// }

	// if !li.exportField.WithdrawlAvailable.IsZero() && cp.Unit.Mul(cv).Mul(price).Sub(preProfit).GreaterThan(li.exportField.WithdrawlAvailable) {
	// 	level.Info(logger).Log("message", "not available for close", "unit", cp.Unit, "bid0_price", price,
	// 		"withdrawl_available", li.exportField.WithdrawlAvailable)
	// 	return nil, nil
	// }
	// or := NewOrderResult()
	// or.Start()
	// or.SetCcy(cp.Ccy)
	// timer := li.newTimeOutTimer(ctx, 5)
	// defer timer.Stop()

	// swapOrder, err := swapC.MarketOrder(ctxlog.SetLog(ctx, logger), ccexgo.OrderSideCloseLong, cp.Unit.Mul(cv), decimal.Zero)
	// if err != nil {
	// 	errType := swapC.HandError(err)
	// 	li.UpdateBannedTime(errType, false)
	// 	level.Error(logger).Log("message", "create swap close long order failed", "err", err.Error())
	// 	return nil, fmt.Errorf("【人工处理】合约平多失败,无敞口产生(根据错误处理)\n错误原因:%s", errType)
	// }

	// updatePosIDs := []string{}
	// var swapProfit decimal.Decimal
	// for _, pos := range closed {
	// 	profit := pos.Unit.Mul(cv).Mul(swapOrder.AvgPrice.Sub(pos.OpenPrice))
	// 	swapProfit = swapProfit.Add(profit)
	// 	updatePosIDs = append(updatePosIDs, pos.ID)
	// }
	// swapEarn := swapProfit.Add(cp.Funding).Add(cp.Fee).Add(swapOrder.Fee)
	// amt := cp.SpotToSwap.Add(swapEarn) //U

	// level.Info(logger).Log("message", "close swap order", "id", swapOrder.ID.String(), "unit", cp.Unit,
	// 	"avg_price", swapOrder.AvgPrice, "profit", swapProfit, "swap_earn", swapEarn, "swap_order_fee", swapOrder.Fee)

	// var (
	// 	duration   time.Duration
	// 	transInAdv bool
	// )
	// tf := amt.Truncate(8)
	// firstSpotAmount := li.SpotAmount // U
	// realAmount := tf.Add(cp.SpotKeep)

	// if firstSpotAmount.LessThanOrEqual(realAmount.Mul(decimal.NewFromFloat(1.2))) {
	// 	transInAdv = true
	// 	ts := time.Now()
	// 	if err := li.transfer.Transfer(ctx, exchange.AccontTypeSwap, exchange.AccontTypeSpot, li.spot.Quote(), tf); err != nil {
	// 		balance, e := swapC.FetchBalance(ctx)
	// 		if e != nil {
	// 			level.Warn(logger).Log("message", "fetch balance fail", "error", e.Error())
	// 			balance = []ccexgo.Balance{}
	// 		}
	// 		li.Poss = newPos
	// 		li.SpotKeep = li.SpotKeep.Sub(cp.SpotKeep)
	// 		li.SwapCcy = li.SwapCcy.Sub(cp.SwapCcy)
	// 		// li.SlipExposure = li.SlipExposure.Sub(cp.SlipExposure)
	// 		errType := swapC.HandError(err)
	// 		li.UpdateBannedTime(errType, false)
	// 		processType := errToProcessType(errType)
	// 		newErr := fmt.Errorf("【%s】(现货买入前)划转合约->现货失败,合约产生敞口:%s,现货产生敞口:%s,总需买入:%s 目前余额:%+v\n错误原因:%s", processType, tf, cp.SpotKeep, realAmount, balance, errType)
	// 		if processType == SystemProcess {
	// 			li.AutoExceptionInfo.addExposure(realAmount, tf, newErr.Error())
	// 		}
	// 		return or, newErr
	// 	}
	// 	duration = time.Since(ts)
	// 	level.Info(logger).Log("message", "transfer from swap to spot", "amount", tf)
	// }

	// spotOrder, err := spotC.MarketOrder(ctx, ccexgo.OrderSideBuy, decimal.Zero, realAmount)
	// if err != nil {
	// 	li.Poss = newPos
	// 	li.SpotKeep = li.SpotKeep.Sub(cp.SpotKeep)
	// 	li.SwapCcy = li.SwapCcy.Sub(cp.SwapCcy)
	// 	// li.SlipExposure = li.SlipExposure.Sub(cp.SlipExposure)
	// 	var newErr error
	// 	errType := spotC.HandError(err)
	// 	li.UpdateBannedTime(errType, false)
	// 	processType := errToProcessType(errType)
	// 	if transInAdv { // 提前转账了
	// 		newErr = fmt.Errorf("【%s】现货买入失败,现货产生敞口:%s,总需买入:%s,错误原因:%s", processType, realAmount, realAmount, errType)
	// 		if processType == SystemProcess {
	// 			li.AutoExceptionInfo.addExposure(realAmount, decimal.Zero, newErr.Error())
	// 		}
	// 	} else {
	// 		newErr = fmt.Errorf("【%s】现货买入失败,合约产生敞口:%s,现货产生敞口:%s,总需买入:%s,错误原因:%s", processType, tf, cp.SpotKeep, realAmount, errType)
	// 		if processType == SystemProcess {
	// 			li.AutoExceptionInfo.addExposure(realAmount, tf, newErr.Error())
	// 		}
	// 	}
	// 	level.Error(logger).Log("message", "create spot close short order failed", "err", err.Error())
	// 	return or, newErr
	// }

	// if spotOrder.Status != ccexgo.OrderStatusDone && spotOrder.Status != ccexgo.OrderStatusCancel { // 不是成交状态就再rest查
	// 	var (
	// 		o *exchange.CfOrder
	// 		e error
	// 	)
	// 	for i := 0; i < 3; i++ {
	// 		o, e = spotC.FetchOrder(ctx, spotOrder)
	// 		if e != nil {
	// 			level.Warn(logger).Log("message", "fetch spot order fail", "id", spotOrder.ID, "error", e.Error())
	// 			time.Sleep(time.Millisecond * 10)
	// 			continue
	// 		}
	// 		spotOrder = o
	// 		break
	// 	}

	// 	if e != nil {
	// 		li.Poss = newPos
	// 		li.SpotKeep = li.SpotKeep.Sub(cp.SpotKeep)
	// 		li.SwapCcy = li.SwapCcy.Sub(cp.SwapCcy)
	// 		errType := spotC.HandError(err)
	// 		li.UpdateBannedTime(errType, false)
	// 		// li.SlipExposure = li.SlipExposure.Sub(cp.SlipExposure)
	// 		return or, fmt.Errorf("【人工处理】查询现货订单失败 order_id=%s\n错误原因:%s", spotOrder.ID, errType)
	// 	}
	// }

	// if firstSpotAmount.GreaterThan(realAmount.Mul(decimal.NewFromFloat(1.2))) {
	// 	ts := time.Now()
	// 	if err := li.transfer.Transfer(ctx, exchange.AccontTypeSwap, exchange.AccontTypeSpot, li.spot.Quote(), tf); err != nil {
	// 		balance, e := swapC.FetchBalance(ctx)
	// 		if e != nil {
	// 			level.Warn(logger).Log("message", "fetch balance fail", "error", e.Error())
	// 			balance = []ccexgo.Balance{}
	// 		}
	// 		errType := spotC.HandError(err)
	// 		li.UpdateBannedTime(errType, false)
	// 		processType := errToProcessType(errType)
	// 		newErr := fmt.Errorf("【%s】(现货买入后)划转合约->现货失败,无敞口产生,只划转到现货:%s 目前余额:%+v\n错误原因:%s", processType, tf, balance, errType)
	// 		if processType == SystemProcess {
	// 			li.AutoExceptionInfo.addExposure(decimal.Zero, tf, newErr.Error())
	// 		}
	// 		if err = message.SendImportant(ctx, message.NewOperateFail(fmt.Sprintf("%s 平仓失败", li.swap.String()), newErr.Error())); err != nil {
	// 			level.Warn(logger).Log("message", "send lark failed", "err", err.Error())
	// 		}
	// 		// return or, newErr
	// 	}
	// 	duration = time.Since(ts)
	// 	level.Info(logger).Log("message", "transfer from swap to spot", "amount", tf)
	// }

	// level.Info(logger).Log("message", "close order done", "swap_order_id", swapOrder.ID.String(), "swap_avg_price", swapOrder.AvgPrice,
	// 	"swap_order_unit", cp.Unit, "swap_to_spot_amount", tf, "swap_to_spot_duration", duration, "spot_order_id", spotOrder.ID.String(),
	// 	"spot_order_avg_price", spotOrder.AvgPrice, "spot_order_fee", spotOrder.Fee)

	// or.End()

	// li.Poss = newPos
	// li.SpotKeep = li.SpotKeep.Sub(cp.SpotKeep)
	// li.SwapCcy = li.SwapCcy.Sub(cp.SwapCcy)
	// // li.SlipExposure = li.SlipExposure.Sub(cp.SlipExposure)
	// // li.RealCurrencyDiff = li.RealCurrencyDiff.Add(realAmount.Sub(spotOrder.Filled))
	// li.TotalExposure = li.TotalExposure.Add(realAmount.Sub(spotOrder.Filled.Mul(spotOrder.AvgPrice)))
	// li.UpdateLocalTotalPos()

	// if err := li.updateAccountInfo(ctx, spotC, swapC, logger); err != nil {
	// 	level.Warn(logger).Log("message", "update account info failed", "err", err.Error(), "sleep", "80s")
	// 	// time.Sleep(80 * time.Second) // 休眠80s
	// 	li.UpdateBannedTime(nil, true)
	// 	// return or, err
	// }

	// //计算盈亏
	// earn := spotOrder.Filled.Sub(cp.Ccy).Sub(spotOrder.Fee)
	// totalEarn := realAmount.Div(spotOrder.AvgPrice).Sub(cp.Ccy).Sub(spotC.FeeRate().OpenTaker.Mul(realAmount.Div(spotOrder.AvgPrice)))

	// or.AddFloat("sr_close", srClose)
	// or.AddDecimal("sr_close_real", swapOrder.AvgPrice.Div(spotOrder.AvgPrice).Sub(decimal.NewFromInt(1)))
	// or.AddOrder("spot", spotOrder)
	// or.AddOrder("swap", swapOrder)
	// or.AddDepth("spot", spot)
	// or.AddDepth("swap", swap)
	// or.AddDecimal("spot_keep", cp.SpotKeep)
	// or.AddDecimal("swap_profit", swapProfit)
	// or.AddDecimal("funding", cp.Funding)
	// or.AddDecimal("usdt_used", cp.Ccy)
	// or.AddDecimal("earn", earn)
	// or.AddDecimal("earn_rate", earn.Div(cp.Ccy).Mul(decimal.NewFromFloat(100)))
	// or.AddDecimal(`total_spot_amount`, realAmount)
	// or.AddDecimal(`total_earn`, totalEarn)
	// or.AddDecimal(`total_earn_rate`, totalEarn.Div(cp.Ccy).Mul(decimal.NewFromFloat(100)))

	// or.AddField("pos_ids", strings.Join(updatePosIDs, ","))
	// or.AddDecimal("spot_order_fee", spotOrder.Fee.Neg())
	// or.AddDecimal("swap_order_fee", swapOrder.Fee)
	// return or, nil
	return nil, nil
}

func (li *listImpl) ProcessSwapExposure(ctx context.Context, swapC1 exchange.SwapClient, swapC2 exchange.SwapClient, swap1 *exchange.Depth, swap2 *exchange.Depth, cfg *config.Config) (bool, error) {
	id := fmt.Sprintf("%d_%d", time.Now().Unix(), li.PosID)
	logger := log.With(li.logger, "id", id, "component", "ProcessSwapExposure")
	level.Info(logger).Log("message", "start process swap exposure")
	ctx = ctxlog.SetLog(ctx, logger)

	limitUsdt := cfg.WarnValueLimit * float64(cfg.P3Level)
	banUsdt := cfg.WarnValueLimit * float64(cfg.P1Level) * 3

	var (
		side1, side2 ccexgo.OrderSide
	)
	currency := li.swap1ActsSymbol.BalanceCurrency
	amt1 := li.TotalSwap1Exposure
	amt2 := li.TotalSwap2Exposure

	if !li.Swap1LongPos.PosAmount.IsZero() && !li.Swap2ShortPos.PosAmount.IsZero() { // 做空价差
		if amt1.IsPositive() {
			side1 = ccexgo.OrderSideCloseLong // sell
		} else {
			side1 = ccexgo.OrderSideBuy
			amt1 = amt1.Abs()
		}
		if amt2.IsPositive() {
			side2 = ccexgo.OrderSideCloseShort // buy
		} else {
			side2 = ccexgo.OrderSideSell
			amt2 = amt2.Abs()
		}
	} else if !li.Swap1ShortPos.PosAmount.IsZero() && !li.Swap2LongPos.PosAmount.IsZero() { // 做多价差
		if amt1.IsPositive() {
			side1 = ccexgo.OrderSideCloseShort
		} else {
			side1 = ccexgo.OrderSideSell
			amt1 = amt1.Abs()
		}
		if amt2.IsPositive() {
			side2 = ccexgo.OrderSideCloseLong
		} else {
			side2 = ccexgo.OrderSideBuy
			amt2 = amt2.Abs()
		}
	} else {
		// 分辨不出是做多还是做空，判断是否存在仓位
		processEx := false
		if amt1.IsPositive() {
			if li.Swap1LongPos.PosAmount.IsPositive() && li.Swap1ShortPos.PosAmount.IsZero() && li.Swap1LongPos.PosAmount.GreaterThanOrEqual(amt1) {
				side1 = ccexgo.OrderSideCloseLong
				processEx = true
			} else if li.Swap1ShortPos.PosAmount.IsPositive() && li.Swap1LongPos.PosAmount.IsZero() && li.Swap1ShortPos.PosAmount.GreaterThanOrEqual(amt1) {
				side1 = ccexgo.OrderSideCloseShort
				processEx = true
			}
		}
		if amt2.IsPositive() {
			if li.Swap2LongPos.PosAmount.IsPositive() && li.Swap2ShortPos.PosAmount.IsZero() && li.Swap2LongPos.PosAmount.GreaterThanOrEqual(amt2) {
				side2 = ccexgo.OrderSideCloseLong
				processEx = true
			} else if li.Swap2ShortPos.PosAmount.IsPositive() && li.Swap2LongPos.PosAmount.IsZero() && li.Swap2ShortPos.PosAmount.GreaterThanOrEqual(amt2) {
				side2 = ccexgo.OrderSideCloseShort
				processEx = true
			}
		}
		if !processEx {
			level.Warn(logger).Log("message", "ProcessSwapExposure check pos failed", "swap1_long_pos", li.Swap1LongPos, "swap1_short_pos", li.Swap1ShortPos, "swap2_long_pos", li.Swap2LongPos, "swap2_short_pos", li.Swap2ShortPos)
			go message.Send(context.Background(), message.NewCommonMsg(fmt.Sprintf("%s 处理合约敞口失败", li.swap1.String()), "检测到有一边仓位为0, 跳过处理敞口, 可能触发自动清理仓位"))
			return true, nil
		}
	}

	processExposure := func(swapC exchange.SwapClient, side ccexgo.OrderSide, amt decimal.Decimal, swap *exchange.Depth, swapName string, minOrderAmount decimal.Decimal) {
		if amt.LessThan(minOrderAmount) {
			return
		}
		exp := li.amount1Exp
		if swapName == "swap2" {
			exp = li.amount2Exp
		}
		amt = amt.RoundFloor(int32(exp))
		value := amt.Mul(decimal.NewFromFloat(swap.Bids[0].Price))
		if value.InexactFloat64() > limitUsdt {
			msg := fmt.Sprintf("本次%s(%s) %s处理敞口超过%.2fU,需要处理的敞口数量预计%sU,请检查！", swapName, swapC.GetExchangeName(), side, limitUsdt, value)
			if value.InexactFloat64() > banUsdt && time.Since(li.p0LastWarnTime).Minutes() > 10 {
				li.BannedUntil = time.Now().Add(time.Minute*10).UnixMilli() + (rand.Int63n(30)+1)*1000
				msg += "\n程序交易将暂停10分钟, 直到" + time.UnixMilli(li.BannedUntil).Format("2006-01-02 15:04:05.000") + "恢复"
				go message.SendP0Important(context.Background(), message.NewCommonMsgWithImport(fmt.Sprintf("%s 处理合约%s敞口失败", li.swap1.String(), swapName), msg))
				li.p0LastWarnTime = time.Now()
			} else {
				go message.SendP3Important(context.Background(), message.NewCommonMsgWithImport(fmt.Sprintf("%s 处理合约%s敞口失败", li.swap1.String(), swapName), msg))
			}
			return
		}
		// 如果开仓价值小于5.1U, 则不处理
		if (side == ccexgo.OrderSideBuy || side == ccexgo.OrderSideSell) && value.LessThan(decimal.NewFromFloat(5.1)) {
			return
		}
		swapOrder, err := swapC.MarketOrder(ctx, side, amt, decimal.Zero, nil)

		if err != nil {
			errType := swapC.HandError(err)
			li.UpdateBannedTime(errType, false)
			level.Error(logger).Log("message", fmt.Sprintf("create %s market %s order failed", swapName, side), "error", err.Error())
			msg := fmt.Sprintf("创建合约%s %s单失败 数量: %s,错误原因: %s\n", swapName, side, amt, errType)
			go message.SendP3Important(context.Background(), message.NewOperateFail(fmt.Sprintf("%s 处理合约%s敞口失败", li.swap1.String(), swapName), msg))
			return
		}
		level.Info(logger).Log("message", fmt.Sprintf("create %s market %s order", swapName, side), "id", swapOrder.ID.String())

		if side == ccexgo.OrderSideCloseShort || side == ccexgo.OrderSideCloseLong {
			if swapName == "swap1" {
				li.TotalSwap1Exposure = li.TotalSwap1Exposure.Sub(swapOrder.Filled)
			} else {
				li.TotalSwap2Exposure = li.TotalSwap2Exposure.Sub(swapOrder.Filled)
			}
		} else {
			if swapName == "swap1" {
				li.TotalSwap1Exposure = li.TotalSwap1Exposure.Add(swapOrder.Filled)
			} else {
				li.TotalSwap2Exposure = li.TotalSwap2Exposure.Add(swapOrder.Filled)
			}
		}

		go message.Send(context.Background(), message.NewExposureOperate(li.swap1.String()+"合约"+swapName, currency, side.String(), swapOrder.Filled, swapOrder.AvgPrice, swapOrder.Fee, swapOrder.FeeCurrency))
	}

	// minOrderAmount1 := li.swap1ActsSymbol.MinimumOrderAmount
	// minOrderAmount2 := li.swap2ActsSymbol.MinimumOrderAmount
	// if swapC1.GetExchangeName() == exchange.Okex5 || swapC1.GetExchangeName() == exchange.GateIO {
	// 	minOrderAmount1 = li.swap1ActsSymbol.MinimumOrderAmount * li.swap1ActsSymbol.ContractValue
	// }
	// if swapC2.GetExchangeName() == exchange.Okex5 || swapC2.GetExchangeName() == exchange.GateIO {
	// 	minOrderAmount2 = li.swap2ActsSymbol.MinimumOrderAmount * li.swap2ActsSymbol.ContractValue
	// }

	processExposure(swapC1, side1, amt1, swap1, "swap1", li.minOrderAmount1)
	processExposure(swapC2, side2, amt2, swap2, "swap2", li.minOrderAmount2)

	return false, nil
}

// func (li *listImpl) ProcessExposure(ctx context.Context, spotC exchange.Client) error {
// id := fmt.Sprintf("%d_%d", time.Now().Unix(), li.PosID)
// logger := log.With(li.logger, "id", id, "component", "processExposure")

// var (
// 	side      ccexgo.OrderSide
// 	currency  string
// 	spotOrder *exchange.CfOrder
// 	err       error
// )
// amt := li.TotalExposure

// if li.isversed {
// 	side = ccexgo.OrderSideSell
// 	currency = li.spot.Base()
// 	spotOrder, err = spotC.MarketOrder(ctx, side, amt, decimal.Zero)

// } else {
// 	side = ccexgo.OrderSideBuy
// 	currency = li.spot.Quote()
// 	spotOrder, err = spotC.MarketOrder(ctx, side, decimal.Zero, amt)

// }
// if err != nil {
// 	errType := spotC.HandError(err)
// 	li.UpdateBannedTime(errType, false)
// 	level.Error(logger).Log("message", fmt.Sprintf("create spot market %s order failed", side), err.Error())
// 	msg := fmt.Sprintf("创建现货%s单失败 数量: %s %s,错误原因: %s\n", side, amt, currency, errType)
// 	message.SendImportant(ctx, message.NewOperateFail(fmt.Sprintf("%s 处理敞口失败", li.swap.String()), msg))
// 	return err
// }
// level.Info(logger).Log("message", fmt.Sprintf("create spot market %s order", side), "id", spotOrder.ID.String())
// if li.isversed {
// 	li.TotalExposure = li.TotalExposure.Sub(spotOrder.Filled)
// } else {
// 	li.TotalExposure = li.TotalExposure.Sub(spotOrder.Filled.Mul(spotOrder.AvgPrice))
// }
// message.Send(ctx, message.NewExposureOperate(li.swap.String(), currency, side.String(), spotOrder.Filled, spotOrder.AvgPrice, spotOrder.Fee))
// return nil
// }

// ProcessExExposure 处理异常敞口
// func (li *listImpl) ProcessExExposure(ctx context.Context, spotC exchange.Client, swapC exchange.SwapClient, processSpot bool) error {
// id := fmt.Sprintf("%d_%d", time.Now().Unix(), li.PosID)
// logger := log.With(li.logger, "id", id, "component", "processExExposure")
// level.Info(logger).Log("message", "start process exception exposure")
// time.Sleep(3 * time.Minute) // 休眠三分钟
// var (
// 	side      ccexgo.OrderSide
// 	currency  string
// 	spotOrder *exchange.CfOrder
// 	err       error
// 	warnStr   string
// )
// tf := li.AutoExceptionInfo.SwapExposure
// amt := li.AutoExceptionInfo.SpotExposure

// if li.isversed {
// 	side = ccexgo.OrderSideSell
// 	currency = li.spot.Base()
// } else {
// 	side = ccexgo.OrderSideBuy
// 	currency = li.spot.Quote()
// }

// defer func() {
// 	var sendImportant bool
// 	if strings.Contains(warnStr, "失败") {
// 		sendImportant = true
// 	}
// 	warnStr += fmt.Sprintf("\n现货原始处理量:%s %s\n", amt, currency)
// 	warnStr += fmt.Sprintf("合约原始处理量:%s %s\n", tf, currency)
// 	warnStr += "处理的异常敞口日志\n\n"
// 	for _, log := range li.AutoExceptionInfo.Logs {
// 		warnStr += fmt.Sprintf("%s\n", log)
// 	}
// 	li.AutoExceptionInfo.SwapExposure = decimal.Zero
// 	li.AutoExceptionInfo.Logs = make([]string, 0)
// 	if sendImportant {
// 		if err := message.SendImportant(ctx, message.NewCommonMsg(fmt.Sprintf("%s 自动处理失败", li.swap.String()), warnStr)); err != nil {
// 			level.Warn(logger).Log("message", "send lark failed", "err", err.Error())
// 		}
// 	} else {
// 		message.Send(ctx, message.NewCommonMsg(fmt.Sprintf("%s 自动处理异常敞口告警", li.swap.String()), warnStr))
// 	}
// }()

// if tf.IsPositive() { // 需要转账到现货
// 	if err = li.transfer.Transfer(ctx, exchange.AccontTypeSwap, exchange.AccontTypeSpot, currency, tf); err != nil {
// 		errType := swapC.HandError(err)
// 		li.UpdateBannedTime(errType, false)
// 		warnStr += fmt.Sprintf("转账到现货失败,错误原因: %s\n", errType)
// 		return nil
// 	} else {
// 		li.AutoExceptionInfo.SwapExposure = decimal.Zero
// 		warnStr += fmt.Sprintf("转账到现货成功,数量:%s %s\n", tf, currency)
// 	}
// } else if tf.IsNegative() {
// 	if err = li.transfer.Transfer(ctx, exchange.AccontTypeSpot, exchange.AccontTypeSwap, currency, tf.Neg()); err != nil {
// 		errType := swapC.HandError(err)
// 		li.UpdateBannedTime(errType, false)
// 		warnStr += fmt.Sprintf("转账到合约失败,错误原因: %s\n", errType)
// 		return nil
// 	} else {
// 		warnStr += fmt.Sprintf("转账到合约成功,数量:%s %s\n", tf, currency)
// 	}
// }

// if processSpot {
// 	if li.isversed {
// 		spotOrder, err = spotC.MarketOrder(ctx, side, amt, decimal.Zero)
// 	} else {
// 		spotOrder, err = spotC.MarketOrder(ctx, side, decimal.Zero, amt)
// 	}

// 	if err != nil {
// 		errType := spotC.HandError(err)
// 		li.UpdateBannedTime(errType, false)
// 		level.Error(logger).Log("message", fmt.Sprintf("create spot market %s order failed", side), err.Error())
// 		// if errType.Code == exchange.NewOrderRejected || errType.Code == exchange.AccountInsufficientBalance {
// 		warnStr += fmt.Sprintf("创建现货%s单失败 数量: %s %s,错误原因:%s\n", side, amt, currency, errType)
// 		// } else {
// 		// warnStr += fmt.Sprintf("创建现货%s单失败 数量: %s %s,错误原因:%s\n", side, amt, currency, errType)
// 		// }
// 		li.AutoExceptionInfo.SpotExposure = decimal.Zero
// 		return err
// 	}
// 	warnStr += fmt.Sprintf("现货%s单成交 数量: %s %s,价值: %s %s\n", side, spotOrder.Filled, currency, spotOrder.Filled.Mul(spotOrder.AvgPrice), li.spot.Quote())
// 	level.Info(logger).Log("message", fmt.Sprintf("create spot market %s order", side), "id", spotOrder.ID.String())
// 	if li.isversed {
// 		li.AutoExceptionInfo.SpotExposure = li.AutoExceptionInfo.SpotExposure.Sub(spotOrder.Filled)
// 	} else {
// 		li.AutoExceptionInfo.SpotExposure = li.AutoExceptionInfo.SpotExposure.Sub(spotOrder.Filled.Mul(spotOrder.AvgPrice))
// 	}
// }

// return nil
// }

func (li *listImpl) AddExposure(ctx context.Context, exposure decimal.Decimal) {
	li.mu.Lock()
	defer li.mu.Unlock()
	li.TotalExposure = li.TotalExposure.Add(exposure)
}

func (li *listImpl) updateAccountInfo(ctx context.Context, spotC exchange.Client, swapC1, swapC2 exchange.SwapClient, logger log.Logger) error {
	return li.updateAccountInfo2(ctx, spotC, swapC1, swapC2, logger, false)
}

func (li *listImpl) updateAccountInfo2(ctx context.Context, spotC exchange.Client, swapC1 exchange.SwapClient, swapC2 exchange.SwapClient, logger log.Logger, forceRest bool) error {
	var (
		pos, pos2 *exchange.Position
		err       error
	)
	if forceRest {
		pos, err = swapC1.GetPosition(ctx)
	} else {
		pos, err = swapC1.Position(ctx)
	}
	if err != nil {
		return errors.WithMessage(err, "get position1 fail")
	}

	// balance, err := spotC.FetchBalance(ctx)
	// if err != nil {
	// 	return errors.WithMessage(err, "fetch balance fail")
	// }
	// var (
	// 	baseBalance  decimal.Decimal
	// 	quoteBalance decimal.Decimal
	// )
	// for _, b := range balance {
	// 	if b.Currency == strings.ToUpper(li.spot.Base()) {
	// 		baseBalance = b.Total
	// 	} else if b.Currency == strings.ToUpper(li.spot.Quote()) {
	// 		quoteBalance = b.Total
	// 	}
	// }
	// if li.isversed {
	// 	li.SpotAmount = baseBalance
	// } else {
	// 	li.SpotAmount = quoteBalance
	// }

	li.mu.Lock()
	li.Swap1LongPos.PosAvgOpenPrice = pos.Long.AvgOpenPrice
	li.Swap1LongPos.PosAmount = pos.Long.Position
	li.Swap1ShortPos.PosAvgOpenPrice = pos.Short.AvgOpenPrice
	li.Swap1ShortPos.PosAmount = pos.Short.Position
	li.mu.Unlock()

	if forceRest {
		pos2, err = swapC2.GetPosition(ctx)
	} else {
		pos2, err = swapC2.Position(ctx)
	}
	if err != nil {
		return errors.WithMessage(err, "get position2 fail")
	}

	li.mu.Lock()
	li.Swap2LongPos.PosAvgOpenPrice = pos2.Long.AvgOpenPrice
	li.Swap2LongPos.PosAmount = pos2.Long.Position
	li.Swap2ShortPos.PosAvgOpenPrice = pos2.Short.AvgOpenPrice
	li.Swap2ShortPos.PosAmount = pos2.Short.Position
	li.mu.Unlock()

	var (
		swapBalance, swapBalance2 []ccexgo.Balance
		swapBal, swapBal2         *exchange.MySyncMap[string, *apis.Balance]
	)
	if forceRest {
		swapBalance, err = swapC1.FetchBalance(ctx)
	} else {
		swapBal = swapC1.Balance()
	}
	if err != nil {
		return errors.WithMessage(err, "swap1 rest balance fail")
	}

	if forceRest {
		if len(swapBalance) != 1 {
			return errors.Errorf("invalid swap1 balance %+v", swapBalance)
		}
		li.mu.Lock()
		li.Swap1WithdrawlAvailable = swapBalance[0].Free
		li.Swap1Amount = swapBalance[0].Total
		li.mu.Unlock()
	} else {
		if swapBal.Len() == 0 {
			return errors.WithMessage(err, "swap1 ws balance fail")
		}
		li.mu.Lock()
		li.Swap1WithdrawlAvailable = swapBal.Get(li.swap1ActsSymbol.BalanceCurrency).Free
		li.Swap1Amount = swapBal.Get(li.swap1ActsSymbol.BalanceCurrency).Total
		li.mu.Unlock()
	}

	if forceRest {
		swapBalance2, err = swapC2.FetchBalance(ctx)
	} else {
		swapBal2 = swapC2.Balance()
	}
	if err != nil {
		return errors.WithMessage(err, "swap2 rest balance fail")
	}

	if forceRest {
		if len(swapBalance2) != 1 {
			return errors.Errorf("invalid swap2 balance %+v", swapBalance2)
		}
		li.mu.Lock()
		li.Swap2WithdrawlAvailable = swapBalance2[0].Free
		li.Swap2Amount = swapBalance2[0].Total
		li.mu.Unlock()
	} else {
		if swapBal2.Len() == 0 {
			return errors.WithMessage(err, "swap2 ws balance fail")
		}
		li.mu.Lock()
		li.Swap2WithdrawlAvailable = swapBal2.Get(li.swap2ActsSymbol.BalanceCurrency).Free
		li.Swap2Amount = swapBal2.Get(li.swap2ActsSymbol.BalanceCurrency).Total
		li.mu.Unlock()
	}

	level.Info(logger).Log("message", "account info", "Swap1LongPos", fmt.Sprintf(`%+v`, pos.Long), "Swap1ShortPos", fmt.Sprintf(`%+v`, pos.Short),
		"Swap2LongPos", fmt.Sprintf(`%+v`, pos2.Long), "Swap2ShortPos", fmt.Sprintf(`%+v`, pos2.Short), "Swap1WithdrawlAvailable", li.Swap1WithdrawlAvailable,
		"swap1_total_margin", li.Swap1Amount, "Swap2WithdrawlAvailable", li.Swap2WithdrawlAvailable,
		"swap2_total_margin", li.Swap2Amount)

	return nil
}

func (li *listImpl) UpdateSwap(swapPos1, swapPos2 *exchange.Position) {
	li.mu.Lock()
	defer li.mu.Unlock()

	li.Swap1LongPos.PosAvgOpenPrice = swapPos1.Long.AvgOpenPrice
	li.Swap1LongPos.PosAmount = swapPos1.Long.Position
	li.Swap1ShortPos.PosAvgOpenPrice = swapPos1.Short.AvgOpenPrice
	li.Swap1ShortPos.PosAmount = swapPos1.Short.Position

	li.Swap2LongPos.PosAvgOpenPrice = swapPos2.Long.AvgOpenPrice
	li.Swap2LongPos.PosAmount = swapPos2.Long.Position
	li.Swap2ShortPos.PosAvgOpenPrice = swapPos2.Short.AvgOpenPrice
	li.Swap2ShortPos.PosAmount = swapPos2.Short.Position
}

func (li *listImpl) UpdateAccount(ctx context.Context, spotC exchange.Client, swapC1 exchange.SwapClient, swapC2 exchange.SwapClient, cnt int64) error {
	if err := li.updateAccountInfo(ctx, spotC, swapC1, swapC2, li.logger); err != nil {
		return errors.WithMessage(err, "update account fail")
	}
	return nil
}

func (li *listImpl) UpdateFunding(ctx context.Context, swapC exchange.SwapClient, typ string) ([][2]string, error) {
	var lastFundingTime time.Time
	if typ == `swap1` {
		lastFundingTime = li.LastSwap1FundingTime
	} else {
		lastFundingTime = li.LastSwap2FundingTime
	}
	funding, err := swapC.Funding(ctx)
	if err != nil {
		return nil, errors.WithMessage(err, "update funding fail")
	}

	level.Info(li.logger).Log("message", "get funding", "type", typ, "size", len(funding))
	sort.Slice(funding, func(i, j int) bool {
		ti := funding[i].Time
		tj := funding[j].Time

		return ti.After(tj)
	})

	var (
		totalFunding decimal.Decimal
		lt           time.Time
		totalUnit    decimal.Decimal
	)
	for _, f := range funding {
		if f.Time.After(lastFundingTime) {
			if f.Time.After(lt) {
				lt = f.Time
			}
			totalFunding = totalFunding.Add(f.Amount)
			level.Info(li.logger).Log("message", "fetch funding", "type", typ, "amount", f.Amount, "time", f.Time)
		} else {
			break
		}
	}

	if totalFunding.IsZero() {
		level.Info(li.logger).Log("message", "no funding update", "type", typ, "last_funding", lastFundingTime)
		return nil, nil
	}

	for _, p := range li.Poss {
		if typ == `swap1` {
			totalUnit = totalUnit.Add(p.Unit1)
		} else {
			totalUnit = totalUnit.Add(p.Unit2)
		}
	}
	level.Info(li.logger).Log("message", "update funding", "type", typ, "total_funding", totalFunding, "total_unit", totalUnit, "lt", lt)

	var totalPosFunding decimal.Decimal // 仓位funding和

	for i, p := range li.Poss {
		var upUnit decimal.Decimal
		if typ == `swap1` {
			if !totalUnit.IsZero() {
				upUnit = totalFunding.Mul(p.Unit1).Div(totalUnit)
			}
			li.Poss[i].Funding1 = p.Funding1.Add(upUnit)
			li.Poss[i].Funding1Real = li.Poss[i].Funding1
			totalPosFunding = totalPosFunding.Add(li.Poss[i].Funding1)
		} else {
			if !totalUnit.IsZero() {
				upUnit = totalFunding.Mul(p.Unit2).Div(totalUnit)
			}
			li.Poss[i].Funding2 = p.Funding2.Add(upUnit)
			var f2Real decimal.Decimal
			if !p.Unit2.IsZero() {
				f2Real = upUnit.Div(p.Unit2).Mul(p.Unit2Real)
			}
			li.Poss[i].Funding2Real = li.Poss[i].Funding2Real.Add(f2Real)
			totalPosFunding = totalPosFunding.Add(li.Poss[i].Funding2)
		}
	}
	if typ == `swap1` {
		li.LastSwap1FundingTime = lt
		li.Funding1 = totalPosFunding
	} else {
		li.LastSwap2FundingTime = lt
		li.Funding2 = totalPosFunding

	}
	fundingRecords := make([][2]string, 0)
	fundingRecords = append(fundingRecords, [2]string{"ts", time.Now().Format("2006-01-02 15:04:05.000")})
	fundingRecords = append(fundingRecords, [2]string{"type", typ})
	fundingRecords = append(fundingRecords, [2]string{"funding", totalFunding.String()})
	fundingRecords = append(fundingRecords, [2]string{"currency", funding[0].Currency})
	return fundingRecords, nil
}

// func (li *listImpl) UpdateEstimateExposure() {
// li.EstimateExposure = li.SwapCcy.Sub(li.SpotKeep.Mul(decimal.NewFromFloat(5))).Sub(li.SwapOpenFees)
// level.Info(li.logger).Log("message", "update estimate exposure", "estimate_exposure", li.EstimateExposure)
// }

func (li *listImpl) UpdateLocalTotalPos() (decimal.Decimal, decimal.Decimal) {
	li.mu.Lock()
	defer li.mu.Unlock()

	return li.updateLocalTotalPos()
}

// 内部方法，不加锁
func (li *listImpl) updateLocalTotalPos() (decimal.Decimal, decimal.Decimal) {
	var (
		localPos1 decimal.Decimal
		localPos2 decimal.Decimal
		// swap1OpenFees decimal.Decimal
	)
	var side string
	for _, p := range li.Poss {
		localPos1 = localPos1.Add(p.Unit1)
		localPos2 = localPos2.Add(p.Unit2)
		side = p.Direction

		// swap1OpenFees = swapOpenFees.Add(p.Fee1)
	}
	// if !li.isversed { // U本位
	// 	localPos = localPos.Mul(li.swap.ContractVal())
	// }
	li.LocalPos1Amount = localPos1
	li.LocalPos2Amount = localPos2
	if side == `buy` {
		localPos1 = localPos1.Neg()
	} else {
		localPos2 = localPos2.Neg()
	}
	// li.SwapOpenFees = swapOpenFees
	return localPos1, localPos2
}

func (li *listImpl) OverMinOrderValue(closeTradeVolume, midPrice, closeTickNum float64) bool {
	// exp := actsSpot.CalcExponent(li.spot.AmountPrecision())
	// priceExp := actsSpot.CalcExponent(li.spot.PricePrecision())

	// price := decimal.NewFromFloat(midPrice).Add(li.spot.PricePrecision().Mul(decimal.NewFromFloat(closeTickNum))).Round(int32(priceExp)).InexactFloat64()

	// amt := decimal.NewFromFloat(math.Min(closeTradeVolume/price, li.SpotKeep.Truncate(int32(exp)).InexactFloat64())) // 实际想卖的币

	// placeCurrency := amt.Round(int32(exp)) // 约到最小下单精度
	// usdt := placeCurrency.Mul(decimal.NewFromFloat(price))

	// if usdt.LessThan(decimal.NewFromFloat(10)) || placeCurrency.LessThan(li.spot.AmountMin()) { //小于10U
	// 	return false
	// }
	return true
}

func (li *listImpl) DealSide(open bool) ccexgo.OrderSide {
	li.mu.RLock()
	defer li.mu.RUnlock()

	if open {
		if li.Swap1ShortPos.PosAmount.IsZero() {
			return ccexgo.OrderSideBuy
		} else {
			if li.Swap2LongPos.PosAmount.IsZero() {
				return ccexgo.OrderSideBuy
			}
			return ccexgo.OrderSideCloseShort
		}
	}
	if li.Swap1LongPos.PosAmount.IsZero() {
		return ccexgo.OrderSideSell
	} else {
		if li.Swap2ShortPos.PosAmount.IsZero() { // 走到开仓里就自动清理仓位了
			return ccexgo.OrderSideSell
		}
		return ccexgo.OrderSideCloseLong
	}
}

func errToProcessType(errType *exchange.ErrorType) AutoExceptionProcessType {
	switch errType.Code {
	case exchange.OtherReason:
		return ManualProcess
	default:
		return SystemProcess
	}
}

func (li *listImpl) UpdateBannedTime(errType *exchange.ErrorType, bannedNow bool) {
	li.UpdateBannedTime2(errType, bannedNow, ``, 1)
}

func (li *listImpl) UpdateBannedTime2(errType *exchange.ErrorType, bannedNow bool, reason string, minutes int) {
	banned := true
	defer func() {
		if banned {
			message.Send(context.Background(), message.NewCommonMsg(fmt.Sprintf("%s 程序触发风控暂停", li.swap2.String()), fmt.Sprintf("程序暂停开平仓,直到%d(%s)恢复, 触发原因:\n%s", li.BannedUntil, time.UnixMilli(li.BannedUntil).Format("2006-01-02 15:04:05.000"), reason)))
		}
	}()
	if bannedNow {
		li.BannedUntil = time.Now().Add(time.Minute*time.Duration(minutes)).UnixMilli() + (rand.Int63n(30)+1)*1000
		return
	}
	reason = errType.String()

	switch errType.Code {
	case exchange.IpBannedUntil:
		compileRegex := regexp.MustCompile(`\d{13}`)
		matchArr := compileRegex.FindStringSubmatch(errType.Message)
		if len(matchArr) == 0 {
			li.BannedUntil = time.Now().Add(time.Minute*2).UnixMilli() + (rand.Int63n(30)+1)*1000
		} else {
			li.BannedUntil = helper.MustInt64(matchArr[0]) + (rand.Int63n(30)+1)*1000
		}

	case exchange.IpLimitPerMinute, exchange.WeightLimitPerMinute:
		li.BannedUntil = time.Now().Add(time.Minute).UnixMilli() + (rand.Int63n(30)+1)*1000
	case exchange.ExceededMaxPosition, exchange.ExceededTheMaximum:
		// 如果是因为仓位超过最大限制，则暂停10分钟, 等待人工干预, 避免另一边仓位持续快速上涨
		li.BannedUntil = time.Now().Add(time.Minute*10).UnixMilli() + (rand.Int63n(30)+1)*1000
	case exchange.Forbidden:
		li.BannedUntil = time.Now().Add(time.Minute*3).UnixMilli() + (rand.Int63n(30)+1)*1000
	case exchange.NewOrderLimit:
		li.BannedUntil = time.Now().Add(time.Second*30).UnixMilli() + (rand.Int63n(30)+1)*1000
	default:
		banned = false
	}
}

func (li *listImpl) CheckErrShareBan(errType *exchange.ErrorType, exchangeName string) {
	if exchangeName != exchange.Okex5 && !strings.Contains(exchangeName, exchange.BinancePortfolio) {
		return
	}

	var (
		until      int64
		banType    shareBanType
		expireTime time.Duration
	)

	switch exchangeName {
	case exchange.Okex5:
		li.handleOkex5Error(errType, &until, &banType, &expireTime)
	case exchange.BinancePortfolio, exchange.BinanceMargin:
		li.handleBinancePortfolioError(errType, &until, &banType, &expireTime)
	}

	if banType == 0 {
		return
	}

	li.setShareBan(banType, until, expireTime)
	li.notifyShareBan(banType, expireTime)
}

func (li *listImpl) handleOkex5Error(errType *exchange.ErrorType, until *int64, banType *shareBanType, expireTime *time.Duration) {
	switch {
	case strings.Contains(errType.Message, "Insufficient USDT available in loan pool to borrow"):
		*until = time.Now().Add(15 * time.Minute).UnixMilli()
		*banType = assetBan
		*expireTime = time.Minute * 15
	case strings.Contains(errType.Message, "context deadline exceeded"):
		*until = time.Now().Add(5 * time.Minute).UnixMilli()
		*banType = netWorkBan
		*expireTime = time.Minute * 5
	case strings.Contains(errType.Message, "adjusted equity in USD is less than IMR"), strings.Contains(errType.Message, "available margin"):
		*banType = riskBan
		*expireTime = time.Minute * 10
	}
}

func (li *listImpl) handleBinancePortfolioError(errType *exchange.ErrorType, until *int64, banType *shareBanType, expireTime *time.Duration) {
	if errType.Code == exchange.WeightLimitPerMinute || errType.Code == exchange.IpLimitPerMinute {
		*until = time.Now().Add(5 * time.Minute).UnixMilli()
		*banType = limitBan
		*expireTime = time.Minute * 5
	} else if errType.Code == exchange.IpBannedUntil {
		li.handleIpBannedUntil(errType, until, banType, expireTime)
	} else {
		li.handleBinancePortfolioKeywordError(errType, until, banType, expireTime)
	}
}

func (li *listImpl) handleIpBannedUntil(errType *exchange.ErrorType, until *int64, banType *shareBanType, expireTime *time.Duration) {
	endTimePart := strings.Split(errType.Message, "until ")
	if len(endTimePart) > 1 {
		endTime := strings.Split(endTimePart[1], `.`)[0]
		untilTime := time.UnixMilli(helper.MustInt64(endTime)).Add(1 * time.Minute)
		*until = untilTime.UnixMilli()
		*banType = limitBan
		*expireTime = time.Until(untilTime)
	} else {
		*until = time.Now().Add(5 * time.Minute).UnixMilli()
		*banType = limitBan
		*expireTime = time.Minute * 5
	}
}

func (li *listImpl) handleBinancePortfolioKeywordError(errType *exchange.ErrorType, until *int64, banType *shareBanType, expireTime *time.Duration) {
	switch {
	case strings.Contains(errType.Message, `current limit is`), strings.Contains(errType.Message, `current limit of IP`), strings.Contains(errType.Message, `2400 requests per minute`):
		*until = time.Now().Add(5 * time.Minute).UnixMilli()
		*banType = limitBan
		*expireTime = time.Minute * 5
	case strings.Contains(errType.Message, `banned until`):
		li.handleIpBannedUntil(errType, until, banType, expireTime)
	case strings.Contains(errType.Message, `Unknown error`):
		*until = time.Now().Add(5 * time.Minute).UnixMilli()
		*banType = netWorkBan
		*expireTime = time.Minute * 5
	}
}

func (li *listImpl) setShareBan(banType shareBanType, until int64, expireTime time.Duration) {
	rds := redis_service.GetGoRedis(0)
	js := common.NewJsonObject()
	switch banType {
	case netWorkBan:
		js["network_ban"] = until
	case assetBan:
		js["asset_ban"] = until
	case limitBan:
		js["limit_ban"] = until
	case riskBan:
		js["risk_ban"] = true
	}
	li.shareBan.mu.Lock()
	defer li.shareBan.mu.Unlock()
	li.shareBan.typ = banType
	li.shareBan.until = until
	b, _ := json.Marshal(js)
	if cmdErr := rds.SetNX(fmt.Sprintf(`cf:%s:share_ban`, li.envName), string(b), expireTime); cmdErr.Err() != nil {
		level.Warn(li.logger).Log("message", "setShareBan set redis failed", "type", li.shareBan.typ, "until", time.UnixMilli(li.shareBan.until), "err", cmdErr.Err())
	}
}

func (li *listImpl) notifyShareBan(banType shareBanType, expireTime time.Duration) {
	msg := message.NewCommonMsg("当前环境暂停下单", fmt.Sprintf("当前环境交易对部分交易行为停止,%f分钟后自动解除", expireTime.Minutes()))
	if banType == riskBan {
		msg = message.NewCommonMsgWithImport("当前环境暂停下单", "暂停所有币对开仓,账户资产小于占用保证金，需要调整杠杆")
	}
	go message.SendImportant(context.Background(), msg)
}

func (li *listImpl) CanOrder(swap1Side, swap2Side apis.OrderSide) bool {
	checkBan := func(typ shareBanType, until int64) bool { // 屏蔽下单规则 https://digifinex.larksuite.com/docx/FkhpdrQovoY0xmxk2dzukeo7sdf
		if until > time.Now().UnixMilli() && typ != 0 {
			if typ == assetBan && swap1Side == apis.OrderSideBuy {
				return true
			}
			if typ == netWorkBan || typ == limitBan {
				return true
			}
		}
		if typ == riskBan && /* (swap1Side == apis.OrderSideBuy ||*/ !swap2Side.IsClose() {
			return true
		}
		return false
	}

	ban := checkBan(li.shareBan.typ, li.shareBan.until)
	if ban {
		level.Warn(li.logger).Log("message", "canOrder check ban", "type", li.shareBan.typ, "until", time.UnixMilli(li.shareBan.until))
		return false
	}

	li.resetShareBan()

	rds := redis_service.GetGoRedis(0)
	result := rds.Get(fmt.Sprintf(`cf:%s:share_ban`, li.envName))
	if result.Err() != nil {
		level.Warn(li.logger).Log("message", "canOrder get result failed", "err", result.Err())
		return true
	}

	res, err := result.Result()
	if err != nil {
		level.Warn(li.logger).Log("message", "canOrder get result string failed", "err", err.Error())
		return true
	}

	li.updateShareBanFromRedis(res)

	if li.shareBan.typ != 0 {
		ban = checkBan(li.shareBan.typ, li.shareBan.until)
		if ban {
			level.Warn(li.logger).Log("message", "canOrder get ban", "type", li.shareBan.typ, "until", time.UnixMilli(li.shareBan.until))
			return false
		}
	}
	return true
}

func (li *listImpl) resetShareBan() {
	li.shareBan.mu.Lock()
	defer li.shareBan.mu.Unlock()
	li.shareBan.until = 0
	li.shareBan.typ = 0
}

func (li *listImpl) updateShareBanFromRedis(res string) {
	js := gjson.Parse(res)
	li.shareBan.mu.Lock()
	defer li.shareBan.mu.Unlock()

	netWorkTime := js.Get("network_ban").Int()
	assetTime := js.Get("asset_ban").Int()
	limitTime := js.Get("limit_ban").Int()
	riskTime := js.Get("risk_ban").Bool()
	if assetTime > 0 {
		li.shareBan.until = assetTime
		li.shareBan.typ = assetBan
	}
	if netWorkTime > 0 {
		li.shareBan.until = netWorkTime
		li.shareBan.typ = netWorkBan
	}
	if limitTime > 0 && limitTime > netWorkTime {
		li.shareBan.until = limitTime
		li.shareBan.typ = limitBan
	}
	if riskTime {
		li.shareBan.until = 0
		li.shareBan.typ = riskBan
	}
}

// ProcessADL 处理adl
func (li *listImpl) ProcessADL(ctx context.Context, swap1Amount, swap2Amount, price decimal.Decimal, side ccexgo.OrderSide) (swap1Order, swap2Order *exchange.CfOrder, err2 error) {
	var side2 ccexgo.OrderSide
	if side == ccexgo.OrderSideBuy {
		side2 = ccexgo.OrderSideCloseShort
	} else if side == ccexgo.OrderSideSell {
		side2 = ccexgo.OrderSideCloseLong
	} else if side == ccexgo.OrderSideCloseShort {
		side2 = ccexgo.OrderSideBuy
	} else if side == ccexgo.OrderSideCloseLong {
		side2 = ccexgo.OrderSideSell
	}

	swap1Order, err := li.swap1C.MarketOrder(ctx, side, swap1Amount, decimal.Zero, nil)
	if err != nil {
		err2 = fmt.Errorf("swap1下单失败 %s", err)
	}
	if swap2Amount.IsZero() {
		swap2Order = &exchange.CfOrder{}
		return
	}

	swap2Order, err = li.swap2C.MarketOrder(ctx, side2, swap2Amount, decimal.Zero, nil)
	if err != nil {
		if err2 != nil {
			err2 = fmt.Errorf("%s\n swap2下单失败 %s", err2, err)
		} else {
			err2 = fmt.Errorf("swap2下单失败 %s", err)
		}
	}
	return
}

func (li *listImpl) ClearPos(ctx context.Context, swapC1 exchange.SwapClient, swapC2 exchange.SwapClient, side1 ccexgo.OrderSide, clearNoOrder bool) (err error) {
	var (
		closeSide  ccexgo.OrderSide
		amt        decimal.Decimal
		swapClient exchange.SwapClient
		msg        string
	)
	if clearNoOrder {
		goto clearData
	}
	if side1 == ccexgo.OrderSideBuy {
		if !li.Swap1ShortPos.PosAmount.IsZero() {
			closeSide = ccexgo.OrderSideCloseShort
			amt = li.Swap1ShortPos.PosAmount
			swapClient = swapC1
		} else if !li.Swap2LongPos.PosAmount.IsZero() {
			closeSide = ccexgo.OrderSideCloseLong
			amt = li.Swap2LongPos.PosAmount
			swapClient = swapC2
		}
	} else if side1 == ccexgo.OrderSideSell {
		if !li.Swap1LongPos.PosAmount.IsZero() {
			closeSide = ccexgo.OrderSideCloseLong
			amt = li.Swap1LongPos.PosAmount
			swapClient = swapC1
		} else if !li.Swap2ShortPos.PosAmount.IsZero() {
			closeSide = ccexgo.OrderSideCloseShort
			amt = li.Swap2ShortPos.PosAmount
			swapClient = swapC2
		}
	}
	_, err = swapClient.MarketOrder(ctx, closeSide, amt, decimal.Zero, nil)
	if err != nil {
		errType := swapClient.HandError(err)
		li.UpdateBannedTime(errType, false)
		return fmt.Errorf("%s %s %s清理仓位失败 %s", swapClient.GetExchangeName(), closeSide, amt.String(), errType.String())
	}
clearData:
	if clearNoOrder {
		msg = "清理仓位成功, 所有方向仓位均为0不需要下单只清理本地数据, 仓位信息已清空"
	} else {
		msg = fmt.Sprintf("清理仓位成功, 清理的是%s %s方向的仓位, 数量是%s, 仓位信息已清空", swapClient.GetExchangeName(), closeSide, amt)
	}
	go message.Send(ctx, message.NewCommonMsg(fmt.Sprintf("%s 自动清理仓位成功", li.swap2.String()), msg))
	li.Poss = make([]Pos, 0)
	li.Funding1 = decimal.Zero
	li.Funding2 = decimal.Zero
	li.SwapCcy = decimal.Zero
	li.LocalPos1Amount = decimal.Zero
	li.LocalPos2Amount = decimal.Zero
	li.TotalExposure = decimal.Zero
	li.TotalSwap1Exposure = decimal.Zero
	li.TotalSwap2Exposure = decimal.Zero
	if !clearNoOrder {
		level.Info(li.logger).Log("message", "clear pos", "swap_client", swapClient.GetExchangeName(), "close_side", closeSide, "amt", amt)
	}
	li.initializeFrozenAmounts()
	return nil
}

// initializeFrozenAmounts 初始化冻结数量（程序启动时调用）
func (li *listImpl) initializeFrozenAmounts() {
	li.mu.Lock()
	defer li.mu.Unlock()

	// 重置冻结数量
	li.frozenCloseAmount1 = decimal.Zero
	li.frozenCloseAmount2 = decimal.Zero

	level.Info(li.logger).Log("message", "initialized frozen amounts",
		"swap1_total", li.LocalPos1Amount.String(),
		"swap2_total", li.LocalPos2Amount.String())
}

// tryFreezeCloseAmount 尝试冻结平仓数量，防止超平, frozen为true时，会冻结数量，否则只判断是否超平
func (li *listImpl) TryFreezeCloseAmount(requestAmount decimal.Decimal, frozen bool) (decimal.Decimal, bool) {
	li.mu.Lock()
	defer li.mu.Unlock()

	localPosAmount1 := li.LocalPos1Amount
	frozenAmount1 := li.frozenCloseAmount1
	localPosAmount2 := li.LocalPos2Amount
	frozenAmount2 := li.frozenCloseAmount2

	// if swapName == "swap1" {
	// localPosAmount = li.LocalPos1Amount
	// frozenAmount = li.frozenCloseAmount1
	// } else if swapName == "swap2" {
	// localPosAmount = li.LocalPos2Amount
	// frozenAmount = li.frozenCloseAmount2
	// } else {
	// 	return decimal.Zero, false
	// }

	// 计算实际可用数量（总仓位 - 冻结数量）
	actualAvailable1 := localPosAmount1.Sub(frozenAmount1)
	actualAvailable2 := localPosAmount2.Sub(frozenAmount2)

	// 如果没有可用数量，直接返回
	actualAvailable := decimal.Min(actualAvailable1, actualAvailable2)
	if actualAvailable.LessThanOrEqual(decimal.Zero) {
		return actualAvailable, false
	}

	// 计算实际可冻结的数量（取请求数量和可用数量的较小值）
	actualFreezeAmount := requestAmount
	if actualAvailable.LessThan(requestAmount) {
		actualFreezeAmount = actualAvailable
	}

	// 判断实际冻结的量能否达到最小下单数量
	if actualFreezeAmount.LessThan(decimal.Min(li.minOrderAmount1, li.minOrderAmount2)) {
		return decimal.Zero, false
	}

	if !frozen {
		return actualFreezeAmount, true
	}

	// 冻结数量
	// if swapName == "swap1" {
	li.frozenCloseAmount1 = li.frozenCloseAmount1.Add(actualFreezeAmount)
	// } else if swapName == "swap2" {
	li.frozenCloseAmount2 = li.frozenCloseAmount2.Add(actualFreezeAmount)
	// }
	level.Info(li.logger).Log("message", "try freeze close amount" /*, "swap_name", swapName*/, "actual_freeze", actualFreezeAmount, "request_amount", requestAmount,
		"in_frozen_amount1", frozenAmount1, "in_frozen_amount2", frozenAmount2, "local_pos_amount1", localPosAmount1, "local_pos_amount2", localPosAmount2)

	return actualFreezeAmount, true
}

// UnfreezeCloseAmount 解冻平仓数量（下单失败或成交时调用）
func (li *listImpl) UnfreezeCloseAmount(amount decimal.Decimal) {
	li.mu.Lock()
	defer li.mu.Unlock()

	// if swapName == "swap1" {
	li.frozenCloseAmount1 = li.frozenCloseAmount1.Sub(amount)
	if li.frozenCloseAmount1.IsNegative() {
		li.frozenCloseAmount1 = decimal.Zero
	}
	// } else if swapName == "swap2" {
	li.frozenCloseAmount2 = li.frozenCloseAmount2.Sub(amount)
	if li.frozenCloseAmount2.IsNegative() {
		li.frozenCloseAmount2 = decimal.Zero
	}
	// }
	level.Info(li.logger).Log("message", "unfreeze close amount" /*, "swap_name", swapName*/, "amount", amount, "frozen_amount1", li.frozenCloseAmount1, "frozen_amount2", li.frozenCloseAmount2, "local_pos1", li.LocalPos1Amount, "local_pos2", li.LocalPos2Amount)
}
