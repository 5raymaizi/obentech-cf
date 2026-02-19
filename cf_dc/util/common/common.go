package common

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"cf_arbitrage/exchange"
	"cf_arbitrage/message"

	gocommon "go.common"
	"go.common/helper"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/shopspring/decimal"
)

const (
	AesKey = "zR0LK[lN5Hrc@7A]"
)

// 挂单成功后开始计数，
var Counter = &OpportunityCounter{}

// 整合多线程的数据
type WorkData struct {
	Data []any
	Err  error
	mutx sync.Mutex
}

type OpportunityCounter struct {
	mu      sync.Mutex
	count   int
	isStart bool
}

func (w *WorkData) AddData(err error, data any) {
	w.mutx.Lock()
	defer w.mutx.Unlock()
	if data != nil {
		w.Data = append(w.Data, data)
	}
	if err != nil {
		if w.Err != nil {
			w.Err = fmt.Errorf(w.Err.Error() + "\n" + err.Error())
		} else {
			w.Err = err
		}
	}
}

// 生成订单的相关记录--为了可读性导致耦合过于严重待优化
func CreatOrderDoneField(ctx context.Context, logger log.Logger, bestPrice float64, pricePrecision decimal.Decimal, order *exchange.CfOrder, multiDonePrice *[]string, multiDoneAmount *[]string, multiDistance *[]string, multiDoneTime *[]string) {
	if order.Filled.IsZero() {
		return
	}
	*multiDonePrice = append(*multiDonePrice, order.AvgPrice.String())
	*multiDoneAmount = append(*multiDoneAmount, order.Filled.String())
	bestPriceDelta := decimal.NewFromFloat(bestPrice).Sub(order.AvgPrice).Abs().Div(pricePrecision).String()
	*multiDistance = append(*multiDistance, bestPriceDelta)
	*multiDoneTime = append(*multiDoneTime, order.Updated.String())
	level.Info(logger).Log("message", "CreatOrderDoneField", "id", order.ID.String(), "amount", order.Amount, "filled", order.Filled, "fee", order.Fee, "avg_price", order.AvgPrice, "status", order.Status, "time", order.Updated.String(), "dis", bestPriceDelta)
}

// 开仓机会计数器
func (opc *OpportunityCounter) Increment() {
	if opc.isStart {
		opc.mu.Lock()
		defer opc.mu.Unlock()
		opc.count++
	}
}

// 启动计数器
func (opc *OpportunityCounter) Start() {
	opc.mu.Lock()
	defer opc.mu.Unlock()
	opc.isStart = true
}

// 初始化计数器
func (opc *OpportunityCounter) Inital() {
	opc.mu.Lock()
	defer opc.mu.Unlock()
	opc.count = 0
	opc.isStart = false
}

// 获取当前计数
func (opc *OpportunityCounter) GetCount() int {
	return opc.count
}

func DecryptKey(key string) (apiSecret, apiPassphrase string) {
	secret, err := helper.AesDecrypt(key, AesKey)
	if err != nil {
		panic(gocommon.NewError("decrypt apikey failed", err))
	}
	if sps := strings.Split(string(secret), ":"); len(sps) > 1 {
		apiSecret, apiPassphrase = sps[0], sps[1]
	} else {
		apiSecret, apiPassphrase = string(secret), ""
	}
	return
}

// MeasureExecutionTime 统计函数的耗时并打印函数名
func MeasureExecutionTime(name string, fn func()) (duration time.Duration) {
	start := time.Now()
	fn() // 执行传入的函数
	duration = time.Since(start)

	// level.Info(logger.GetLogger()).Log("message", "MeasureExecutionTime", "function", name, "duration", duration)
	return duration
}

// RecoverWithLog 创建一个用于恢复panic的函数，会记录错误日志并返回格式化的错误
func RecoverWithLog(logger log.Logger, funcName string, start time.Time, e any) error {
	timeDelay := time.Since(start)
	level.Error(logger).Log("message", funcName+" recover", "time_delay", timeDelay.String(), "err", e)
	return gocommon.NewErrorf(e, `【人工处理】 %s 崩溃出错, 联系开发定位, 函数运行耗时%s`, funcName, timeDelay.String())
}

func NewTimeOutTimer(ctx context.Context, symbol string, minute int, info string) *time.Timer {
	return time.AfterFunc(time.Minute*time.Duration(minute), func() {
		message.SendImportant(ctx, message.NewTimeOut(symbol, minute, info))
	})
}
