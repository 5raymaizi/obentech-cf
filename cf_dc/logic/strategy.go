package logic

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cf_arbitrage/exchange"
	"cf_arbitrage/exchange/acts"
	actsSpot "cf_arbitrage/exchange/acts/spot"
	"cf_arbitrage/logic/config"
	"cf_arbitrage/logic/position"
	"cf_arbitrage/logic/rate"
	"cf_arbitrage/message"
	"cf_arbitrage/output"
	"cf_arbitrage/util/common"
	"cf_arbitrage/util/logger"
	"cf_arbitrage/util/reader"

	"cf_factor"

	gocommon "go.common"
	"go.common/apis"
	"go.common/float"
	"go.common/helper"
	"go.common/redis_service"

	ccexgo "github.com/NadiaSama/ccexgo/exchange"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/go-redis/redis/v7"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
)

type (
	NewClient     func(ctx context.Context) (exchange.Client, error)
	NewSwapClient func(ctx context.Context) (exchange.SwapClient, error)

	Loader interface {
		Load() config.Config
	}

	Strategy struct { //TODO 使用inteface抽象tt,mt,tm方法
		ctx             context.Context // 进程上下文
		rdb             *redis.Client
		slMu            sync.Mutex // 数据操作互斥锁
		flag            int32      // 当前是否在进行开仓平仓操作
		spotS           ccexgo.SpotSymbol
		swapS1          ccexgo.SwapSymbol
		swapS2          ccexgo.SwapSymbol
		swap1ActsSymbol *apis.Symbol
		swap2ActsSymbol *apis.Symbol
		envName         string // 环境名-唯一标识
		exchange        string // 交易所
		inverse         bool   // 标记是否反向合约
		//newSpot       NewClient
		newSwap1      NewSwapClient
		newSwap2      NewSwapClient
		load          Loader
		logger        log.Logger
		out           chan *output.Data
		openWriter    csvWriter
		closeWriter   csvWriter
		fundingWriter csvWriter
		queue         rate.Queue

		minOrderAmount1 float64
		minOrderAmount2 float64

		lastProcessExTime time.Time         // 上次处理敞口时间
		FundingRate1      *apis.FundingRate `json:"funding_rate1"`
		FundingRate2      *apis.FundingRate `json:"funding_rate2"`
		// lastFailedTime   time.Time
		TotalFailedTimes int           `json:"total_failed_times"`
		PosList          position.List `json:"pos_list"`
		CurrentPos1      float64       `json:"current_pos1"`
		CurrentPos2      float64       `json:"current_pos2"`
		ContractVal1     float64       `json:"contract_val1"`
		ContractVal2     float64       `json:"contract_val2"`

		// 禁止操作时间
		forbiddenTime time.Time

		serializeTimer *time.Timer

		bookTickerEvent *gocommon.Event // 用于通知下单进程

		ADLProcessor   *ADLProcessor `json:"adl_processor"`
		swap1OrderBook *exchange.Depth
		swap2OrderBook *exchange.Depth

		swapC1 exchange.SwapClient
		swapC2 exchange.SwapClient

		// 用于等待loop内所有协程退出
		loopWg sync.WaitGroup

		// 并发控制
		// concurrencyCount   int32 // 当前运行的协程数量
		ttConcurrencyCount int32 // 当前运行的tt协程数量
		mtConcurrencyCount int32 // 当前运行的mt协程数量
		tmConcurrencyCount int32 // 当前运行的tm协程数量
		mmConcurrencyCount int32 // 当前运行的mm协程数量
		// 统一的函数操作锁（所有函数操作都需要互斥）
		operationMu sync.Mutex
	}

	csvOutput struct {
		mu          sync.Mutex
		file        *os.File
		csv         *csv.Writer
		writeHeader bool
	}

	csvWriter interface {
		Write([][2]string)
	}

	Operate struct {
		IsOpen bool
		Spot   *exchange.Depth
		Swap   *exchange.Depth
		SR     float64
		Unit   int
	}
)

type csvType int

const (
	flagNoOperate = iota
	flagOperate

	depthMaxDuration time.Duration = time.Second * 2

	openCsv csvType = iota
	closeCsv
	fundingCsv
)

const (
	spotBalChangeEvent = iota + 9997
	swapBalChangeEvent
	swapPosChangeEvent
	swapADLEvent
)

// NewStrategy
func NewStrategy(envName, exchangeName string, loader Loader, nSwap1 NewSwapClient,
	nSwap2 NewSwapClient, spotS ccexgo.SpotSymbol, swapS1 ccexgo.SwapSymbol,
	swapS2 ccexgo.SwapSymbol, swap1ActsSymbol, swap2ActsSymbol *apis.Symbol, list position.List, logger log.Logger) *Strategy {

	cfg := loader.Load()
	inverse := swap1ActsSymbol.ContractValueIsInQuoteCurrency
	queue := rate.NewQueue(cfg.Queue, cfg.MmSwap1OpenTickNum, cfg.MmSwap2OpenTickNum, cfg.MmSwap1CloseTickNum, cfg.MmSwap2CloseTickNum, swap1ActsSymbol.PriceTickSize, swap2ActsSymbol.PriceTickSize)
	return &Strategy{
		rdb:      redis_service.GetGoRedis(0),
		envName:  envName,
		exchange: exchangeName,
		load:     loader,
		queue:    queue,
		//newSpot:  nSpot,
		newSwap1:        nSwap1,
		newSwap2:        nSwap2,
		spotS:           spotS,
		swapS1:          swapS1,
		swapS2:          swapS2,
		swap1ActsSymbol: swap1ActsSymbol,
		swap2ActsSymbol: swap2ActsSymbol,
		inverse:         inverse,
		out:             make(chan *output.Data, 48),
		PosList:         list,
		logger:          logger,
		flag:            flagNoOperate,
		ContractVal1:    swapS1.ContractVal().InexactFloat64(),
		ContractVal2:    swapS2.ContractVal().InexactFloat64(),

		bookTickerEvent: gocommon.NewEvent(),
		swap1OrderBook:  &exchange.Depth{OrderBook: &ccexgo.OrderBook{Symbol: swapS1}},
		swap2OrderBook:  &exchange.Depth{OrderBook: &ccexgo.OrderBook{Symbol: swapS2}},
	}
}

func (sg *Strategy) Init() error {
	if err := sg.unserialize(); err != nil {
		level.Warn(sg.logger).Log("message", "load from file fail", "error", err.Error())
		return err
	}

	openWriter, err := sg.newCSV("open")
	if err != nil {
		level.Warn(sg.logger).Log("message", "create open csv fail", "error", err.Error())
		return err
	}
	closeWriter, err := sg.newCSV("close")
	if err != nil {
		level.Warn(sg.logger).Log("message", "create close csv fail", "error", err.Error())
		return err
	}
	fundingWriter, err := sg.newCSV("funding")
	if err != nil {
		level.Warn(sg.logger).Log("message", "create funding csv fail", "error", err.Error())
		return err
	}

	sg.openWriter = openWriter
	sg.closeWriter = closeWriter
	sg.fundingWriter = fundingWriter

	sg.PosList.InitAvgOpenSpread()

	if sg.ADLProcessor == nil {
		sg.ADLProcessor = NewADLProcessor(sg.swapS1.String(), sg.spotS.Base())
	}

	//将数据serialzie确保manager可以读取数据
	if err := sg.serialize(); err != nil {
		level.Warn(sg.logger).Log("message", "serialzie fail", "error", err.Error())
		return err
	}

	return nil
}

func (sg *Strategy) AddLoopWg() {
	sg.loopWg.Add(1)
}

func (sg *Strategy) FInit() {
	if sg.openWriter != nil {
		cw := sg.openWriter.(*csvOutput)
		cw.Close()
	}

	if sg.closeWriter != nil {
		cw := sg.closeWriter.(*csvOutput)
		cw.Close()
	}

	if sg.fundingWriter != nil {
		cw := sg.fundingWriter.(*csvOutput)
		cw.Close()
	}
}

func (sg *Strategy) Test(config config.Config, spot reader.GzDepthReader, swap reader.GzDepthReader, mode rate.RunMode) (open []Operate, close []Operate, err error) {
	var (
		spotDepth *exchange.Depth
		swapDepth *exchange.Depth
	)

	for {
		spotDepth, err = spot.Read()
		if err != nil {
			if err != io.EOF {
				level.Warn(sg.logger).Log("message", "read spot failed", "error", err.Error())
			} else {
				err = nil
			}
			return
		}
		sg.queue.PushSpot(spotDepth)

		swapDepth, err = swap.Read()
		if err != nil {
			if err != io.EOF {
				level.Warn(sg.logger).Log("message", "read swap failed", "error", err.Error())
			} else {
				err = nil
			}
			return
		}
		sg.queue.PushSwap2(swapDepth)

		if ok, rate, unit, _ := sg.canOpen(config, spotDepth, swapDepth, mode); ok {
			open = append(open, Operate{
				IsOpen: true,
				Spot:   spotDepth,
				Swap:   swapDepth,
				SR:     rate,
				Unit:   int(unit),
			})
			continue
		}

		if ok, rate, unit, _ := sg.canClose(config, spotDepth, swapDepth, mode); ok {
			close = append(close, Operate{
				IsOpen: false,
				Spot:   spotDepth,
				Swap:   swapDepth,
				SR:     rate,
				Unit:   int(unit),
			})
			continue
		}
	}
}

func (sg *Strategy) Loop(ctx context.Context, subscribeTrade bool) {
	defer sg.loopWg.Done()
	sg.ctx = ctx
	sg.recordConfig()
	var (
		cnt    int64
		err    error
		swapC1 exchange.SwapClient
		swapC2 exchange.SwapClient
	)
	for {

		// spotC, err := sg.newSpot(ctx)
		// if err != nil {
		// 	level.Warn(sg.logger).Log("message", "spot client create fail", "err", err.Error())
		// 	time.Sleep(time.Second * 5) //避免请求太频繁
		// 	continue
		// }

		if cnt == 0 {
			if sg.swapC1 == nil {
				swapC1, err = sg.newSwap1(ctx)
				if err != nil {
					level.Warn(sg.logger).Log("message", "swap1 client create fail", "err", err.Error())
					time.Sleep(time.Second * 5) //避免请求太频繁
					continue
				}

				swapC2, err = sg.newSwap2(ctx)
				if err != nil {
					level.Warn(sg.logger).Log("message", "swap2 client create fail", "err", err.Error())
					swapC1.Close()
					time.Sleep(time.Second * 5)
					continue
				}

				sg.swapC1 = swapC1
				sg.swapC2 = swapC2
				sg.PosList.SetSwap1Swap2(swapC1, swapC2)
				sg.updateMinOrderAmount(swapC1, swapC2)
			}

			// 初始化费率失败直接panic
			if err := sg.swapC1.InitFeeRate(ctx); err != nil {
				level.Warn(sg.logger).Log("message", "init swap1 fee rate fail", "err", err.Error())
				time.Sleep(time.Second * 5)
				gocommon.PanicWithTime(err)
			}

			if err := sg.swapC2.InitFeeRate(ctx); err != nil {
				level.Warn(sg.logger).Log("message", "init swap2 fee rate fail", "err", err.Error())
				time.Sleep(time.Second * 5)
				gocommon.PanicWithTime(err)

			}

			if err := swapC1.Start(ctx, subscribeTrade); err != nil { // 先启动swap，可能减少一次获取资金费率请求
				level.Warn(sg.logger).Log("message", "start swap1 websocket", "error", err.Error())
				time.Sleep(time.Second * 5)
				continue
			}

			if err := swapC2.Start(ctx, subscribeTrade); err != nil { // 先启动swap，可能减少一次获取资金费率请求
				level.Warn(sg.logger).Log("message", "start swap2 websocket", "error", err.Error())
				time.Sleep(time.Second * 5)
				continue
			}

		} else { // 只重连ws,不重新创建client
			if err := swapC1.ReconnectWs(); err != nil {
				level.Warn(sg.logger).Log("message", "swap1 client reconnect ws fail", "err", err.Error())
				time.Sleep(time.Second * 3)
				continue
			}
			if err := swapC2.ReconnectWs(); err != nil {
				level.Warn(sg.logger).Log("message", "swap2 client reconnect ws fail", "err", err.Error())
				time.Sleep(time.Second * 3)
				continue
			}
		}
		sg.loopInternal(ctx, nil, swapC1, swapC2, cnt)
		cnt++

		select {
		case <-ctx.Done():
			level.Info(sg.logger).Log("message", "strategy loop quit")
			return
		default:
			// if err := spotC.Close(); err != nil {
			// 	level.Warn(sg.logger).Log("message", "spot client quit error", "error", err.Error())
			// }
			// if err := swapC1.Close(); err != nil {
			// 	level.Warn(sg.logger).Log("message", "swap1 client quit error", "error", err.Error())
			// }
			// if err := swapC2.Close(); err != nil {
			// 	level.Warn(sg.logger).Log("message", "swap2 client quit error", "error", err.Error())
			// }
			go message.Send(context.Background(), message.NewOperateFail(fmt.Sprintf("%s 数据订阅协程异常退出", sg.swapS1), "数据订阅协程异常退出，请值班同学注意策略程序状态！"))
			time.Sleep(time.Second * 5) //避免请求太频繁
		}
	}
}

func (sg *Strategy) Wait() {
	// 等待所有并发下单协程完成
	timeout := 5 * time.Second
	var timeoutInfo []string
	var mu sync.Mutex

	// 定义需要等待的并发计数器
	concurrencyTypes := []struct {
		name    string
		counter *int32
	}{
		{"tt", &sg.ttConcurrencyCount},
		{"mt", &sg.mtConcurrencyCount},
		{"tm", &sg.tmConcurrencyCount},
		{"mm", &sg.mmConcurrencyCount},
	}

	// 并发等待所有协程
	var wg sync.WaitGroup
	for _, ct := range concurrencyTypes {
		wg.Add(1)
		go func(name string, counter *int32) {
			defer wg.Done()
			startTime := time.Now()
			for atomic.LoadInt32(counter) > 0 {
				if time.Since(startTime) > timeout {
					count := atomic.LoadInt32(counter)
					mu.Lock()
					timeoutInfo = append(timeoutInfo, fmt.Sprintf("%s concurrency timeout, count=%d", name, count))
					mu.Unlock()
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			level.Info(sg.logger).Log("message", fmt.Sprintf("%s all concurrency done", name))
		}(ct.name, ct.counter)
	}
	wg.Wait()

	// 如果有超时，聚合发送告警
	if len(timeoutInfo) > 0 {
		alertMsg := fmt.Sprintf("策略退出超时:\n%s", strings.Join(timeoutInfo, "\n"))
		level.Warn(sg.logger).Log("message", "strategy wait timeout", "info", alertMsg)
		go message.SendP3Important(context.Background(), message.NewCommonMsgWithAt("策略退出超时", alertMsg))
	} else {
		level.Info(sg.logger).Log("message", "sg all concurrency done")
	}

	sg.loopWg.Wait()
	level.Info(sg.logger).Log("message", "sg loop wg all done")

	// 所有协程退出后再关闭client
	if sg.swapC1 != nil {
		if err := sg.swapC1.Close(); err != nil {
			level.Warn(sg.logger).Log("message", "swap1 client quit error", "error", err.Error())
		}
	}

	if sg.swapC2 != nil {
		if err := sg.swapC2.Close(); err != nil {
			level.Warn(sg.logger).Log("message", "swap2 client quit error", "error", err.Error())
		}
	}

	// 确保最终序列化成功（带重试机制）
	sg.flushSerialize()

	level.Info(sg.logger).Log("message", "strategy done")
}

func (sg *Strategy) Output() chan *output.Data {
	return sg.out
}

func (sg *Strategy) openTradeUnit(cfg config.Config, swap1 *exchange.Depth, swap2 *exchange.Depth, mode rate.RunMode) float64 {
	cv1, _ := sg.swapS1.ContractVal().Float64()
	cv2, _ := sg.swapS2.ContractVal().Float64()
	var openTradeUnit float64
	if sg.inverse { // 币本位
		switch mode {
		default:
			swap2Unit := (swap2.Bids[0].Amount + swap2.Bids[1].Amount) / 2
			spotUnit := ((swap1.Asks[0].Price*swap1.Asks[0].Amount)*cv1 + (swap1.Asks[1].Price*swap1.Asks[1].Amount)*cv1) / 2

			openTradeUnit = math.Min(swap2Unit, spotUnit) * cfg.VolumeThreshold1
			return math.Floor(math.Min(openTradeUnit, cfg.VolumeOpenLimit))

		case rate.Swap1MSwap2T: // U个数
			openTradeUnit = math.Min(swap2.Bids[0].Amount*cv2*cfg.VolumeThreshold1, cfg.MTVolumeOpenLimit)
		case rate.Swap1TSwap2M: // U个数
			openTradeUnit = math.Min(swap1.Asks[0].Amount*cv1*cfg.VolumeThreshold1, cfg.TMVolumeOpenLimit)
		}
		return openTradeUnit
	}

	exp1 := actsSpot.CalcExponent(sg.swapS1.ContractVal())
	if swap1.Exchange == exchange.Okex5 || swap1.Exchange == exchange.GateIO {
		exp1 = actsSpot.CalcExponent(decimal.NewFromFloat(sg.swap1ActsSymbol.AmountTickSize * sg.swap1ActsSymbol.ContractValue))
	}
	exp2 := actsSpot.CalcExponent(sg.swapS2.ContractVal())
	if swap2.Exchange == exchange.Okex5 || swap2.Exchange == exchange.GateIO {
		exp2 = actsSpot.CalcExponent(decimal.NewFromFloat(sg.swap2ActsSymbol.AmountTickSize * sg.swap2ActsSymbol.ContractValue))
	}
	exp := min(exp1, exp2)

	// U本位
	switch mode {
	default:
		openTradeUnit = math.Floor(math.Min(swap2.Asks[0].Amount, swap1.Bids[0].Amount) * cfg.VolumeThreshold1)
		return decimal.NewFromFloat(math.Min(openTradeUnit, cfg.VolumeOpenLimit)).Round(int32(exp)).InexactFloat64()
	case rate.Swap1MSwap2T:
		openTradeUnit = math.Min(swap2.Bids[0].Amount*cfg.VolumeThreshold1, cfg.MTVolumeOpenLimit)
		if cfg.PTMode == config.PreTaker {
			openTradeUnit = math.Min(swap2.Bids[0].Amount*cfg.PreTakerVolumeThresholdOpen, cfg.PreTakerVolumeOpenLimit/swap2.Bids[0].Price)
		}
		openTradeUnit = decimal.NewFromFloat(openTradeUnit).Round(int32(exp)).InexactFloat64()
	case rate.Swap1TSwap2M:
		openTradeUnit = math.Min(swap1.Asks[0].Amount*cfg.VolumeThreshold1, cfg.TMVolumeOpenLimit)
		if cfg.PTMode == config.PreTaker {
			openTradeUnit = math.Min(swap1.Asks[0].Amount*cfg.PreTakerVolumeThresholdOpen, cfg.PreTakerVolumeOpenLimit/swap1.Asks[0].Price)
		}
		openTradeUnit = decimal.NewFromFloat(openTradeUnit).Round(int32(exp)).InexactFloat64()
	case rate.Swap1MSwap2M: // usdt
		openTradeUnit = cfg.MmOpenVolume
	}
	return openTradeUnit
}

func (sg *Strategy) closeTradeUnit(cfg config.Config, swap1 *exchange.Depth, swap2 *exchange.Depth, mode rate.RunMode) float64 {
	// if config.CloseNow {
	// 	amt, _ := sg.PosList.Position().Amount.Float64()
	// 	return math.Min(amt, config.VolumeCloseLimit)
	// }
	cv1, _ := sg.swapS1.ContractVal().Float64()
	cv2, _ := sg.swapS2.ContractVal().Float64()
	var closeTradeUnit float64
	if sg.inverse { // 币本位 输出u
		switch mode {
		default:
			swapUnit := (swap2.Asks[0].Amount + swap2.Asks[1].Amount) / 2
			spotUnit := ((swap1.Bids[0].Price*swap1.Bids[0].Amount)/cv1 + (swap1.Bids[1].Price * swap1.Bids[1].Amount)) / 2

			closeTradeUnit = math.Min(swapUnit, spotUnit) * cfg.VolumeThreshold2
			return math.Floor(math.Min(closeTradeUnit, cfg.VolumeCloseLimit))
		case rate.Swap1MSwap2T:
			return math.Min(swap2.Asks[0].Amount*cv2*cfg.VolumeThreshold2, cfg.MTVolumeCloseLimit)
		case rate.Swap1TSwap2M:
			return math.Min(swap1.Bids[0].Amount*cv1*cfg.VolumeThreshold2, cfg.TMVolumeCloseLimit)
		}
	}

	exp1 := actsSpot.CalcExponent(sg.swapS1.ContractVal())
	if swap1.Exchange == exchange.Okex5 || swap1.Exchange == exchange.GateIO {
		exp1 = actsSpot.CalcExponent(decimal.NewFromFloat(sg.swap1ActsSymbol.AmountTickSize * sg.swap1ActsSymbol.ContractValue))
	}
	exp2 := actsSpot.CalcExponent(sg.swapS2.ContractVal())
	if swap2.Exchange == exchange.Okex5 || swap2.Exchange == exchange.GateIO {
		exp2 = actsSpot.CalcExponent(decimal.NewFromFloat(sg.swap2ActsSymbol.AmountTickSize * sg.swap2ActsSymbol.ContractValue))
	}
	exp := min(exp1, exp2)
	// U本位 输出币
	switch mode {
	default:
		closeTradeUnit = math.Floor(math.Min(swap2.Bids[0].Amount, swap1.Asks[0].Amount) * cfg.VolumeThreshold2)
		return decimal.NewFromFloat(math.Min(closeTradeUnit, cfg.VolumeCloseLimit)).Round(int32(exp)).InexactFloat64()
	case rate.Swap1MSwap2T:
		closeTradeUnit = math.Min(swap2.Asks[0].Amount*cfg.VolumeThreshold2, cfg.MTVolumeCloseLimit)
		if cfg.PTMode == config.PreTaker {
			closeTradeUnit = math.Min(swap2.Asks[0].Amount*cfg.PreTakerVolumeThresholdClose, cfg.PreTakerVolumeCloseLimit/swap2.Asks[0].Price)
		}
		closeTradeUnit = decimal.NewFromFloat(closeTradeUnit).Round(int32(exp)).InexactFloat64()
	case rate.Swap1TSwap2M:
		closeTradeUnit = math.Min(swap1.Bids[0].Amount*cfg.VolumeThreshold2, cfg.TMVolumeCloseLimit)
		if cfg.PTMode == config.PreTaker {
			closeTradeUnit = math.Min(swap1.Bids[0].Amount*cfg.PreTakerVolumeThresholdClose, cfg.PreTakerVolumeCloseLimit/swap1.Bids[0].Price)
		}
		closeTradeUnit = decimal.NewFromFloat(closeTradeUnit).Round(int32(exp)).InexactFloat64()
	case rate.Swap1MSwap2M: // usdt
		closeTradeUnit = cfg.MmCloseVolume
	}
	return closeTradeUnit
}

// 返回buy方向 和本位没关系
func (sg *Strategy) canOpen(cfg config.Config, swap1 *exchange.Depth, swap2 *exchange.Depth, mode rate.RunMode) (ok bool, srOpen float64, openTradeUnit float64, side ccexgo.OrderSide) {
	ok, srOpen = sg.queue.CanOpen(mode, cfg.SrMode)
	if cfg.OpenNow {
		ok = true
	}
	// cv1, _ := sg.swapS1.ContractVal().Float64()
	// cv2, _ := sg.swapS2.ContractVal().Float64()
	openTradeUnit = sg.openTradeUnit(cfg, swap1, swap2, mode)

	if mode == rate.Swap1TSwap2T {
		// 	srOpen = swap2.Asks[0].Price/swap1.Bids[0].Price - 1

		if !(ok && openTradeUnit >= cfg.VolumeOpenMin) {
			if ok {
				level.Info(sg.logger).Log("message", "open condition check failed",
					"c4", openTradeUnit >= cfg.VolumeOpenMin, "spot_depth_id", swap1.ID,
					"swap2_depth_id", swap2.ID, "open_trade_volume", openTradeUnit,
					"volume_threshold1", cfg.VolumeThreshold1, "volume_open_min", cfg.VolumeOpenMin,
					"spot_ask_amount", swap1.Asks[0].Amount, "spot_ask_price", swap1.Asks[0].Price,
					"spot_bid_price", swap1.Bids[0].Price,
					"swap_bid_amount", swap2.Bids[0].Amount,
					"swap_ask_price", swap2.Asks[0].Price,
					"swap_bid_price", swap2.Bids[0].Price,
					"mode", mode,
				)
			}
			ok = false
		}
	} else {
		// 	if config.Mode == rate.SpotTSwapM {
		// swapMid := (swap2.Asks[0].Price + swap2.Bids[0].Price) / 2
		// srOpen = swapMid/swap1.Bids[0].Price - 1
		// 	} else {
		// spotMid := (swap1.Asks[0].Price + swap1.Bids[0].Price) / 2
		// srOpen = swap2.Bids[0].Price/swap1.Bids[0].Price - 1
		// 	}
		if mode != rate.Swap1MSwap2M {
			f_mode := cf_factor.SpotMSwapT
			volumeOpenMin := cfg.MTVolumeOpenMin
			if mode == rate.Swap1TSwap2M {
				f_mode = cf_factor.SpotTSwapM
				volumeOpenMin = cfg.TMVolumeOpenMin
			}
			var passIMB bool
			if cfg.PTMode == config.PreTaker && cfg.Factor.Enable {
				passIMB = cf_factor.CalculateIMBPass(f_mode, swap1.OrderBook, swap2.OrderBook, &cfg.Factor, false)
			} else {
				passIMB = true
			}
			if ok && openTradeUnit >= volumeOpenMin && passIMB {
				// ok = true
			} else {
				if ok {
					level.Info(sg.logger).Log("message", "open condition check failed",
						"c1", passIMB,
						"c4", openTradeUnit >= volumeOpenMin, "swap1_depth_id", swap1.ID,
						"swap2_depth_id", swap2.ID, "open_trade_volume", openTradeUnit,
						"volume_threshold1", cfg.VolumeThreshold1, "volume_open_min", volumeOpenMin,
						"spot_ask_amount", swap1.Asks[0].Amount, "spot_ask_price", swap1.Asks[0].Price,
						"spot_bid_price", swap1.Bids[0].Price,
						"swap_bid_amount", swap2.Bids[0].Amount,
						"swap_ask_price", swap2.Asks[0].Price,
						"swap_bid_price", swap2.Bids[0].Price,
						"mode", mode,
					)
				}
				ok = false
			}
		}
	}

	side = sg.PosList.DealSide(true)

	if side == ccexgo.OrderSideBuy && (sg.FundingRate1.FundingRate-sg.FundingRate2.FundingRate) > cfg.ShortFundingLimit {
		ok = false
		// level.Info(sg.logger).Log("message", "open condition check bad funding", "funding1", sg.FundingRate1.FundingRate, "funding2", sg.FundingRate2.FundingRate, "short_funding_limit", cfg.ShortFundingLimit)
	}

	if ok {
		level.Info(sg.logger).Log("message", "open condition check passed", "swap1_depth", swap1.ID, "swap2_depth", swap2.ID, "mode", mode)
	}

	return
}

// 返回sell方向
func (sg *Strategy) canClose(cfg config.Config, swap1 *exchange.Depth, swap2 *exchange.Depth, mode rate.RunMode) (ok bool, srClose float64, closeTradeUnit float64, side ccexgo.OrderSide) {
	ok, srClose = sg.queue.CanClose(mode, cfg.SrMode)
	if cfg.CloseNow {
		ok = true
	}
	// cv, _ := sg.swapS.ContractVal().Float64()

	closeTradeUnit = sg.closeTradeUnit(cfg, swap1, swap2, mode)

	if mode == rate.Swap1TSwap2T {
		// srClose = swap2.Bids[0].Price/swap1.Asks[0].Price - 1
		if ok && closeTradeUnit >= cfg.VolumeCloseMin {
			// level.Info(sg.logger).Log("message", "close condition check pass", "swap1_depth", swap1.ID, "swap2_depth", swap2.ID, "mode", mode)
		} else {
			if ok {
				level.Info(sg.logger).Log("message", "close condition check failed",
					// "c1", current > 0,
					"c4", closeTradeUnit >= cfg.VolumeCloseMin, "swap1_depth_id", swap1.ID,
					"swap2_depth_id", swap2.ID, "close_trade_volume", closeTradeUnit,
					"volume_threshold2", cfg.VolumeThreshold2, "volume_close_min", cfg.VolumeCloseMin,
					"swap1_bid_amount", swap1.Bids[0].Amount, "swap1_bid_price", swap1.Bids[0].Price,
					"swap1_ask_price", swap1.Asks[0].Price,
					"swap2_ask_amount", swap2.Asks[0].Amount,
					"swap2_ask_price", swap2.Asks[0].Price,
					"swap2_bid_price", swap2.Bids[0].Price,
					"mode", mode,
				)
			}
			ok = false
		}
	} else {
		// if config.Mode == rate.SpotTSwapM {
		// 	swapMid := (swap.Asks[0].Price + swap.Bids[0].Price) / 2
		// 	srClose = swapMid/spot.Asks[0].Price - 1
		// 	if sg.PosList.Position().Amount.LessThan(sg.swapS.ContractVal()) {
		// 		ok = false
		// 	}
		// } else {
		// spotMid := (swap1.Asks[0].Price + swap1.Bids[0].Price) / 2
		// srClose = swap2.Asks[0].Price/swap1.Asks[0].Price - 1
		// }
		if mode != rate.Swap1MSwap2M {
			f_mode := cf_factor.SpotMSwapT
			volumeCloseMin := cfg.MTVolumeCloseMin
			if mode == rate.Swap1TSwap2M {
				f_mode = cf_factor.SpotTSwapM
				volumeCloseMin = cfg.TMVolumeCloseMin
			}
			var passIMB bool
			if cfg.PTMode == config.PreTaker && cfg.Factor.Enable {
				passIMB = cf_factor.CalculateIMBPass(f_mode, swap1.OrderBook, swap2.OrderBook, &cfg.Factor, true)
			} else {
				passIMB = true
			}

			if ok && closeTradeUnit >= volumeCloseMin && passIMB {
			} else {
				if ok {
					level.Info(sg.logger).Log("message", "close condition check failed",
						"c1", passIMB,
						// "c2", sg.PosList.Position().Amount.LessThan(sg.swapS.ContractVal()),
						"c4", closeTradeUnit >= volumeCloseMin, "swap1_depth_id", swap1.ID,
						"swap2_depth_id", swap2.ID, "close_trade_volume", closeTradeUnit,
						"volume_threshold2", cfg.VolumeThreshold2, "volume_close_min", volumeCloseMin,
						"swap1_bid_amount", swap1.Bids[0].Amount, "swap1_bid_price", swap1.Bids[0].Price,
						"swap1_ask_price", swap1.Asks[0].Price,
						"swap2_ask_amount", swap2.Asks[0].Amount,
						"swap2_ask_price", swap2.Asks[0].Price,
						"swap2_bid_price", swap2.Bids[0].Price,
						"mode", mode,
					)
				}
				ok = false
			}
		}
	}

	side = sg.PosList.DealSide(false)

	if side == ccexgo.OrderSideSell && (sg.FundingRate1.FundingRate-sg.FundingRate2.FundingRate) < cfg.LongFundingLimit {
		ok = false
		// level.Info(sg.logger).Log("message", "close condition check bad funding", "funding1", sg.FundingRate1.FundingRate, "funding2", sg.FundingRate2.FundingRate, "long_funding_limit", cfg.LongFundingLimit)
	}

	if ok {
		level.Info(sg.logger).Log("message", "close condition check pass", "swap1_depth", swap1.ID, "swap2_depth", swap2.ID, "swap2_ask_price", swap2.Asks[0].Price,
			"swap2_bid_price", swap2.Bids[0].Price, "swap1_ask_price", swap1.Asks[0].Price, "swap1_bid_price", swap1.Bids[0].Price, "close_volume", closeTradeUnit, "mode", mode)
	}

	// if ok {
	// 	if sg.isversed && sg.fundingRate1.FundingRate.GreaterThanOrEqual(sg.fundingRate2.FundingRate) {
	// 		level.Info(sg.logger).Log("message", "close forbid due to bad funding", "funding1", sg.fundingRate1.FundingRate,
	// 			"funding2", sg.fundingRate2.FundingRate)
	// 		ok = false
	// 	}
	// }

	return
}

func (sg *Strategy) loopInternal(ctx context.Context, spot exchange.Client, swap1 exchange.SwapClient, swap2 exchange.SwapClient, cnt int64) {
	// var (
	// spotOrderBook  *exchange.Depth
	// 	swap1OrderBook *exchange.Depth
	// 	swap2OrderBook *exchange.Depth
	// )

	sg.swap1OrderBook.Exchange = swap1.GetExchangeName()
	sg.swap2OrderBook.Exchange = swap2.GetExchangeName()

	if err := sg.updateAccount(ctx, spot, swap1, swap2, cnt, true); err != nil {
		level.Warn(sg.logger).Log("message", "update account fail", "error", err.Error())
		if err := message.SendP3Important(ctx, message.NewLogicStartFail(sg.swapS1.String(), fmt.Errorf("init update account fail,err:%+v", err))); err != nil {
			level.Warn(sg.logger).Log("message", "send lark failed", "err", err.Error())
		}
		return
	}

	if err := sg.updateFundingRate(ctx, swap1, swap2); err != nil {
		level.Warn(sg.logger).Log("message", "update funding rate fail", "error", err.Error())
		if err := message.SendP3Important(ctx, message.NewLogicStartFail(sg.swapS1.String(), fmt.Errorf("init update funding rate fail,err:%+v", err))); err != nil {
			level.Warn(sg.logger).Log("message", "send lark failed", "err", err.Error())
		}
		return
	}

	rand.Seed(time.Now().UnixNano())
	ticker := time.NewTicker(time.Second * 1)
	depthTicker := time.NewTicker(depthMaxDuration)
	fundingTicker := time.NewTicker(time.Second * 30) // 币安一分钟更新一次预测费率,定时30s取一次

	checkPosTicker := time.NewTimer(time.Minute * time.Duration(1+rand.Intn(10))) //防止挤一块
	exposureTimer := time.NewTimer(10 * time.Second)
	funding := sg.fundingTimer()
	exitEvent := gocommon.NewEvent()
	bookEvent := gocommon.NewEvent()
	adlEvent := gocommon.NewEvent()

	defer func() {
		exitEvent.Set()
		ticker.Stop()
		depthTicker.Stop()
		fundingTicker.Stop()
		checkPosTicker.Stop()
		exposureTimer.Stop()
	}()

	level.Info(sg.logger).Log("message", "loop start", "cnt", cnt)
	go message.Send(context.Background(), message.NewCommonMsg(fmt.Sprintf("%s 数据订阅协程启动", sg.swapS1), "启动完成"))

	sg.loopWg.Add(1)
	go func() { // 更新ws余额
		defer sg.loopWg.Done()
		var lastSwapBalTime time.Time // 避免合约盈亏导致的频繁序列化
		// 注册余额、仓位变化的回调函数, 30ms抖动，理论是在一次内序列化
		var debounceTimer *time.Timer
		checkUpdate := func(flag int64) {
			if debounceTimer != nil {
				debounceTimer.Stop()
			}
			level.Info(sg.logger).Log("message", "checkUpdate", "flag", flag)

			// 立马调用, 及时更新内存字段, 可以延后序列化
			sg.updateAccount(ctx, spot, swap1, swap2, flag, false)

			debounceTimer = time.AfterFunc(30*time.Millisecond, func() {
				// 检查是否已经退出，避免在退出过程中序列化
				select {
				case <-exitEvent.Done():
					return
				default:
					if err := sg.serialize(); err != nil {
						level.Warn(sg.logger).Log("message", "serialize fail", "error", err.Error())
					}
				}
			})
		}

		for {
			select {
			case <-exitEvent.Done():
				// 在退出前完成最后一次序列化
				if debounceTimer != nil {
					debounceTimer.Stop()
					// 立即执行序列化，确保数据被保存
					if err := sg.serialize(); err != nil {
						level.Warn(sg.logger).Log("message", "final serialize on exit fail", "error", err.Error())
					}
				}
				return
			case <-swap1.BalanceNotify().Done():
				swap1.BalanceNotify().Unset()
				if time.Since(lastSwapBalTime).Minutes() >= 20 { // 合约余额变动20分钟序列化一次
					checkUpdate(swapBalChangeEvent)
					lastSwapBalTime = time.Now()
				}
			case <-swap2.BalanceNotify().Done():
				swap2.BalanceNotify().Unset()
				if time.Since(lastSwapBalTime).Minutes() >= 20 { // 合约余额变动20分钟序列化一次
					checkUpdate(swapBalChangeEvent)
					lastSwapBalTime = time.Now()
				}
			case <-swap1.PosNotify().Done():
				swap1.PosNotify().Unset()
				checkUpdate(swapPosChangeEvent)
			case <-swap2.PosNotify().Done():
				swap2.PosNotify().Unset()
				checkUpdate(swapPosChangeEvent)

			case o := <-swap1.ADLOrder():
				if sg.inverse {
					continue
				}
				if !sg.ADLProcessor.InADL {
					sg.ADLProcessor.InADL = true
					sg.slMu.Lock()
					sg.TotalFailedTimes = 0 // 重置失败次数
					sg.slMu.Unlock()
				}
				sg.ADLProcessor.ADLNotFinish = true
				sg.slMu.Lock()
				sg.ADLProcessor.Exposure1 = sg.ADLProcessor.Exposure1.Add(decimal.NewFromFloat(o.TradeAmount))
				sg.ADLProcessor.Side1 = o.Side
				level.Info(sg.logger).Log("message", "swap1 get adl order", "id", o.OrderID, "trade_Amount", o.TradeAmount, "status", o.Status, "now_adl_exposure", sg.ADLProcessor.Exposure1)
				sg.slMu.Unlock()
				adlEvent.Set()
				checkUpdate(swapADLEvent)
			case o := <-swap2.ADLOrder():
				if sg.inverse {
					continue
				}
				if !sg.ADLProcessor.InADL {
					sg.ADLProcessor.InADL = true
					sg.slMu.Lock()
					sg.TotalFailedTimes = 0 // 重置失败次数
					sg.slMu.Unlock()
				}
				sg.ADLProcessor.ADLNotFinish = true
				sg.slMu.Lock()
				sg.ADLProcessor.Exposure2 = sg.ADLProcessor.Exposure2.Add(decimal.NewFromFloat(o.TradeAmount))
				sg.ADLProcessor.Side2 = o.Side
				level.Info(sg.logger).Log("message", "swap2 get adl order", "id", o.OrderID, "trade_Amount", o.TradeAmount, "status", o.Status, "now_adl_exposure", sg.ADLProcessor.Exposure2)
				sg.slMu.Unlock()
				adlEvent.Set()
				checkUpdate(swapADLEvent)
			}
		}
	}()

	// adl处理逻辑
	sg.loopWg.Add(3)
	go func() {
		defer sg.loopWg.Done()
		sg.reportADLExposure(adlEvent, exitEvent)
	}()
	go func() {
		defer sg.loopWg.Done()
		sg.ProcessADL(adlEvent, exitEvent)
	}()

	go func() {
		defer sg.loopWg.Done()
		for {
			select {
			case <-exitEvent.Done():
				return
			case depth := <-swap1.Depth():
				sg.swap1OrderBook.Asks = depth.Asks
				sg.swap1OrderBook.Bids = depth.Bids
				sg.swap1OrderBook.Created = depth.Created
				sg.swap1OrderBook.ID = depth.ID
				common.MeasureExecutionTime("PushSwap1", func() {
					sg.queue.PushSwap1(depth)
				})
				common.MeasureExecutionTime("pushOrderBook", func() {
					go sg.pushOrderBook(sg.swap1OrderBook, sg.swap2OrderBook)
				})
				bookEvent.Set()

			case depth := <-swap2.Depth():
				sg.swap2OrderBook.Asks = depth.Asks
				sg.swap2OrderBook.Bids = depth.Bids
				sg.swap2OrderBook.Created = depth.Created
				sg.swap2OrderBook.ID = depth.ID
				common.MeasureExecutionTime("PushSwap2", func() {
					sg.queue.PushSwap2(depth)
				})
				common.MeasureExecutionTime("pushOrderBook", func() {
					go sg.pushOrderBook(sg.swap1OrderBook, sg.swap2OrderBook)
				})
				bookEvent.Set()

			case <-swap1.BookTickerNotify().Done():
				swap1.BookTickerNotify().Unset()
				if sg.swap1OrderBook == nil || (len(sg.swap1OrderBook.Asks) == 0 && len(sg.swap1OrderBook.Bids) == 0) {
					continue
				}

				if atomic.LoadInt32(&sg.mtConcurrencyCount) > 0 || atomic.LoadInt32(&sg.tmConcurrencyCount) > 0 || atomic.LoadInt32(&sg.mmConcurrencyCount) > 0 { // 有活跃协程就通知
					sg.bookTickerEvent.Set()
				}

				bookTicker := swap1.BookTicker()
				if len(sg.swap1OrderBook.Asks) > 0 {
					sg.swap1OrderBook.Asks[0].Price = bookTicker.Ask1Price
					sg.swap1OrderBook.Asks[0].Amount = bookTicker.Ask1Amount
				}
				if len(sg.swap1OrderBook.Bids) > 0 {
					sg.swap1OrderBook.Bids[0].Price = bookTicker.Bid1Price
					sg.swap1OrderBook.Bids[0].Amount = bookTicker.Bid1Amount
				}
				sg.swap1OrderBook.Created = bookTicker.TimeStamp
				common.MeasureExecutionTime("bt PushSwap1", func() {
					sg.queue.PushSwap1(sg.swap1OrderBook)
				})
				bookEvent.Set()

			case <-swap2.BookTickerNotify().Done():
				swap2.BookTickerNotify().Unset()
				if sg.swap2OrderBook == nil || (len(sg.swap2OrderBook.Asks) == 0 && len(sg.swap2OrderBook.Bids) == 0) {
					continue
				}

				if atomic.LoadInt32(&sg.mtConcurrencyCount) > 0 || atomic.LoadInt32(&sg.tmConcurrencyCount) > 0 || atomic.LoadInt32(&sg.mmConcurrencyCount) > 0 { // 有活跃协程就通知
					sg.bookTickerEvent.Set()
				}

				bookTicker := swap2.BookTicker()
				if len(sg.swap2OrderBook.Asks) > 0 {
					sg.swap2OrderBook.Asks[0].Price = bookTicker.Ask1Price
					sg.swap2OrderBook.Asks[0].Amount = bookTicker.Ask1Amount
				}
				if len(sg.swap2OrderBook.Bids) > 0 {
					sg.swap2OrderBook.Bids[0].Price = bookTicker.Bid1Price
					sg.swap2OrderBook.Bids[0].Amount = bookTicker.Bid1Amount
				}
				sg.swap2OrderBook.Created = bookTicker.TimeStamp
				common.MeasureExecutionTime("bt PushSwap2", func() {
					sg.queue.PushSwap2(sg.swap2OrderBook)
				})
				bookEvent.Set()
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			level.Info(sg.logger).Log("message", "ctx cancelled")
			return

		case <-swap1.Done():
			var errmsg string
			if err := swap1.Error(); err != nil {
				errmsg = err.Error()
			}
			level.Info(sg.logger).Log("message", "swap1 client quit", "error", errmsg)
			return

		case <-swap2.Done():
			var errmsg string
			if err := swap2.Error(); err != nil {
				errmsg = err.Error()
			}
			level.Info(sg.logger).Log("message", "swap2 client quit", "error", errmsg)
			return

		case <-bookEvent.Done():
			bookEvent.Unset()
			if !isSettlement() && !sg.judgeFailed() && !sg.ADLProcessor.InADL {
				// 支持多个协程并发运行process方法
				cfg := sg.load.Load()

				if cfg.TTEnabled {
					currentCount := atomic.LoadInt32(&sg.ttConcurrencyCount)
					for i := 0; i < cfg.TTConcurrencyLimit-int(currentCount); i++ {
						// sg.loopWg.Go()
						sg.loopWg.Go(func() {
							sg.process(ctx, nil, sg.swap1OrderBook, sg.swap2OrderBook, spot, swap1, swap2, cfg)
						})
					}
				}
				if cfg.MTEnabled {
					currentCount := atomic.LoadInt32(&sg.mtConcurrencyCount)
					for i := 0; i < cfg.MTConcurrencyLimit-int(currentCount); i++ {
						sg.loopWg.Go(func() {
							sg.process2(ctx, nil, sg.swap1OrderBook, sg.swap2OrderBook, spot, swap1, swap2, cfg, rate.Swap1MSwap2T)
						})
					}
				}
				if cfg.TMEnabled {
					currentCount := atomic.LoadInt32(&sg.tmConcurrencyCount)
					for i := 0; i < cfg.TMConcurrencyLimit-int(currentCount); i++ {
						sg.loopWg.Go(func() {
							sg.process2(ctx, nil, sg.swap1OrderBook, sg.swap2OrderBook, spot, swap1, swap2, cfg, rate.Swap1TSwap2M)
						})
					}
				}
				if cfg.MMEnabled {
					currentCount := atomic.LoadInt32(&sg.mmConcurrencyCount)
					for i := 0; i < cfg.MMConcurrencyLimit-int(currentCount); i++ {
						sg.loopWg.Go(func() {
							sg.process2(ctx, nil, sg.swap1OrderBook, sg.swap2OrderBook, spot, swap1, swap2, cfg, rate.Swap1MSwap2M)
						})
					}
				}
			}

		case <-funding:
			sg.loopWg.Go(func() {
				if err := sg.updateAccount(ctx, spot, swap1, swap2, 0, true); err != nil {
					level.Warn(sg.logger).Log("message", "update account fail", "error", err.Error())
					nextRun := helper.RandIntRange(100, 300)
					funding = time.NewTimer(time.Duration(nextRun) * time.Second).C // 随机时间点
				} else {
					sg.updateFundingRate(ctx, swap1, swap2)
					funding = sg.fundingTimer()
				}
			})

		case <-ticker.C:
			sg.reloadQueue()

		case now := <-depthTicker.C:
			// if spotOrderBook != nil && spotOrderBook.Created.Add(time.Minute).Before(now) {
			// 	level.Warn(sg.logger).Log("message", "spot depth queue too late", "ts", spotOrderBook.Created)
			// 	return
			// }

			if sg.swap1OrderBook != nil && sg.swap1OrderBook.Created.Add(time.Minute).Before(now) {
				level.Warn(sg.logger).Log("message", "swap1 depth queue too late", "ts", sg.swap1OrderBook.Created)
				return
			}

			if sg.swap2OrderBook != nil && sg.swap2OrderBook.Created.Add(time.Minute).Before(now) {
				level.Warn(sg.logger).Log("message", "swap2 depth queue too late", "ts", sg.swap2OrderBook.Created)
				return
			}

		case <-fundingTicker.C:
			sg.loopWg.Go(func() {
				sg.updateFundingRate(ctx, swap1, swap2)
			})

		case <-checkPosTicker.C:
			sg.loopWg.Go(func() {
				sg.checkPosition(ctx, swap1, swap2)
			})
			checkPosTicker = helper.NextTimer([2]time.Duration{time.Minute * 7, time.Minute * 13})

		case <-exposureTimer.C:
			exposureTimer = time.NewTimer(10 * time.Second)

			sg.loopWg.Add(1)
			go func() {
				defer sg.loopWg.Done()
				sg.processExposure(ctx, swap1, swap2)
			}()
		}
	}
}

func (sg *Strategy) reloadQueue() {
	if locked := sg.tryOperationLock(); !locked {
		return
	}

	defer sg.unOperationLock()
	cfg := sg.load.Load()
	if changed := sg.queue.Reset(cfg.Queue, cfg.MmSwap1OpenTickNum, cfg.MmSwap2OpenTickNum, cfg.MmSwap1CloseTickNum, cfg.MmSwap2CloseTickNum); changed {
		level.Info(sg.logger).Log("message", "queue config reload", "duration", cfg.Queue.MSecs,
			"open_limit", cfg.Queue.OpenThreshold, "close_limit", cfg.Queue.CloseThreshold)
	}
}

func (sg *Strategy) updateAccount(ctx context.Context, spotC exchange.Client, swapC1 exchange.SwapClient, swapC2 exchange.SwapClient, cnt int64, serializeNow bool) error {
	if cnt < spotBalChangeEvent || cnt > swapPosChangeEvent { // TODO 不加锁看看会怎么样
		sg.mustLock()
		defer sg.unLock()
	}

	level.Info(sg.logger).Log("message", "update account", "cnt", cnt)

	if err := sg.PosList.UpdateAccount(ctx, spotC, swapC1, swapC2, cnt); err != nil {
		return errors.WithMessage(err, "update account fail")
	}
	if cnt == 0 { // 避免每次loop启动都更新, 程序初次启动还是会获取
		if fundingRecords, err := sg.PosList.UpdateFunding(ctx, swapC1, `swap1`); err != nil {
			return errors.WithMessage(err, "update swap1 funding fail")
		} else {
			if fundingRecords != nil {
				sg.record(ctx, fundingCsv, fundingRecords...)
			}
		}
		if fundingRecords, err := sg.PosList.UpdateFunding(ctx, swapC2, `swap2`); err != nil {
			return errors.WithMessage(err, "update swap2 funding fail")
		} else {
			if fundingRecords != nil {
				sg.record(ctx, fundingCsv, fundingRecords...)
			}
		}
	} else {
		level.Info(sg.logger).Log("message", "update account skip get funding")
	}
	// sg.PosList.UpdateEstimateExposure()
	if !serializeNow {
		return nil
	}
	if err := sg.serialize(); err != nil {
		return errors.WithMessage(err, "store to file fail")
	}
	return nil
}

func (sg *Strategy) checkPosition(ctx context.Context, swapC1, swapC2 exchange.SwapClient) {
	if locked := sg.tryOperationLock(); !locked {
		return
	}
	defer sg.unOperationLock()

	position1, err := swapC1.GetPosition(ctx)
	if err != nil {
		level.Warn(sg.logger).Log("message", "check position1 failed", "err", err.Error())
		return
	}

	position2, err := swapC2.GetPosition(ctx)
	if err != nil {
		level.Warn(sg.logger).Log("message", "check position2 failed", "err", err.Error())
		return
	}

	if (!position1.Short.Position.IsZero() && !position1.Long.Position.IsZero()) ||
		(!position2.Short.Position.IsZero() && !position2.Long.Position.IsZero()) { // 两边都有持仓，说明有问题
		gocommon.PanicWithTime(fmt.Sprintf("合约多空方向都有持仓,请检查:\n%s:%+v\n%s:%+v", swapC1.GetExchangeName(), position1, swapC2.GetExchangeName(), position2))
	}
	localPos1, localPos2 := sg.PosList.UpdateLocalTotalPos()

	var pos1, pos2 ccexgo.Position
	// 取本地持仓方向的仓位
	if !localPos1.IsPositive() {
		pos1 = position1.Short
	} else {
		pos1 = position1.Long
	}
	if !localPos2.IsPositive() {
		pos2 = position2.Short
	} else {
		pos2 = position2.Long
	}

	diff1 := localPos1.Abs().Sub(pos1.Position)
	diff2 := localPos2.Abs().Sub(pos2.Position)

	cfg := sg.load.Load()

	var limit1, limit2 decimal.Decimal // 默认warnValueLimit 价值的usdt
	var swap1MidPrice float64
	threshold := cfg.WarnValueLimit
	cv1 := sg.swapS1.ContractVal()
	cv2 := sg.swapS2.ContractVal()
	if !sg.inverse { // 换成张
		diff1 = diff1.Div(cv1)
		diff2 = diff2.Div(cv2)
		if !orderBookIsValid(sg.swap1OrderBook) {
			level.Warn(sg.logger).Log("message", "check position failed", "quit", "swap depth is nil")
			return
		}
		threshold = threshold / sg.swap1OrderBook.Bids[0].Price
		swap1MidPrice = (sg.swap1OrderBook.Asks[0].Price + sg.swap1OrderBook.Bids[0].Price) / 2
	}

	limit1 = decimal.NewFromFloat(threshold).Div(cv1)
	limit2 = decimal.NewFromFloat(threshold).Div(cv2)

	level.Info(sg.logger).Log("message", "check position", "local_pos", localPos1, "pos1", pos1.Position, "diff1", diff1,
		"limit1", limit1, "cv1", cv1, "local_pos2", localPos2, "pos2", pos2.Position, "diff2", diff2,
		"limit2", limit2, "cv2", cv2)

	if diff1.Abs().GreaterThanOrEqual(limit1) || diff2.Abs().GreaterThanOrEqual(limit2) {
		var (
			sendP1, sendP3, sendP0        bool
			minLimitValue, uDiff1, uDiff2 float64
		)
		if !sg.inverse { // 换成币，u本位下单单位是币
			diff1 = diff1.Mul(cv1)
			limit1 = limit1.Mul(cv1)
			diff2 = diff2.Mul(cv2)
			limit2 = limit2.Mul(cv2)

			minLimitValue = cfg.WarnValueLimit
			uDiff1 = diff1.Abs().InexactFloat64() * swap1MidPrice
			uDiff2 = diff2.Abs().InexactFloat64() * swap1MidPrice

			if uDiff1 >= minLimitValue*float64(cfg.P3Level) || uDiff2 >= minLimitValue*float64(cfg.P3Level) {
				sendP3 = true
			}
			if uDiff1 >= minLimitValue*float64(cfg.P1Level) || uDiff2 >= minLimitValue*float64(cfg.P1Level) {
				sendP1 = true
			}
			if uDiff1 >= minLimitValue*float64(cfg.P1Level)*3 || uDiff2 >= minLimitValue*float64(cfg.P1Level)*3 {
				sendP0 = true
				sg.PosList.UpdateBannedTime2(nil, true, fmt.Sprintf("仓位差距价值过大, 超过%fU", minLimitValue*float64(cfg.P1Level)*3), 10)
			}
		}

		msg := message.NewCommonMsgWithImport(fmt.Sprintf(`%s 仓位差距超过限制`, sg.swapS1), fmt.Sprintf(
			"%s[%s]本地仓位:%s,交易所仓位:%s,限制:%s,差值:%s, u差值价值: %.2fU\n%s[%s]本地仓位:%s,交易所仓位:%s,限制:%s,差值:%s, u差值价值: %.2fU", swapC1.GetExchangeName(), sg.swapS1, localPos1, pos1.Position, limit1, diff1, uDiff1,
			swapC2.GetExchangeName(), sg.swapS2, localPos2, pos2.Position, limit2, diff2, uDiff2))

		if sendP3 {
			go message.SendP3Important(ctx, msg)
		}
		if sendP1 {
			go message.SendImportant(ctx, msg)
		}
		if sendP0 {
			go message.SendP0Important(ctx, msg)
		}
	}
	sg.PosList.UpdateSwap(position1, position2)
	if err := sg.serialize(); err != nil {
		level.Warn(sg.logger).Log("message", "check position failed", "store", err.Error())
	}
}

func (sg *Strategy) checkBalance(ctx context.Context, spotC exchange.Client, swapC1 exchange.SwapClient, swapC2 exchange.SwapClient) {
	if err := sg.checkBalanceInternal(ctx, spotC, swapC1, swapC2); err != nil {
		level.Warn(sg.logger).Log("message", "check balance fail", "error", err.Error())
		if err := message.Send(ctx, message.NewCheckBalanceFail(sg.swapS1.String(), err)); err != nil {
			level.Warn(sg.logger).Log("message", "send lark failed", "err", err.Error())
		}
	}
}

func (sg *Strategy) checkBalanceInternal(ctx context.Context, spotC exchange.Client, swapC1 exchange.SwapClient, swapC2 exchange.SwapClient) error {
	cacheBalance := sg.PosList.Position()

	// balance, err := spotC.FetchBalance(ctx)
	// if err != nil {
	// 	return errors.WithMessage(err, "fetch spot balance fail")
	// }

	// var baseBalance, quoteBalance, bal decimal.Decimal
	// for _, b := range balance {
	// 	if strings.ToUpper(sg.spotS.Base()) == b.Currency {
	// 		baseBalance = b.Total
	// 	} else if b.Currency == strings.ToUpper(sg.spotS.Quote()) {
	// 		quoteBalance = b.Total
	// 	}
	// }
	// bal = baseBalance
	// if !sg.isversed { // U本位
	// 	bal = quoteBalance
	// }

	position, err := swapC1.Position(ctx)
	if err != nil {
		return errors.WithMessage(err, "fetch position1 fail")
	}

	position2, err := swapC2.Position(ctx)
	if err != nil {
		return errors.WithMessage(err, "fetch position2 fail")
	}

	level.Info(sg.logger).Log("message", "check balance", "cache_swap1_long_pos", fmt.Sprintf("%+v", cacheBalance.Swap1LongPos),
		"cache_swap1_short_pos", fmt.Sprintf("%+v", cacheBalance.Swap1ShortPos), "cache_swap2_long_pos", fmt.Sprintf("%+v", cacheBalance.Swap2LongPos),
		"cache_swap2_short_pos", fmt.Sprintf("%+v", cacheBalance.Swap2ShortPos), "swap1_long_pos", fmt.Sprintf("%+v", position.Long), "swap1_short_pos", fmt.Sprintf("%+v", position.Short),
		"swap2_long_pos", fmt.Sprintf("%+v", position2.Long), "swap2_short_pos", fmt.Sprintf("%+v", position2.Short))

	if !cacheBalance.Swap1LongPos.PosAmount.Equal(position.Long.Position) || !cacheBalance.Swap1ShortPos.PosAmount.Equal(position.Short.Position) ||
		!cacheBalance.Swap2LongPos.PosAmount.Equal(position2.Long.Position) || !cacheBalance.Swap2ShortPos.PosAmount.Equal(position2.Short.Position) {
		if err = message.Send(ctx, message.NewBalanceMismatch(
			sg.swapS1.String(), cacheBalance.Swap1LongPos.PosAmount, cacheBalance.Swap1ShortPos.PosAmount, cacheBalance.Swap2LongPos.PosAmount,
			cacheBalance.Swap2ShortPos.PosAmount, position.Long.Position, position.Short.Position, position2.Long.Position, position2.Short.Position)); err != nil {
			level.Warn(sg.logger).Log("message", "send lark failed", "err", err.Error())
		}
	}
	return nil
}

func (sg *Strategy) pushOrderBook(spot *exchange.Depth, swap *exchange.Depth) {
	if !orderBookIsValid2(spot) || !orderBookIsValid2(swap) {
		return
	}

	data := map[string]string{}

	datas := []struct {
		prefix string
		depth  *exchange.Depth
	}{
		{
			prefix: "spot", depth: spot,
		},
		{
			prefix: "swap", depth: swap,
		},
	}

	for _, d := range datas {
		ob := d.depth
		data[fmt.Sprintf("%s_id", d.prefix)] = strconv.Itoa(int(ob.ID))
		data[fmt.Sprintf("%s_timestamp", d.prefix)] = ob.Created.String()
		var spotI, swapI int
		for i, elem := range ob.Bids {
			if i > 4 {
				break
			}
			data[fmt.Sprintf("%s_bid%d_price", d.prefix, i)] = decimal.NewFromFloat(elem.Price).String()
			data[fmt.Sprintf("%s_bid%d_amount", d.prefix, i)] = decimal.NewFromFloat(elem.Amount).String()
			spotI = i
		}
		for spotI < 4 { //补上0,1,2,3,4
			spotI++
			data[fmt.Sprintf("%s_bid%d_price", d.prefix, spotI)] = decimal.Zero.String()
			data[fmt.Sprintf("%s_bid%d_amount", d.prefix, spotI)] = decimal.Zero.String()
		}

		for i, elem := range ob.Asks {
			if i > 4 {
				break
			}
			data[fmt.Sprintf("%s_ask%d_price", d.prefix, i)] = decimal.NewFromFloat(elem.Price).String()
			data[fmt.Sprintf("%s_ask%d_amount", d.prefix, i)] = decimal.NewFromFloat(elem.Amount).String()
			swapI = i
		}
		for swapI < 4 { //补上0,1,2,3,4
			swapI++
			data[fmt.Sprintf("%s_ask%d_price", d.prefix, swapI)] = decimal.Zero.String()
			data[fmt.Sprintf("%s_ask%d_amount", d.prefix, swapI)] = decimal.Zero.String()
		}
	}

	put := output.Data{
		Exchange: sg.exchange,
		Chan:     sg.spotS.Base(),
		Data:     data,
	}

	select {
	case sg.out <- &put:
	default:
	}
}

func (sg *Strategy) mustLock() {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		ticker := time.NewTicker(time.Second * 5)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return

			case <-ticker.C:
				level.Info(sg.logger).Log("message", "must lock not done")
			}
		}
	}()

	defer cancel()

	for {
		if locked := sg.tryLock(); !locked {
			time.Sleep(time.Millisecond * 10)
			continue
		}
		return
	}
}

// tryTradingLock 尝试获取交易锁（用于process2等下单操作）
func (sg *Strategy) tryTradingLock(mode rate.RunMode) bool {
	// 先尝试获取操作锁（所有操作都需要互斥）
	if !sg.operationMu.TryLock() {
		return false
	}

	// 获取当前配置
	cfg := sg.load.Load()
	var (
		concurrencyLimit int
		currentCount     int32
	)
	switch mode {
	case rate.Swap1TSwap2T:
		concurrencyLimit = cfg.TTConcurrencyLimit
		currentCount = atomic.LoadInt32(&sg.ttConcurrencyCount)
	case rate.Swap1TSwap2M:
		concurrencyLimit = cfg.TMConcurrencyLimit
		currentCount = atomic.LoadInt32(&sg.tmConcurrencyCount)
	case rate.Swap1MSwap2T:
		concurrencyLimit = cfg.MTConcurrencyLimit
		currentCount = atomic.LoadInt32(&sg.mtConcurrencyCount)
	case rate.Swap1MSwap2M:
		concurrencyLimit = cfg.MMConcurrencyLimit
		currentCount = atomic.LoadInt32(&sg.mmConcurrencyCount)
	}

	// 检查当前并发数是否超过限制
	if currentCount >= int32(concurrencyLimit) {
		// 记录并发限制日志
		level.Debug(sg.logger).Log("message", "concurrency limit reached",
			"current", currentCount, "limit", concurrencyLimit, "mode", mode)
		sg.operationMu.Unlock() // 释放操作锁
		return false
	}

	// 尝试增加并发计数
	var newCount int32
	switch mode {
	case rate.Swap1TSwap2T:
		newCount = atomic.AddInt32(&sg.ttConcurrencyCount, 1)
	case rate.Swap1TSwap2M:
		newCount = atomic.AddInt32(&sg.tmConcurrencyCount, 1)
	case rate.Swap1MSwap2T:
		newCount = atomic.AddInt32(&sg.mtConcurrencyCount, 1)
	case rate.Swap1MSwap2M:
		newCount = atomic.AddInt32(&sg.mmConcurrencyCount, 1)
	}
	level.Debug(sg.logger).Log("message", "acquired trading lock",
		"current", newCount, "limit", concurrencyLimit, "mode", mode)

	// 释放操作锁，允许其他交易协程并发执行
	sg.operationMu.Unlock()
	return true
}

// tryOperationLock 尝试获取操作锁（用于reloadQueue、checkPosition等）
func (sg *Strategy) tryOperationLock() bool {
	// 检查是否有交易协程在运行
	if atomic.LoadInt32(&sg.ttConcurrencyCount) > 0 || atomic.LoadInt32(&sg.mtConcurrencyCount) > 0 || atomic.LoadInt32(&sg.tmConcurrencyCount) > 0 || atomic.LoadInt32(&sg.mmConcurrencyCount) > 0 {
		return false // 有交易协程在运行，不能执行操作
	}

	return sg.operationMu.TryLock()
}

// tryLock 保持向后兼容，根据调用场景选择不同的锁
func (sg *Strategy) tryLock() bool {
	// 这里需要根据调用栈来判断使用哪种锁
	// 为了简化，我们先使用操作互斥锁，后续可以根据需要调整
	return sg.tryOperationLock()
}

// unTradingLock 释放交易锁
func (sg *Strategy) unTradingLock(mode rate.RunMode) {
	sg.operationMu.Lock()
	// 减少并发计数
	var newCount int32
	switch mode {
	case rate.Swap1TSwap2T:
		newCount = atomic.AddInt32(&sg.ttConcurrencyCount, -1)
	case rate.Swap1TSwap2M:
		newCount = atomic.AddInt32(&sg.tmConcurrencyCount, -1)
	case rate.Swap1MSwap2T:
		newCount = atomic.AddInt32(&sg.mtConcurrencyCount, -1)
	case rate.Swap1MSwap2M:
		newCount = atomic.AddInt32(&sg.mmConcurrencyCount, -1)
	}
	sg.operationMu.Unlock()

	level.Debug(sg.logger).Log("message", "released trading lock", "current", newCount, "mode", mode)
}

// unOperationLock 释放操作锁
func (sg *Strategy) unOperationLock() {
	sg.operationMu.Unlock()
}

// unLock 保持向后兼容
func (sg *Strategy) unLock() {
	// 这里需要根据调用栈来判断使用哪种锁
	// 为了简化，我们先使用操作互斥锁，后续可以根据需要调整
	sg.unOperationLock()
}

func (sg *Strategy) process(ctx context.Context, spot *exchange.Depth, swap1 *exchange.Depth, swap2 *exchange.Depth,
	spotC exchange.Client, swapC1 exchange.SwapClient, swapC2 exchange.SwapClient, cfg config.Config) {

	if !orderBookIsValid(swap1) || !orderBookIsValid(swap2) {
		return
	}
	mode := rate.Swap1TSwap2T
	if locked := sg.tryTradingLock(mode); !locked || sg.judgeFailed() || sg.ADLProcessor.InADL {
		if locked {
			sg.unTradingLock(mode)
		}
		return
	}
	operateFail := false //开平仓是否出错
	defer func() {
		if operateFail {
			// 使用锁保护共享状态修改
			sg.slMu.Lock()
			totalFailedTimes := sg.TotalFailedTimes
			sg.slMu.Unlock()
			if totalFailedTimes == 3 {
				if err := message.SendP3Important(ctx, message.NewCommonMsgWithAt(fmt.Sprintf("%s 开平仓连续失败达3次", sg.swapS1), `请关闭程序，登录账户手动处理,处理完成重启程序`)); err != nil {
					level.Warn(sg.logger).Log("message", "send lark failed", "err", err.Error())
				}
			}
			sg.checkBalance(ctx, spotC, swapC1, swapC2)
			if err := sg.serialize(); err != nil { // 有错误再序列化一下
				level.Warn(sg.logger).Log("message", "store to file fail", "error", err.Error())
			}
		}
		sg.unTradingLock(mode)
	}()

	openFunc := func(srOpen float64, openTradeVolume float64, side1 ccexgo.OrderSide) {
		swap1Side := acts.CcexSide2Acts(side1)
		var swap2Side apis.OrderSide
		if swap1Side == apis.OrderSideBuy {
			swap2Side = apis.OrderSideSell
		} else {
			swap2Side = apis.OrderSideBuy
		}
		if !sg.shareBanCanOrder(swap1Side, swap2Side) {
			return
		}
		// 检查一下本地poss及仓位, 发现一边为0, 一边不是0, 和要操作的方向相反就先清除仓位
		if clear, err := sg.checkLocalPosClear(ctx, swapC1, swapC2, swap1, swap2, side1); err != nil {
			level.Warn(sg.logger).Log("message", "check local pos clear error", "error", err.Error())
			message.SendP3Important(ctx, message.NewCommonMsg(fmt.Sprintf("%s 自动清理仓位失败", sg.swapS1), err.Error()))
			// 使用锁保护forbiddenTime修改
			sg.slMu.Lock()
			sg.forbiddenTime = time.Now().Add(time.Minute * 1)
			sg.slMu.Unlock()
			return
		} else if clear {
			// 清理仓位后, 等待300ms, 让ws缓存更新
			time.Sleep(time.Millisecond * 300)
			return
		}

		placeSide := side1
		if mode == rate.Swap1TSwap2M {
			placeSide = reverseSide(side1)
		}
		op, err := sg.PosList.Open(ctx, swapC1, swapC2, srOpen, openTradeVolume, swap1, swap2, placeSide)

		if err != nil {
			errStr := err.Error()
			level.Warn(sg.logger).Log("message", "open position error", "error", errStr)
			var (
				sendP1, sendP3 bool
				msg            message.Message
			)
			// if op != nil {
			// 	usdt, _ := op.USDT().Float64() // 加上开仓敞口
			// 	sg.CurrentPos += usdt
			// }
			msg = message.NewOperateFail(fmt.Sprintf("%s 开仓失败(%s)", sg.swapS1, placeSide), errStr)
			if !strings.Contains(errStr, "订单会立即成交") && !strings.Contains(errStr, "成交价值") && !strings.Contains(errStr, "等待自动处理") {
				operateFail = true
				// 使用锁保护失败次数修改
				if strings.Contains(errStr, "账户余额不足") || strings.Contains(errStr, "初始杠杆下的最大值") || strings.Contains(errStr, "平台仓位限制") {
					sg.slMu.Lock()
					sg.TotalFailedTimes = 3
					sg.slMu.Unlock()
				} else {
					sg.addFailed()
				}
				// MinNotional 大概率不需要发送重要消息
				if errorToP3(errStr) {
					sendP1 = true
					sendP3 = true
				}
			} else { // 有可能有两个错误
				if errorToP3(errStr) {
					operateFail = true
					if strings.Contains(errStr, "账户余额不足") || strings.Contains(errStr, "初始杠杆下的最大值") || strings.Contains(errStr, "平台仓位限制") {
						sg.slMu.Lock()
						sg.TotalFailedTimes = 3
						sg.slMu.Unlock()
					}
					sendP1 = true
					sendP3 = true
				}
			}
			if strings.Contains(errStr, `人工处理`) {
				sendP1 = true
				msg = message.NewCommonMsgWithImport(fmt.Sprintf("%s 开仓失败(%s)", sg.swapS1, placeSide), errStr)
			}
			if sendP1 {
				if sendP3 {
					message.SendP3Important(ctx, msg)
				} else {
					message.SendImportant(ctx, msg)
				}
			} else {
				message.Send(ctx, msg)
			}

			return
		}
		sg.resetFailed()
		if op == nil { // 成交为0退出
			return
		}
		ccy1, ccy2 := op.Ccy()
		// 使用锁保护仓位状态修改
		sg.slMu.Lock()
		sg.CurrentPos1 += ccy1.InexactFloat64()
		sg.CurrentPos2 += ccy2.InexactFloat64()
		sg.slMu.Unlock()
		if err := sg.serialize(); err != nil {
			level.Warn(sg.logger).Log("message", "store to file fail", "error", err.Error())
		}
		fields := op.Fields()
		// fields = append(fields, [2]string{"open_limit", strconv.Itoa(config.VolumeOpenLimit)})
		sg.record(ctx, openCsv, fields...)
	}

	closeFunc := func(srClose float64, closeTradeUnit float64, side1 ccexgo.OrderSide) {
		swap1Side := acts.CcexSide2Acts(side1)
		var swap2Side apis.OrderSide
		if swap1Side == apis.OrderSideCloseLong {
			swap2Side = apis.OrderSideCloseShort
		} else {
			swap2Side = apis.OrderSideCloseLong
		}
		if !sg.shareBanCanOrder(swap1Side, swap2Side) {
			return
		}

		// 判断一下是否还有可平仓位数量
		placeSide := side1
		// swapName := "swap1"
		if mode == rate.Swap1TSwap2M {
			placeSide = reverseSide(side1)
			// swapName = "swap2"
		}

		_, success := sg.PosList.TryFreezeCloseAmount(decimal.NewFromFloat(closeTradeUnit), false)
		if !success {
			return
		}

		op, err := sg.PosList.Close(ctx, swapC1, swapC2, srClose, closeTradeUnit, swap1, swap2, placeSide, &cfg)

		if err != nil {
			errStr := err.Error()
			level.Warn(sg.logger).Log("message", "close position error", "error", errStr)
			var (
				sendP1, sendP3 bool
				msg            message.Message
			)

			msg = message.NewOperateFail(fmt.Sprintf("%s 平仓失败(%s)", sg.swapS1, placeSide), errStr)
			if !strings.Contains(errStr, "订单会立即成交") && !strings.Contains(errStr, "跳过") && !strings.Contains(errStr, "成交价值") &&
				!strings.Contains(errStr, "无法平仓") && !strings.Contains(errStr, "等待自动处理") {
				operateFail = true
				// 使用锁保护失败次数修改
				if strings.Contains(errStr, "账户余额不足") { // 不能再操作, 会造成敞口
					sg.slMu.Lock()
					sg.TotalFailedTimes = 3
					sg.slMu.Unlock()
				} else {
					sg.addFailed()
				}
				if errorToP3(errStr) {
					sendP1 = true
					sendP3 = true
				}
			} else {
				if errorToP3(errStr) {
					if strings.Contains(errStr, "账户余额不足") { // 不能再操作, 会造成敞口
						sg.slMu.Lock()
						sg.TotalFailedTimes = 3
						sg.slMu.Unlock()
					}
					sendP1 = true
					sendP3 = true
				}
			}

			if strings.Contains(errStr, `人工处理`) {
				sendP1 = true
				msg = message.NewCommonMsgWithImport(fmt.Sprintf("%s 平仓失败(%s)", sg.swapS1, placeSide), errStr)
			}
			if sendP1 {
				if sendP3 {
					message.SendP3Important(ctx, msg)
				} else {
					message.SendImportant(ctx, msg)
				}
			} else {
				message.Send(ctx, msg)
			}
			if op != nil {
				ccy1, ccy2 := op.Ccy() // 默认平仓成功
				// 使用锁保护仓位状态修改
				sg.slMu.Lock()
				sg.CurrentPos1 -= ccy1.InexactFloat64()
				sg.CurrentPos2 -= ccy2.InexactFloat64()
				sg.slMu.Unlock()
			}
			return
		}
		if op == nil {
			return //no operate
		}
		sg.resetFailed()

		ccy1, ccy2 := op.Ccy()
		// 使用锁保护仓位状态修改
		sg.slMu.Lock()
		sg.CurrentPos1 -= ccy1.InexactFloat64()
		sg.CurrentPos2 -= ccy2.InexactFloat64()
		sg.slMu.Unlock()

		if err := sg.serialize(); err != nil {
			level.Warn(sg.logger).Log("message", "store to file fail", "error", err.Error())
		}

		fields := op.Fields()
		// fields = append(fields, [2]string{"close_limit", strconv.Itoa(config.VolumeCloseLimit)})
		sg.record(ctx, closeCsv, fields...)
	}

	swap1Mid := (swap1.Asks[0].Price + swap1.Bids[0].Price) / 2
	swap2Mid := (swap2.Asks[0].Price + swap2.Bids[0].Price) / 2

	sgPos := sg.PosList.Position()
	if cfg.OpenAllowed {
		// 加限制
		// 使用锁保护仓位状态读取
		sg.slMu.Lock()
		currentPos1 := sg.CurrentPos1
		currentPos2 := sg.CurrentPos2
		sg.slMu.Unlock()

		// https://digifinex.sg.larksuite.com/wiki/CoC8wQEKqi9S4JkvTUslORgOgch
		buy := ((cfg.LimitPosShort-currentPos1)*swap1Mid >= 1.1*cfg.VolumeOpenMin) && ((cfg.LimitPosShort-currentPos2)*swap2Mid >= 1.1*cfg.VolumeOpenMin)
		if !sg.inverse {
			volumeOpenMin := cfg.VolumeOpenMin * swap1Mid
			if mode == rate.Swap1MSwap2M {
				volumeOpenMin = cfg.MmOpenVolume
			}
			buy = (cfg.LimitPosShort-currentPos1 >= 1.1*volumeOpenMin) && ((cfg.LimitPosShort - currentPos2) >= 1.1*volumeOpenMin)
			// 增加二层限制
			if decimal.Max(sgPos.Swap1LongPos.PosAmount, sgPos.Swap2ShortPos.PosAmount).Mul(decimal.NewFromFloat(max(swap1.Asks[0].Price, swap2.Bids[0].Price))).Div(decimal.NewFromInt(int64(cfg.Level1))).GreaterThanOrEqual(decimal.NewFromFloat(cfg.LimitPosShort)) {
				buy = false
			}
		}
		var skip bool
		if sg.PosList.DealSide(true) == ccexgo.OrderSideBuy && !buy {
			skip = true
		}
		if skip {
			goto close
		}
		if ok, sr, tradeVolume, side := sg.canOpen(cfg, swap1, swap2, mode); ok { // 返回buy数量，根据仓位判断是swap1做多还是平空
			if side == ccexgo.OrderSideBuy { // swap1开多, swap2开空 做空价差
				openFunc(sr, tradeVolume, ccexgo.OrderSideBuy)
				return
			} else { // swap1平空 swap2平多 平多价差 buy
				if !float.Equal(sg.CurrentPos1, 0) && sg.CurrentPos1 > 0 {
					closeFunc(sr, tradeVolume, ccexgo.OrderSideCloseShort)
					return
				}
			}
		}
	}

close:
	if cfg.CloseAllowed { // 开仓和平仓都有
		sg.slMu.Lock()
		currentPos1 := sg.CurrentPos1
		currentPos2 := sg.CurrentPos2
		sg.slMu.Unlock()

		sell := ((cfg.LimitPosLong-currentPos1)*swap1Mid >= 1.1*cfg.VolumeCloseMin) && ((cfg.LimitPosLong-currentPos2)*swap2Mid >= 1.1*cfg.VolumeCloseMin)
		if !sg.inverse {
			volumeCloseMin := cfg.VolumeCloseMin * swap1Mid
			if mode == rate.Swap1MSwap2M {
				volumeCloseMin = cfg.MmCloseVolume
			}
			sell = ((cfg.LimitPosLong - sg.CurrentPos1) >= 1.1*volumeCloseMin) && ((cfg.LimitPosLong - sg.CurrentPos2) >= 1.1*volumeCloseMin)
			// 增加二层限制
			if decimal.Max(sgPos.Swap1ShortPos.PosAmount, sgPos.Swap2LongPos.PosAmount).Mul(decimal.NewFromFloat(max(swap1.Bids[0].Price, swap2.Asks[0].Price))).Div(decimal.NewFromInt(int64(cfg.Level1))).GreaterThanOrEqual(decimal.NewFromFloat(cfg.LimitPosLong)) {
				sell = false
			}
		}
		var skip bool
		if sg.PosList.DealSide(false) == ccexgo.OrderSideSell && !sell {
			skip = true
		}
		if skip {
			return
		}
		if ok, sr, tradeVolume, side := sg.canClose(cfg, swap1, swap2, mode); ok { // 返回sell数量，根据仓位判断是swap1做空还是平多
			if side == ccexgo.OrderSideSell { // swap1开空, swap2开多 做多价差
				openFunc(sr, tradeVolume, ccexgo.OrderSideSell)
				return
			} else { // swap1平多, swap2平空 平掉做空价差
				if !float.Equal(sg.CurrentPos1, 0) && sg.CurrentPos1 > 0 {
					closeFunc(sr, tradeVolume, ccexgo.OrderSideCloseLong)
					return
				}
			}
		}
	}
}

// process2 处理mt tm
func (sg *Strategy) process2(ctx context.Context, spot *exchange.Depth, swap1 *exchange.Depth, swap2 *exchange.Depth,
	spotC exchange.Client, swapC1 exchange.SwapClient, swapC2 exchange.SwapClient, cfg config.Config, mode rate.RunMode) {

	if !orderBookIsValid(swap1) || !orderBookIsValid(swap2) {
		return
	}
	if locked := sg.tryTradingLock(mode); !locked || sg.judgeFailed() || sg.ADLProcessor.InADL {
		if locked {
			sg.unTradingLock(mode)
		}
		return
	}
	operateFail := false //开平仓是否出错
	defer func() {
		if operateFail {
			// 使用锁保护共享状态修改
			sg.slMu.Lock()
			totalFailedTimes := sg.TotalFailedTimes
			sg.slMu.Unlock()
			if totalFailedTimes == 3 {
				if err := message.SendP3Important(ctx, message.NewCommonMsgWithAt(fmt.Sprintf("%s 开平仓连续失败达3次", sg.swapS1), `请关闭程序，登录账户手动处理,处理完成重启程序`)); err != nil {
					level.Warn(sg.logger).Log("message", "send lark failed", "err", err.Error())
				}
			}
			sg.checkBalance(ctx, spotC, swapC1, swapC2)
			if err := sg.serialize(); err != nil { // 有错误再序列化一下
				level.Warn(sg.logger).Log("message", "store to file fail", "error", err.Error())
			}
		}
		sg.unTradingLock(mode)
	}()

	openFunc := func(srOpen float64, openTradeVolume float64, side1 ccexgo.OrderSide) {
		swap1Side := acts.CcexSide2Acts(side1)
		var swap2Side apis.OrderSide
		if swap1Side == apis.OrderSideBuy {
			swap2Side = apis.OrderSideSell
		} else {
			swap2Side = apis.OrderSideBuy
		}
		if !sg.shareBanCanOrder(swap1Side, swap2Side) {
			return
		}
		// 检查一下本地poss及仓位, 发现一边为0, 一边不是0, 和要操作的方向相反就先清除仓位
		if clear, err := sg.checkLocalPosClear(ctx, swapC1, swapC2, swap1, swap2, side1); err != nil {
			level.Warn(sg.logger).Log("message", "check local pos clear error", "error", err.Error())
			message.SendP3Important(ctx, message.NewCommonMsg(fmt.Sprintf("%s 自动清理仓位失败", sg.swapS1), err.Error()))
			// 使用锁保护forbiddenTime修改
			sg.slMu.Lock()
			sg.forbiddenTime = time.Now().Add(time.Minute * 1)
			sg.slMu.Unlock()
			return
		} else if clear {
			// 清理仓位后, 等待300ms, 让ws缓存更新
			time.Sleep(time.Millisecond * 300)
			return
		}

		placeSide := side1
		if mode == rate.Swap1TSwap2M {
			placeSide = reverseSide(side1)
		}

		op, err := sg.openAndHedge(ctx, spotC, swapC1, swapC2, srOpen, openTradeVolume, spot, swap1, swap2, mode, placeSide, &cfg)

		if err != nil {
			errStr := err.Error()
			level.Warn(sg.logger).Log("message", "open position error", "error", errStr)
			var (
				sendP1, sendP3 bool
				msg            message.Message
			)
			// if op != nil {
			// 	usdt, _ := op.USDT().Float64() // 加上开仓敞口
			// 	sg.CurrentPos += usdt
			// }
			msg = message.NewOperateFail(fmt.Sprintf("%s 开仓失败(%s)[%s]", sg.swapS1, placeSide, mode), errStr)
			if !strings.Contains(errStr, "订单会立即成交") && !strings.Contains(errStr, "成交价值") && !strings.Contains(errStr, "等待自动处理") {
				operateFail = true
				// 使用锁保护失败次数修改
				if strings.Contains(errStr, "账户余额不足") || strings.Contains(errStr, "初始杠杆下的最大值") || strings.Contains(errStr, "平台仓位限制") {
					sg.slMu.Lock()
					sg.TotalFailedTimes = 3
					sg.slMu.Unlock()
				} else {
					sg.addFailed()
				}
				// MinNotional 大概率不需要发送重要消息
				if errorToP3(errStr) {
					sendP1 = true
					sendP3 = true
				}
			} else { // 有可能有两个错误
				if errorToP3(errStr) {
					operateFail = true
					if strings.Contains(errStr, "账户余额不足") || strings.Contains(errStr, "初始杠杆下的最大值") || strings.Contains(errStr, "平台仓位限制") {
						sg.slMu.Lock()
						sg.TotalFailedTimes = 3
						sg.slMu.Unlock()
					}
					sendP1 = true
					sendP3 = true
				}
			}
			if strings.Contains(errStr, `人工处理`) {
				sendP1 = true
				msg = message.NewCommonMsgWithImport(fmt.Sprintf("%s 开仓失败(%s)[%s]", sg.swapS1, placeSide, mode), errStr)
			}
			if sendP1 {
				if sendP3 {
					message.SendP3Important(ctx, msg)
				} else {
					message.SendImportant(ctx, msg)
				}
			} else {
				message.Send(ctx, msg)
			}

			return
		}
		sg.resetFailed()
		if op == nil { // 成交为0退出
			return
		}
		ccy1, ccy2 := op.Ccy()
		// 使用锁保护仓位状态修改
		sg.slMu.Lock()
		sg.CurrentPos1 += ccy1.InexactFloat64()
		sg.CurrentPos2 += ccy2.InexactFloat64()
		sg.slMu.Unlock()
		if err := sg.serialize(); err != nil {
			level.Warn(sg.logger).Log("message", "store to file fail", "error", err.Error())
		}
		fields := op.Fields()
		// fields = append(fields, [2]string{"open_limit", strconv.Itoa(config.VolumeOpenLimit)})
		sg.record(ctx, openCsv, fields...)
	}

	closeFunc := func(srClose float64, closeTradeUnit float64, side1 ccexgo.OrderSide) {
		swap1Side := acts.CcexSide2Acts(side1)
		var swap2Side apis.OrderSide
		if swap1Side == apis.OrderSideCloseLong {
			swap2Side = apis.OrderSideCloseShort
		} else {
			swap2Side = apis.OrderSideCloseLong
		}
		if !sg.shareBanCanOrder(swap1Side, swap2Side) {
			return
		}

		// 判断一下是否还有可平仓位数量
		placeSide := side1
		// swapName := "swap1"
		if mode == rate.Swap1TSwap2M {
			placeSide = reverseSide(side1)
			// swapName = "swap2"
		}

		_, success := sg.PosList.TryFreezeCloseAmount(decimal.NewFromFloat(closeTradeUnit), false)
		if !success {
			return
		}

		op, err := sg.closeAndHedge(ctx, spotC, swapC1, swapC2, srClose, closeTradeUnit, swap1, swap2, mode, placeSide, &cfg)

		if err != nil {
			errStr := err.Error()
			level.Warn(sg.logger).Log("message", "open position error", "error", errStr)
			var (
				sendP1, sendP3 bool
				msg            message.Message
			)

			msg = message.NewOperateFail(fmt.Sprintf("%s 平仓失败(%s)[%s]", sg.swapS1, placeSide, mode), errStr)
			if !strings.Contains(errStr, "订单会立即成交") && !strings.Contains(errStr, "跳过") && !strings.Contains(errStr, "成交价值") &&
				!strings.Contains(errStr, "无法平仓") && !strings.Contains(errStr, "等待自动处理") {
				operateFail = true
				// 使用锁保护失败次数修改
				if strings.Contains(errStr, "账户余额不足") { // 不能再操作, 会造成敞口
					sg.slMu.Lock()
					sg.TotalFailedTimes = 3
					sg.slMu.Unlock()
				} else {
					sg.addFailed()
				}
				if errorToP3(errStr) {
					sendP1 = true
					sendP3 = true
				}
			} else {
				if errorToP3(errStr) {
					if strings.Contains(errStr, "账户余额不足") { // 不能再操作, 会造成敞口
						sg.slMu.Lock()
						sg.TotalFailedTimes = 3
						sg.slMu.Unlock()
					}
					sendP1 = true
					sendP3 = true
				}
			}

			if strings.Contains(errStr, `人工处理`) {
				sendP1 = true
				msg = message.NewCommonMsgWithImport(fmt.Sprintf("%s 平仓失败(%s)[%s]", sg.swapS1, placeSide, mode), errStr)
			}
			if sendP1 {
				if sendP3 {
					message.SendP3Important(ctx, msg)
				} else {
					message.SendImportant(ctx, msg)
				}
			} else {
				message.Send(ctx, msg)
			}
			if op != nil {
				ccy1, ccy2 := op.Ccy() // 默认平仓成功
				// 使用锁保护仓位状态修改
				sg.slMu.Lock()
				sg.CurrentPos1 -= ccy1.InexactFloat64()
				sg.CurrentPos2 -= ccy2.InexactFloat64()
				sg.slMu.Unlock()
			}
			return
		}
		if op == nil {
			return //no operate
		}
		sg.resetFailed()

		ccy1, ccy2 := op.Ccy()
		// 使用锁保护仓位状态修改
		sg.slMu.Lock()
		sg.CurrentPos1 -= ccy1.InexactFloat64()
		sg.CurrentPos2 -= ccy2.InexactFloat64()
		sg.slMu.Unlock()

		if err := sg.serialize(); err != nil {
			level.Warn(sg.logger).Log("message", "store to file fail", "error", err.Error())
		}

		fields := op.Fields()
		// fields = append(fields, [2]string{"close_limit", strconv.Itoa(config.VolumeCloseLimit)})
		sg.record(ctx, closeCsv, fields...)
	}

	swap1Mid := (swap1.Asks[0].Price + swap1.Bids[0].Price) / 2
	swap2Mid := (swap2.Asks[0].Price + swap2.Bids[0].Price) / 2

	sgPos := sg.PosList.Position()
	if cfg.OpenAllowed {
		// 加限制
		// buy := true
		// if config.MakerType == 1 || config.MakerType == 2 {
		// 	if swap1Mid-swap1.Bids[0].Price >= float64(config.SpreadLimitNum1)*sg.swapS1.PricePrecision().InexactFloat64() {
		// 		buy = false
		// 	}
		// 	if config.MakerType == 2 && buy {
		// 		if swap1.Bids[0].Price-swap1.Bids[1].Price >= float64(config.SpreadLimitNum2)*sg.swapS1.PricePrecision().InexactFloat64() {
		// 			buy = false
		// 		}
		// 	}
		// }
		// if buy {
		// 使用锁保护仓位状态读取
		sg.slMu.Lock()
		currentPos1 := sg.CurrentPos1
		currentPos2 := sg.CurrentPos2
		sg.slMu.Unlock()

		volumeOpenMin := cfg.MTVolumeOpenMin
		if mode == rate.Swap1TSwap2M {
			volumeOpenMin = cfg.TMVolumeOpenMin
		}

		// https://digifinex.sg.larksuite.com/wiki/CoC8wQEKqi9S4JkvTUslORgOgch
		buy := ((cfg.LimitPosShort-currentPos1)*swap1Mid >= 1.1*volumeOpenMin) && ((cfg.LimitPosShort-currentPos2)*swap2Mid >= 1.1*volumeOpenMin)
		if !sg.inverse {
			volumeOpenMin = volumeOpenMin * swap1Mid
			if mode == rate.Swap1MSwap2M {
				volumeOpenMin = cfg.MmOpenVolume
			}
			buy = (cfg.LimitPosShort-currentPos1 >= 1.1*volumeOpenMin) && ((cfg.LimitPosShort - currentPos2) >= 1.1*volumeOpenMin)
			// 增加二层限制
			if decimal.Max(sgPos.Swap1LongPos.PosAmount, sgPos.Swap2ShortPos.PosAmount).Mul(decimal.NewFromFloat(max(swap1.Asks[0].Price, swap2.Bids[0].Price))).Div(decimal.NewFromInt(int64(cfg.Level1))).GreaterThanOrEqual(decimal.NewFromFloat(cfg.LimitPosShort)) {
				buy = false
			}
		}
		var skip bool
		if sg.PosList.DealSide(true) == ccexgo.OrderSideBuy && !buy {
			skip = true
		}
		if skip {
			goto close
		}
		if ok, sr, tradeVolume, side := sg.canOpen(cfg, swap1, swap2, mode); ok { // 返回buy数量，根据仓位判断是swap1做多还是平空
			if side == ccexgo.OrderSideBuy { // swap1开多, swap2开空 做空价差
				// if decimal.NewFromFloat(sg.fundingRate1.FundingRate).GreaterThanOrEqual(decimal.NewFromFloat(sg.fundingRate2.FundingRate)) {
				// 	level.Info(sg.logger).Log("message", "open long forbid due to bad funding", "funding1", sg.fundingRate1.FundingRate,
				// 		"funding2", sg.fundingRate2.FundingRate)
				// } else {
				openFunc(sr, tradeVolume, ccexgo.OrderSideBuy)
				return
				// }
			} else { // swap1平空 swap2平多 平多价差 buy
				if !float.Equal(sg.CurrentPos1, 0) && sg.CurrentPos1 > 0 {
					closeFunc(sr, tradeVolume, ccexgo.OrderSideCloseShort)
					return
				}
			}
		}
		// }
	}

close:
	if cfg.CloseAllowed { // 开仓和平仓都有
		// 加限制
		// sell := true
		// if config.MakerType == 1 || config.MakerType == 2 {
		// 	if swap1.Asks[0].Price-swap1Mid >= float64(config.SpreadLimitNum1)*sg.swapS1.PricePrecision().InexactFloat64() {
		// 		sell = false
		// 	}
		// 	if config.MakerType == 2 && sell {
		// 		if swap1.Asks[1].Price-swap1.Asks[0].Price >= float64(config.SpreadLimitNum2)*sg.swapS1.PricePrecision().InexactFloat64() {
		// 			sell = false
		// 		}
		// 	}
		// }
		// if sell {
		sg.slMu.Lock()
		currentPos1 := sg.CurrentPos1
		currentPos2 := sg.CurrentPos2
		sg.slMu.Unlock()

		volumeCloseMin := cfg.MTVolumeCloseMin
		if mode == rate.Swap1TSwap2M {
			volumeCloseMin = cfg.TMVolumeCloseMin
		}

		sell := ((cfg.LimitPosLong-currentPos1)*swap1Mid >= 1.1*volumeCloseMin) && ((cfg.LimitPosLong-currentPos2)*swap2Mid >= 1.1*volumeCloseMin)
		if !sg.inverse {
			volumeCloseMin = volumeCloseMin * swap1Mid
			if mode == rate.Swap1MSwap2M {
				volumeCloseMin = cfg.MmCloseVolume
			}
			sell = ((cfg.LimitPosLong - sg.CurrentPos1) >= 1.1*volumeCloseMin) && ((cfg.LimitPosLong - sg.CurrentPos2) >= 1.1*volumeCloseMin)
			// 增加二层限制
			if decimal.Max(sgPos.Swap1ShortPos.PosAmount, sgPos.Swap2LongPos.PosAmount).Mul(decimal.NewFromFloat(max(swap1.Bids[0].Price, swap2.Asks[0].Price))).Div(decimal.NewFromInt(int64(cfg.Level1))).GreaterThanOrEqual(decimal.NewFromFloat(cfg.LimitPosLong)) {
				sell = false
			}
		}
		var skip bool
		if sg.PosList.DealSide(false) == ccexgo.OrderSideSell && !sell {
			skip = true
		}
		if skip {
			return
		}
		if ok, sr, tradeVolume, side := sg.canClose(cfg, swap1, swap2, mode); ok { // 返回sell数量，根据仓位判断是swap1做空还是平多
			if side == ccexgo.OrderSideSell { // swap1开空, swap2开多 做多价差
				// if decimal.NewFromFloat(sg.fundingRate1.FundingRate).LessThan(decimal.NewFromFloat(sg.fundingRate2.FundingRate)) {
				// 	level.Info(sg.logger).Log("message", "open short forbid due to bad funding", "funding1", sg.fundingRate1.FundingRate,
				// 		"funding2", sg.fundingRate2.FundingRate)
				// } else {
				openFunc(sr, tradeVolume, ccexgo.OrderSideSell)
				return
				// }
			} else { // swap1平多, swap2平空 平掉做空价差
				if !float.Equal(sg.CurrentPos1, 0) && sg.CurrentPos1 > 0 {
					closeFunc(sr, tradeVolume, ccexgo.OrderSideCloseLong)
					return
				}
			}
		}
		// }
	}
}

func (sg *Strategy) processExposure(ctx context.Context, swapC1 exchange.SwapClient, swapC2 exchange.SwapClient) {
	if sg.ADLProcessor.InADL || sg.ADLProcessor.ADLNotFinish || time.Since(sg.lastProcessExTime).Seconds() < 30 {
		return
	}

	if !sg.tryOperationLock() {
		return
	}
	defer sg.unOperationLock()

	config := sg.load.Load()

	sgPos := sg.PosList.Position()

	var processSwapEx bool
	// change := sg.PosList.CheckExceptionExposure(ctx)

	defer func() {
		if processSwapEx {
			if err := sg.serialize(); err != nil {
				level.Warn(sg.logger).Log("message", "process store to file fail", "error", err.Error())
			}
		}
	}()

	swap1ExposureValue := sgPos.TotalSwap1Exposure.Abs()
	swap2ExposureValue := sgPos.TotalSwap2Exposure.Abs()
	if sg.inverse {
		swap1ExposureValue = swap1ExposureValue.Mul(sg.swapS1.ContractVal())
		swap2ExposureValue = swap2ExposureValue.Mul(sg.swapS2.ContractVal())
	} else {
		swap1Price := sg.swap1OrderBook.Bids[0].Price
		swap2Price := sg.swap2OrderBook.Bids[0].Price
		swap1ExposureValue = swap1ExposureValue.Mul(decimal.NewFromFloat(swap1Price))
		swap2ExposureValue = swap2ExposureValue.Mul(decimal.NewFromFloat(swap2Price))
	}

	if swap1ExposureValue.GreaterThan(decimal.NewFromFloat(config.TotalExThreshold)) || swap2ExposureValue.GreaterThan(decimal.NewFromFloat(config.TotalExThreshold)) {
		processSwapEx = true
	}

	// 平仓可以不用考虑价值, 但至少要大于最小下单数量
	if (sgPos.TotalSwap1Exposure.IsPositive() && sgPos.TotalSwap1Exposure.GreaterThanOrEqual(decimal.NewFromFloat(sg.minOrderAmount1))) ||
		(sgPos.TotalSwap2Exposure.IsPositive() && sgPos.TotalSwap2Exposure.GreaterThanOrEqual(decimal.NewFromFloat(sg.minOrderAmount2))) {
		processSwapEx = true
	}

	// 尝试触发敞口检测, 如果当前有仓位, 则触发敞口检测, 触发自动清理仓位
	if !sgPos.TotalExposure.IsZero() || sg.CurrentPos1 != 0 || sg.CurrentPos2 != 0 {
		if (!sgPos.Swap1ShortPos.PosAmount.IsZero() && sgPos.Swap2LongPos.PosAmount.IsZero()) ||
			(sgPos.Swap1ShortPos.PosAmount.IsZero() && !sgPos.Swap2LongPos.PosAmount.IsZero()) {
			processSwapEx = true
		}

		if (!sgPos.Swap1LongPos.PosAmount.IsZero() && sgPos.Swap2ShortPos.PosAmount.IsZero()) ||
			(sgPos.Swap1LongPos.PosAmount.IsZero() && !sgPos.Swap2ShortPos.PosAmount.IsZero()) {
			processSwapEx = true
		}
	}

	if processSwapEx {
		checkClear, err := sg.PosList.ProcessSwapExposure(ctx, swapC1, swapC2, sg.swap1OrderBook, sg.swap2OrderBook, &config)
		sg.lastProcessExTime = time.Now()
		if err != nil {
			sg.addFailed()
			return
		}
		if checkClear { // 虚构side, 清仓
			var side1 ccexgo.OrderSide
			if (sgPos.Swap1LongPos.PosAmount.IsZero() && !sgPos.Swap2ShortPos.PosAmount.IsZero()) ||
				(!sgPos.Swap1LongPos.PosAmount.IsZero() && sgPos.Swap2ShortPos.PosAmount.IsZero()) {
				side1 = ccexgo.OrderSideSell
			} else if (sgPos.Swap1ShortPos.PosAmount.IsZero() && !sgPos.Swap2LongPos.PosAmount.IsZero()) ||
				(!sgPos.Swap1ShortPos.PosAmount.IsZero() && sgPos.Swap2LongPos.PosAmount.IsZero()) {
				side1 = ccexgo.OrderSideBuy
			} else if sgPos.Swap1LongPos.PosAmount.IsZero() && sgPos.Swap1ShortPos.PosAmount.IsZero() &&
				sgPos.Swap2LongPos.PosAmount.IsZero() && sgPos.Swap2ShortPos.PosAmount.IsZero() { // 可能平完仓位后还有敞口数据
				sg.slMu.Lock()
				sg.CurrentPos1 = 0
				sg.CurrentPos2 = 0
				sg.TotalFailedTimes = 0
				sg.slMu.Unlock()
				sg.PosList.ClearPos(ctx, swapC1, swapC2, side1, true)
				return
			}
			level.Info(sg.logger).Log("message", "processExposure check local pos clear", "side1", side1)
			// 检查一下本地poss及仓位, 发现一边为0, 一边不是0, 和要操作的方向相反就先清除仓位
			if clear, err := sg.checkLocalPosClear(ctx, swapC1, swapC2, sg.swap1OrderBook, sg.swap2OrderBook, side1); err != nil {
				level.Warn(sg.logger).Log("message", "processExposure check local pos clear error", "error", err.Error())
				message.SendP3Important(ctx, message.NewCommonMsg(fmt.Sprintf("%s 自动清理仓位失败", sg.swapS1), err.Error()))
				// 使用锁保护forbiddenTime修改
				sg.slMu.Lock()
				sg.forbiddenTime = time.Now().Add(time.Minute * 1)
				sg.slMu.Unlock()
				return
			} else if clear {
				// 清理仓位后, 等待300ms, 让ws缓存更新
				time.Sleep(time.Millisecond * 300)
				return
			}
		}
	}

}

// limit开仓单并检查对冲
func (sg *Strategy) openAndHedge(ctx context.Context, spotC exchange.Client, swapC1 exchange.SwapClient, swapC2 exchange.SwapClient,
	srOpen float64, openTradeVolume float64, spot *exchange.Depth, swap1 *exchange.Depth, swap2 *exchange.Depth, mode rate.RunMode,
	side ccexgo.OrderSide, cfg *config.Config) (*position.OrderResult, error) {
	// config := sg.load.Load()
	// if mode == rate.Swap1TSwap2M {
	// var posValue decimal.Decimal
	// if sg.isversed { // 币本位
	// 	posValue = sg.PosList.Position().Amount.Mul(sg.swapS.ContractVal())
	// } else {
	// 	posValue = sg.PosList.Position().SwapAmount
	// }
	// if posValue.GreaterThan(decimal.NewFromFloat(config.SwitchSwapThreshold)) {
	// 	return sg.openSwapAndHedge(ctx, spotC, swapC, srOpen, openTradeVolume, spot, swap)
	// }
	// }
	if mode == rate.Swap1MSwap2M {
		return sg.openSwap1Swap2(ctx, swapC1, swapC2, srOpen, openTradeVolume, swap1, swap2, side)
	}
	return sg.openSwap1AndHedge(ctx, spotC, swapC1, swapC2, srOpen, openTradeVolume, swap1, swap2, side, cfg, mode)
}

// limit平仓单并检查对冲
func (sg *Strategy) closeAndHedge(ctx context.Context, spotC exchange.Client, swapC1 exchange.SwapClient, swapC2 exchange.SwapClient,
	srClose float64, closeTradeVolume float64, swap1 *exchange.Depth, swap2 *exchange.Depth, mode rate.RunMode,
	side ccexgo.OrderSide, cfg *config.Config) (*position.OrderResult, error) {
	// if mode == rate.Swap1TSwap2M {
	// return sg.closeSwapAndHedge(ctx, spotC, swapC1, srClose, closeTradeVolume, spot, swap)
	// }
	if mode == rate.Swap1MSwap2M {
		return sg.closeSwap1Swap2(ctx, swapC1, swapC2, srClose, closeTradeVolume, swap1, swap2, side)
	}
	return sg.closeSwapAndHedge(ctx, spotC, swapC1, swapC2, srClose, closeTradeVolume, swap1, swap2, side, cfg, mode)
}

func (sg *Strategy) storePath() string {
	return fmt.Sprintf("%s.data", sg.spotS)
}

func (sg *Strategy) resetFailed() {
	// sg.lastFailedTime = time.Time{} // reset
	sg.slMu.Lock()
	if sg.TotalFailedTimes < 3 {
		sg.TotalFailedTimes = 0
	}
	sg.slMu.Unlock()
}

func (sg *Strategy) addFailed() {
	// sg.lastFailedTime = time.Now() // reset
	sg.slMu.Lock()
	if sg.TotalFailedTimes < 3 {
		sg.TotalFailedTimes += 1
	}
	sg.slMu.Unlock()
}

func (sg *Strategy) judgeFailed() bool {
	sg.slMu.Lock()
	failedTimes := sg.TotalFailedTimes
	forbiddenTime := sg.forbiddenTime
	sg.slMu.Unlock()

	if failedTimes >= 3 {
		// level.Info(sg.logger).Log("message", "open and close failed over three times, skip")
		return true
	}

	// if !sg.lastFailedTime.IsZero() && time.Since(sg.lastFailedTime) < 5*time.Minute {
	// level.Info(sg.logger).Log("message", "last failed time must wait five min, skip")
	// return true
	// }
	//

	if !forbiddenTime.IsZero() && time.Now().Before(forbiddenTime) {
		return true
	}

	return sg.PosList.Banned()
}

func (sg *Strategy) serialize() error {
	sg.slMu.Lock()
	defer func() {
		// json.Marshal可能会panic, recover不至于程序崩溃，开定时协程再次序列化应该问题不大不丢数据
		if r := recover(); r != nil {
			level.Error(sg.logger).Log("message", "serialize recover", "err", fmt.Sprintf("%+v", r))
			if sg.serializeTimer != nil {
				sg.serializeTimer.Stop()
			}
			sg.serializeTimer = time.AfterFunc(100*time.Millisecond, func() { sg.serialize() })
		}
		sg.slMu.Unlock()
	}()

	raw, err := json.Marshal(sg)
	if err != nil {
		return errors.WithMessage(err, "marshal to json fail")
	}

	if err := os.WriteFile(sg.storePath(), raw, 0644); err != nil {
		return errors.WithMessage(err, "write to file fail")
	}
	return nil
}

// flushSerialize 确保最终序列化成功，带重试机制
func (sg *Strategy) flushSerialize() {
	// 停止所有待处理的定时器
	if sg.serializeTimer != nil {
		sg.serializeTimer.Stop()
		sg.serializeTimer = nil
	}

	// 重试序列化，最多重试9次，避免CPU问题
	maxRetries := 9
	for i := range maxRetries {
		if err := sg.serialize(); err != nil {
			level.Warn(sg.logger).Log("message", "final serialize attempt failed", "attempt", i+1, "error", err.Error())
			if i < maxRetries-1 {
				// 短暂休眠，避免CPU问题，但不要太长影响退出速度
				time.Sleep(10 * time.Millisecond)
			}
		} else {
			// 序列化成功，退出
			return
		}
	}

	sg.slMu.Lock()
	defer sg.slMu.Unlock()

	// 停止所有待处理的定时器
	if sg.serializeTimer != nil {
		sg.serializeTimer.Stop()
		sg.serializeTimer = nil
	}

	// 如果所有重试都失败，记录错误但不阻塞退出， 数据记录到日志中避免丢失
	raw, err := json.Marshal(sg)
	if err != nil {
		level.Error(sg.logger).Log("message", "final serialize failed after all retries", "json_error", err.Error())
		message.SendP3Important(context.Background(), message.NewCommonMsgWithImport(fmt.Sprintf("%s 数据序列化失败", sg.swapS1), fmt.Sprintf("序列化到数据文件失败并且json写入日志序列化失败, error=%s", err.Error())))
		return
	}
	level.Warn(sg.logger).Log("message", "final serialize failed after all retries", "raw", string(raw))
}

func (sg *Strategy) unserialize() error {
	raw, err := os.ReadFile(sg.storePath())
	if err != nil {
		if !os.IsNotExist(err) {
			return errors.WithMessage(err, "read file fail")
		}
		return nil
	}

	if err := json.Unmarshal(raw, sg); err != nil {
		return errors.WithMessage(err, "unmarshal json fail")
	}
	sg.TotalFailedTimes = 0
	sg.ContractVal1 = sg.swapS1.ContractVal().InexactFloat64()
	sg.ContractVal2 = sg.swapS2.ContractVal().InexactFloat64()
	return nil
}

func (sg *Strategy) newCSV(typ string) (*csvOutput, error) {
	fileName := fmt.Sprintf("%s.%s.csv", sg.spotS, typ)
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_RDWR, 0644)
	wh := true
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, errors.WithMessage(err, "open file fail")
		}

		file, err = os.OpenFile(fileName, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, errors.WithMessage(err, "openfile fail")
		}
		wh = false
	}
	// 看看有没有数据
	cr := csv.NewReader(file)
	_, err = cr.Read()
	if err == io.EOF { // 读一条报EOF,说明没有数据，需要写header
		wh = false
	}

	return &csvOutput{
		file:        file,
		writeHeader: wh,
		csv:         csv.NewWriter(file),
	}, nil
}

func (co *csvOutput) Close() {
	co.mu.Lock()
	defer co.mu.Unlock()

	co.csv.Flush()
	co.file.Close()
}

func (co *csvOutput) Write(data [][2]string) {
	co.mu.Lock()
	defer co.mu.Unlock()

	values := []string{}
	fileds := []string{}
	for _, elems := range data {
		fileds = append(fileds, elems[0])
		values = append(values, elems[1])
	}
	if !co.writeHeader {
		co.csv.Write(fileds)
		co.writeHeader = true
	}
	err := co.csv.Write(values)
	if err != nil {
		level.Error(logger.GetLogger()).Log("message", "csv write failed", "err", err.Error())
	}
	co.csv.Flush()
}

func (sg *Strategy) record(ctx context.Context, typ csvType, data ...[2]string) {
	var (
		title  string
		writer csvWriter
	)

	switch typ {
	case openCsv:
		writer = sg.openWriter
		title = "open"
	case closeCsv:
		writer = sg.closeWriter
		title = "close"
	case fundingCsv:
		writer = sg.fundingWriter
	default:
		return
	}

	writer.Write(data)
	if typ != fundingCsv {
		if err := message.Send(ctx, message.NewPosOperate(fmt.Sprintf("%s(%s)", sg.swapS1, sg.exchange), title, data)); err != nil {
			level.Warn(sg.logger).Log("message", "send lark failed", "err", err.Error())
		}
	}
}

func (sg *Strategy) recordConfig() {
	cfg := sg.load.Load()
	var kv map[string]interface{}
	raw, _ := json.Marshal(cfg)

	json.Unmarshal(raw, &kv)

	fields := []interface{}{"message", "record config"}
	for k, v := range kv {
		if k == "queue" {
			kv2 := v.(map[string]interface{})
			for a, b := range kv2 {
				fields = append(fields, a, b)
			}
		} else {
			fields = append(fields, k, v)
		}
	}
	level.Info(sg.logger).Log(fields...)
}

func (sg *Strategy) fundingTimer() <-chan time.Time {
	sec := rand.Intn(60)
	min := rand.Intn(10) + 1
	return time.After(time.Until(sg.FundingRate1.FundingTime.Add(time.Minute * time.Duration(min)).Add(time.Second * time.Duration(sec))))
}

func (sg *Strategy) updateFundingRate(ctx context.Context, swapC1 exchange.SwapClient, swapC2 exchange.SwapClient) error {
	// sg.mustLock()
	// defer sg.unLock()

	funding, err := swapC1.FundingRate(ctx)
	if err != nil {
		level.Warn(sg.logger).Log("message", "update funding1 rate failed", "err", err.Error())
		return errors.WithMessage(err, "update funding1 rate fail")
	}

	if sg.FundingRate1 != nil && (!float.Equal(sg.FundingRate1.FundingRate, funding.FundingRate) || !sg.FundingRate1.FundingTime.Equal(funding.FundingTime)) {
		level.Info(sg.logger).Log("message", "update funding1", "rate", funding.FundingRate, "next",
			funding.FundingTime, "old_rate", sg.FundingRate1.FundingRate)
		sg.FundingRate1 = funding
	} else {
		sg.FundingRate1 = funding
	}

	funding2, err := swapC2.FundingRate(ctx)
	if err != nil {
		level.Warn(sg.logger).Log("message", "update funding2 rate failed", "err", err.Error())
		return errors.WithMessage(err, "update funding2 rate fail")
	}

	if sg.FundingRate2 != nil && (!float.Equal(sg.FundingRate2.FundingRate, funding2.FundingRate) || !sg.FundingRate2.FundingTime.Equal(funding2.FundingTime)) {
		level.Info(sg.logger).Log("message", "update funding2", "rate", funding2.FundingRate, "next",
			funding2.FundingTime, "old_rate", sg.FundingRate2.FundingRate)
		sg.FundingRate2 = funding2
	} else {
		sg.FundingRate2 = funding2
	}
	return nil
}

// isSettlement 判断永续合约是否在交割
// 交割时间为 0:00, 8:00, 16:00, 提前一分钟认为交割持续30秒
func isSettlement() bool {
	now := time.Now()
	minute := now.Minute()

	checkMinuteSecs := func() bool {
		return now.Minute() == 0 && now.Second() < 30
	}

	switch now.Hour() {
	case 0:
		return checkMinuteSecs()

	case 7:
		return minute >= 59
	case 8:
		return checkMinuteSecs()

	case 15:
		return minute >= 59
	case 16:
		return checkMinuteSecs()

	case 23:
		return minute >= 59

	default:
		return false
	}
}

func orderBookIsValid(book *exchange.Depth) bool {
	if book == nil || len(book.Asks) < 2 || len(book.Bids) < 2 {
		return false
	}

	//判断时间是否太久
	now := time.Now()
	if book.Created.Add(depthMaxDuration).Before(now) {
		return false
	}
	//判断盘口是否出现倒挂
	return book.Asks[0].Price > book.Bids[0].Price
}

func orderBookIsValid2(book *exchange.Depth) bool {
	if book == nil || len(book.Asks) < 2 || len(book.Bids) < 2 {
		return false
	}
	return true
}

func (sg *Strategy) shareBanCanOrder(swap1Side, swap2Side apis.OrderSide) bool {
	return sg.PosList.CanOrder(swap1Side, swap2Side)
}

// 应该只是开仓时检查, 平仓时不用检查, 返回是否清仓
func (sg *Strategy) checkLocalPosClear(ctx context.Context, swapC1 exchange.SwapClient, swapC2 exchange.SwapClient, swap1 *exchange.Depth, swap2 *exchange.Depth, side1 ccexgo.OrderSide) (clear bool, err error) {
	sg.slMu.Lock()
	defer sg.slMu.Unlock()

	sgPos := sg.PosList.Position()
	if sg.CurrentPos1 <= 0 && sg.CurrentPos2 <= 0 && sgPos.TotalExposure.IsZero() { // 不操作
		return
	}

	cfg := sg.load.Load()
	valueLimit := decimal.NewFromFloat(cfg.WarnValueLimit).Mul(decimal.NewFromFloat(float64(cfg.P3Level)))

	// swap1 开多, swap2 开空
	// swap1有空仓, 且swap2没多持仓, 并且有本地 CurrentPos1 则清掉swap1空仓
	// swap2有多仓, 且swap1没空持仓, 并且有本地 CurrentPos2 则清掉swap2多仓
	// 清仓时需判断仓位价值 < valueLimit, 否则不操作
	// 判断仓位价值需要判断4个方向避免误判
	pos1LongValue := sgPos.Swap1LongPos.PosAmount.Mul(decimal.NewFromFloat(swap1.Bids[0].Price))
	pos1ShortValue := sgPos.Swap1ShortPos.PosAmount.Mul(decimal.NewFromFloat(swap1.Asks[0].Price))
	pos2LongValue := sgPos.Swap2LongPos.PosAmount.Mul(decimal.NewFromFloat(swap2.Bids[0].Price))
	pos2ShortValue := sgPos.Swap2ShortPos.PosAmount.Mul(decimal.NewFromFloat(swap2.Asks[0].Price))

	if side1 == ccexgo.OrderSideBuy {
		if (!sgPos.Swap1ShortPos.PosAmount.IsZero() && sgPos.Swap2LongPos.PosAmount.IsZero()) ||
			(sgPos.Swap1ShortPos.PosAmount.IsZero() && !sgPos.Swap2LongPos.PosAmount.IsZero()) {

			if pos1ShortValue.LessThanOrEqual(valueLimit) && pos2LongValue.LessThanOrEqual(valueLimit) && pos1LongValue.LessThanOrEqual(valueLimit) && pos2ShortValue.LessThanOrEqual(valueLimit) {
				err2 := sg.PosList.ClearPos(ctx, swapC1, swapC2, side1, false)
				if err2 == nil {
					sg.CurrentPos1 = 0
					sg.CurrentPos2 = 0
					sg.TotalFailedTimes = 0
				} else {
					err = err2
				}
			} else {
				err = fmt.Errorf("程序判断需要自动清理但仓位价值大于%su, pos1LongValue: %s, pos1ShortValue: %s, pos2LongValue: %s, pos2ShortValue: %s, 需要人为判断处理", valueLimit.String(), pos1LongValue.String(), pos1ShortValue.String(), pos2LongValue.String(), pos2ShortValue.String())
			}
			clear = true
		}
	} else {
		// swap1 开空, swap2 开多
		// swap1有多仓, 且swap2没空持仓, 并且有本地 CurrentPos1 则清掉swap1多仓
		// swap2有空仓, 且swap1没多持仓, 并且有本地 CurrentPos2 则清掉swap2空仓
		// 清仓时需判断仓位价值 < valueLimit, 否则不操作
		if (!sgPos.Swap1LongPos.PosAmount.IsZero() && sgPos.Swap2ShortPos.PosAmount.IsZero()) ||
			(sgPos.Swap1LongPos.PosAmount.IsZero() && !sgPos.Swap2ShortPos.PosAmount.IsZero()) {

			if pos1LongValue.LessThanOrEqual(valueLimit) && pos2ShortValue.LessThanOrEqual(valueLimit) && pos1ShortValue.LessThanOrEqual(valueLimit) && pos2LongValue.LessThanOrEqual(valueLimit) {
				err2 := sg.PosList.ClearPos(ctx, swapC1, swapC2, side1, false)
				if err2 == nil {
					sg.CurrentPos1 = 0
					sg.CurrentPos2 = 0
					sg.TotalFailedTimes = 0
				} else {
					err = err2
				}
			} else {
				err = fmt.Errorf("程序判断需要自动清理但仓位价值大于%su, pos1LongValue: %s, pos1ShortValue: %s, pos2LongValue: %s, pos2ShortValue: %s, 需要人为判断处理", valueLimit.String(), pos1LongValue.String(), pos1ShortValue.String(), pos2LongValue.String(), pos2ShortValue.String())
			}
			clear = true
		}
	}
	return
}

func (sg *Strategy) updateMinOrderAmount(swapC1 exchange.SwapClient, swapC2 exchange.SwapClient) {
	minOrderAmount1 := sg.swap1ActsSymbol.MinimumOrderAmount
	minOrderAmount2 := sg.swap2ActsSymbol.MinimumOrderAmount
	if swapC1.GetExchangeName() == exchange.Okex5 || swapC1.GetExchangeName() == exchange.GateIO {
		minOrderAmount1 = sg.swap1ActsSymbol.MinimumOrderAmount * sg.swap1ActsSymbol.ContractValue
	}
	if swapC2.GetExchangeName() == exchange.Okex5 || swapC2.GetExchangeName() == exchange.GateIO {
		minOrderAmount2 = sg.swap2ActsSymbol.MinimumOrderAmount * sg.swap2ActsSymbol.ContractValue
	}
	sg.minOrderAmount1 = minOrderAmount1
	sg.minOrderAmount2 = minOrderAmount2
	level.Info(sg.logger).Log("message", "update min order amount", "min_order_amount1", minOrderAmount1, "min_order_amount2", minOrderAmount2, "cv1", sg.swapS1.ContractVal(), "cv2", sg.swapS2.ContractVal())
}

func errorToP3(errStr string) bool {
	return helper.PartInArray(errStr, []string{`最小下单数量`, `持仓`, "限制", "余额", "拒绝", "名义价值", "服务器过载", "超时"})
}
