package rate

import (
	"container/ring"
	"fmt"
	"reflect"
	"sync"
	"time"

	"cf_arbitrage/exchange"
	"cf_arbitrage/util/logger"
	"cf_arbitrage/util/statistics"

	"go.common/apis"

	ccexgo "github.com/NadiaSama/ccexgo/exchange"
	"github.com/go-kit/log/level"
	"github.com/shopspring/decimal"
	"github.com/szmcdull/glinq/gmap"
)

const (
	pushSpot = iota
	pushSwap1
	pushSwap2
)

const (
	// 时间稳定性
	SrNormal int = iota
	// 平均值
	SrMean
	// 中位数
	SrMedian
)

type (
	QueueConfig struct {
		MSecs                  int     `mapstructure:"msecs" json:"msecs"`
		OpenThreshold          float64 `mapstructure:"open_threshold" yaml:"open_threshold" json:"open_threshold"`
		CloseThreshold         float64 `mapstructure:"close_threshold" yaml:"close_threshold" json:"close_threshold"`
		MTOpenThreshold        float64 `mapstructure:"mt_open_threshold" yaml:"mt_open_threshold" json:"mt_open_threshold"`
		MTCloseThreshold       float64 `mapstructure:"mt_close_threshold" yaml:"mt_close_threshold" json:"mt_close_threshold"`
		TMOpenThreshold        float64 `mapstructure:"tm_open_threshold" yaml:"tm_open_threshold" json:"tm_open_threshold"`
		TMCloseThreshold       float64 `mapstructure:"tm_close_threshold" yaml:"tm_close_threshold" json:"tm_close_threshold"`
		MTCancelOpenThreshold  float64 `mapstructure:"mt_cancel_open_threshold" yaml:"mt_cancel_open_threshold" json:"mt_cancel_open_threshold"`
		MTCancelCloseThreshold float64 `mapstructure:"mt_cancel_close_threshold" yaml:"mt_cancel_close_threshold" json:"mt_cancel_close_threshold"`
		TMCancelOpenThreshold  float64 `mapstructure:"tm_cancel_open_threshold" yaml:"tm_cancel_open_threshold" json:"tm_cancel_open_threshold"`
		TMCancelCloseThreshold float64 `mapstructure:"tm_cancel_close_threshold" yaml:"tm_cancel_close_threshold" json:"tm_cancel_close_threshold"`

		OpenStableThreshold  float64 `mapstructure:"open_stable_threshold" yaml:"open_stable_threshold" json:"open_stable_threshold"`
		CloseStableThreshold float64 `mapstructure:"close_stable_threshold" yaml:"close_stable_threshold" json:"close_stable_threshold"`
		AbnormalThreshold    float64 `mapstructure:"abnormal_threshold" yaml:"abnormal_threshold" json:"abnormal_threshold"`

		MmOpenThreshold        float64 `mapstructure:"mm_open_threshold" yaml:"mm_open_threshold" json:"mm_open_threshold"`
		MmCancelOpenThreshold  float64 `mapstructure:"mm_cancel_open_threshold" yaml:"mm_cancel_open_threshold" json:"mm_cancel_open_threshold"`
		MmCloseThreshold       float64 `mapstructure:"mm_close_threshold" yaml:"mm_close_threshold" json:"mm_close_threshold"`
		MmCancelCloseThreshold float64 `mapstructure:"mm_cancel_close_threshold" yaml:"mm_cancel_close_threshold" json:"mm_cancel_close_threshold"`
	}

	//Queue 用于判断指定时间区间(milli second)内的sr_open, sr_close是否均大于指定阈值
	Queue interface {
		//Reset 调整时间区间及阈值大小
		Reset(cfg QueueConfig, swap1OpenTickNum, swap2OpenTickNum, swap1CloseTickNum, swap2CloseTickNum int) bool
		GetSrOpen(side apis.OrderSide, makerPrice float64, mode RunMode) float64
		GetSrClose(side apis.OrderSide, makerPrice float64, mode RunMode) float64
		GetSrOpen2(mode RunMode, srMode int) string
		GetSrClose2(mode RunMode, srMode int) string

		PushSwap1(book *exchange.Depth)
		PushSwap2(book *exchange.Depth)
		PushSpot(book *exchange.Depth)
		CanOpen(mode RunMode, srMode int) (bool, float64)
		CanClose(mode RunMode, srMode int) (bool, float64)
		CancelOpen(makerPrice float64, mode RunMode) Sr
		CancelClose(makerPrice float64, mode RunMode) Sr
		GetSpotMid() float64
		GetSwap1Mid() float64
		GetSwap2Mid() float64
		GetSpotDepth() *exchange.Depth
		GetSwap1Depth() *exchange.Depth
		GetSwap2Depth() *exchange.Depth
	}

	queueImpl struct {
		mu                     sync.RWMutex // 只保护需要并发安全的字段
		lastSwap1              *exchange.Depth
		lastSwap2              *exchange.Depth
		lastSpot               *exchange.Depth
		spotMid                float64
		swap1Mid               float64
		swap2Mid               float64
		srOpenThreshold        float64
		srCloseThreshold       float64
		mtOpenThreshold        float64
		mtCloseThreshold       float64
		tmOpenThreshold        float64
		tmCloseThreshold       float64
		mtCancelOpenThreshold  float64
		mtCancelCloseThreshold float64
		tmCancelOpenThreshold  float64
		tmCancelCloseThreshold float64
		openStableThreshold    float64
		closeStableThreshold   float64
		abnormalThreshold      float64
		mmSwap1OpenTickNum     int
		mmSwap2OpenTickNum     int
		mmSwap1CloseTickNum    int
		mmSwap2CloseTickNum    int
		mmSrOpenThreshold      float64
		mmSrCloseThreshold     float64
		TickSize1              float64
		TickSize2              float64
		maxDuration            time.Duration
		minDuration            time.Duration
		openRingTT             *rateRing //用于快速判断指定时间区间[now-maxDuration, now-minDuration]的rate是否均大于阈值
		closeRingTT            *rateRing
		openRingTM             *rateRing //用于快速判断指定时间区间[now-maxDuration, now-minDuration]的rate是否均大于阈值
		closeRingTM            *rateRing
		openRingMT             *rateRing //用于快速判断指定时间区间[now-maxDuration, now-minDuration]的rate是否均大于阈值
		closeRingMT            *rateRing
		openRingMM             *rateRing //用于快速判断指定时间区间[now-maxDuration, now-minDuration]的rate是否均大于阈值
		closeRingMM            *rateRing
		openWindows            *gmap.SyncMap[RunMode, *statistics.OptimizedWindowStats]
		closeWindows           *gmap.SyncMap[RunMode, *statistics.OptimizedWindowStats]
		config                 QueueConfig
	}

	//rateRing 用于快速判断阈值条件
	rateRing struct {
		m        sync.RWMutex
		capacity int
		rear     *ring.Ring
		front    *ring.Ring
	}

	goodSr struct {
		tt       Sr
		tm       Sr
		mt       Sr
		mm       Sr
		cancelTm Sr
		cancelMt Sr
	}

	Sr struct {
		Ok     bool
		Spread float64
	}

	RunMode string
)

var (
	Swap1TSwap2T RunMode = `Swap1TSwap2T`
	Swap1TSwap2M RunMode = `Swap1TSwap2M`
	Swap1MSwap2T RunMode = `Swap1MSwap2T`
	Swap1MSwap2M RunMode = `Swap1MSwap2M`
)

func (qc *QueueConfig) Equal(val *QueueConfig) bool {
	return reflect.DeepEqual(*qc, *val)

}

func newRateRing(size int) *rateRing {
	r := ring.New(size)
	return &rateRing{
		capacity: size,
		rear:     r,
		front:    r,
	}
}

// reset 当计算值不满足阈值条件将ring reset
func (rr *rateRing) reset() {
	rr.m.Lock()
	defer rr.m.Unlock()
	if rr.front != nil && rr.front.Value != nil {
		rr.front.Value = nil
	}
	rr.front = rr.rear
}

// push 添加最新的元素，rate需要满足阈值判断条件
func (rr *rateRing) push(ts time.Time) {
	rr.m.Lock()
	defer rr.m.Unlock()
	rr.front = rr.front.Next()
	rr.front.Value = ts
}

// pass 判断rear的时间戳是否满足[now-maxDuration, now-minDuration]
func (rr *rateRing) pass(minDuration, maxDuration time.Duration) bool {
	rr.m.Lock()
	defer func() {
		rr.m.Unlock()
		if r := recover(); r != nil {
			level.Error(logger.GetLogger()).Log("message", "queue pass recover", "err", fmt.Sprintf("%+v", r))
			return
		}
	}()

	if rr.front == rr.rear {
		return false
	}

	now := time.Now()
	newRearPos := rr.rear
	tmpPos := rr.rear.Next()
	endTs := now.Add(minDuration * -1)
	startTs := now.Add(maxDuration * -1)
	ok := false

	for {
		if tmpPos.Value == nil {
			break
		}
		ts := tmpPos.Value.(time.Time)

		if ts.After(endTs) { //数据太少
			break
		}

		if ts.Before(startTs) {
			if tmpPos == rr.front {
				break
			}

			tmpPos = tmpPos.Next()
			continue
		}
		newRearPos = tmpPos
		ok = true
		break
	}

	rr.rear = newRearPos
	return ok
}

func NewQueue(cfg QueueConfig, swap1OpenTickNum, swap2OpenTickNum, swap1CloseTickNum, swap2CloseTickNum int, tickSize1, tickSize2 float64) *queueImpl {

	openWindows := gmap.NewSyncMap[RunMode, *statistics.OptimizedWindowStats]()
	closeWindows := gmap.NewSyncMap[RunMode, *statistics.OptimizedWindowStats]()

	for _, mode := range []RunMode{Swap1TSwap2T, Swap1TSwap2M, Swap1MSwap2T, Swap1MSwap2M} {
		openWindows.Store(mode, statistics.NewOptimizedWindowStats(time.Millisecond*time.Duration(cfg.MSecs), string(mode)+"_open"))
		closeWindows.Store(mode, statistics.NewOptimizedWindowStats(time.Millisecond*time.Duration(cfg.MSecs), string(mode)+"_close"))
	}

	return &queueImpl{
		maxDuration:            time.Millisecond * time.Duration(cfg.MSecs),
		minDuration:            time.Millisecond * time.Duration(int(float64(cfg.MSecs)*0.7)),
		srOpenThreshold:        cfg.OpenThreshold,
		srCloseThreshold:       cfg.CloseThreshold,
		mtOpenThreshold:        cfg.MTOpenThreshold,
		mtCloseThreshold:       cfg.MTCloseThreshold,
		tmOpenThreshold:        cfg.TMOpenThreshold,
		tmCloseThreshold:       cfg.TMCloseThreshold,
		mtCancelOpenThreshold:  cfg.MTCancelOpenThreshold,
		mtCancelCloseThreshold: cfg.MTCancelCloseThreshold,
		tmCancelOpenThreshold:  cfg.TMCancelOpenThreshold,
		tmCancelCloseThreshold: cfg.TMCancelCloseThreshold,
		openStableThreshold:    cfg.OpenStableThreshold,
		closeStableThreshold:   cfg.CloseStableThreshold,
		abnormalThreshold:      cfg.AbnormalThreshold,
		openRingTT:             newRateRing(cfg.MSecs / 5),
		closeRingTT:            newRateRing(cfg.MSecs / 5),
		openRingTM:             newRateRing(cfg.MSecs / 5),
		closeRingTM:            newRateRing(cfg.MSecs / 5),
		openRingMT:             newRateRing(cfg.MSecs / 5),
		closeRingMT:            newRateRing(cfg.MSecs / 5),
		openRingMM:             newRateRing(cfg.MSecs / 5),
		closeRingMM:            newRateRing(cfg.MSecs / 5),
		openWindows:            openWindows,
		closeWindows:           closeWindows,
		mmSwap1OpenTickNum:     swap1OpenTickNum,
		mmSwap2OpenTickNum:     swap2OpenTickNum,
		mmSwap1CloseTickNum:    swap1CloseTickNum,
		mmSwap2CloseTickNum:    swap2CloseTickNum,
		mmSrOpenThreshold:      cfg.MmOpenThreshold,
		mmSrCloseThreshold:     cfg.MmCloseThreshold,
		TickSize1:              tickSize1,
		TickSize2:              tickSize2,
		config:                 cfg,
	}
}

func (q *queueImpl) PushSpot(book *exchange.Depth) {
	q.pushInternal(book, pushSpot)
}

func (q *queueImpl) PushSwap1(book *exchange.Depth) {
	q.pushInternal(book, pushSwap1)
}

func (q *queueImpl) PushSwap2(book *exchange.Depth) {
	q.pushInternal(book, pushSwap2)
}

func (q *queueImpl) CanOpen(mode RunMode, srMode int) (bool, float64) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if srMode != SrNormal {
		window, ok := q.openWindows.Load(mode)
		if !ok {
			return false, 0
		}
		lastDataPoint := window.GetLastDataPoint()
		srOpenThreshold := q.srOpenThreshold
		switch mode {
		case Swap1TSwap2M:
			srOpenThreshold = q.tmOpenThreshold
		case Swap1MSwap2T:
			srOpenThreshold = q.mtOpenThreshold
		case Swap1MSwap2M:
			srOpenThreshold = q.mmSrOpenThreshold
		}
		if lastDataPoint < srOpenThreshold {
			return false, 0
		}
		switch srMode {
		case SrMean:
			mean, ok := window.GetMean()
			if !ok {
				return false, 0
			}
			// level.Info(logger.GetLogger()).Log("message", "can open", "mean", mean, "lastDataPoint", lastDataPoint, "count", window.GetCount(), "mode", mode)
			return mean >= q.openStableThreshold && lastDataPoint < srOpenThreshold+q.abnormalThreshold, lastDataPoint
		case SrMedian:
			median, ok := window.GetMedian()
			if !ok {
				return false, 0
			}
			// level.Info(logger.GetLogger()).Log("message", "can open", "median", median, "lastDataPoint", lastDataPoint, "count", window.GetCount(), "mode", mode)
			return median >= q.openStableThreshold && lastDataPoint < srOpenThreshold+q.abnormalThreshold, lastDataPoint
		}
	}

	goods := q.bookIsGoodForOpen(0)
	switch mode {
	case Swap1TSwap2M:
		return q.openRingTM.pass(q.minDuration, q.maxDuration), goods.tm.Spread
	case Swap1MSwap2T:
		return q.openRingMT.pass(q.minDuration, q.maxDuration), goods.mt.Spread
	case Swap1MSwap2M:
		return q.openRingMM.pass(q.minDuration, q.maxDuration), goods.mm.Spread
	default:
		return q.openRingTT.pass(q.minDuration, q.maxDuration), goods.tt.Spread
	}
}

func (q *queueImpl) CanClose(mode RunMode, srMode int) (bool, float64) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if srMode != SrNormal {
		window, ok := q.closeWindows.Load(mode)
		if !ok {
			return false, 0
		}
		lastDataPoint := window.GetLastDataPoint()
		srCloseThreshold := q.srCloseThreshold
		switch mode {
		case Swap1TSwap2M:
			srCloseThreshold = q.tmCloseThreshold
		case Swap1MSwap2T:
			srCloseThreshold = q.mtCloseThreshold
		case Swap1MSwap2M:
			srCloseThreshold = q.mmSrCloseThreshold
		}
		if lastDataPoint >= srCloseThreshold {
			return false, 0
		}
		switch srMode {
		case SrMean:
			mean, ok := window.GetMean()
			if !ok {
				return false, 0
			}
			// level.Info(logger.GetLogger()).Log("message", "can close", "mean", mean, "lastDataPoint", lastDataPoint, "count", window.GetCount(), "mode", mode)
			return mean < q.closeStableThreshold && lastDataPoint > srCloseThreshold-q.abnormalThreshold, lastDataPoint
		case SrMedian:
			median, ok := window.GetMedian()
			if !ok {
				return false, 0
			}
			// level.Info(logger.GetLogger()).Log("message", "can close", "median", median, "lastDataPoint", lastDataPoint, "count", window.GetCount(), "mode", mode)
			return median < q.closeStableThreshold && lastDataPoint > srCloseThreshold-q.abnormalThreshold, lastDataPoint
		}
	}

	goods := q.bookIsGoodForClose(0)
	switch mode {
	case Swap1TSwap2M:
		return q.closeRingTM.pass(q.minDuration, q.maxDuration), goods.tm.Spread
	case Swap1MSwap2T:
		return q.closeRingMT.pass(q.minDuration, q.maxDuration), goods.mt.Spread
	case Swap1MSwap2M:
		return q.closeRingMM.pass(q.minDuration, q.maxDuration), goods.mm.Spread
	default:
		return q.closeRingTT.pass(q.minDuration, q.maxDuration), goods.tt.Spread
	}
}

func (q *queueImpl) CancelOpen(makerPrice float64, mode RunMode) Sr {
	q.mu.RLock()
	defer q.mu.RUnlock()
	goods := q.bookIsGoodForOpen(makerPrice)
	if mode == Swap1TSwap2M {
		return goods.cancelTm
	}
	return goods.cancelMt
}

func (q *queueImpl) CancelClose(makerPrice float64, mode RunMode) Sr {
	q.mu.RLock()
	defer q.mu.RUnlock()
	goods := q.bookIsGoodForClose(makerPrice)
	if mode == Swap1TSwap2M {
		return goods.cancelTm
	}
	return goods.cancelMt
}

func (q *queueImpl) GetSrOpen(side apis.OrderSide, makerPrice float64, mode RunMode) float64 {
	q.mu.RLock()
	defer q.mu.RUnlock()
	var srOpen float64

	if mode == Swap1MSwap2T {
		if side.IsLong() {
			srOpen = q.lastSwap2.Bids[0].Price/makerPrice - 1
		} else {
			srOpen = q.lastSwap2.Asks[0].Price/makerPrice - 1
		}
	} else {
		if side.IsLong() {
			srOpen = makerPrice/q.lastSwap1.Bids[0].Price - 1
		} else {
			srOpen = makerPrice/q.lastSwap1.Asks[0].Price - 1
		}
	}

	return srOpen
}

func (q *queueImpl) GetSrOpen2(mode RunMode, srMode int) string {
	if srMode == SrNormal {
		return ""
	}
	window, ok := q.openWindows.Load(mode)
	if !ok {
		return "NAN"
	}
	switch srMode {
	case SrMean:
		mean, ok := window.GetMean()
		if !ok {
			return "NAN"
		}
		return fmt.Sprintf("%f", mean)
	case SrMedian:
		median, ok := window.GetMedian()
		if !ok {
			return "NAN"
		}
		return fmt.Sprintf("%f", median)
	}
	return "NAN"
}

// 获取当前时间点的sr_close
func (q *queueImpl) GetSrClose(side apis.OrderSide, makerPrice float64, mode RunMode) float64 {
	q.mu.RLock()
	defer q.mu.RUnlock()
	var srClose float64

	if mode == Swap1MSwap2T {
		if side.IsShort() {
			srClose = q.lastSwap2.Asks[0].Price/makerPrice - 1
		} else {
			srClose = q.lastSwap2.Bids[0].Price/makerPrice - 1
		}
	} else {
		if side.IsShort() {
			srClose = makerPrice/q.lastSwap1.Asks[0].Price - 1
		} else {
			srClose = makerPrice/q.lastSwap1.Bids[0].Price - 1
		}
	}

	return srClose
}

func (q *queueImpl) GetSrClose2(mode RunMode, srMode int) string {
	if srMode == SrNormal {
		return ""
	}
	window, ok := q.closeWindows.Load(mode)
	if !ok {
		return "NAN"
	}
	switch srMode {
	case SrMean:
		mean, ok := window.GetMean()
		if !ok {
			return "NAN"
		}
		return fmt.Sprintf("%f", mean)
	case SrMedian:
		median, ok := window.GetMedian()
		if !ok {
			return "NAN"
		}
		return fmt.Sprintf("%f", median)
	}
	return ""
}

func (q *queueImpl) GetSpotMid() float64 {
	return q.spotMid
}

func (q *queueImpl) GetSwap1Mid() float64 {
	return q.swap1Mid
}

func (q *queueImpl) GetSwap2Mid() float64 {
	return q.swap2Mid
}

func (q *queueImpl) GetSpotDepth() *exchange.Depth {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.lastSpot
}

func (q *queueImpl) GetSwap1Depth() *exchange.Depth {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.lastSwap1
}

func (q *queueImpl) GetSwap2Depth() *exchange.Depth {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.lastSwap2
}

func (q *queueImpl) Reset(cfg QueueConfig, swap1OpenTickNum, swap2OpenTickNum, swap1CloseTickNum, swap2CloseTickNum int) bool {
	if !q.config.Equal(&cfg) || swap1OpenTickNum != q.mmSwap1OpenTickNum || swap2OpenTickNum != q.mmSwap2OpenTickNum || swap1CloseTickNum != q.mmSwap1CloseTickNum || swap2CloseTickNum != q.mmSwap2CloseTickNum {
		// 创建新的配置，但保留原有的锁
		q.mu.Lock()
		defer q.mu.Unlock()

		// 更新配置字段
		q.config = cfg
		q.maxDuration = time.Millisecond * time.Duration(cfg.MSecs)
		q.minDuration = time.Millisecond * time.Duration(int(float64(cfg.MSecs)*0.7))
		q.srOpenThreshold = cfg.OpenThreshold
		q.srCloseThreshold = cfg.CloseThreshold
		q.mtOpenThreshold = cfg.MTOpenThreshold
		q.mtCloseThreshold = cfg.MTCloseThreshold
		q.tmOpenThreshold = cfg.TMOpenThreshold
		q.tmCloseThreshold = cfg.TMCloseThreshold
		q.mtCancelOpenThreshold = cfg.MTCancelOpenThreshold
		q.mtCancelCloseThreshold = cfg.MTCancelCloseThreshold
		q.tmCancelOpenThreshold = cfg.TMCancelOpenThreshold
		q.tmCancelCloseThreshold = cfg.TMCancelCloseThreshold
		q.mmSwap1OpenTickNum = swap1OpenTickNum
		q.mmSwap2OpenTickNum = swap2OpenTickNum
		q.mmSwap1CloseTickNum = swap1CloseTickNum
		q.mmSwap2CloseTickNum = swap2CloseTickNum
		q.mmSrOpenThreshold = cfg.MmOpenThreshold
		q.mmSrCloseThreshold = cfg.MmCloseThreshold
		q.openStableThreshold = cfg.OpenStableThreshold
		q.closeStableThreshold = cfg.CloseStableThreshold
		q.abnormalThreshold = cfg.AbnormalThreshold

		rateDuration := cfg.MSecs / 5
		// 仅当capacity发生变化时重新创建
		updateRateRing := func(oldRing **rateRing, newCapacity int) {
			if (*oldRing).capacity != newCapacity {
				*oldRing = newRateRing(newCapacity)
			}
		}
		updateRateRing(&q.openRingTT, rateDuration)
		updateRateRing(&q.openRingTM, rateDuration)
		updateRateRing(&q.openRingMT, rateDuration)
		updateRateRing(&q.openRingMM, rateDuration)
		updateRateRing(&q.closeRingTT, rateDuration)
		updateRateRing(&q.closeRingTM, rateDuration)
		updateRateRing(&q.closeRingMT, rateDuration)
		updateRateRing(&q.closeRingMM, rateDuration)

		q.openWindows.Range(func(key RunMode, value *statistics.OptimizedWindowStats) bool {
			value.SetWindowSize(q.maxDuration)
			return true
		})
		q.closeWindows.Range(func(key RunMode, value *statistics.OptimizedWindowStats) bool {
			value.SetWindowSize(q.maxDuration)
			return true
		})

		return true
	}
	return false
}

func (q *queueImpl) pushInternal(book *exchange.Depth, typ int) {
	if len(book.Asks) < 2 || len(book.Bids) < 2 {
		return
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	switch typ {
	case pushSpot:
		q.lastSpot = book
		q.spotMid = (q.lastSpot.Asks[0].Price + q.lastSpot.Bids[0].Price) / 2
	case pushSwap1:
		q.lastSwap1 = book
		q.swap1Mid = (q.lastSwap1.Asks[0].Price + q.lastSwap1.Bids[0].Price) / 2
	case pushSwap2:
		q.lastSwap2 = book
		q.swap2Mid = (q.lastSwap2.Asks[0].Price + q.lastSwap2.Bids[0].Price) / 2
	}

	if q.lastSwap1 == nil || q.lastSwap2 == nil {
		return
	}

	var ts time.Time
	if q.lastSwap1.Created.Before(q.lastSwap2.Created) {
		ts = q.lastSwap2.Created
	} else {
		ts = q.lastSwap1.Created
	}

	if decimal.NewFromInt(q.lastSwap1.Created.UnixMilli() - q.lastSwap2.Created.UnixMilli()).Abs().GreaterThan(decimal.NewFromInt(q.maxDuration.Milliseconds())) { //两边时间戳差距超过配置稳定性时长，直接重置稳定性
		q.openRingTT.reset()
		q.openRingTM.reset()
		q.openRingMT.reset()
		q.openRingMM.reset()

		q.closeRingTT.reset()
		q.closeRingTM.reset()
		q.closeRingMT.reset()
		q.closeRingMM.reset()

		for _, mode := range []RunMode{Swap1TSwap2T, Swap1TSwap2M, Swap1MSwap2T, Swap1MSwap2M} {
			window, _ := q.openWindows.Load(mode)
			if window != nil {
				window.Clear()
			}
			window, _ = q.closeWindows.Load(mode)
			if window != nil {
				window.Clear()
			}
		}
		return
	}

	openGoods := q.bookIsGoodForOpen(0)
	if openGoods.tt.Ok {
		q.openRingTT.push(ts)
	} else {
		q.openRingTT.reset()
	}
	if openGoods.tm.Ok {
		q.openRingTM.push(ts)
	} else {
		q.openRingTM.reset()
	}
	if openGoods.mt.Ok {
		q.openRingMT.push(ts)
	} else {
		q.openRingMT.reset()
	}
	if openGoods.mm.Ok {
		q.openRingMM.push(ts)
	} else {
		q.openRingMM.reset()
	}

	for _, mode := range []RunMode{Swap1TSwap2T, Swap1TSwap2M, Swap1MSwap2T, Swap1MSwap2M} {
		window, _ := q.openWindows.Load(mode)
		value := openGoods.tt.Spread
		switch mode {
		case Swap1TSwap2T:
			value = openGoods.tt.Spread
		case Swap1TSwap2M:
			value = openGoods.tm.Spread
		case Swap1MSwap2T:
			value = openGoods.mt.Spread
		case Swap1MSwap2M:
			value = openGoods.mm.Spread
		}
		if window != nil {
			window.Add(value)
		}
	}

	closeGoods := q.bookIsGoodForClose(0)
	if closeGoods.tt.Ok {
		q.closeRingTT.push(ts)
	} else {
		q.closeRingTT.reset()
	}
	if closeGoods.tm.Ok {
		q.closeRingTM.push(ts)
	} else {
		q.closeRingTM.reset()
	}
	if closeGoods.mt.Ok {
		q.closeRingMT.push(ts)
	} else {
		q.closeRingMT.reset()
	}
	if closeGoods.mm.Ok {
		q.closeRingMM.push(ts)
	} else {
		q.closeRingMM.reset()
	}

	for _, mode := range []RunMode{Swap1TSwap2T, Swap1TSwap2M, Swap1MSwap2T, Swap1MSwap2M} {
		window, _ := q.closeWindows.Load(mode)
		value := closeGoods.tt.Spread
		switch mode {
		case Swap1TSwap2T:
			value = closeGoods.tt.Spread
		case Swap1TSwap2M:
			value = closeGoods.tm.Spread
		case Swap1MSwap2T:
			value = closeGoods.mt.Spread
		case Swap1MSwap2M:
			value = closeGoods.mm.Spread
		}
		if window != nil {
			window.Add(value)
		}
	}
}

// 在锁保护下调用函数
func (q *queueImpl) bookIsGoodForOpen(makerPrice float64) *goodSr {
	// swap1_mid := q.swap1Mid
	// swap2_mid := q.swap2Mid
	var (
		swap1Book ccexgo.OrderElem
		swap2Book ccexgo.OrderElem

		sr_00_mt_cancel, sr_00_tm_cancel float64
	)
	// if q.isversed {
	swap2Book = q.lastSwap2.Bids[0]
	swap1Book = q.lastSwap1.Asks[0]
	// } else {
	// 	swap2Book = q.lastSwap2.Asks[0]
	// 	swap1Book = q.lastSwap1.Bids[0]
	// }
	sr_00_tt := swap2Book.Price/swap1Book.Price - 1
	sr_00_tm := q.lastSwap2.Asks[0].Price/q.lastSwap1.Asks[0].Price - 1
	sr_00_mt := q.lastSwap2.Bids[0].Price/q.lastSwap1.Bids[0].Price - 1
	if makerPrice != 0 {
		sr_00_mt_cancel = q.lastSwap2.Bids[0].Price/makerPrice - 1
	}
	sr_00_tm_cancel = makerPrice/q.lastSwap1.Asks[0].Price - 1

	mm_swap1_price := max(q.lastSwap1.Bids[0].Price, q.lastSwap1.Asks[0].Price-float64(q.mmSwap1OpenTickNum)*q.TickSize1)
	mm_swap2_price := min(q.lastSwap2.Asks[0].Price, q.lastSwap2.Bids[0].Price+float64(q.mmSwap2OpenTickNum)*q.TickSize2)

	sr_mm := mm_swap2_price/mm_swap1_price - 1
	level.Debug(logger.GetLogger()).Log("message", "check good for open", "sr_00_tt", sr_00_tt, "sr_mm", sr_mm,
		"sr_00_tm", sr_00_tm, "sr_00_mt", sr_00_mt, "threshold", q.srOpenThreshold, "mm_threshold", q.mmSrOpenThreshold,
		"swap1_id", q.lastSwap1.ID, "swap2_id", q.lastSwap2.ID)

	return &goodSr{
		tt:       Sr{sr_00_tt >= q.srOpenThreshold, sr_00_tt},
		mt:       Sr{sr_00_mt >= q.mtOpenThreshold, sr_00_mt},
		tm:       Sr{sr_00_tm >= q.tmOpenThreshold, sr_00_tm},
		mm:       Sr{sr_mm >= q.mmSrOpenThreshold, sr_mm},
		cancelMt: Sr{sr_00_mt_cancel < q.mtCancelOpenThreshold, sr_00_mt_cancel},
		cancelTm: Sr{sr_00_tm_cancel < q.tmCancelOpenThreshold, sr_00_tm_cancel},
	}
	// if q.isversed {
	// return sr_00_tt >= q.srOpenThreshold, sr_00_tm >= q.srOpenThreshold, sr_00_mt >= q.srOpenThreshold, sr_00_tm < q.srCancelOpenThreshold, sr_00_mt < q.srCancelOpenThreshold
	// }
	// return sr_00_tt < q.srOpenThreshold, sr_00_tm < q.srOpenThreshold, sr_00_mt < q.srOpenThreshold, sr_00_tm >= q.srCancelOpenThreshold, sr_00_mt >= q.srCancelOpenThreshold
}

// 在锁保护下调用函数
func (q *queueImpl) bookIsGoodForClose(makerPrice float64) *goodSr {
	// swap1_mid := q.swap1Mid
	// swap2_mid := q.swap2Mid
	var (
		swap1Book ccexgo.OrderElem
		swap2Book ccexgo.OrderElem

		sr_00_mt_cancel, sr_00_tm_cancel float64
	)
	// if q.isversed {
	swap2Book = q.lastSwap2.Asks[0]
	swap1Book = q.lastSwap1.Bids[0]
	// } else {
	// 	swap2Book = q.lastSwap2.Bids[0]
	// 	swap1Book = q.lastSwap1.Asks[0]
	// }
	sr_00_tt := swap2Book.Price/swap1Book.Price - 1
	sr_00_tm := q.lastSwap2.Bids[0].Price/q.lastSwap1.Bids[0].Price - 1
	sr_00_mt := q.lastSwap2.Asks[0].Price/q.lastSwap1.Asks[0].Price - 1
	if makerPrice != 0 {
		sr_00_mt_cancel = q.lastSwap2.Asks[0].Price/makerPrice - 1
	}
	sr_00_tm_cancel = makerPrice/q.lastSwap1.Bids[0].Price - 1

	mm_swap1_price := min(q.lastSwap1.Asks[0].Price, q.lastSwap1.Bids[0].Price+float64(q.mmSwap1CloseTickNum)*q.TickSize1)
	mm_swap2_price := max(q.lastSwap2.Bids[0].Price, q.lastSwap2.Asks[0].Price-float64(q.mmSwap2CloseTickNum)*q.TickSize2)
	sr_mm := mm_swap2_price/mm_swap1_price - 1

	level.Debug(logger.GetLogger()).Log("message", "check good for close", "sr_00_tt", sr_00_tt, "sr_mm", sr_mm,
		"sr_00_tm", sr_00_tm, "sr_00_mt", sr_00_mt, "threshold", q.srCloseThreshold, "mm_threshold", q.mmSrCloseThreshold,
		"swap1_id", q.lastSwap1.ID, "swap2_id", q.lastSwap2.ID)

	return &goodSr{
		tt:       Sr{sr_00_tt < q.srCloseThreshold, sr_00_tt},
		mt:       Sr{sr_00_mt < q.mtCloseThreshold, sr_00_mt},
		tm:       Sr{sr_00_tm < q.tmCloseThreshold, sr_00_tm},
		mm:       Sr{sr_mm < q.mmSrCloseThreshold, sr_mm},
		cancelMt: Sr{sr_00_mt_cancel > q.mtCancelCloseThreshold, sr_00_mt_cancel},
		cancelTm: Sr{sr_00_tm_cancel > q.tmCancelCloseThreshold, sr_00_tm_cancel},
	}
	// if q.isversed {
	// return sr_00_tt <= q.srCloseThreshold, sr_00_tm <= q.srCloseThreshold, sr_00_mt <= q.srCloseThreshold, sr_00_tm > q.srCancelCloseThreshold, sr_00_mt > q.srCancelCloseThreshold
	// }
	// return sr_00_tt > q.srCloseThreshold, sr_00_tm > q.srCloseThreshold, sr_00_mt > q.srCloseThreshold, sr_00_tm <= q.srCancelCloseThreshold, sr_00_mt <= q.srCancelCloseThreshold
}
