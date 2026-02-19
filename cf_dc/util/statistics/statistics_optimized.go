package statistics

import (
	"cf_arbitrage/util/logger"
	"container/heap"
	"sync"
	"time"

	"github.com/go-kit/kit/log/level"
)

// DataPointPtr 使用指针的数据点，避免重复存储
type DataPointPtr struct {
	Value     float64
	Timestamp time.Time
}

// OptimizedWindowStats 内存优化版本的滑动窗口统计
type OptimizedWindowStats struct {
	mu             sync.RWMutex    // 读写锁，保证线程安全
	points         []*DataPointPtr // 所有数据点指针按时间顺序存储
	maxHeap        *MaxHeapPtr     // 存储较小一半数据的最大堆
	minHeap        *MinHeapPtr     // 存储较大一半数据的最小堆
	windowSize     time.Duration   // 固定的时间窗口大小
	sum            float64         // 当前窗口内数据的总和
	count          int             // 当前窗口内数据的数量
	currentMedian  float64         // 当前计算的中位数
	currentMean    float64         // 当前计算的均值
	lastDataPoint  float64         // 最后一个数据点
	hasData        bool            // 是否有数据
	cleanupCounter int             // 清理计数器，用于延迟重建堆
}

// MaxHeapPtr 和 MinHeapPtr 使用指针的堆实现
type MaxHeapPtr struct {
	items      []*DataPointPtr
	timeToIdx  map[int64]int
	validItems map[int64]bool
}

type MinHeapPtr struct {
	items      []*DataPointPtr
	timeToIdx  map[int64]int
	validItems map[int64]bool
}

// 堆接口实现（指针版本）
func (h MaxHeapPtr) Len() int           { return len(h.items) }
func (h MaxHeapPtr) Less(i, j int) bool { return h.items[i].Value > h.items[j].Value } // 最大堆
func (h MaxHeapPtr) Swap(i, j int) {
	// 获取元素的时间戳键值
	iTimeKey := timeToKeyOriginal(h.items[i].Timestamp)
	jTimeKey := timeToKeyOriginal(h.items[j].Timestamp)

	// 保存有效性状态
	iValid := h.validItems[iTimeKey]
	jValid := h.validItems[jTimeKey]

	// 交换元素
	h.items[i], h.items[j] = h.items[j], h.items[i]

	// 直接更新映射，交换后 i 位置的元素原来在 j 位置，j 位置的元素原来在 i 位置
	// 注意：交换后，h.items[i]的时间戳键值对应原来的jTimeKey
	// 交换后，h.items[j]的时间戳键值对应原来的iTimeKey
	if jValid {
		h.timeToIdx[jTimeKey] = i
	}
	if iValid {
		h.timeToIdx[iTimeKey] = j
	}

	// 更新validItems映射，保持与timeToIdx一致
	if !jValid {
		delete(h.validItems, jTimeKey)
	}
	if !iValid {
		delete(h.validItems, iTimeKey)
	}
}
func (h *MaxHeapPtr) Push(x any) {
	item := x.(*DataPointPtr)
	h.items = append(h.items, item)
	h.timeToIdx[timeToKeyOriginal(item.Timestamp)] = len(h.items) - 1
	h.validItems[timeToKeyOriginal(item.Timestamp)] = true
}
func (h *MaxHeapPtr) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[0 : n-1]
	timeKey := timeToKeyOriginal(item.Timestamp)
	delete(h.timeToIdx, timeKey)
	delete(h.validItems, timeKey) // 同时删除validItems中的条目
	return item
}

func (h MinHeapPtr) Len() int           { return len(h.items) }
func (h MinHeapPtr) Less(i, j int) bool { return h.items[i].Value < h.items[j].Value } // 最小堆
func (h MinHeapPtr) Swap(i, j int) {
	// 获取元素的时间戳键值
	iTimeKey := timeToKeyOriginal(h.items[i].Timestamp)
	jTimeKey := timeToKeyOriginal(h.items[j].Timestamp)

	// 保存有效性状态
	iValid := h.validItems[iTimeKey]
	jValid := h.validItems[jTimeKey]

	// 交换元素
	h.items[i], h.items[j] = h.items[j], h.items[i]

	// 直接更新映射，交换后 i 位置的元素原来在 j 位置，j 位置的元素原来在 i 位置
	// 注意：交换后，h.items[i]的时间戳键值对应原来的jTimeKey
	// 交换后，h.items[j]的时间戳键值对应原来的iTimeKey
	if jValid {
		h.timeToIdx[jTimeKey] = i
	}
	if iValid {
		h.timeToIdx[iTimeKey] = j
	}

	// 更新validItems映射，保持与timeToIdx一致
	if !jValid {
		delete(h.validItems, jTimeKey)
	}
	if !iValid {
		delete(h.validItems, iTimeKey)
	}
}
func (h *MinHeapPtr) Push(x any) {
	item := x.(*DataPointPtr)
	h.items = append(h.items, item)
	h.timeToIdx[timeToKeyOriginal(item.Timestamp)] = len(h.items) - 1
	h.validItems[timeToKeyOriginal(item.Timestamp)] = true
}
func (h *MinHeapPtr) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[0 : n-1]
	timeKey := timeToKeyOriginal(item.Timestamp)
	delete(h.timeToIdx, timeKey)
	delete(h.validItems, timeKey) // 同时删除validItems中的条目
	return item
}

// NewOptimizedWindowStats 创建内存优化版本的滑动窗口统计实例
func NewOptimizedWindowStats(windowSize time.Duration, name string) *OptimizedWindowStats {
	if windowSize <= 0 {
		windowSize = 100 * time.Millisecond // 默认为100毫秒
	}

	maxHeap := &MaxHeapPtr{
		items:      []*DataPointPtr{},
		timeToIdx:  make(map[int64]int),
		validItems: make(map[int64]bool),
	}
	minHeap := &MinHeapPtr{
		items:      []*DataPointPtr{},
		timeToIdx:  make(map[int64]int),
		validItems: make(map[int64]bool),
	}

	heap.Init(maxHeap)
	heap.Init(minHeap)

	ows := &OptimizedWindowStats{
		points:     []*DataPointPtr{},
		maxHeap:    maxHeap,
		minHeap:    minHeap,
		windowSize: windowSize,
		sum:        0,
		count:      0,
		hasData:    false,
	}

	// go ows.loopReport(name)

	return ows
}

// Add 添加一个带当前时间戳的数据点并立即更新统计信息
func (ws *OptimizedWindowStats) Add(value float64) bool {
	return ws.AddWithTime(value, time.Now())
}

// AddWithTime 添加一个带指定时间戳的数据点并立即更新统计信息
func (ws *OptimizedWindowStats) AddWithTime(value float64, timestamp time.Time) bool {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	// 创建新数据点指针
	dp := &DataPointPtr{
		Value:     value,
		Timestamp: timestamp,
	}

	// 添加到数据点列表
	ws.points = append(ws.points, dp)

	// 首先清理过期数据
	cutoff := timestamp.Add(-ws.windowSize)
	ws.cleanupOldDataLocked(cutoff)

	// 更新总和和计数
	ws.sum += value
	ws.count++

	// 更新堆结构前，确保堆顶是有效的（因为延迟清理可能遗留无效元素）
	ws.cleanupInvalidHeapItemsLocked()

	if ws.maxHeap.Len() == 0 || value <= ws.maxHeap.items[0].Value {
		heap.Push(ws.maxHeap, dp)
	} else {
		heap.Push(ws.minHeap, dp)
	}
	ws.rebalanceHeapsLocked()

	// 更新当前统计值前，再次确保堆顶是有效的
	ws.cleanupInvalidHeapItemsLocked()
	ws.updateStatsLocked()
	ws.lastDataPoint = value

	return true
}

// cleanupOldDataLocked 清理特定时间前的数据（无锁版本，调用者需负责锁定）
func (ws *OptimizedWindowStats) cleanupOldDataLocked(cutoff time.Time) {
	if len(ws.points) == 0 {
		return
	}

	// 找到第一个不过期的数据点
	var idx int
	for idx = 0; idx < len(ws.points); idx++ {
		if !ws.points[idx].Timestamp.Before(cutoff) {
			break
		}

		// 从总和和计数中减去
		ws.sum -= ws.points[idx].Value
		ws.count--

		// 标记堆中对应的元素为无效并立即删除映射条目
		timeKey := timeToKeyOriginal(ws.points[idx].Timestamp)
		if _, exists := ws.maxHeap.timeToIdx[timeKey]; exists {
			delete(ws.maxHeap.timeToIdx, timeKey)
			delete(ws.maxHeap.validItems, timeKey)
		}
		if _, exists := ws.minHeap.timeToIdx[timeKey]; exists {
			delete(ws.minHeap.timeToIdx, timeKey)
			delete(ws.minHeap.validItems, timeKey)
		}
	}

	if idx == 0 {
		return
	}

	// 修复内存泄漏：显式释放过期指针的引用
	// 将过期的指针设置为nil，帮助GC回收
	for i := 0; i < idx; i++ {
		ws.points[i] = nil
	}

	// 更新points数组
	ws.points = ws.points[idx:]

	// 智能清理：只在必要时重建堆
	ws.smartCleanupHeapsLocked()
}

// rebalanceHeapsLocked 确保两个堆的大小平衡（无锁版本）
func (ws *OptimizedWindowStats) rebalanceHeapsLocked() {
	// 清理堆顶的无效元素
	ws.cleanupInvalidHeapItemsLocked()

	// 确保maxHeap的大小等于minHeap或比它多一个元素
	if ws.maxHeap.Len() > ws.minHeap.Len()+1 {
		// 将maxHeap的最大元素移到minHeap
		item := heap.Pop(ws.maxHeap).(*DataPointPtr)
		heap.Push(ws.minHeap, item)
	} else if ws.minHeap.Len() > ws.maxHeap.Len() {
		// 将minHeap的最小元素移到maxHeap
		item := heap.Pop(ws.minHeap).(*DataPointPtr)
		heap.Push(ws.maxHeap, item)
	}
}

// cleanupInvalidHeapItemsLocked 清理堆中无效的元素（无锁版本）
func (ws *OptimizedWindowStats) cleanupInvalidHeapItemsLocked() {
	// 清理maxHeap中的无效元素
	for ws.maxHeap.Len() > 0 {
		if !ws.maxHeap.validItems[timeToKeyOriginal(ws.maxHeap.items[0].Timestamp)] {
			heap.Pop(ws.maxHeap)
		} else {
			break
		}
	}

	// 清理minHeap中的无效元素
	for ws.minHeap.Len() > 0 {
		if !ws.minHeap.validItems[timeToKeyOriginal(ws.minHeap.items[0].Timestamp)] {
			heap.Pop(ws.minHeap)
		} else {
			break
		}
	}
}

// smartCleanupHeapsLocked 智能清理堆：只在必要时重建堆
func (ws *OptimizedWindowStats) smartCleanupHeapsLocked() {
	ws.cleanupCounter++

	// 先做懒惰清理（清理堆顶无效元素）
	ws.cleanupInvalidHeapItemsLocked()

	// 使用映射大小差异估计无效元素比例，避免遍历堆
	maxHeapTotal := ws.maxHeap.Len()
	minHeapTotal := ws.minHeap.Len()

	// 如果堆太小，不需要清理
	if maxHeapTotal < 100 && minHeapTotal < 100 {
		return
	}

	// 使用映射大小差异估计无效元素数量
	maxHeapValidItems := len(ws.maxHeap.validItems)
	minHeapValidItems := len(ws.minHeap.validItems)

	// 如果有效元素数量接近堆大小，说明无效元素很少，不需要重建
	maxHeapInvalidRatio := 0.0
	if maxHeapTotal > 0 {
		maxHeapInvalidRatio = 1.0 - float64(maxHeapValidItems)/float64(maxHeapTotal)
	}

	minHeapInvalidRatio := 0.0
	if minHeapTotal > 0 {
		minHeapInvalidRatio = 1.0 - float64(minHeapValidItems)/float64(minHeapTotal)
	}

	// 如果无效元素比例超过15%，或者计数器达到300次，才重建堆
	// 增加计数器阈值，减少重建频率
	shouldRebuild := maxHeapInvalidRatio > 0.15 || minHeapInvalidRatio > 0.15 || ws.cleanupCounter >= 300

	if shouldRebuild {
		ws.deepCleanInvalidHeapItemsLocked()
		ws.cleanupCounter = 0 // 重置计数器
	}
}

// deepCleanInvalidHeapItemsLocked 深度清理堆中所有无效元素
func (ws *OptimizedWindowStats) deepCleanInvalidHeapItemsLocked() {
	// 重建maxHeap，只保留有效元素
	validMaxItems := make([]*DataPointPtr, 0, ws.maxHeap.Len())
	// 创建新的映射
	newMaxTimeToIdx := make(map[int64]int)
	newMaxValidItems := make(map[int64]bool)

	for _, item := range ws.maxHeap.items {
		if ws.maxHeap.validItems[timeToKeyOriginal(item.Timestamp)] {
			validMaxItems = append(validMaxItems, item)
			// 更新新映射
			timeKey := timeToKeyOriginal(item.Timestamp)
			newMaxTimeToIdx[timeKey] = len(validMaxItems) - 1
			newMaxValidItems[timeKey] = true
		}
	}
	// 替换堆和映射
	ws.maxHeap.items = validMaxItems
	ws.maxHeap.timeToIdx = newMaxTimeToIdx
	ws.maxHeap.validItems = newMaxValidItems
	heap.Init(ws.maxHeap)

	// 重建minHeap，只保留有效元素
	validMinItems := make([]*DataPointPtr, 0, ws.minHeap.Len())
	// 创建新的映射
	newMinTimeToIdx := make(map[int64]int)
	newMinValidItems := make(map[int64]bool)

	for _, item := range ws.minHeap.items {
		if ws.minHeap.validItems[timeToKeyOriginal(item.Timestamp)] {
			validMinItems = append(validMinItems, item)
			// 更新新映射
			timeKey := timeToKeyOriginal(item.Timestamp)
			newMinTimeToIdx[timeKey] = len(validMinItems) - 1
			newMinValidItems[timeKey] = true
		}
	}
	// 替换堆和映射
	ws.minHeap.items = validMinItems
	ws.minHeap.timeToIdx = newMinTimeToIdx
	ws.minHeap.validItems = newMinValidItems
	heap.Init(ws.minHeap)
}

// cleanupMapsLocked 清理映射中的过期条目，防止内存泄漏
// 优化版本：只清理本次过期的条目，而不是遍历所有条目
func (ws *OptimizedWindowStats) cleanupMapsLocked() {
	// 这个函数现在被移除，因为cleanupOldDataLocked中已经处理了映射清理
	// 避免在每次清理时遍历所有映射条目，提高性能
}

// updateStatsLocked 更新当前的统计值（无锁版本）
func (ws *OptimizedWindowStats) updateStatsLocked() {
	// 更新均值
	if ws.count > 0 {
		ws.currentMean = ws.sum / float64(ws.count)
		ws.hasData = true
	} else {
		ws.currentMean = 0
		ws.hasData = false
	}

	// 更新中位数前，确保堆顶是有效的（防御性编程）
	// 清理堆顶的无效元素，确保中位数计算基于有效数据
	for ws.maxHeap.Len() > 0 && !ws.maxHeap.validItems[timeToKeyOriginal(ws.maxHeap.items[0].Timestamp)] {
		heap.Pop(ws.maxHeap)
	}
	for ws.minHeap.Len() > 0 && !ws.minHeap.validItems[timeToKeyOriginal(ws.minHeap.items[0].Timestamp)] {
		heap.Pop(ws.minHeap)
	}

	// 更新中位数
	if ws.maxHeap.Len() == 0 {
		ws.currentMedian = 0
		ws.hasData = false
	} else if ws.maxHeap.Len() > ws.minHeap.Len() {
		ws.currentMedian = ws.maxHeap.items[0].Value
	} else if ws.minHeap.Len() > 0 {
		ws.currentMedian = (ws.maxHeap.items[0].Value + ws.minHeap.items[0].Value) / 2
	} else {
		ws.currentMedian = ws.maxHeap.items[0].Value
	}
}

// GetMedian 获取当前计算好的中位数
func (ws *OptimizedWindowStats) GetMedian() (float64, bool) {
	ws.mu.RLock()
	defer ws.mu.RUnlock()

	return ws.currentMedian, ws.hasData
}

// GetMean 获取当前计算好的均值
func (ws *OptimizedWindowStats) GetMean() (float64, bool) {
	ws.mu.RLock()
	defer ws.mu.RUnlock()

	return ws.currentMean, ws.hasData
}

// GetLastDataPoint 获取最后一个数据点
func (ws *OptimizedWindowStats) GetLastDataPoint() float64 {
	ws.mu.RLock()
	defer ws.mu.RUnlock()

	return ws.lastDataPoint
}

// GetCount 获取当前窗口内的数据点数量
func (ws *OptimizedWindowStats) GetCount() int {
	ws.mu.RLock()
	defer ws.mu.RUnlock()

	return ws.count
}

// SetWindowSize 设置时间窗口的大小并立即更新统计信息
func (ws *OptimizedWindowStats) SetWindowSize(windowSize time.Duration) {
	if windowSize <= 0 || windowSize == ws.windowSize {
		return
	}

	ws.mu.Lock()
	defer ws.mu.Unlock()

	ws.windowSize = windowSize

	// 立即清理过期数据并更新统计值
	cutoff := time.Now().Add(-windowSize)
	ws.cleanupOldDataLocked(cutoff)
	ws.updateStatsLocked()
}

// Clear 清空所有数据
func (ws *OptimizedWindowStats) Clear() {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	ws.points = []*DataPointPtr{}

	ws.maxHeap = &MaxHeapPtr{
		items:      []*DataPointPtr{},
		timeToIdx:  make(map[int64]int),
		validItems: make(map[int64]bool),
	}
	ws.minHeap = &MinHeapPtr{
		items:      []*DataPointPtr{},
		timeToIdx:  make(map[int64]int),
		validItems: make(map[int64]bool),
	}

	heap.Init(ws.maxHeap)
	heap.Init(ws.minHeap)

	ws.sum = 0
	ws.count = 0
	ws.currentMedian = 0
	ws.currentMean = 0
	ws.lastDataPoint = 0
	ws.hasData = false
}

// GetWindowSize 获取当前窗口大小
func (ws *OptimizedWindowStats) GetWindowSize() time.Duration {
	ws.mu.RLock()
	defer ws.mu.RUnlock()

	return ws.windowSize
}

// fixMapsConsistencyLocked 修复映射一致性问题
func (ws *OptimizedWindowStats) fixMapsConsistencyLocked() {
	// 如果映射大小已经很接近，则不需要检查
	maxHeapTimeToIdxSize := len(ws.maxHeap.timeToIdx)
	maxHeapValidItemsSize := len(ws.maxHeap.validItems)
	minHeapTimeToIdxSize := len(ws.minHeap.timeToIdx)
	minHeapValidItemsSize := len(ws.minHeap.validItems)

	// 如果差异小于10%，则不需要检查
	maxHeapDiff := maxHeapTimeToIdxSize - maxHeapValidItemsSize
	minHeapDiff := minHeapTimeToIdxSize - minHeapValidItemsSize

	// 只有当差异超过一定比例时才进行修复
	maxHeapThreshold := maxHeapTimeToIdxSize / 10 // 10%的阈值
	minHeapThreshold := minHeapTimeToIdxSize / 10

	if maxHeapDiff > maxHeapThreshold || minHeapDiff > minHeapThreshold {
		// 检查maxHeap的映射
		for timeKey := range ws.maxHeap.timeToIdx {
			// 如果timeToIdx中有，但validItems中没有的项
			if _, exists := ws.maxHeap.validItems[timeKey]; !exists {
				// 从timeToIdx中删除
				delete(ws.maxHeap.timeToIdx, timeKey)
			}
		}

		// 检查minHeap的映射
		for timeKey := range ws.minHeap.timeToIdx {
			// 如果timeToIdx中有，但validItems中没有的项
			if _, exists := ws.minHeap.validItems[timeKey]; !exists {
				// 从timeToIdx中删除
				delete(ws.minHeap.timeToIdx, timeKey)
			}
		}

		// 检查validItems中的项是否都在timeToIdx中
		for timeKey := range ws.maxHeap.validItems {
			if _, exists := ws.maxHeap.timeToIdx[timeKey]; !exists {
				// 如果在validItems中但不在timeToIdx中，说明有不一致
				delete(ws.maxHeap.validItems, timeKey)
			}
		}

		for timeKey := range ws.minHeap.validItems {
			if _, exists := ws.minHeap.timeToIdx[timeKey]; !exists {
				// 如果在validItems中但不在timeToIdx中，说明有不一致
				delete(ws.minHeap.validItems, timeKey)
			}
		}
	}
}

func (ws *OptimizedWindowStats) loopReport(name string) {
	ticker := time.NewTicker(time.Second * 30)
	defer ticker.Stop()

	// 每3次报告才进行一次映射一致性修复，减少CPU使用
	counter := 0

	for range ticker.C {
		// 每3次报告才进行一次映射一致性修复
		if counter%3 == 0 {
			ws.mu.Lock()
			ws.fixMapsConsistencyLocked()
			ws.mu.Unlock()
		}
		counter++

		// 再获取读锁读取数据
		ws.mu.RLock()
		// 复制需要的数据
		median := ws.currentMedian
		mean := ws.currentMean
		count := ws.count
		lastDataPoint := ws.lastDataPoint
		pointsLen := len(ws.points)
		maxHeapLen := ws.maxHeap.Len()
		minHeapLen := ws.minHeap.Len()
		timeToIdxLen := len(ws.maxHeap.timeToIdx)
		validItemsLen := len(ws.maxHeap.validItems)
		timeToIdxMinLen := len(ws.minHeap.timeToIdx)
		validItemsMinLen := len(ws.minHeap.validItems)
		ws.mu.RUnlock()

		// 在锁外打印日志
		level.Info(logger.GetLogger()).Log(
			"message", "optimized window stats",
			"name", name,
			"median", median, "mean", mean,
			"count", count, "lastDataPoint", lastDataPoint,
			"len(points)", pointsLen, "len(maxHeap)", maxHeapLen,
			"len(minHeap)", minHeapLen, "len(timeToIdx)", timeToIdxLen,
			"len(validItems)", validItemsLen, "len(timeToIdx)min", timeToIdxMinLen,
			"len(validItems)min", validItemsMinLen)
	}
}
