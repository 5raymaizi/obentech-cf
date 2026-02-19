package statistics

import (
	"container/heap"
	"sync"
	"time"
)

// !仅做测试用，不要在生产环境使用

// DataPoint 表示带时间戳的数据点
type DataPoint struct {
	Value     float64
	Timestamp time.Time
}

// WindowStats 用于实时计算固定滑动窗口的统计信息
type WindowStats struct {
	mu             sync.RWMutex  // 读写锁，保证线程安全
	points         []DataPoint   // 所有数据点按时间顺序存储
	maxHeap        *MaxHeap      // 存储较小一半数据的最大堆
	minHeap        *MinHeap      // 存储较大一半数据的最小堆
	windowSize     time.Duration // 固定的时间窗口大小
	sum            float64       // 当前窗口内数据的总和
	count          int           // 当前窗口内数据的数量
	currentMedian  float64       // 当前计算的中位数
	currentMean    float64       // 当前计算的均值
	lastDataPoint  float64       // 最后一个数据点
	hasData        bool          // 是否有数据
	cleanupCounter int           // 清理计数器，用于延迟重建堆
}

// MaxHeap 和 MinHeap 实现（与之前相同）
type MaxHeap struct {
	items      []DataPoint
	timeToIdx  map[int64]int
	validItems map[int64]bool
}

type MinHeap struct {
	items      []DataPoint
	timeToIdx  map[int64]int
	validItems map[int64]bool
}

// 堆接口实现（与之前相同）
func (h MaxHeap) Len() int           { return len(h.items) }
func (h MaxHeap) Less(i, j int) bool { return h.items[i].Value > h.items[j].Value } // 最大堆
func (h MaxHeap) Swap(i, j int) {
	// 交换前先保存时间戳键值
	iTimeKey := timeToKeyOriginal(h.items[i].Timestamp)
	jTimeKey := timeToKeyOriginal(h.items[j].Timestamp)

	// 保存有效性状态
	iValid := h.validItems[iTimeKey]
	jValid := h.validItems[jTimeKey]

	// 交换元素
	h.items[i], h.items[j] = h.items[j], h.items[i]

	// 更新索引映射
	h.timeToIdx[timeToKeyOriginal(h.items[i].Timestamp)] = i
	h.timeToIdx[timeToKeyOriginal(h.items[j].Timestamp)] = j

	// 更新有效性映射
	newITimeKey := timeToKeyOriginal(h.items[i].Timestamp)
	newJTimeKey := timeToKeyOriginal(h.items[j].Timestamp)

	// 只有当元素之前是有效的，才设置为有效
	if jValid {
		h.validItems[newITimeKey] = true
	}
	if iValid {
		h.validItems[newJTimeKey] = true
	}
}
func (h *MaxHeap) Push(x any) {
	item := x.(DataPoint)
	h.items = append(h.items, item)
	h.timeToIdx[timeToKeyOriginal(item.Timestamp)] = len(h.items) - 1
	h.validItems[timeToKeyOriginal(item.Timestamp)] = true
}
func (h *MaxHeap) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[0 : n-1]
	timeKey := timeToKeyOriginal(item.Timestamp)
	delete(h.timeToIdx, timeKey)
	delete(h.validItems, timeKey) // 同时删除validItems中的条目
	return item
}

func (h MinHeap) Len() int           { return len(h.items) }
func (h MinHeap) Less(i, j int) bool { return h.items[i].Value < h.items[j].Value } // 最小堆
func (h MinHeap) Swap(i, j int) {
	// 交换前先保存时间戳键值
	iTimeKey := timeToKeyOriginal(h.items[i].Timestamp)
	jTimeKey := timeToKeyOriginal(h.items[j].Timestamp)

	// 保存有效性状态
	iValid := h.validItems[iTimeKey]
	jValid := h.validItems[jTimeKey]

	// 交换元素
	h.items[i], h.items[j] = h.items[j], h.items[i]

	// 更新索引映射
	h.timeToIdx[timeToKeyOriginal(h.items[i].Timestamp)] = i
	h.timeToIdx[timeToKeyOriginal(h.items[j].Timestamp)] = j

	// 更新有效性映射
	newITimeKey := timeToKeyOriginal(h.items[i].Timestamp)
	newJTimeKey := timeToKeyOriginal(h.items[j].Timestamp)

	// 只有当元素之前是有效的，才设置为有效
	if jValid {
		h.validItems[newITimeKey] = true
	}
	if iValid {
		h.validItems[newJTimeKey] = true
	}
}
func (h *MinHeap) Push(x any) {
	item := x.(DataPoint)
	h.items = append(h.items, item)
	h.timeToIdx[timeToKeyOriginal(item.Timestamp)] = len(h.items) - 1
	h.validItems[timeToKeyOriginal(item.Timestamp)] = true
}
func (h *MinHeap) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[0 : n-1]
	timeKey := timeToKeyOriginal(item.Timestamp)
	delete(h.timeToIdx, timeKey)
	delete(h.validItems, timeKey) // 同时删除validItems中的条目
	return item
}

// timeToKeyOriginal 将时间转换为int64键值
func timeToKeyOriginal(t time.Time) int64 {
	return t.UnixNano() // 使用纳秒精度以保证唯一性
}

// NewWindowStats 创建新的固定滑动窗口的统计实例
// windowSize: 固定的时间窗口大小
func NewWindowStats(windowSize time.Duration) *WindowStats {
	if windowSize <= 0 {
		windowSize = 100 * time.Millisecond // 默认为100毫秒
	}

	maxHeap := &MaxHeap{
		items:      []DataPoint{},
		timeToIdx:  make(map[int64]int),
		validItems: make(map[int64]bool),
	}
	minHeap := &MinHeap{
		items:      []DataPoint{},
		timeToIdx:  make(map[int64]int),
		validItems: make(map[int64]bool),
	}

	heap.Init(maxHeap)
	heap.Init(minHeap)

	return &WindowStats{
		points:     []DataPoint{},
		maxHeap:    maxHeap,
		minHeap:    minHeap,
		windowSize: windowSize,
		sum:        0,
		count:      0,
		hasData:    false,
	}
}

// Add 添加一个带当前时间戳的数据点并立即更新统计信息
func (ws *WindowStats) Add(value float64) bool {
	return ws.AddWithTime(value, time.Now())
}

// AddWithTime 添加一个带指定时间戳的数据点并立即更新统计信息
func (ws *WindowStats) AddWithTime(value float64, timestamp time.Time) bool {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	// 创建新数据点
	dp := DataPoint{
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
	ws.cleanInvalidHeapItemsLocked()

	if ws.maxHeap.Len() == 0 || value <= ws.maxHeap.items[0].Value {
		heap.Push(ws.maxHeap, dp)
	} else {
		heap.Push(ws.minHeap, dp)
	}
	ws.rebalanceHeapsLocked()

	// 更新当前统计值前，再次确保堆顶是有效的
	ws.cleanInvalidHeapItemsLocked()
	ws.updateStatsLocked()
	ws.lastDataPoint = value

	return true
}

// cleanupOldDataLocked 清理特定时间前的数据（无锁版本，调用者需负责锁定）
func (ws *WindowStats) cleanupOldDataLocked(cutoff time.Time) {
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

	// 更新points数组
	ws.points = ws.points[idx:]

	// 智能清理：只在必要时重建堆
	ws.smartCleanupHeapsLocked()
}

// rebalanceHeapsLocked 确保两个堆的大小平衡（无锁版本）
func (ws *WindowStats) rebalanceHeapsLocked() {
	// 清理堆顶的无效元素
	ws.cleanInvalidHeapItemsLocked()

	// 确保maxHeap的大小等于minHeap或比它多一个元素
	if ws.maxHeap.Len() > ws.minHeap.Len()+1 {
		// 将maxHeap的最大元素移到minHeap
		item := heap.Pop(ws.maxHeap).(DataPoint)
		heap.Push(ws.minHeap, item)
	} else if ws.minHeap.Len() > ws.maxHeap.Len() {
		// 将minHeap的最小元素移到maxHeap
		item := heap.Pop(ws.minHeap).(DataPoint)
		heap.Push(ws.maxHeap, item)
	}
}

// cleanInvalidHeapItemsLocked 清理堆中无效的元素（无锁版本）
func (ws *WindowStats) cleanInvalidHeapItemsLocked() {
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
func (ws *WindowStats) smartCleanupHeapsLocked() {
	ws.cleanupCounter++

	// 先做懒惰清理（清理堆顶无效元素）
	ws.cleanInvalidHeapItemsLocked()

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
func (ws *WindowStats) deepCleanInvalidHeapItemsLocked() {
	// 重建maxHeap，只保留有效元素
	validMaxItems := make([]DataPoint, 0, ws.maxHeap.Len())
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
	validMinItems := make([]DataPoint, 0, ws.minHeap.Len())
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

// updateStatsLocked 更新当前的统计值（无锁版本）
func (ws *WindowStats) updateStatsLocked() {
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
// 返回中位数和是否有数据的标志
func (ws *WindowStats) GetMedian() (float64, bool) {
	ws.mu.RLock()
	defer ws.mu.RUnlock()

	return ws.currentMedian, ws.hasData
}

// GetMean 获取当前计算好的均值
// 返回均值和是否有数据的标志
func (ws *WindowStats) GetMean() (float64, bool) {
	ws.mu.RLock()
	defer ws.mu.RUnlock()

	return ws.currentMean, ws.hasData
}

// GetLastDataPoint 获取最后一个数据点
func (ws *WindowStats) GetLastDataPoint() float64 {
	ws.mu.RLock()
	defer ws.mu.RUnlock()

	return ws.lastDataPoint
}

// GetCount 获取当前窗口内的数据点数量
func (ws *WindowStats) GetCount() int {
	ws.mu.RLock()
	defer ws.mu.RUnlock()

	return ws.count
}

// SetWindowSize 设置时间窗口的大小并立即更新统计信息
func (ws *WindowStats) SetWindowSize(windowSize time.Duration) {
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
func (ws *WindowStats) Clear() {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	ws.points = []DataPoint{}

	ws.maxHeap = &MaxHeap{
		items:      []DataPoint{},
		timeToIdx:  make(map[int64]int),
		validItems: make(map[int64]bool),
	}
	ws.minHeap = &MinHeap{
		items:      []DataPoint{},
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
func (ws *WindowStats) GetWindowSize() time.Duration {
	ws.mu.RLock()
	defer ws.mu.RUnlock()

	return ws.windowSize
}
