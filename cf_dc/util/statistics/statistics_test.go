package statistics

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

// BenchmarkMemoryUsage 内存使用对比测试
func BenchmarkMemoryUsage(b *testing.B) {
	// 模拟30秒高频数据
	windowSize := 30 * time.Second
	dataPoints := 500000 // 假设30秒内有50万个数据点

	fmt.Printf("=== 内存使用对比测试 ===\n")
	fmt.Printf("窗口大小: %v\n", windowSize)
	fmt.Printf("数据点数量: %d\n", dataPoints)
	fmt.Printf("数据频率: %.0f 次/秒\n", float64(dataPoints)/30.0)
	fmt.Println()

	// 测试原始版本
	fmt.Println("--- 原始 WindowStats ---")
	originalStats := NewWindowStats(windowSize)

	// 记录开始内存
	var m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	// 添加数据
	start := time.Now()
	for i := 0; i < dataPoints; i++ {
		value := float64(i%1000) + float64(i%100)/100.0 // 模拟价格数据
		timestamp := start.Add(time.Duration(i) * time.Millisecond)
		originalStats.AddWithTime(value, timestamp)
	}

	// 记录结束内存
	var m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m2)

	originalMemory := m2.Alloc - m1.Alloc
	fmt.Printf("原始版本内存占用: %.2f MB\n", float64(originalMemory)/1024/1024)
	fmt.Printf("数据点数量: %d\n", originalStats.GetCount())
	fmt.Printf("中位数: %.2f\n", func() float64 { m, _ := originalStats.GetMedian(); return m }())
	fmt.Printf("均值: %.2f\n", func() float64 { m, _ := originalStats.GetMean(); return m }())
	fmt.Println()

	// 测试高效版本
	fmt.Println("--- 高效 WindowStats ---")
	efficientStats := NewOptimizedWindowStats(windowSize, "test")

	// 记录开始内存
	var m3 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m3)

	fmt.Println("--- 高效 WindowStats RUN ---")
	// 添加数据
	start = time.Now()
	for i := 0; i < dataPoints; i++ {
		value := float64(i%1000) + float64(i%100)/100.0 // 模拟价格数据
		timestamp := start.Add(time.Duration(i) * time.Millisecond)
		efficientStats.AddWithTime(value, timestamp)
	}

	fmt.Println("--- 高效 WindowStats RUN END ---")
	// 记录结束内存
	var m4 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m4)

	efficientMemory := m4.Alloc - m3.Alloc
	fmt.Printf("高效版本内存占用: %.2f MB\n", float64(efficientMemory)/1024/1024)
	fmt.Printf("数据点数量: %d\n", efficientStats.GetCount())
	fmt.Printf("中位数: %.2f\n", func() float64 { m, _ := efficientStats.GetMedian(); return m }())
	fmt.Printf("均值: %.2f\n", func() float64 { m, _ := efficientStats.GetMean(); return m }())
	fmt.Println()

	// 计算优化效果
	reduction := float64(originalMemory-efficientMemory) / float64(originalMemory) * 100
	fmt.Printf("=== 优化效果 ===\n")
	fmt.Printf("内存减少: %.1f%%\n", reduction)
	fmt.Printf("内存节省: %.2f MB\n", float64(originalMemory-efficientMemory)/1024/1024)
	fmt.Printf("优化比例: %.1fx\n", float64(originalMemory)/float64(efficientMemory))
}

// TestMemoryOptimization 内存优化验证测试
func TestMemoryOptimization(t *testing.T) {
	windowSize := 5 * time.Second
	dataPoints := 10000

	// 创建两个实例
	original := NewWindowStats(windowSize)
	efficient := NewOptimizedWindowStats(windowSize, "test")

	// 添加相同的数据
	start := time.Now()
	for i := 0; i < dataPoints; i++ {
		value := float64(i%1000) + float64(i%100)/100.0
		timestamp := start.Add(time.Duration(i) * time.Millisecond)

		original.AddWithTime(value, timestamp)
		efficient.AddWithTime(value, timestamp)
	}

	// 验证结果一致性
	origMedian, origHasData := original.GetMedian()
	effMedian, effHasData := efficient.GetMedian()

	origMean, _ := original.GetMean()
	effMean, _ := efficient.GetMean()

	if origHasData != effHasData {
		t.Errorf("数据状态不一致: 原始=%v, 高效=%v", origHasData, effHasData)
	}

	if origHasData {
		if abs(origMedian-effMedian) > 0.01 {
			t.Errorf("中位数不一致: 原始=%.4f, 高效=%.4f", origMedian, effMedian)
		}

		if abs(origMean-effMean) > 0.01 {
			t.Errorf("均值不一致: 原始=%.4f, 高效=%.4f", origMean, effMean)
		}
	}

	// 验证数据点数量
	if original.GetCount() != efficient.GetCount() {
		t.Errorf("数据点数量不一致: 原始=%d, 高效=%d", original.GetCount(), efficient.GetCount())
	}

	t.Logf("测试通过: 数据点数量=%d, 中位数=%.4f, 均值=%.4f",
		efficient.GetCount(), effMedian, effMean)
}

// abs 计算绝对值
func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// TestMedianCorrectness 测试中位数计算的正确性
func TestMedianCorrectness(t *testing.T) {
	testCases := []struct {
		name     string
		values   []float64
		expected float64
	}{
		{
			name:     "奇数个数据点",
			values:   []float64{1, 2, 3, 4, 5},
			expected: 3.0,
		},
		{
			name:     "偶数个数据点",
			values:   []float64{1, 2, 3, 4, 5, 6},
			expected: 3.5,
		},
		{
			name:     "重复值",
			values:   []float64{2, 2, 2, 2, 2},
			expected: 2.0,
		},
		{
			name:     "乱序数据",
			values:   []float64{5, 1, 3, 2, 4},
			expected: 3.0,
		},
		{
			name:     "负数数据",
			values:   []float64{-3, -1, -2, -4, -5},
			expected: -3.0,
		},
		{
			name:     "小数数据",
			values:   []float64{1.1, 2.2, 3.3, 4.4, 5.5},
			expected: 3.3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name+"_Original", func(t *testing.T) {
			ws := NewWindowStats(10 * time.Second)
			start := time.Now()

			for i, value := range tc.values {
				ws.AddWithTime(value, start.Add(time.Duration(i)*time.Millisecond))
			}

			median, hasData := ws.GetMedian()
			if !hasData {
				t.Errorf("应该有数据")
			}

			if abs(median-tc.expected) > 0.001 {
				t.Errorf("期望中位数 %.3f, 实际得到 %.3f", tc.expected, median)
			}
		})

		t.Run(tc.name+"_Optimized", func(t *testing.T) {
			ws := NewOptimizedWindowStats(10*time.Second, "test")
			start := time.Now()

			for i, value := range tc.values {
				ws.AddWithTime(value, start.Add(time.Duration(i)*time.Millisecond))
			}

			median, hasData := ws.GetMedian()
			if !hasData {
				t.Errorf("应该有数据")
			}

			if abs(median-tc.expected) > 0.001 {
				t.Errorf("期望中位数 %.3f, 实际得到 %.3f", tc.expected, median)
			}
		})
	}
}

// TestMeanCorrectness 测试均值计算的正确性
func TestMeanCorrectness(t *testing.T) {
	testCases := []struct {
		name     string
		values   []float64
		expected float64
	}{
		{
			name:     "简单数据",
			values:   []float64{1, 2, 3, 4, 5},
			expected: 3.0,
		},
		{
			name:     "重复值",
			values:   []float64{2, 2, 2, 2, 2},
			expected: 2.0,
		},
		{
			name:     "负数数据",
			values:   []float64{-1, -2, -3, -4, -5},
			expected: -3.0,
		},
		{
			name:     "小数数据",
			values:   []float64{1.1, 2.2, 3.3, 4.4, 5.5},
			expected: 3.3,
		},
		{
			name:     "混合数据",
			values:   []float64{-2, 0, 2, 4, 6},
			expected: 2.0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name+"_Original", func(t *testing.T) {
			ws := NewWindowStats(10 * time.Second)
			start := time.Now()

			for i, value := range tc.values {
				ws.AddWithTime(value, start.Add(time.Duration(i)*time.Millisecond))
			}

			mean, hasData := ws.GetMean()
			if !hasData {
				t.Errorf("应该有数据")
			}

			if abs(mean-tc.expected) > 0.001 {
				t.Errorf("期望均值 %.3f, 实际得到 %.3f", tc.expected, mean)
			}
		})

		t.Run(tc.name+"_Optimized", func(t *testing.T) {
			ws := NewOptimizedWindowStats(10*time.Second, "test")
			start := time.Now()

			for i, value := range tc.values {
				ws.AddWithTime(value, start.Add(time.Duration(i)*time.Millisecond))
			}

			mean, hasData := ws.GetMean()
			if !hasData {
				t.Errorf("应该有数据")
			}

			if abs(mean-tc.expected) > 0.001 {
				t.Errorf("期望均值 %.3f, 实际得到 %.3f", tc.expected, mean)
			}
		})
	}
}

// TestConsistencyBetweenVersions 测试两个版本的一致性
func TestConsistencyBetweenVersions(t *testing.T) {
	testCases := []struct {
		name   string
		values []float64
	}{
		{
			name:   "随机数据",
			values: []float64{1.5, 3.2, 2.8, 4.1, 1.9, 3.7, 2.3, 4.5, 1.2, 3.9},
		},
		{
			name:   "递增数据",
			values: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
		{
			name:   "递减数据",
			values: []float64{10, 9, 8, 7, 6, 5, 4, 3, 2, 1},
		},
		{
			name:   "重复数据",
			values: []float64{5, 5, 5, 5, 5, 5, 5, 5, 5, 5},
		},
		{
			name:   "大范围数据",
			values: []float64{0.1, 1000.5, 0.2, 999.8, 0.3, 1001.2, 0.4, 998.7},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			original := NewWindowStats(10 * time.Second)
			optimized := NewOptimizedWindowStats(10*time.Second, "test")

			start := time.Now()
			for i, value := range tc.values {
				timestamp := start.Add(time.Duration(i) * time.Millisecond)
				original.AddWithTime(value, timestamp)
				optimized.AddWithTime(value, timestamp)
			}

			// 比较中位数
			origMedian, origHasData := original.GetMedian()
			optMedian, optHasData := optimized.GetMedian()

			if origHasData != optHasData {
				t.Errorf("数据状态不一致: 原始=%v, 优化=%v", origHasData, optHasData)
			}

			if origHasData && abs(origMedian-optMedian) > 0.001 {
				t.Errorf("中位数不一致: 原始=%.6f, 优化=%.6f", origMedian, optMedian)
			}

			// 比较均值
			origMean, _ := original.GetMean()
			optMean, _ := optimized.GetMean()

			if abs(origMean-optMean) > 0.001 {
				t.Errorf("均值不一致: 原始=%.6f, 优化=%.6f", origMean, optMean)
			}

			// 比较数据点数量
			if original.GetCount() != optimized.GetCount() {
				t.Errorf("数据点数量不一致: 原始=%d, 优化=%d", original.GetCount(), optimized.GetCount())
			}

			t.Logf("测试通过: 数据点数量=%d, 中位数=%.6f, 均值=%.6f",
				optimized.GetCount(), optMedian, optMean)
		})
	}
}

// TestSlidingWindowCorrectness 测试滑动窗口的正确性
func TestSlidingWindowCorrectness(t *testing.T) {
	windowSize := 5 * time.Second

	original := NewWindowStats(windowSize)
	optimized := NewOptimizedWindowStats(windowSize, "test")

	// 添加数据，模拟滑动窗口
	start := time.Now()
	testData := []struct {
		value     float64
		timestamp time.Time
	}{
		{1.0, start},
		{2.0, start.Add(1 * time.Second)},
		{3.0, start.Add(2 * time.Second)},
		{4.0, start.Add(3 * time.Second)},
		{5.0, start.Add(4 * time.Second)},
		{6.0, start.Add(5 * time.Second)}, // 这会触发清理，移除第一个数据点
		{7.0, start.Add(6 * time.Second)}, // 这会触发清理，移除第二个数据点
	}

	for _, data := range testData {
		original.AddWithTime(data.value, data.timestamp)
		optimized.AddWithTime(data.value, data.timestamp)

		// 验证两个版本的一致性
		origMedian, origHasData := original.GetMedian()
		optMedian, optHasData := optimized.GetMedian()

		if origHasData != optHasData {
			t.Errorf("时间 %v: 数据状态不一致", data.timestamp)
		}

		if origHasData && abs(origMedian-optMedian) > 0.001 {
			t.Errorf("时间 %v: 中位数不一致 原始=%.6f, 优化=%.6f",
				data.timestamp, origMedian, optMedian)
		}

		origMean, _ := original.GetMean()
		optMean, _ := optimized.GetMean()

		if abs(origMean-optMean) > 0.001 {
			t.Errorf("时间 %v: 均值不一致 原始=%.6f, 优化=%.6f",
				data.timestamp, origMean, optMean)
		}
	}

	// 最终验证：应该有5个数据点（窗口大小内的数据）
	// 最后添加的数据点时间戳是 start.Add(6*time.Second)
	// 窗口大小是5秒，所以应该保留时间戳在 [1秒, 6秒] 范围内的数据点
	// 即：2.0, 3.0, 4.0, 5.0, 6.0, 7.0 (6个数据点)
	finalCount := optimized.GetCount()
	if finalCount != 6 {
		t.Errorf("最终数据点数量应该是6，实际是%d", finalCount)
	}

	t.Logf("滑动窗口测试通过: 最终数据点数量=%d", finalCount)
}

// TestHeapCleanup 测试堆清理是否有效
func TestHeapCleanup(t *testing.T) {
	windowSize := 2 * time.Second

	original := NewWindowStats(windowSize)
	optimized := NewOptimizedWindowStats(windowSize, "test")

	start := time.Now()

	// 添加大量数据，然后等待窗口滑动
	for i := 0; i < 100; i++ {
		value := float64(i)
		timestamp := start.Add(time.Duration(i) * 100 * time.Millisecond)
		original.AddWithTime(value, timestamp)
		optimized.AddWithTime(value, timestamp)
	}

	// 等待窗口滑动，让大部分数据过期
	time.Sleep(3 * time.Second)

	// 添加新数据触发清理
	finalTimestamp := start.Add(4 * time.Second)
	original.AddWithTime(999.0, finalTimestamp)
	optimized.AddWithTime(999.0, finalTimestamp)

	// 检查堆大小是否合理
	origCount := original.GetCount()
	optCount := optimized.GetCount()

	// 应该只有少量数据在窗口内（窗口大小2秒，每100ms一个数据点，所以最多20个）
	if origCount > 25 {
		t.Errorf("原始版本堆清理可能有问题: 数据点数量=%d", origCount)
	}
	if optCount > 25 {
		t.Errorf("优化版本堆清理可能有问题: 数据点数量=%d", optCount)
	}

	t.Logf("堆清理测试通过: 原始版本数据点=%d, 优化版本数据点=%d", origCount, optCount)
}

// TestDelayedCleanupCorrectness 测试延迟清理是否影响中位数准确性
func TestDelayedCleanupCorrectness(t *testing.T) {
	windowSize := 1 * time.Second

	original := NewWindowStats(windowSize)
	optimized := NewOptimizedWindowStats(windowSize, "test")

	start := time.Now()

	// 添加数据，模拟延迟清理场景
	// 先添加大量数据，让它们过期，但堆清理被延迟
	for i := 0; i < 50; i++ {
		value := float64(i + 1)
		timestamp := start.Add(time.Duration(i) * 20 * time.Millisecond)
		original.AddWithTime(value, timestamp)
		optimized.AddWithTime(value, timestamp)
	}

	// 等待窗口滑动，让大部分数据过期
	time.Sleep(1200 * time.Millisecond)

	// 添加新数据，触发清理（此时应该触发延迟清理）
	for i := 50; i < 60; i++ {
		value := float64(i + 1)
		timestamp := start.Add(1200*time.Millisecond + time.Duration(i-50)*20*time.Millisecond)

		original.AddWithTime(value, timestamp)
		optimized.AddWithTime(value, timestamp)

		// 每次添加后都验证中位数是否准确
		origMedian, origHasData := original.GetMedian()
		optMedian, optHasData := optimized.GetMedian()

		if origHasData != optHasData {
			t.Errorf("数据状态不一致: 原始=%v, 优化=%v", origHasData, optHasData)
		}

		if origHasData && abs(origMedian-optMedian) > 0.001 {
			t.Errorf("中位数不一致: 原始=%.6f, 优化=%.6f", origMedian, optMedian)
		}

		// 验证中位数是否在合理范围内（应该在最近的数据范围内）
		if origHasData && (origMedian < 1 || origMedian > 60) {
			t.Errorf("中位数值异常: %.6f (应该在1-60范围内)", origMedian)
		}
	}

	t.Logf("延迟清理正确性测试通过")
}

// TestMapsCleanup 测试映射清理是否有效
func TestMapsCleanup(t *testing.T) {
	windowSize := 1 * time.Second

	optimized := NewOptimizedWindowStats(windowSize, "test")

	start := time.Now()

	// 添加大量数据
	for i := 0; i < 10000; i++ {
		value := float64(i % 100)
		timestamp := start.Add(time.Duration(i) * time.Millisecond / 20)
		optimized.AddWithTime(value, timestamp)
	}

	// 等待窗口滑动，让大部分数据过期
	time.Sleep(1500 * time.Millisecond)

	// 添加新数据触发清理
	for i := 0; i < 100; i++ {
		value := float64(i)
		timestamp := start.Add(1500*time.Millisecond + time.Duration(i)*10*time.Millisecond)
		optimized.AddWithTime(value, timestamp)
	}

	// 检查映射大小
	maxHeapTimeToIdxSize := len(optimized.maxHeap.timeToIdx)
	maxHeapValidItemsSize := len(optimized.maxHeap.validItems)
	minHeapTimeToIdxSize := len(optimized.minHeap.timeToIdx)
	minHeapValidItemsSize := len(optimized.minHeap.validItems)

	// 映射大小应该与当前数据点数量相近
	dataPointCount := optimized.GetCount()

	t.Logf("数据点数量: %d", dataPointCount)
	t.Logf("maxHeap.timeToIdx 大小: %d", maxHeapTimeToIdxSize)
	t.Logf("maxHeap.validItems 大小: %d", maxHeapValidItemsSize)
	t.Logf("minHeap.timeToIdx 大小: %d", minHeapTimeToIdxSize)
	t.Logf("minHeap.validItems 大小: %d", minHeapValidItemsSize)

	// 映射大小不应该远大于数据点数量
	maxAllowedSize := dataPointCount * 2 // 允许一定冗余
	if maxHeapTimeToIdxSize > maxAllowedSize || maxHeapValidItemsSize > maxAllowedSize ||
		minHeapTimeToIdxSize > maxAllowedSize || minHeapValidItemsSize > maxAllowedSize {
		t.Errorf("映射大小异常: 数据点=%d, maxHeap.timeToIdx=%d, maxHeap.validItems=%d, minHeap.timeToIdx=%d, minHeap.validItems=%d",
			dataPointCount, maxHeapTimeToIdxSize, maxHeapValidItemsSize, minHeapTimeToIdxSize, minHeapValidItemsSize)
	}
}

// BenchmarkAddPerformance 添加性能对比测试
func BenchmarkAddPerformance(b *testing.B) {
	windowSize := 1 * time.Second

	b.Run("Original", func(b *testing.B) {
		stats := NewWindowStats(windowSize)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			stats.Add(float64(i % 1000))
		}
	})

	b.Run("Efficient", func(b *testing.B) {
		stats := NewOptimizedWindowStats(windowSize, "test")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			stats.Add(float64(i % 1000))
		}
	})
}

// BenchmarkGetStats 获取统计信息性能对比
func BenchmarkGetStats(b *testing.B) {
	windowSize := 1 * time.Second
	dataPoints := 10000

	// 准备数据
	original := NewWindowStats(windowSize)
	efficient := NewOptimizedWindowStats(windowSize, "test")

	start := time.Now()
	for i := 0; i < dataPoints; i++ {
		value := float64(i % 1000)
		timestamp := start.Add(time.Duration(i) * time.Millisecond)
		original.AddWithTime(value, timestamp)
		efficient.AddWithTime(value, timestamp)
	}

	b.Run("Original_GetMedian", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			original.GetMedian()
		}
	})

	b.Run("Efficient_GetMedian", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			efficient.GetMedian()
		}
	})

	b.Run("Original_GetMean", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			original.GetMean()
		}
	})

	b.Run("Efficient_GetMean", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			efficient.GetMean()
		}
	})
}
