package common

import "time"

type TimerWithInterval struct {
	*time.Timer
	interval time.Duration // 私有变量，仅内部维护
}

// NewTimerWithInterval 创建定时器（兼容 time.NewTimer 的用法）
func NewTimerWithInterval(d time.Duration) *TimerWithInterval {
	return &TimerWithInterval{
		Timer:    time.NewTimer(d),
		interval: d,
	}
}

// GetInterval 获取当前设定的时间间隔
func (t *TimerWithInterval) GetInterval() time.Duration {
	return t.interval
}
