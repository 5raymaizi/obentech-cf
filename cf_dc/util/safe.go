package util

import (
	"fmt"

	"cf_arbitrage/util/logger"

	"github.com/go-kit/kit/log/level"
)

// 适用于在channel关闭后不重要的信息
func ChanSafeSend[T any](ch chan T, msg T, onFull func()) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// 捕获到 panic 后输出日志，防止程序崩溃
			err = fmt.Errorf("%+v", r)
			level.Error(logger.GetLogger()).Log("message", "send channel err", "err", err.Error())
		}
	}()
	select {
	case ch <- msg:
		return nil
	default:
		// 通道满了，调用用户提供的 onFull 逻辑
		if onFull != nil {
			onFull() // 执行处理逻辑
		}
	}
	return nil
}
