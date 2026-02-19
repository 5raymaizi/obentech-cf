package logger

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"go.common/log"

	kitlog "github.com/go-kit/kit/log"
	"github.com/go-kit/log/level"
)

type (
	Config struct {
		Path  string `mapstructure:"path" json:"path"`
		Level string `mapstructure:"level" json:"level"`
	}

	kitLoggerWrapper struct {
		asyncLogger *log.AsyncLogger
		defaultLvl  log.Level
	}
)

var (
	globalLogger kitlog.Logger
	asyncLog     *log.AsyncLogger
	initOnce     sync.Once
)

// NewLogger 初始化日志系统 (线程安全)
func NewLogger(cfg Config, isDebug bool) error {
	var err error
	initOnce.Do(func() {
		err = initializeLogger(cfg, isDebug)
	})
	return err
}

func initializeLogger(cfg Config, isDebug bool) error {
	// 设置日志文件路径
	path := cfg.Path
	if path == "" {
		path = "cf_arbitrage.log"
	}

	// 初始化异步日志器
	writer := log.NewFileRotate4(path, 8192) // 8KB缓冲区
	asyncLog = log.NewAsyncLog(parseLevel(cfg.Level), 1024, writer)

	// 创建kit兼容的logger
	wrapper := &kitLoggerWrapper{
		asyncLogger: asyncLog,
		defaultLvl:  parseLevel(cfg.Level),
	}

	// 设置输出目标
	if isDebug {
		globalLogger = kitlog.NewSyncLogger(kitlog.NewLogfmtLogger(os.Stdout))
	} else {
		globalLogger = wrapper
	}

	// 应用日志级别过滤
	globalLogger = level.NewFilter(globalLogger, getLevelOption(cfg.Level))

	// 记录初始化日志
	level.Info(globalLogger).Log(
		"message", "logger initialized",
		"path", path,
		"level", cfg.Level,
		"debug_mode", isDebug,
	)

	return nil
}

// GetLogger 获取全局logger实例
func GetLogger() kitlog.Logger {
	if globalLogger == nil {
		// 默认回退到控制台输出
		globalLogger = kitlog.NewLogfmtLogger(os.Stdout)
	}
	return globalLogger
}

// Close 关闭日志系统
func Close() error {
	if asyncLog != nil {
		log.CloseFileLog(asyncLog)
	}
	return nil
}

// parseLevel 将字符串级别转换为gocommon Level类型
func parseLevel(lvl string) log.Level {
	switch strings.ToLower(lvl) {
	case "debug":
		return log.LevelDebug
	case "warn":
		return log.LevelWarn
	case "error":
		return log.LevelError
	case "trace":
		return log.LevelTrace
	default:
		return log.LevelInfo
	}
}

// getLevelOption 获取go-kit的level过滤选项
func getLevelOption(lvl string) level.Option {
	switch strings.ToLower(lvl) {
	case "debug":
		return level.AllowDebug()
	case "warn":
		return level.AllowWarn()
	case "error":
		return level.AllowError()
	default:
		return level.AllowInfo()
	}
}

// Log 实现go-kit/log.Logger接口
func (k *kitLoggerWrapper) Log(keyvals ...any) error {
	lvl := k.defaultLvl

	if len(keyvals)%2 != 0 {
		keyvals = append(keyvals, "") // 补空值
	}
	for i := 0; i < len(keyvals)-1; i += 2 {
		if key, ok := keyvals[i].(string); ok && key == "level" {
			lvl = parseLevel(fmt.Sprintf("%s", keyvals[i+1]))
		}
	}
	var msg strings.Builder
	for i := 0; i < len(keyvals); i += 2 {
		if i > 0 {
			msg.WriteByte(' ')
		}
		msg.WriteString(fmt.Sprint(keyvals[i]))
		msg.WriteByte('=')
		msg.WriteString(fmt.Sprint(keyvals[i+1]))
	}

	// 调用异步日志器
	k.asyncLogger.Log(lvl, msg.String())
	return nil
}
