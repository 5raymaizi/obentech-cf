package app

import (
	"encoding/json"
)

type (
	//App 一个服务运行实例
	App interface {
		Start() error
		Stop() error
		Done() chan struct{}
		Error() error
		UpdateConfigJSON(raw json.RawMessage) error
		RefreshConfig() error
		GetConfig() (any, error)
		Key() string
		RedisUniqueName() string
		// 导出账户信息
		ExportAccount() (map[string]string, error)
		// 策略数据
		AppData() (map[string]any, error)
		// 配置+数据+账户信息
		Detail() (any, error)
		// 清理策略数据
		Clear() error
		// 清理敞口
		ClearExposure(isSwap1 bool) error
		// 恢复正常下单状态
		SetADLEnd() error
		AddOrder(params json.RawMessage, operator string) error
		Transfer(params json.RawMessage, operator string) error
		// acts raw requests 支持
		RawRequest(params json.RawMessage, operator string) (string, error)
		Running() bool
		Version() string
	}

	//Factory 用于处理App创建，配置更新
	Factory interface {
		GetRunInfo() (env string)
		Create(key string) (App, error)
		Clear(key string) error
	}
)
