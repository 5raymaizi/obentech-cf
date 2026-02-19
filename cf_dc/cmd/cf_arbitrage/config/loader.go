package config

import (
	"sync/atomic"
	"time"

	"cf_arbitrage/logic/config"
	"cf_arbitrage/util/logger"

	"go.common/redis_service"

	"github.com/go-kit/kit/log/level"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type (
	//Config 配置结构
	Config struct {
		Account     string              `mapstructure:"account" yaml:"account"`
		Exchanges   map[string]Exchange `mapstructure:"exchanges" yaml:"exchanges"`
		Spot        string
		Swap1       string
		Swap2       string
		BotID       string                       `mapstructure:"bot_id" yaml:"bot_id"`
		ImBotID     string                       `mapstructure:"im_bot_id" yaml:"im_bot_id"`
		QImBotID    string                       `mapstructure:"qim_bot_id" yaml:"qim_bot_id"`
		P0BotID     string                       `mapstructure:"p0_bot_id" yaml:"p0_bot_id"`
		P3BotID     string                       `mapstructure:"p3_bot_id" yaml:"p3_bot_id"`
		Redis       []redis_service.RedisConfig  `mapstructure:"redis" yaml:"redis"`
		Config      config.Config                `mapstructure:"config" yaml:"config"`
		Acts        ActsConfig                   `mapstructure:"acts" yaml:"acts"`
		ActsFM      ActsConfig                   `mapstructure:"acts_fm" yaml:"acts_fm"`
		WarnAt      map[string]map[string]string `mapstructure:"warn_at" yaml:"warn_at"` // 告警消息@规则
		DepthOutPut string                       `mapstructure:"depth_output" yaml:"depth_output"`
	}

	ActsConfig struct {
		Rest string `mapstructure:"rest" yaml:"rest"`
		Ws   string `mapstructure:"ws" yaml:"ws"`

		// 加密acts地址,优先使用
		Vault string `mapstructure:"vault" yaml:"vault"`
	}

	SecretsConfig struct {
		Preserve bool        `mapstructure:"preserve" yaml:"preserve"`
		Acts     EncryptActs `mapstructure:"acts" yaml:"acts"`
	}

	EncryptActs struct {
		Key string `mapstructure:"encryptKey" yaml:"encryptKey"`
		IV  string `mapstructure:"encryptIV" yaml:"encryptIV"`
	}

	Exchange struct {
		Exchange string `mapstructure:"exchange" yaml:"exchange"`
		Key      string `mapstructure:"key" yaml:"key"`
		Secret   string `mapstructure:"secret" yaml:"secret"`
	}

	//Loader 负载动态加载配置
	Loader struct {
		config atomic.Value
		done   chan struct{}
	}
)

var (
	loader *Loader
)

func Init(paths ...string) error {
	for _, p := range paths {
		viper.AddConfigPath(p)
	}

	if err := viper.ReadInConfig(); err != nil {
		return errors.WithMessage(err, "read config fail")
	}

	viper.WatchConfig()

	l := logger.GetLogger()
	loader = newLoader(l)
	if err := loader.reloadConfig(); err != nil {
		return errors.WithMessage(err, "reload config fail")
	}
	go loader.startJob()
	return nil
}

func newLoader(logger log.Logger) *Loader {
	ret := Loader{
		done: make(chan struct{}),
	}
	return &ret
}

func Ins() *Loader {
	return loader
}

func (l *Loader) LoadConfig() Config {
	val := l.config.Load().(*Config)
	return *val
}

func (l *Loader) Close() {
	close(l.done)
}

// Load implement Loader interface
func (l *Loader) Load() config.Config {
	cfg := l.LoadConfig()
	funcLimit := func(concurrencyLimit int) int {
		if concurrencyLimit <= 0 {
			concurrencyLimit = 1 // 默认限制为1，保持原有行为
		}

		if concurrencyLimit > 3 {
			concurrencyLimit = 3 // 最大限制为3
		}
		return concurrencyLimit
	}

	cfg.Config.TTConcurrencyLimit = funcLimit(cfg.Config.TTConcurrencyLimit)
	cfg.Config.MTConcurrencyLimit = funcLimit(cfg.Config.MTConcurrencyLimit)
	cfg.Config.TMConcurrencyLimit = funcLimit(cfg.Config.TMConcurrencyLimit)
	cfg.Config.MMConcurrencyLimit = funcLimit(cfg.Config.MMConcurrencyLimit)

	if cfg.Config.WarnValueLimit <= 0 {
		cfg.Config.WarnValueLimit = 500
	}
	if cfg.Config.P1Level <= 0 {
		cfg.Config.P1Level = 3
	}
	if cfg.Config.P3Level <= 0 {
		cfg.Config.P3Level = 1
	}
	return cfg.Config
}

func (l *Loader) startJob() {
	ticker := time.NewTicker(time.Second * 5)

	for {
		select {
		case <-ticker.C:
			if err := l.reloadConfig(); err != nil {
				lg := logger.GetLogger()
				level.Warn(lg).Log("message", "reload config fail", "error", err.Error())
			}

		case <-l.done:
			return
		}
	}
}

func (l *Loader) reloadConfig() error {
	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return errors.WithMessage(err, "unmarshal config fail")
	}

	l.config.Store(&cfg)
	return nil
}
