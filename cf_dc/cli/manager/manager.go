package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"

	"cf_arbitrage/cli/app"
	"cf_arbitrage/exchange"
	"cf_arbitrage/message"
	"cf_arbitrage/util/logger"

	common "go.common"
	"go.common/redis_service"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/go-redis/redis/v7"
	"github.com/pkg/errors"
	"github.com/tidwall/gjson"
)

type (
	Manager struct {
		mu      sync.Mutex
		apps    map[string]app.App
		rds     *redis.Client
		factory app.Factory
		logger  log.Logger
	}

	AppInfo struct {
		Key     string `json:"key"`
		Config  any    `json:"config"`
		Detail  any    `json:"detail"`
		Running bool   `json:"running"`
		Version string `json:"version"`
	}
)

const (
	managerStorage = "manager.data"
)

func NewManager(factory app.Factory) *Manager {
	l := logger.GetLogger()
	return &Manager{
		apps:    make(map[string]app.App),
		rds:     redis_service.GetGoRedis(0),
		factory: factory,
		logger:  log.With(l, "timestamp", log.DefaultTimestamp),
	}
}

// 返回所有交易对
func (m *Manager) Keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var ret []string
	for k := range m.apps {
		ret = append(ret, k)
	}
	// 排序
	sort.Strings(ret)
	return ret
}

// 返回每个交易对的状态
func (m *Manager) KeysStatus() map[string]map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	ret := make(map[string]map[string]any)
	for k, a := range m.apps {
		info := common.NewJsonObject()
		info["status"] = "Off"
		if a.Running() {
			info["status"] = "On"
			appData, err := a.AppData()
			if err != nil {
				info["total_failed_times"] = err.Error()
			} else {
				info["total_failed_times"] = appData["total_failed_times"]
			}
		}
		info["version"] = a.Version()
		ret[k] = info
	}
	return ret
}

func (m *Manager) Reload() error {
	file, err := os.Open(managerStorage)
	if err != nil && !os.IsNotExist(err) {
		return errors.WithMessagef(err, "open storage file fail")
	} else if file != nil {
		defer file.Close()
		var keys []string
		raw, err := io.ReadAll(file)
		if err != nil {
			return errors.WithMessage(err, "read storage file fail")
		}

		if err := json.Unmarshal(raw, &keys); err != nil {
			return errors.WithMessage(err, "unmarshal config fail")
		}

		for _, k := range keys {
			app, err := m.factory.Create(k)
			if err != nil {
				return errors.WithMessagef(err, "create app fail key='%s'", k)
			}
			m.apps[app.Key()] = app
		}
	}
	return nil
}

func (m *Manager) Quit() error {
	if err := m.StopAll(); err != nil {
		return errors.WithMessage(err, "stop all app fail")
	}

	if err := m.Save(); err != nil {
		return errors.WithMessage(err, "save fail")
	}
	return nil
}

func (m *Manager) Save() error {
	file, err := os.OpenFile(managerStorage, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		return errors.WithMessage(err, "open storage file fail")
	}

	defer file.Close()

	var keys []string
	for key := range m.apps {
		keys = append(keys, key)
	}
	// 排序
	sort.Strings(keys)
	raw, _ := json.Marshal(keys)
	if _, err := file.Write(raw); err != nil {
		return errors.WithMessage(err, "write file fail")
	}
	return nil
}

func (m *Manager) StartAll() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key := range m.apps {

		if err := m.start(key); err != nil {
			return errors.WithMessagef(err, "start app fail key=%s", key)
		}
	}
	return nil
}

func (m *Manager) StopAll() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key := range m.apps {
		if err := m.stop(key); err != nil {
			return errors.WithMessagef(err, "start app fail key=%s", key)
		}
	}
	return nil
}

func (m *Manager) Add(key string, conf json.RawMessage) error {
	if err := m.Create(key, conf); err != nil {
		return errors.WithMessage(err, "create app fail")
	}

	if err := m.Start(key); err != nil {
		return errors.WithMessage(err, "start app fail")
	}
	if err := m.Save(); err != nil {
		return errors.WithMessage(err, "start save fail")
	}
	return nil
}

func (m *Manager) Del(key string) error {
	if err := m.Stop(key); err != nil {
		return errors.WithMessage(err, "stop fail")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.apps, key)
	if err := m.Save(); err != nil {
		return errors.WithMessage(err, "del save fail")
	}
	return nil
}

func (m *Manager) Update(key string, conf json.RawMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	app, ok := m.apps[key]
	if !ok {
		return errors.Errorf("unkown app '%s'", key)
	}

	if err := app.UpdateConfigJSON(conf); err != nil {
		return errors.WithMessage(err, "update config fail")
	}
	return nil
}

func (m *Manager) UpdateAll() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, app := range m.apps {
		if err := app.RefreshConfig(); err != nil {
			level.Warn(m.logger).Log("message", "UpdateAll", "ins", key, "error", err.Error())
		}
	}
	return nil
}

func (m *Manager) Get(key string) (*AppInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ins, ok := m.apps[key]
	if !ok {
		return nil, errors.Errorf("unkown app '%s'", key)
	}

	conf, err := ins.GetConfig()
	if err != nil {
		return nil, errors.WithMessage(err, "get config fail")
	}

	detail, err := ins.Detail()
	if err != nil {
		return nil, errors.WithMessage(err, "get detail fail")
	}
	m.rds.SetNX(fmt.Sprintf(`cf:%s:alive`, ins.RedisUniqueName()), ``, time.Second*10)

	return &AppInfo{
		Config:  conf,
		Detail:  detail,
		Key:     key,
		Running: ins.Running(),
		Version: ins.Version(),
	}, nil
}

func (m *Manager) Create(key string, conf json.RawMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.apps[key]
	if ok {
		return errors.Errorf("duplicate app '%s'", key)
	}

	app, err := m.factory.Create(key)
	if err != nil {
		return errors.WithMessage(err, "create app fail")
	}

	if err := app.UpdateConfigJSON(conf); err != nil {
		return errors.WithMessage(err, "update config fail")
	}

	m.apps[key] = app
	return nil
}

func (m *Manager) Start(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.start(key)
}

func (m *Manager) start(key string) error {
	ins, ok := m.apps[key]
	if !ok {
		return errors.Errorf("unkown app '%s'", key)
	}

	if ins.Running() {
		return fmt.Errorf("app '%s' already running", key)
	}

	if err := ins.Start(); err != nil {
		return errors.WithMessage(err, "start app fail")
	}
	message.Send(context.Background(), message.NewLogicStart(key))
	level.Info(m.logger).Log("message", "ins start", "ins", ins.Key())

	go func() {
		<-ins.Done()
		if err := ins.Error(); err != nil {
			message.SendP3Important(context.Background(), message.NewLogicExitError(key, err))
			level.Warn(m.logger).Log("message", "ins quit", "ins", ins.Key(), "error", err.Error())
		}
	}()
	return nil
}

func (m *Manager) Stop(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stop(key)
}

func (m *Manager) stop(key string) error {
	ins, ok := m.apps[key]
	if !ok {
		return errors.Errorf("unkown app '%s'", key)
	}

	if !ins.Running() {
		return nil
	}

	if err := ins.Stop(); err != nil {
		return errors.WithMessage(err, "stop app fail")
	}
	return nil
}

func (m *Manager) Clear(key string) error {
	ins, ok := m.apps[key]
	if !ok {
		return errors.Errorf("unkown app '%s'", key)
	}

	var msg string
	defer func() {
		go message.SendImportant(context.Background(), message.NewCommonMsg(fmt.Sprintf(`%s 仓位数据清理`, key), msg))
	}()

	if ins.Running() { // 进程启动状态不clear
		msg = "清理失败,进程正在运行中,请关闭后重试"
		return errors.Errorf("clear app data fail, app in run")
	}

	if err := ins.Clear(); err != nil {
		msg = fmt.Sprintf("清理失败,err=%+s", err.Error())
		return errors.WithMessage(err, "clear app data fail")
	}
	msg = "清理完成"
	return nil
}

func (m *Manager) ClearExposure(key string, params json.RawMessage) error {
	ins, ok := m.apps[key]
	if !ok {
		return errors.Errorf("unkown app '%s'", key)
	}

	jsRaw := gjson.ParseBytes(params)
	isSwap1 := jsRaw.Get("isSwap1").Bool()
	flag := "swap1"
	if !isSwap1 {
		flag = "swap2"
	}
	var msg string
	defer func() {
		go message.SendImportant(context.Background(), message.NewCommonMsg(fmt.Sprintf(`%s 清理%s敞口`, key, flag), msg))
	}()

	if ins.Running() { // 进程启动状态不clear
		msg = "清理失败,进程正在运行中,请关闭后重试"
		return errors.Errorf("clear app exposure fail, app in run")
	}

	if err := ins.ClearExposure(isSwap1); err != nil {
		msg = fmt.Sprintf("清理失败,err=%+s", err.Error())
		return errors.WithMessage(err, "clear app exposure fail")
	}
	msg = "清理完成"
	return nil
}

func (m *Manager) SetADLEnd(key string) error {
	ins, ok := m.apps[key]
	if !ok {
		return errors.Errorf("unkown app '%s'", key)
	}

	var msg string
	defer func() {
		go message.SendImportant(context.Background(), message.NewCommonMsg(fmt.Sprintf(`%s 恢复正常下单状态`, key), msg))
	}()

	if ins.Running() { // 进程启动状态不clear
		msg = "恢复失败,进程正在运行中,请关闭后重试"
		return errors.Errorf("clear app data fail, app in run")
	}

	if err := ins.SetADLEnd(); err != nil {
		msg = fmt.Sprintf("恢复失败,err=%+s", err.Error())
		return errors.WithMessage(err, "clear app data fail")
	}
	msg = "完成,ADL状态解除,恢复正常下单"
	return nil
}

func (m *Manager) AddOrder(key string, params json.RawMessage, operator string) error {
	ins, ok := m.apps[key]
	if !ok {
		return errors.Errorf("unkown app '%s'", key)
	}
	err := ins.AddOrder(params, operator)
	if err != nil {
		return errors.WithMessage(err, "start add order fail")
	}
	return nil
}

func (m *Manager) Transfer(key string, params json.RawMessage, operator string) error {
	ins, ok := m.apps[key]
	if !ok {
		return errors.Errorf("unkown app '%s'", key)
	}
	err := ins.Transfer(params, operator)
	if err != nil {
		return errors.WithMessage(err, "start transfer fail")
	}
	return nil
}

func (m *Manager) RawRequest(params json.RawMessage, operator string) (string, error) {
	var ins app.App
	for _, val := range m.apps {
		ins = val
		break
	}
	if ins == nil {
		return "", common.NewErrorf(nil, "no instance")
	}
	return ins.RawRequest(params, operator)
}

// 检测维持保证金率
func (m *Manager) CheckMmrRisk() {
	var ins app.App
	for _, val := range m.apps {
		ins = val
		break
	}
	if ins == nil {
		return
	}
	threshold := 0.0
	thresholdP0 := 4.5

	accInfos, err := ins.ExportAccount()
	if err != nil {
		level.Error(m.logger).Log("message", "CheckMmrRisk", "error", err.Error())
		return
	}
	env := m.factory.GetRunInfo()
	key := fmt.Sprintf("cf:%s:initBalance:mCcy", env)
	rds := redis_service.GetGoRedis(9)
	initBalance, err := rds.Get(key).Result()
	if err == nil {
		js := gjson.Parse(initBalance)
		if js.IsObject() {
			threshold = js.Get(`risk`).Float()
			if js.Get(`riskP0`).Float() != 0 {
				thresholdP0 = js.Get(`riskP0`).Float()
			}
		}
	} else {
		level.Warn(m.logger).Log("message", "CheckMmrRisk err", "key", key, "error", err)
	}

	var (
		riskBanned  bool
		exchangeMMR string
		sendP0      bool
	)

	for exchangeName, accInfo := range accInfos {
		if exchangeName == exchange.Bybit5 {
			continue
		}
		gjs := gjson.Parse(accInfo)
		defaultThreshold := 4.5
		if exchangeName == exchange.Okex5 {
			defaultThreshold = 4
		}
		if threshold != 0.0 {
			defaultThreshold = threshold
		}

		nowMmr := gjs.Get(`uniMMR`).Float()

		switch exchangeName {
		case exchange.Okex5:
			mgnRatio := gjs.Get(`data.0.mgnRatio`)
			if mgnRatio.String() == "" {
				return
			}
			nowMmr = mgnRatio.Float()
		case exchange.Bybit5:
			nowMmr = gjs.Get(`result.list.0.accountMMRate`).Float()
		case exchange.GateIO:
			nowMmr = gjs.Get(`cross_mmr`).Float()
			if nowMmr == 0 {
				return
			}
		}
		if nowMmr <= defaultThreshold {
			riskBanned = true
		}
		if nowMmr <= thresholdP0 {
			sendP0 = true
		}
		exchangeMMR += fmt.Sprintf("%s:%f,", exchangeName, nowMmr)
	}
	if !riskBanned {
		return
	}
	banJs := common.NewJsonObject()
	banJs["risk_ban"] = true
	b, _ := json.Marshal(banJs)
	if cmdErr := m.rds.SetNX(fmt.Sprintf(`cf:%s:share_ban`, env), string(b), time.Minute*10); cmdErr.Err() != nil {
		level.Warn(m.logger).Log("message", "CheckMmrRisk set redis failed", "err", cmdErr.Err())
	}
	msg := message.NewCommonMsgWithImport("当前环境暂停下单", fmt.Sprintf("账户维持保证金率过低(%s),暂停所有币对开仓,需要人工调整保证金率并重启", exchangeMMR))
	go message.SendImportant(context.Background(), msg)
	if sendP0 {
		go message.SendP0Important(context.Background(), msg)
	}
}
