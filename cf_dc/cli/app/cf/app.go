package cf

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"cf_arbitrage/cli/app"
	"cf_arbitrage/cmd/cf_arbitrage/config"
	"cf_arbitrage/exchange"
	config2 "cf_arbitrage/logic/config"
	"cf_arbitrage/message"
	utilCommon "cf_arbitrage/util/common"

	common "go.common"
	"go.common/apis/actsApi"
	"go.common/apis/apiPool"
	"go.common/cache_service"
	"go.common/redis_service"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"gopkg.in/yaml.v2"
)

type (
	//App 单个cf_arbitrage运行实例
	App struct {
		mu              sync.Mutex
		addOrderMu      sync.Mutex // 防止手动下单,转账并发
		transferMu      sync.Mutex
		tradePair       string // 程序运行标识(BTCUSDT/BTCUSD)
		redisUniqueName string // redis数据标识
		coin            string // 现货标的
		workingDir      string
		appPath         string
		done            chan struct{}
		err             atomic.Value
		config          Config
		running         bool
		addOrderRunning bool
		transferRunning bool
		process         *os.Process
		encryptActs     *config.EncryptActs
		enableUsdc      bool
	}

	//Factory 创建cf_arbitrage app
	Factory struct {
		env         string
		appPath     string
		baseDir     string
		config      Config
		encryptActs *config.EncryptActs
		// lg      log.Logger
	}

	Config struct {
		Account     string                       `mapstructure:"account"`
		Exchanges   map[string]config.Exchange   `mapstructure:"exchanges"`
		BotID       string                       `mapstructure:"bot_id"`
		ImBotID     string                       `mapstructure:"im_bot_id"`
		QImBotID    string                       `mapstructure:"qim_bot_id"` // 量化重要告警群
		P0BotID     string                       `mapstructure:"p0_bot_id"`  // P0告警群
		P3BotID     string                       `mapstructure:"p3_bot_id"`  // P3告警群
		Acts        config.ActsConfig            `mapstructure:"acts"`
		ActsFM      config.ActsConfig            `mapstructure:"acts_fm"` // 币安用于切换mm acts
		Redis       []redis_service.RedisConfig  `mapstructure:"redis"`
		WarnAt      map[string]map[string]string `mapstructure:"warn_at"` // 告警消息@规则
		DepthOutPut string                       `mapstructure:"depth_output" yaml:"depth_output"`
	}
)

const (
	configFileName = "config.yaml"

	// 加密api用到
	secretsFileName = "secrets.yaml"
)

func NewFactory(appPath string, baseDir string, cfg Config, encryptActs *config.EncryptActs) *Factory {
	return &Factory{
		env:         config.GenerateUniqueName(cfg.Account),
		baseDir:     baseDir,
		appPath:     appPath,
		config:      cfg,
		encryptActs: encryptActs,
		// lg:      log.With(logger.GetLogger(), "app", appPath),
	}
}

func (f *Factory) Create(tradePair string) (app.App, error) {
	if err := f.prepare(tradePair); err != nil {
		return nil, errors.WithMessage(err, "prepare fail")
	}
	coin := strings.Split(tradePair, `USD`)[0]

	return &App{
		tradePair:       tradePair,
		redisUniqueName: fmt.Sprintf("%s:%s", strings.Split(strings.SplitN(f.config.Account, `_`, 2)[1], `(`)[0], tradePair),
		coin:            coin,
		appPath:         f.appPath,
		workingDir:      filepath.Join(f.baseDir, tradePair),
		config:          f.config,
		encryptActs:     f.encryptActs,
	}, nil
}

func (f *Factory) Clear(tradePair string) error {
	path := filepath.Join(f.baseDir, tradePair)
	if err := os.Remove(path); err != nil {
		return errors.WithMessagef(err, "remove dir fail path='%s'", path)
	}
	return nil
}

func (f *Factory) prepare(tradePair string) error {
	targetDir := filepath.Join(f.baseDir, tradePair)

	stat, err := os.Stat(targetDir)
	if err != nil && !os.IsNotExist(err) {
		return errors.WithMessagef(err, "start dir fail dir='%s'", targetDir)
	} else if err != nil {
		if err := os.MkdirAll(targetDir, 0755); err != nil {
			return errors.WithMessagef(err, "mkdir all fail dir='%s'", targetDir)
		}
	} else {
		if !stat.IsDir() {
			return errors.Errorf("file already exist '%s'", targetDir)
		}
	}
	return nil
}

func (a *App) Version() string {
	path := filepath.Join(a.workingDir, "version")
	raw, err := os.ReadFile(path)
	if err != nil {
		return "failed"
	}
	return string(raw)
}

func (a *App) UpdateConfigJSON(raw json.RawMessage) error {
	// 单所内设置usdc套利
	if a.config.Exchanges[`symbol1`].Exchange == a.config.Exchanges[`symbol2`].Exchange {
		a.enableUsdc = true
	}

	v := viper.New()
	v.SetConfigType("json")
	if err := v.ReadConfig(bytes.NewBuffer(raw)); err != nil {
		return errors.WithMessage(err, "read config fail")
	}

	var lconf config2.Config
	if err := v.Unmarshal(&lconf); err != nil {
		return errors.WithMessage(err, "unmarshal config fail")
	}
	// if lconf.Mode == `` {
	// 	lconf.Mode = rate.Swap1MSwap2T
	// }

	if lconf.Queue.MSecs == 0 {
		lconf.Queue.MSecs = 100
	}

	appConf := config.Config{
		Account:   a.config.Account,
		Exchanges: a.config.Exchanges,
		BotID:     a.config.BotID,
		ImBotID:   a.config.ImBotID,
		QImBotID:  a.config.QImBotID,
		P0BotID:   a.config.P0BotID,
		P3BotID:   a.config.P3BotID,
		Spot:      strings.ToUpper(fmt.Sprintf("%s_USDT", strings.ToUpper(a.coin))),
		Swap1:     a.transformSwapSymbol(a.config.Exchanges[`symbol1`].Exchange, false),
		Swap2:     a.transformSwapSymbol(a.config.Exchanges[`symbol2`].Exchange, a.enableUsdc),
		Redis:     a.config.Redis,
		Config:    lconf,
		Acts: config.ActsConfig{
			Rest:  a.config.Acts.Rest,
			Ws:    a.config.Acts.Ws,
			Vault: a.config.Acts.Vault,
		},
		ActsFM: config.ActsConfig{
			Rest:  a.config.ActsFM.Rest,
			Ws:    a.config.ActsFM.Ws,
			Vault: a.config.ActsFM.Vault,
		},
		WarnAt:      a.config.WarnAt,
		DepthOutPut: a.config.DepthOutPut,
	}

	raw, err := yaml.Marshal(&appConf)
	if err != nil {
		return errors.WithMessage(err, "marshal config to yaml failed")
	}

	dst := filepath.Join(a.workingDir, configFileName)
	file, err := os.OpenFile(dst, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return errors.WithMessagef(err, "open config file failed path='%s'", dst)
	}
	defer file.Close()

	if _, err := file.Write(raw); err != nil {
		return errors.WithMessage(err, "write file fail")
	}

	return nil
}

// 策略子进程启动前要调用
func (a *App) setSecretsConfig() error {
	if a.encryptActs == nil {
		return nil
	}

	appConf := config.SecretsConfig{
		Preserve: false,
		Acts: config.EncryptActs{
			Key: a.encryptActs.Key,
			IV:  a.encryptActs.IV,
		},
	}

	raw, err := yaml.Marshal(&appConf)
	if err != nil {
		return errors.WithMessage(err, "marshal config to yaml failed")
	}

	dst := filepath.Join(a.workingDir, secretsFileName)
	file, err := os.OpenFile(dst, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return errors.WithMessagef(err, "open config file failed path='%s'", dst)
	}
	defer file.Close()

	if _, err := file.Write(raw); err != nil {
		return errors.WithMessage(err, "write file fail")
	}

	return nil
}

func (a *App) GetConfig() (any, error) {
	raw, err := a.readFile(configFileName)
	if err != nil {
		return nil, errors.WithMessage(err, "read config file fail")
	}

	var cfg config.Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, errors.WithMessage(err, "unmarshal config fail")
	}

	return &cfg.Config, nil
}

// 刷新配置
func (a *App) RefreshConfig() error {
	cfg, err := a.GetConfig()
	if err != nil {
		return errors.WithMessage(err, "read config2 file fail")
	}
	raw, err := json.Marshal(cfg.(*config2.Config))
	if err != nil {
		return errors.WithMessage(err, "marshal config2 fail")
	}
	return a.UpdateConfigJSON(raw)
}

func (a *App) Detail() (any, error) {
	output, err := a.AppData()
	if err != nil {
		return nil, err
	}
	// 加上统一账户信息
	account := common.NewJsonObject()
	for s, ex := range a.config.Exchanges {
		if strings.Contains(ex.Exchange, exchange.BinancePortfolio) || ex.Exchange == exchange.Okex5 ||
			ex.Exchange == exchange.Bybit5 || ex.Exchange == exchange.GateIO {
			js, _, err := a.accountInfo(s, ex.Exchange, false)
			if err != nil {
				log.Printf("%s get account config, err:%+v, js: %+v", a.config.Account, err, js)
			} else {
				account[ex.Exchange] = js
			}
		}
	}
	output["account"] = account
	return output, nil
}

func (a *App) AppData() (map[string]any, error) {
	raw, err := a.readFile(fmt.Sprintf("%s_USDT.data", a.coin))
	if err != nil {
		return nil, errors.WithMessage(err, "read data file fail")
	}

	var output map[string]any
	if err := json.Unmarshal(raw, &output); err != nil {
		return nil, errors.WithMessage(err, "unmarshal data fail")
	}
	return output, nil
}

func (a *App) Key() string {
	return a.tradePair
}

func (a *App) RedisUniqueName() string {
	return a.redisUniqueName
}

func (a *App) Running() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.running
}

func (a *App) Start() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.running {
		return errors.Errorf("already running")
	}

	if err := a.setSecretsConfig(); err != nil {
		return errors.WithMessage(err, "set secrets config fail")
	}

	args := []string{"-w", a.workingDir, "start"}
	cmd := exec.Command(a.appPath, args...)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return errors.WithMessage(err, "pipe stderr failed")
	}
	a.done = make(chan struct{})

	if err := cmd.Start(); err != nil {
		close(a.done)
		return errors.WithMessage(err, "start cmd failed")
	}

	go func() {
		defer close(a.done)
		defer func() {
			a.mu.Lock()
			a.running = false
			a.process = nil
			a.mu.Unlock()
		}()
		errmsg, _ := io.ReadAll(stderr)
		if err := cmd.Wait(); err != nil {
			a.err.Store(errors.WithMessagef(err, "cmd return failed emsg='%s'", strings.Trim(string(errmsg), " \n\t")))
		} else {
			a.err = atomic.Value{}
		}
	}()

	a.process = cmd.Process
	a.running = true
	return nil
}

func (a *App) Stop() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.process == nil {
		return errors.Errorf("no process info")
	}

	if err := a.process.Signal(syscall.SIGINT); err != nil {
		return errors.WithMessage(err, "send signal to process failed")
	}
	return nil
}

func (a *App) Done() chan struct{} {
	return a.done
}

func (a *App) Error() error {
	if val := a.err.Load(); val != nil {
		return val.(error)
	}
	return nil
}

func (a *App) readFile(fileName string) ([]byte, error) {
	path := filepath.Join(a.workingDir, fileName)
	raw, err := os.ReadFile(path) // 内部自动关闭文件
	if err != nil {
		return nil, errors.WithMessagef(err, "read file fail path='%s'", path)
	}

	return raw, nil
}

func (a *App) writeFile(fileName string, str string) error {
	path := filepath.Join(a.workingDir, fileName)
	raw := []byte(str)

	if err := os.WriteFile(path, raw, 0644); err != nil {
		return errors.WithMessage(err, "write to file fail")
	}

	return nil
}

// TODO
func (a *App) Clear() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.running {
		return errors.Errorf("in running")
	}
	//判断合约持仓是不是0，是0才能清理
	fileName := fmt.Sprintf("%s_USDT.data", a.coin)
	raw, err := a.readFile(fileName)
	if err != nil {
		return errors.WithMessage(err, "read data file fail")
	}
	jsonStr := string(raw)
	data := gjson.Parse(jsonStr)
	if !data.IsObject() || !data.Get("pos_list").IsObject() {
		return errors.Errorf("raw not json %s", string(raw))
	}
	if data.Get(`pos_list.pos_amount`).Int() != 0 {
		return fmt.Errorf("swap pos not zero")
	}

	jsonStr, _ = sjson.Set(jsonStr, `pos_list.swap1_long_pos.pos_avg_open_price`, `0`)
	jsonStr, _ = sjson.Set(jsonStr, `pos_list.swap1_short_pos.pos_amount`, `0`)
	jsonStr, _ = sjson.Set(jsonStr, `pos_list.swap2_long_pos.pos_avg_open_price`, `0`)
	jsonStr, _ = sjson.Set(jsonStr, `pos_list.swap2_short_pos.pos_amount`, `0`)
	jsonStr, _ = sjson.Set(jsonStr, `pos_list.swap1_withdraw_available`, `0`)
	jsonStr, _ = sjson.Set(jsonStr, `pos_list.swap2_withdraw_available`, `0`)
	// jsonStr, _ = sjson.Set(jsonStr, `pos_list.pos_amount`, `0`)
	jsonStr, _ = sjson.Set(jsonStr, `pos_list.local1_pos_amount`, `0`)
	jsonStr, _ = sjson.Set(jsonStr, `pos_list.local2_pos_amount`, `0`)
	jsonStr, _ = sjson.Set(jsonStr, `pos_list.swap1_amount`, `0`)
	jsonStr, _ = sjson.Set(jsonStr, `pos_list.swap2_amount`, `0`)
	jsonStr, _ = sjson.Set(jsonStr, `pos_list.spot_amount`, `0`)
	jsonStr, _ = sjson.Set(jsonStr, `pos_list.real_currency_diff`, `0`)
	jsonStr, _ = sjson.Set(jsonStr, `pos_list.spot_keep`, `0`)
	jsonStr, _ = sjson.Set(jsonStr, `pos_list.total_exposure`, `0`)
	jsonStr, _ = sjson.Set(jsonStr, `pos_list.total_swap1_exposure`, `0`)
	jsonStr, _ = sjson.Set(jsonStr, `pos_list.total_swap2_exposure`, `0`)
	jsonStr, _ = sjson.Set(jsonStr, `pos_list.estimate_exposure`, `0`)
	jsonStr, _ = sjson.Set(jsonStr, `pos_list.slip_exposure`, `0`)
	jsonStr, _ = sjson.Set(jsonStr, `pos_list.swap_ccy`, `0`)
	jsonStr, _ = sjson.Set(jsonStr, `pos_list.funding1`, `0`)
	jsonStr, _ = sjson.Set(jsonStr, `pos_list.funding2`, `0`)
	jsonStr, _ = sjson.Set(jsonStr, `pos_list.poss`, []any{})
	jsonStr, _ = sjson.Set(jsonStr, `pos_list.pos_id`, 0)
	jsonStr, _ = sjson.Set(jsonStr, `current_pos1`, 0)
	jsonStr, _ = sjson.Set(jsonStr, `current_pos2`, 0)
	jsonStr, _ = sjson.Set(jsonStr, `pos_list.auto_exception_info.spot_exposure`, 0)
	jsonStr, _ = sjson.Set(jsonStr, `pos_list.auto_exception_info.swap_exposure`, 0)
	jsonStr, _ = sjson.Set(jsonStr, `pos_list.auto_exception_info.logs`, []string{})
	if data.Get(`adl_processor`).Exists() {
		jsonStr, _ = sjson.Set(jsonStr, `adl_processor.exposure1`, `0`)
		jsonStr, _ = sjson.Set(jsonStr, `adl_processor.exposure2`, `0`)
	}

	err = a.writeFile(fileName, jsonStr)
	if err != nil {
		return errors.WithMessage(err, "write data file fail")
	}
	return nil
}

func (a *App) ClearExposure(isSwap1 bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.running {
		return errors.Errorf("in running")
	}

	fileName := fmt.Sprintf("%s_USDT.data", a.coin)
	raw, err := a.readFile(fileName)
	if err != nil {
		return errors.WithMessage(err, "read data file fail")
	}
	jsonStr := string(raw)
	data := gjson.Parse(jsonStr)
	if !data.IsObject() || !data.Get("pos_list").IsObject() {
		return errors.Errorf("raw not json %s", string(raw))
	}
	swap1Exposure := data.Get(`pos_list.total_swap1_exposure`).Float()
	swap2Exposure := data.Get(`pos_list.total_swap2_exposure`).Float()
	if (isSwap1 && swap1Exposure == 0) || (!isSwap1 && swap2Exposure == 0) {
		return nil
	}
	if isSwap1 {
		jsonStr, err = sjson.Set(jsonStr, `pos_list.total_swap1_exposure`, `0`)
	} else {
		jsonStr, err = sjson.Set(jsonStr, `pos_list.total_swap2_exposure`, `0`)
	}
	if err != nil {
		return common.NewErrorf(err, "set exposure = 0 failed")
	}
	err = a.writeFile(fileName, jsonStr)
	if err != nil {
		return errors.WithMessage(err, "write data file fail")
	}
	return nil
}

func (a *App) SetADLEnd() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.running {
		return errors.Errorf("in running")
	}
	fileName := fmt.Sprintf("%s_USDT.data", a.coin)
	raw, err := a.readFile(fileName)
	if err != nil {
		return errors.WithMessage(err, "read data file fail")
	}
	jsonStr := string(raw)
	data := gjson.Parse(jsonStr)

	if data.Get(`adl_processor`).Exists() {
		jsonStr, _ = sjson.Set(jsonStr, `adl_processor.adl_not_finish`, false)
	}

	err = a.writeFile(fileName, jsonStr)
	if err != nil {
		return errors.WithMessage(err, "write data file fail")
	}
	return nil
}

func (f *Factory) GetRunInfo() string {
	return f.env
}

// 启动新进程下单,完成后自动退出
// TODO
func (a *App) AddOrder(params json.RawMessage, operator string) error {
	a.addOrderMu.Lock()
	defer a.addOrderMu.Unlock()
	if a.addOrderRunning {
		return errors.Errorf("already running")
	}

	if err := a.setSecretsConfig(); err != nil {
		return errors.WithMessage(err, "set secrets config fail")
	}

	js := gjson.ParseBytes(params)
	typ := js.Get(`type`).String()
	side := js.Get(`side`).String()
	amount := js.Get(`amount`).String()
	args := []string{"-w", a.workingDir, "client", "addOrder", "-t", typ, "-s", side, "-a", amount, "-o", operator}
	cmd := exec.Command(a.appPath, args...)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return errors.WithMessage(err, "pipe stderr failed")
	}

	if err := cmd.Start(); err != nil {
		return errors.WithMessage(err, "start cmd failed")
	}

	go func() {
		defer func() {
			a.addOrderMu.Lock()
			a.addOrderRunning = false
			a.addOrderMu.Unlock()
		}()
		errmsg, _ := io.ReadAll(stderr)
		if err := cmd.Wait(); err != nil {
			fmt.Printf("err=%s,add order cmd return failed emsg='%s'", err.Error(), strings.Trim(string(errmsg), " \n\t"))
			// a.err.Store(errors.WithMessagef(err, "cmd return failed emsg='%s'", strings.Trim(string(errmsg), " \n\t")))
		}
	}()

	a.addOrderRunning = true
	return nil
}

// 启动新进程转账,完成后自动退出
// TODO
func (a *App) Transfer(params json.RawMessage, operator string) error {
	a.transferMu.Lock()
	defer a.transferMu.Unlock()
	if a.transferRunning {
		return errors.Errorf("already running")
	}

	if err := a.setSecretsConfig(); err != nil {
		return errors.WithMessage(err, "set secrets config fail")
	}

	js := gjson.ParseBytes(params)
	d := js.Get(`dest`).String()
	amount := js.Get(`amount`).String()

	args := []string{"-w", a.workingDir, "client", "transfer", "-d", d, "-a", amount, "-o", operator}
	cmd := exec.Command(a.appPath, args...)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return errors.WithMessage(err, "pipe stderr failed")
	}

	if err := cmd.Start(); err != nil {
		return errors.WithMessage(err, "start cmd failed")
	}

	go func() {
		defer func() {
			a.transferMu.Lock()
			a.transferRunning = false
			a.transferMu.Unlock()
		}()
		errmsg, _ := io.ReadAll(stderr)
		if err := cmd.Wait(); err != nil {
			fmt.Printf("err=%s,tranfer cmd return failed emsg='%s'", err.Error(), strings.Trim(string(errmsg), " \n\t"))
			// a.err.Store(errors.WithMessagef(err, "cmd return failed emsg='%s'", strings.Trim(string(errmsg), " \n\t")))
		}
	}()

	a.transferRunning = true
	return nil
}

func (a *App) getActsApi(s, confExN string) (*actsApi.ActsAPI, error) {
	exConfig := a.config.Exchanges[s]
	if confExN == exchange.BinanceMargin {
		confExN = exchange.Binance
	}
	key, _ := utilCommon.DecryptKey(exConfig.Key)
	var logStr string
	if a.config.Acts.Vault != "" {
		logStr = fmt.Sprintf(`acts,%s,vault:%s`, confExN, key)
	} else {
		logStr = fmt.Sprintf(`acts,%s,%s`, confExN, key)
		apiSecret, apiPassphrase := utilCommon.DecryptKey(exConfig.Secret)
		if confExN == `okex5` {
			if apiPassphrase == `` {
				log.Printf("invalid okex5 api secret,%s", a.config.Account)
				return nil, common.NewError("invalid okex5 api secret,%s", a.config.Account)
			}
			logStr = fmt.Sprintf(`%s,%s,password=%s`, logStr, apiSecret, apiPassphrase)
		} else {
			logStr = fmt.Sprintf(`%s,%s`, logStr, apiSecret)
		}
	}
	api, err := apiPool.GetApi(logStr)
	if err != nil {
		return nil, err
	}
	return api.(*actsApi.ActsAPI), nil
}

// 统一账号信息，币安统一账户及okx跨币种
func (a *App) accountInfo(s, confExN string, export bool) (map[string]any, *gjson.Result, error) {
	if !strings.Contains(confExN, exchange.BinancePortfolio) && confExN != exchange.Okex5 && confExN != exchange.Bybit5 && confExN != exchange.GateIO {
		return nil, nil, fmt.Errorf("not support %s", confExN)
	}
	accountInfo, err := cache_service.LoadOrStore("account:"+a.config.Exchanges[s].Key, time.Minute, 0, func() (*gjson.Result, error) {
		actsapi, err := a.getActsApi(s, confExN)
		if err != nil {
			return nil, err
		}
		rawParams := &actsApi.RawRequestParams{
			Method:  http.MethodGet,
			Private: true,
			Headers: make(map[string]string),
			Url:     "https://papi.binance.com/papi/v1/account",
		}
		rawParams.Headers["Content-Type"] = "application/json"
		if confExN == exchange.Okex5 {
			rawParams.Url = `/account/balance`
		} else if confExN == exchange.Bybit5 {
			rawParams.Url = `/v5/account/wallet-balance`
			rawParams.Params = make(map[string]any)
			rawParams.Params[`accountType`] = `UNIFIED`
		} else if confExN == exchange.BinanceMargin {
			rawParams.Url = `https://api.binance.com/sapi/v1/portfolio/account`
		} else if confExN == exchange.GateIO {
			rawParams.Url = `/futures/usdt/accounts`
		}
		js, err := actsapi.RawRequestJSON(rawParams)
		if err != nil {
			return nil, err
		}
		if checkCodeFailed(js) {
			err = fmt.Errorf("invalid %s account info,%s", confExN, js.String())
		}
		log.Printf("%s account %s\n", a.config.Account, js.String())
		return js, err
	})
	if err != nil {
		return nil, nil, err
	}
	if export {
		return nil, accountInfo, nil
	}
	output := common.NewJsonObject()
	if strings.Contains(confExN, exchange.BinancePortfolio) {
		accountInfo.ForEach(func(key, value gjson.Result) bool {
			output[key.String()] = value.String()
			return true
		})
		// 再获取一下余额信息
		balanceInfo, err := cache_service.LoadOrStore("balance:"+a.config.Exchanges[s].Key, time.Minute, 0, func() (*gjson.Result, error) {
			actsapi, err := a.getActsApi(s, confExN)
			if err != nil {
				return nil, err
			}
			rawParams := &actsApi.RawRequestParams{
				Method:  http.MethodGet,
				Private: true,
				Headers: make(map[string]string),
				Url:     "https://papi.binance.com/papi/v1/balance",
			}
			if confExN == exchange.BinanceMargin {
				rawParams.Url = `https://api.binance.com/sapi/v1/portfolio/balance`
			}
			rawParams.Headers["Content-Type"] = "application/json"
			js, err := actsapi.RawRequestJSON(rawParams)
			if err != nil {
				return nil, err
			}
			if checkCodeFailed(js) {
				err = fmt.Errorf("invalid %s balance info,%s", confExN, js.String())
			}
			log.Printf("%s balance %s\n", a.config.Account, js.String())
			return js, err
		})
		if err == nil {
			for _, v := range balanceInfo.Array() {
				if v.Get(`asset`).String() == `USDT` {
					output["quoteCcy"] = `USDT`
					output["quoteAmount"] = v.Get(`totalWalletBalance`).String()
					output["quoteBorrowed"] = v.Get(`crossMarginBorrowed`).String()
					output["quoteSwapAmount"] = v.Get(`umWalletBalance`).String() // 合约钱包余额
					output["quoteUpl"] = v.Get(`umUnrealizedPNL`).String()
					output["quoteUpdateTime"] = v.Get(`updateTime`).String()
				}
				if v.Get(`asset`).String() == a.coin {
					fieldName := `cmWalletBalance`
					if strings.Contains(a.tradePair, `USDT`) || strings.Contains(a.tradePair, `USDC`) {
						fieldName = `umWalletBalance`
					}
					output["baseCcy"] = a.coin
					output["baseAmount"] = v.Get(`totalWalletBalance`).String()
					output["baseBorrowed"] = v.Get(`crossMarginBorrowed`).String()
					output["baseSwapAmount"] = v.Get(fieldName).String() // 合约钱包余额
					output["baseUpl"] = v.Get(`cmUnrealizedPNL`).String()
					output["baseUpdateTime"] = v.Get(`updateTime`).String()
				}
			}
		}
	} else if confExN == exchange.Okex5 {
		/* // 用币安格式，前端兼容
				"uniMMR": "5167.92171923",   // 统一账户维持保证金率
		        "accountEquity": "122607.35137903",   // 以USD计价的账户权益
		        "actualEquity": "73.47428058",   // 考虑质押率后的以USD计价账户权益
		        "accountInitialMargin": "23.72469206",
		        "accountMaintMargin": "23.72469206", // 以USD计价统一账户维持保证金
		        "accountStatus": "NORMAL"   // 统一账户账户状态："NORMAL", "MARGIN_CALL", "SUPPLY_MARGIN", "REDUCE_ONLY", "ACTIVE_LIQUIDATION", "FORCE_LIQUIDATION", "BANKRUPTED"
		        "virtualMaxWithdrawAmount": "1627523.32459208"  // 以USD计价的最大可转出
		        "totalAvailableBalance":"",
		        "totalMarginOpenLoss":"",
		        "updateTime": 1657707212154 // 更新时间
		*/
		output["uniMMR"] = accountInfo.Get(`data.0.mgnRatio`).String()
		output["accountEquity"] = accountInfo.Get(`data.0.totalEq`).String()
		output["actualEquity"] = accountInfo.Get(`data.0.adjEq`).String()
		output["accountInitialMargin"] = accountInfo.Get(`data.0.imr`).String()
		output["accountMaintMargin"] = accountInfo.Get(`data.0.mmr`).String()
		output["updateTime"] = accountInfo.Get(`data.0.uTime`).String()
		// 找出usdt,及币种余额
		for _, v := range accountInfo.Get(`data.0.details`).Array() {
			if v.Get(`ccy`).String() == `USDT` {
				output["quoteCcy"] = `USDT`
				output["quoteAmount"] = v.Get(`eq`).String()
				output["quoteBorrowed"] = v.Get(`liab`).String()
				output["quoteUpl"] = v.Get(`upl`).String()
				output["quoteUpdateTime"] = v.Get(`uTime`).String()
			}
			if v.Get(`ccy`).String() == a.coin {
				output["baseCcy"] = a.coin
				output["baseAmount"] = v.Get(`eq`).String()
				output["baseBorrowed"] = v.Get(`liab`).String()
				output["baseUpl"] = v.Get(`upl`).String()
				output["baseUpdateTime"] = v.Get(`uTime`).String()
			}
		}
	} else if confExN == exchange.GateIO {
		output["uniMMR"] = accountInfo.Get(`cross_mmr`).String()
		output["accountEquity"] = accountInfo.Get(`total`).String()
		output["actualEquity"] = accountInfo.Get(`total`).String()
		output["accountInitialMargin"] = accountInfo.Get(`cross_initial_margin`).String()
		output["accountMaintMargin"] = accountInfo.Get(`cross_maintenance_margin`).String()
		output["updateTime"] = accountInfo.Get(`update_time`).String() + "000"
		// 选出usdt
		output["quoteCcy"] = `USDT`
		output["quoteAmount"] = ``
		output["quoteBorrowed"] = ``
		output["quoteSwapAmount"] = accountInfo.Get(`total`).String() // 合约钱包余额
		output["quoteUpl"] = accountInfo.Get(`cross_unrealised_pnl`).String()
		output["quoteUpdateTime"] = accountInfo.Get(`update_time`).String() + "000"
	} else if confExN == exchange.Bybit5 { // bybit5
		ts := accountInfo.Get(`time`).String()
		output["uniMMR"] = accountInfo.Get(`result.list.0.accountMMRate`).String()
		output["accountEquity"] = accountInfo.Get(`result.list.0.totalEquity`).String()
		output["actualEquity"] = accountInfo.Get(`result.list.0.totalWalletBalance`).String()
		output["accountInitialMargin"] = accountInfo.Get(`result.list.0.totalInitialMargin`).String()
		output["accountMaintMargin"] = accountInfo.Get(`result.list.0.totalMaintenanceMargin`).String()
		output["updateTime"] = ts
		// 找出usdt,及币种余额
		for _, v := range accountInfo.Get(`result.list.0.coin`).Array() {
			if v.Get(`coin`).String() == `USDT` {
				output["quoteCcy"] = `USDT`
				output["quoteAmount"] = v.Get(`equity`).String()
				output["quoteBorrowed"] = v.Get(`borrowAmount`).String()
				output["quoteUpl"] = v.Get(`unrealisedPnl`).String()
				output["quoteUpdateTime"] = ts
			}
			if v.Get(`coin`).String() == a.coin {
				output["baseCcy"] = a.coin
				output["baseAmount"] = v.Get(`equity`).String()
				output["baseBorrowed"] = v.Get(`borrowAmount`).String()
				output["baseUpl"] = v.Get(`unrealisedPnl`).String()
				output["baseUpdateTime"] = ts
			}
		}
	}

	output["exchange"] = confExN

	return output, accountInfo, nil
}

func (a *App) ExportAccount() (map[string]string, error) {
	accounts := make(map[string]string)
	for s, e := range a.config.Exchanges {
		_, resp, err := a.accountInfo(s, e.Exchange, true)
		if err != nil {
			return nil, err
		}
		accounts[e.Exchange] = resp.String()
	}
	return accounts, nil
}

func (a *App) transformSwapSymbol(ex string, enableUsdc bool) string {
	var swapSymbol string
	switch ex {
	case exchange.Binance, exchange.BinancePortfolio, exchange.BinanceMargin:
		if strings.Contains(a.tradePair, `USDT`) { // U本位
			swapSymbol = strings.ToUpper(fmt.Sprintf("%sUSDT", strings.ToUpper(a.coin)))
			if enableUsdc {
				swapSymbol = strings.ToUpper(fmt.Sprintf("%sUSDC", strings.ToUpper(a.coin)))
			}
		} else {
			swapSymbol = strings.ToUpper(fmt.Sprintf("%sUSD_PERP", strings.ToUpper(a.coin)))
		}
	case exchange.Okex5:
		if strings.Contains(a.tradePair, `USDT`) { // U本位
			swapSymbol = strings.ToUpper(fmt.Sprintf("%s-USDT-SWAP", strings.ToUpper(a.coin)))
			if enableUsdc {
				swapSymbol = strings.ToUpper(fmt.Sprintf("%s-USDC-SWAP", strings.ToUpper(a.coin)))
			}
		} else {
			swapSymbol = strings.ToUpper(fmt.Sprintf("%s-USD-SWAP", strings.ToUpper(a.coin)))
		}
	case exchange.Bybit5, exchange.HyperLiquid:
		if strings.Contains(a.tradePair, `USDT`) { // U本位
			swapSymbol = fmt.Sprintf("%sUSDT", strings.ToUpper(a.coin))
			if enableUsdc {
				swapSymbol = fmt.Sprintf("%sUSDC", strings.ToUpper(a.coin))
			}
		} else {
			swapSymbol = fmt.Sprintf("%sUSD", strings.ToUpper(a.coin))
		}
	case exchange.HuobiPro:
		if strings.Contains(a.tradePair, `USDT`) { // U本位
			swapSymbol = strings.ToUpper(fmt.Sprintf("%s-USDT", strings.ToUpper(a.coin)))
			if enableUsdc {
				swapSymbol = strings.ToUpper(fmt.Sprintf("%s-USDC", strings.ToUpper(a.coin)))
			}
		} else {
			swapSymbol = strings.ToUpper(fmt.Sprintf("%s-USD", strings.ToUpper(a.coin)))
		}
	case exchange.GateIO:
		if strings.Contains(a.tradePair, `USDT`) { // U本位
			swapSymbol = strings.ToUpper(fmt.Sprintf("%s_USDT", strings.ToUpper(a.coin)))
			if enableUsdc {
				swapSymbol = strings.ToUpper(fmt.Sprintf("%s_USDC", strings.ToUpper(a.coin)))
			}
		} else {
			swapSymbol = strings.ToUpper(fmt.Sprintf("%s_USD", strings.ToUpper(a.coin)))
		}
	}
	return swapSymbol
}

// 自定义接口，调用acts raw request
func (a *App) RawRequest(params json.RawMessage, operator string) (string, error) {
	raw := gjson.ParseBytes(params)
	exchange := raw.Get(`exchange`).String()
	var (
		flag string
		ex   config.Exchange
	)
	for s, e := range a.config.Exchanges {
		if strings.Contains(e.Exchange, exchange) {
			flag = s
			ex = e
			break
		}
	}

	if flag == `` {
		return "", fmt.Errorf("找不到交易所，请检查")
	}

	actsapi, err := a.getActsApi(flag, ex.Exchange)
	if err != nil {
		return "", err
	}

	log.Printf("raw request %+v", raw.String())

	msg := &message.ManualOperate{
		Operator: operator,
		OPEvent:  "调用RawRequest",
		OPMsg:    raw.String(),
		Time:     time.Now(),
	}
	method := strings.ToUpper(raw.Get(`method`).String())

	defer func() {
		if method != http.MethodGet {
			log.Printf("raw request success %+v", msg.String())
			go message.SendImportant(context.Background(), message.NewManualOperate(`自定义接口POST`, msg))
		}
	}()

	rawParams := &actsApi.RawRequestParams{
		ApiType: raw.Get(`apiType`).String(),
		Url:     raw.Get(`url`).String(),
		Method:  method,
		Private: raw.Get(`private`).Bool(),
		Headers: make(map[string]string),
		Params:  make(map[string]any),
	}

	rawParams.Headers["Content-Type"] = "application/json"
	p := raw.Get(`params`).Value()
	if p == nil {
		rawParams.Params = nil
	} else {
		rawParams.Params = p.(map[string]any)
	}
	js, err := actsapi.RawRequestJSON(rawParams)
	if err != nil {
		msg.Result = fmt.Sprintf("操作失败，请求错误:%+v", err)
		return "", err
	}

	if checkCodeFailed(js) {
		msg.Result = fmt.Sprintf("操作失败，返回错误:%+v", js.String())
	} else {
		msg.Result = fmt.Sprintf("操作成功，交易所返回:%+v", js.String())
	}
	return js.String(), nil
}

func checkCodeFailed(js *gjson.Result) bool {
	// 检查 code 字段
	if code := js.Get("code"); code.Exists() && code.Int() != 0 {
		return true
	}

	// 检查 msg 字段
	if msg := js.Get("msg"); msg.Exists() && msg.String() != "success" && msg.String() != "" {
		return true
	}

	// 检查 retCode 字段
	if retCode := js.Get("retCode"); retCode.Exists() && retCode.Int() != 0 {
		return true
	}

	// 检查 success 字段（支持布尔值和字符串）
	if success := js.Get("success"); success.Exists() {
		if success.Type == gjson.True || success.Type == gjson.False {
			return !success.Bool()
		}
		if success.String() != "true" {
			return true
		}
	}

	return false
}
