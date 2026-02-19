package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"cf_arbitrage/cmd/cf_arbitrage/config"
	"cf_arbitrage/exchange"
	"cf_arbitrage/exchange/acts"
	"cf_arbitrage/exchange/acts/swap"
	"cf_arbitrage/logic"
	"cf_arbitrage/logic/position"
	"cf_arbitrage/message"
	"cf_arbitrage/output"
	loggerUtil "cf_arbitrage/util/logger"

	common "go.common"
	"go.common/apis"
	config2 "go.common/loader/config"
	"go.common/loader/dao"
	"go.common/log"

	ccexgo "github.com/NadiaSama/ccexgo/exchange"
	"github.com/NadiaSama/ccexgo/exchange/binance/spot"
	"github.com/NadiaSama/ccexgo/misc/ctxlog"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"github.com/spf13/viper"
	"github.com/urfave/cli/v2"
)

type ()

var (
	version string = "unknown"
	commit  string
	build   string
)

func init() {
	cli.VersionPrinter = func(c *cli.Context) {
		fmt.Printf("version: %s\tbuild: %s\tcommit: %s\n", version, build, commit)
	}
}

func recordVersion() error {
	file, err := os.OpenFile("version", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		return errors.WithMessage(err, "create version file fail")
	}
	defer file.Close()

	var c string
	if len(commit) < 8 {
		c = commit
	} else {
		c = commit[:8]
	}

	if _, err := file.Write([]byte(fmt.Sprintf("%s.%s", version, c))); err != nil {
		return errors.WithMessage(err, "write version info fail")
	}
	return nil
}

func start(c *cli.Context) error {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)

	isDebug := c.Bool("d")
	workingDir := c.String("w")
	if err := os.Chdir(workingDir); err != nil {
		panic(fmt.Sprintf("change workding_dir to %s fail %s", workingDir, err.Error()))
	}

	if err := config.Init("./"); err != nil {
		return errors.WithMessage(err, "loader init fail")
	}

	loader := config.Ins()
	defer loader.Close()
	cfg := loader.LoadConfig()

	if cfg.BotID == "" || cfg.ImBotID == "" {
		return errors.Errorf("missing BotID")
	}

	if err := loggerUtil.NewLogger(cfg.Config.Log, isDebug); err != nil {
		return errors.WithMessage(err, "create logger fail")
	}
	defer loggerUtil.Close()
	logger := loggerUtil.GetLogger()

	message.Init(cfg.Account, cfg.BotID, cfg.ImBotID)
	message.InitBot2(cfg.QImBotID, cfg.P0BotID, cfg.P3BotID)
	message.ReloadWarnAtRule(cfg.WarnAt)

	wStr := strings.Split(workingDir, "/")
	tradePair := wStr[len(wStr)-1]

	defer func() {
		message.Send(context.Background(), message.NewLogicExit(cfg.Swap1))
	}()
	config2.SetDefViper(viper.GetViper())
	config2.InitSecrets()

	vault := cfg.Acts.Vault != ""
	if (strings.Contains(cfg.Exchanges[`symbol1`].Exchange, exchange.Binance) || strings.Contains(cfg.Exchanges[`symbol2`].Exchange, exchange.Binance)) && cfg.Config.UseMMApi {
		viper.Set("acts.rest", cfg.ActsFM.Rest)
		viper.Set("acts.ws", cfg.ActsFM.Ws)
		viper.Set("acts.vault", cfg.ActsFM.Vault)
		vault = cfg.ActsFM.Vault != ""
	}

	if err := dao.RegistRedis(); err != nil {
		return err
	}
	log.AddFileLog4(`common.log`, log.LevelDebug, 8192, 1024)

	ctx, cancel := context.WithCancel(context.Background())

	var (
		swap1, swap2     string
		level1, level2   int
		symbol1, symbol2 config.Exchange
	)

	// if cfg.Config.Mode == rate.Swap1MSwap2T || cfg.Config.Mode == rate.Swap1TSwap2M || cfg.Config.Mode == rate.Swap1MSwap2M || cfg.Config.Mode == rate.Swap1TSwap2T {
	symbol1 = cfg.Exchanges[`symbol1`]
	symbol2 = cfg.Exchanges[`symbol2`]
	swap1 = cfg.Swap1
	swap2 = cfg.Swap2
	level1 = cfg.Config.Level1
	level2 = cfg.Config.Level2
	// } else {
	// 	cancel()
	// 	return common.NewErrorf(nil, "invalid mode")
	// }

	api, err := acts.NewActsApi(symbol1.Exchange, symbol1.Key, symbol1.Secret, vault)
	if err != nil {
		cancel()
		return errors.WithMessage(err, fmt.Sprintf("%s init fail", symbol1.Exchange))
	}

	api2, err := acts.NewActsApi(symbol2.Exchange, symbol2.Key, symbol2.Secret, vault)
	if err != nil {
		cancel()
		return errors.WithMessage(err, fmt.Sprintf("%s init fail", symbol2.Exchange))
	}
	api2.Close()

	// 双合约现货交易对写死，主要用于初始化open,close文件名。必须以_分割，如：ETH_USDT
	split := strings.Split(cfg.Spot, `_`)
	if len(split) != 2 {
		cancel()
		return errors.Errorf("invalid spot symbol '%s'", cfg.Spot)
	}
	spotS := &spot.SpotSymbol{
		BaseSpotSymbol: ccexgo.NewBaseSpotSymbol(split[0], split[1], ccexgo.SymbolConfig{}, cfg.Spot),
		Symbol:         cfg.Spot,
	}
	swapS1, swapAs1, err := acts.ParseSwapSymbol(swap1, symbol1.Exchange, apis.ProductTypeSwap)
	if err != nil {
		cancel()
		return errors.WithMessage(err, "parse swap1 symbol fail")
	}
	swapS2, swapAs2, err := acts.ParseSwapSymbol(swap2, symbol2.Exchange, apis.ProductTypeSwap)
	if err != nil {
		cancel()
		return errors.WithMessage(err, "parse swap2 symbol fail")
	}

	redisUniqueName := config.GenerateUniqueName(cfg.Account) // mmadmin env

	ql := position.NewList(spotS, swapS1, swapS2, redisUniqueName, acts.NewFundTransfer(api), logger, int64(level1), int64(level2), swapAs1, swapAs2, symbol1.Exchange, symbol2.Exchange)

	sg := logic.NewStrategy(
		redisUniqueName,
		fmt.Sprintf("%s_%s", symbol1.Exchange, symbol2.Exchange),
		loader,
		swap.NewClientCB(swapS1, swapAs1, symbol1.Exchange, symbol1.Key, symbol1.Secret, false, vault),
		swap.NewClientCB(swapS2, swapAs2, symbol2.Exchange, symbol2.Key, symbol2.Secret, false, vault),
		spotS, swapS1, swapS2, swapAs1, swapAs2, ql,
		logger)
	// redisUniqueName := strings.Split(strings.SplitN(cfg.Account, `_`, 2)[1], `(`)[0]
	// if !strings.Contains(swap1, `USDT`) {
	// 	redisUniqueName += fmt.Sprintf(`:%sUSD`, spotS.Base())
	// } else {
	// 	redisUniqueName += fmt.Sprintf(`:%sUSDT`, spotS.Base())
	// }
	if cfg.Config.Factor.ColoOkx {
		acts.SetColoOkxWs()
	}

	orderBookUnique := redisUniqueName + ":" + tradePair

	level.Info(logger).Log("message", "redis unique name", "name", redisUniqueName)
	manager, err := output.NewManager(sg.Output(), cfg.DepthOutPut+tradePair+"/", fmt.Sprintf("%s_%s", symbol1.Exchange, symbol2.Exchange), tradePair, orderBookUnique)
	if err != nil {
		cancel()
		return errors.WithMessage(err, "create manager fail")
	}
	if err := sg.Init(); err != nil {
		cancel()
		return errors.WithMessage(err, "init strategy fail")
	}

	if err := recordVersion(); err != nil {
		cancel()
		return errors.WithMessage(err, "recordVersion fail")
	}

	defer sg.FInit()
	go manager.Loop(ctx)
	sg.AddLoopWg()
	go sg.Loop(ctxlog.SetLog(ctx, logger), cfg.Config.Factor.SubscribeTrade)

	<-sig
	cancel()
	level.Info(logger).Log("message", "cancel wait strategy")
	sg.Wait()
	manager.Wait() // 等待manager完成数据刷新
	common.SetProgramDone()
	log.CloseFileLogs()
	level.Info(logger).Log("message", "all wait done")
	select {
	case <-manager.Done():
		return nil
	case <-time.After(time.Second):
		return errors.Errorf("quit timeout")
	}
}

func initClient(c *cli.Context) error {
	viper.AddConfigPath(c.String("w"))
	if err := viper.ReadInConfig(); err != nil {
		return errors.WithMessage(err, "read config fail")
	}

	var cfg config.Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return errors.WithMessage(err, "unmarshal config fail")
	}

	message.Init(cfg.Account, cfg.BotID, cfg.ImBotID)
	message.InitBot2(cfg.QImBotID, cfg.P0BotID, cfg.P3BotID)
	message.ReloadWarnAtRule(cfg.WarnAt)

	var (
		swap1, swap2     string
		symbol1, symbol2 config.Exchange
	)

	// if cfg.Config.Mode == rate.Swap1MSwap2T || cfg.Config.Mode == rate.Swap1TSwap2M || cfg.Config.Mode == rate.Swap1MSwap2M || cfg.Config.Mode == rate.Swap1TSwap2T {
	symbol1 = cfg.Exchanges[`symbol1`]
	symbol2 = cfg.Exchanges[`symbol2`]
	swap1 = cfg.Swap1
	swap2 = cfg.Swap2
	// } else {
	// 	return common.NewErrorf(nil, "invalid mode")
	// }
	vault := cfg.Acts.Vault != ""
	api, err := acts.NewActsApi(symbol1.Exchange, symbol1.Key, symbol1.Secret, vault)
	if err != nil {
		return errors.WithMessage(err, fmt.Sprintf("%s init fail", symbol1.Exchange))
	}

	api2, err := acts.NewActsApi(symbol2.Exchange, symbol2.Key, symbol2.Secret, vault)
	if err != nil {
		return errors.WithMessage(err, fmt.Sprintf("%s init fail", symbol2.Exchange))
	}
	api2.Close()

	swapS1, swapAs1, err := acts.ParseSwapSymbol(swap1, symbol1.Exchange, apis.ProductTypeSwap)
	if err != nil {
		return errors.WithMessage(err, "parse swap1 symbol fail")
	}
	swapS2, swapAs2, err := acts.ParseSwapSymbol(swap2, symbol2.Exchange, apis.ProductTypeSwap)
	if err != nil {
		return errors.WithMessage(err, "parse swap2 symbol fail")
	}

	c.App.Metadata["swap1"] = swapS1
	c.App.Metadata["swap2"] = swapS2
	c.App.Metadata["swapAs"] = swapAs1
	c.App.Metadata["conf"] = cfg

	cl, err := swap.NewClient(swapS1, swapAs1, symbol1.Exchange, symbol1.Key, symbol1.Secret, false, vault)
	if err != nil {
		return errors.WithMessage(err, "new swap1 clint fail")
	}
	c.App.Metadata["swapC1"] = cl
	ctx, _ := context.WithCancel(context.Background())
	cl.InitFeeRate(ctx)
	sl, err := swap.NewClient(swapS2, swapAs2, symbol2.Exchange, symbol2.Key, symbol2.Secret, false, vault)
	if err != nil {
		return errors.WithMessage(err, "new swap2 clint fail")
	}
	c.App.Metadata["swapC2"] = sl
	sl.InitFeeRate(ctx)
	tf := acts.NewFundTransfer(api)
	c.App.Metadata["tf"] = tf
	acts.SetManual()

	return nil
}

func main() {

	if version == "" {
		version = "unknown"
	}

	app := cli.App{
		Version: version,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "dir",
				Aliases:     []string{"w"},
				Required:    true,
				DefaultText: "specific working dir. workding dir should contain config.yaml",
			},
		},

		Commands: []*cli.Command{
			{
				Name:        "start",
				Description: "start cf_arbitrage",
				Action: func(c *cli.Context) error {
					return start(c)
				},
			},
			{
				Name:        "client",
				Description: "check specific order match_results",

				Before: func(c *cli.Context) error {
					//set client
					return initClient(c) //TODO 初始化失败告警
				},
				Subcommands: []*cli.Command{
					{
						Name:        "order",
						Description: "check specific order",
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:        "id",
								Aliases:     []string{"i"},
								Required:    true,
								DefaultText: "specific order_id",
							},

							&cli.StringFlag{
								Name:        "type",
								Aliases:     []string{"t"},
								Required:    true,
								DefaultText: "specific client type (spot|swap1|swap2)",
							},
							&cli.StringFlag{
								Name:        "operator",
								Aliases:     []string{"o"},
								Required:    true,
								DefaultText: "specific transfer operator",
							},
						},

						Action: func(c *cli.Context) error {
							var (
								cl         exchange.Client
								swapSymbol ccexgo.SwapSymbol
							)
							swapSymbol1 := c.App.Metadata["swap1"].(ccexgo.SwapSymbol)
							swapSymbol2 := c.App.Metadata["swap2"].(ccexgo.SwapSymbol)
							t := c.String("t")
							if t == "swap1" {
								cl = c.App.Metadata["swapC1"].(exchange.SwapClient)
								swapSymbol = swapSymbol1
							} else if t == "swap2" {
								cl = c.App.Metadata["swapC2"].(exchange.SwapClient)
								swapSymbol = swapSymbol2
							} else {
								return errors.Errorf("unkown type '%s'", t)
							}

							operator := c.String("o")
							i := c.String("i")
							msg := &message.ManualOperate{
								Operator: operator,
								OPEvent:  "查询",
								OPMsg:    fmt.Sprintf("交易所 %s 交易对:%s, 类型 %s, 订单号 %s", cl.GetExchangeName(), swapSymbol, t, i),
								Time:     time.Now(),
							}
							defer func() {
								message.Send(context.Background(), message.NewManualOperate("订单查询", msg))
							}()

							order, err := cl.FetchOrder(context.Background(), &exchange.CfOrder{
								Order: ccexgo.Order{
									Symbol: swapSymbol,
									ID:     ccexgo.NewStrID(c.String("i")),
								},
							})
							if err != nil {
								msg.Result = fmt.Sprintf("查询失败,err=%s", err)
								return errors.WithMessage(err, "fetch order fail")
							}
							msg.Result = fmt.Sprintf("%+v", order)
							fmt.Printf("order=%+v\n", *order)
							return nil
						},
					},
					{
						Name:        "addOrder",
						Description: "add order",
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:        "type",
								Aliases:     []string{"t"},
								Required:    true,
								DefaultText: "specific client type (binance|okex5|bybit5)",
							},
							&cli.StringFlag{
								Name:        "side",
								Aliases:     []string{"s"},
								Required:    true,
								DefaultText: "specific client side (buy|sell|closeLong|closeShort)",
							},
							&cli.Float64Flag{
								Name:        "amount",
								Aliases:     []string{"a"},
								Required:    true,
								DefaultText: "specific transfer amount",
							},
							&cli.StringFlag{
								Name:        "operator",
								Aliases:     []string{"o"},
								Required:    true,
								DefaultText: "specific transfer operator",
							},
						},

						Action: func(c *cli.Context) error {

							swapC1 := c.App.Metadata["swapC1"].(exchange.SwapClient)
							swapC2 := c.App.Metadata["swapC2"].(exchange.SwapClient)
							swapSymbol1 := c.App.Metadata["swap1"].(ccexgo.SwapSymbol)
							swapSymbol2 := c.App.Metadata["swap2"].(ccexgo.SwapSymbol)
							swapActsSymbol := c.App.Metadata["swapAs"].(*apis.Symbol)
							inverse := swapActsSymbol.ContractValueIsInQuoteCurrency

							t := c.String("t")
							amount := c.Float64("a")
							side := c.String("s")
							operator := c.String("o")
							param := c.String("p")
							msg := &message.ManualOperate{
								Operator: operator,
								OPEvent:  "下单",
								Time:     time.Now(),
							}
							warnSwapSymbol := swapSymbol1

							defer func() {
								msg.OPMsg = fmt.Sprintf("方向 %s,数量 %v,参数 %s", t+` `+side, amount, param)
								message.SendImportant(context.Background(), message.NewManualOperate(fmt.Sprintf("%s 手动下单告警", warnSwapSymbol.String()), msg))
							}()

							if amount <= .0 {
								msg.Result = fmt.Sprintf("非法数量%f,拒绝操作", amount)
								return errors.Errorf("invalid amount %f", amount)
							}

							var sid ccexgo.OrderSide
							switch side {
							case `buy`:
								sid = ccexgo.OrderSideBuy
							case `sell`:
								sid = ccexgo.OrderSideSell
							case `closeLong`:
								sid = ccexgo.OrderSideCloseLong
							case `closeShort`:
								sid = ccexgo.OrderSideCloseShort
							default:
								msg.Result = fmt.Sprintf("非法方向%s,拒绝操作", side)
								return errors.Errorf("invalid side %s", side)
							}

							addOrder := func(swapC exchange.SwapClient, swapSymbol ccexgo.SwapSymbol) error {
								if inverse && amount*swapSymbol.ContractVal().InexactFloat64() > 20000 { // 大于1000U
									msg.Result = "价值大于20000U,拒绝操作"
									return errors.Errorf("err params '%s'", msg.String())
								}
								amt := decimal.NewFromFloat(amount)
								if inverse {
									amt = amt.Truncate(0)
								}

								swapOrder, err := swapC.MarketOrder(context.Background(), sid, amt, decimal.Zero, nil)
								if err != nil {
									msg.Result = fmt.Sprintf("下单错误,err=%s", swapC.HandError(err))
									return err
								}
								msg.Result = fmt.Sprintf("合约平仓成功,order=%+v\n", swapOrder)
								fmt.Printf("%s swap success, amount=%s\n", sid.String(), swapOrder.Filled.String())
								if sid < ccexgo.OrderSideCloseLong { // 开仓
									msg.Result = fmt.Sprintf("合约开仓成功,order=%+v\n", swapOrder)
								}
								return nil
							}

							if swapC1.GetExchangeName() == swapC2.GetExchangeName() {
								if t == swapC1.GetExchangeName() {
									return addOrder(swapC1, swapSymbol1)
								} else {
									t = swapC1.GetExchangeName()
									warnSwapSymbol = swapSymbol2
									return addOrder(swapC2, swapSymbol2)
								}
							} else {
								if strings.Contains(swapC1.GetExchangeName(), t) {
									return addOrder(swapC1, swapSymbol1)
								} else if strings.Contains(swapC2.GetExchangeName(), t) {
									warnSwapSymbol = swapSymbol2
									return addOrder(swapC2, swapSymbol2)
								}
							}

							msg.Result = fmt.Sprintf("非法类型%s,拒绝操作", t)
							return errors.Errorf("unkown type '%s'", t)
						},
					},
					{
						Name:        "account",
						Description: "check spot/swap account",
						Action: func(c *cli.Context) error {
							keys := []string{"swapC1", "swapC2"}
							ctx := context.Background()
							for _, k := range keys {
								cl := c.App.Metadata[k].(exchange.SwapClient)
								ac, err := cl.FetchBalance(ctx)
								if err != nil {
									return errors.WithMessage(err, "fetch balance fail")
								}
								fmt.Printf("%s balance=%+v\n", k, ac)

								pos, err := cl.Position(ctx)
								if err != nil {
									return errors.WithMessage(err, "fetch position fail")
								}
								fmt.Printf("%s pos1=%+v pos2=%+v\n", k, pos.Long, pos.Short)

							}
							return nil
						},
					},
					{
						Name:        "transfer",
						Description: "transfer balances between swap and spot account",
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:        "dest",
								Aliases:     []string{"d"},
								Required:    true,
								DefaultText: "specific dest account (spot/swap)",
							},
							&cli.Float64Flag{
								Name:        "amount",
								Aliases:     []string{"a"},
								Required:    true,
								DefaultText: "specific transfer amount",
							},
							&cli.StringFlag{
								Name:        "operator",
								Aliases:     []string{"o"},
								Required:    true,
								DefaultText: "specific transfer operator",
							},
						},
						Action: func(c *cli.Context) error {
							tf := c.App.Metadata["tf"].(exchange.FundTransfer)
							swapC := c.App.Metadata["swapC"].(exchange.SwapClient)
							spotSymbol := c.App.Metadata["spot"].(ccexgo.SpotSymbol)
							swapSymbol := c.App.Metadata["swap"].(ccexgo.SwapSymbol)

							var (
								from exchange.AccountType
								to   exchange.AccountType
							)
							d := c.String("d")
							amount := c.Float64("a")
							operator := c.String("o")

							msg := &message.ManualOperate{
								Operator: operator,
								OPEvent:  "转账",
								OPMsg:    fmt.Sprintf("方向 %s,数量 %v", "To "+d, amount),
								Time:     time.Now(),
							}
							var isversed bool
							if !strings.Contains(swapSymbol.String(), `USDT`) {
								isversed = true
							}

							defer func() {
								message.SendImportant(context.Background(), message.NewManualOperate(fmt.Sprintf("%s 手动转账告警", swapSymbol.String()), msg))
							}()

							if amount <= .0 {
								msg.Result = fmt.Sprintf("非法数量:%f,拒绝操作", amount)
								return errors.Errorf("invalid amount %f", amount)
							}

							if d == "spot" {
								from = exchange.AccountTypeSwap
								to = exchange.AccountTypeSpot
							} else if d == "swap" {
								from = exchange.AccountTypeSpot
								to = exchange.AccountTypeSwap
							} else {
								msg.Result = fmt.Sprintf("非法方向%s,拒绝操作", d)
								return errors.Errorf("unkown dest '%s'", d)
							}

							amt := decimal.NewFromFloat(amount).Truncate(8)
							tfAsset := spotSymbol.Base()
							if !isversed {
								tfAsset = spotSymbol.Quote()
							}
							if err := tf.Transfer(context.Background(), from, to, tfAsset, amt); err != nil {
								msg.Result = fmt.Sprintf("转账失败,数量:%s %s,err=%+v", amt.String(), tfAsset, swapC.HandError(err))
								return errors.WithMessage(err, "transfer failed")
							}
							msg.Result = fmt.Sprintf("转账成功,数量:%s %s", amt.String(), tfAsset)

							fmt.Printf("transfer to %+v success, amount=%s, asset=%s\n", d, amt.String(), tfAsset)
							return nil
						},
					},
					{
						Name:  "funding",
						Usage: "fetch funding list",
						Action: func(c *cli.Context) error {
							cl := c.App.Metadata["swapC"].(exchange.SwapClient)
							funding, err := cl.Funding(context.Background())
							if err != nil {
								return errors.WithMessage(err, "fetch funding fail")
							}

							for _, f := range funding {
								fmt.Printf("funding %+v\n", f)
							}
							return nil
						},
					},
					{
						Name:  "feeRate",
						Usage: "fetch feeRate",
						Action: func(c *cli.Context) error {
							swapC1 := c.App.Metadata["swapC1"].(exchange.SwapClient)
							swapC2 := c.App.Metadata["swapC2"].(exchange.SwapClient)
							swap1FeeRate := swapC1.FeeRate()
							swap2FeeRate := swapC2.FeeRate()
							fmt.Printf("swap1 feeRate %+v\nswap2 feeRate %+v\n", swap1FeeRate, swap2FeeRate)
							return nil
						},
					},
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		panic(err.Error())
	}
}

func tomorrow() *time.Timer {
	next := time.Now().Add(time.Hour * 24)
	return time.NewTimer(time.Until(time.Date(next.Year(), next.Month(), next.Day(), 0, 0, 0, 0, next.Location())))
}
