package main

import (
	"cf_arbitrage/cmd/cf_arbitrage/config"
	"cf_arbitrage/exchange"
	"cf_arbitrage/logic"
	"cf_arbitrage/logic/rate"
	"cf_arbitrage/util/logger"
	"cf_arbitrage/util/reader"
	"fmt"
	"os"

	ccexgo "github.com/NadiaSama/ccexgo/exchange"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"github.com/urfave/cli/v2"
)

/*
根据采集的depth文件判断是否可以开仓
*/

type (
	Checker struct {
	}

	testSwapSymbol struct {
		ccexgo.BaseSwapSymbol
	}
)

func (tsw *testSwapSymbol) String() string {
	return "test_swap_symbol"
}

func isGoodOpen(spotDepth *exchange.Depth, swapDepth *exchange.Depth, limit float64) bool {
	sr00 := swapDepth.Bids[0].Price/spotDepth.Asks[0].Price - 1
	sr01 := swapDepth.Bids[0].Price/spotDepth.Asks[1].Price - 1
	sr10 := swapDepth.Bids[1].Price/spotDepth.Asks[0].Price - 1
	sr11 := swapDepth.Bids[1].Price/spotDepth.Asks[1].Price - 1

	return sr00 > limit && sr01 > limit && sr10 > limit && sr11 > limit
}

func (ch *Checker) Check(limit float64, spotReader reader.GzDepthReader, swapReader reader.GzDepthReader) error {
	var (
		spotDepth *exchange.Depth
		swapDepth *exchange.Depth
		err       error
	)
	spotDepth, err = spotReader.Read()
	if err != nil {
		return errors.WithMessage(err, "read spot fail")
	}

	swapDepth, err = swapReader.Read()
	if err != nil {
		return errors.WithMessage(err, "read swap fail")
	}

	for {
		if isGoodOpen(spotDepth, swapDepth, limit) {
			fmt.Printf("spotDepth=%s swapDepth=%s\n", spotDepth, swapDepth)
		}

		if spotDepth.Created.Before(swapDepth.Created) {
			spotDepth, err = spotReader.Read()
			if err != nil {
				return errors.WithMessage(err, "read spot depth fail")
			}
		} else {
			swapDepth, err = swapReader.Read()
			if err != nil {
				return errors.WithMessage(err, "read swap depth fial")
			}
		}
	}
}

func main() {
	app := cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "date",
				Aliases:     []string{"d"},
				Required:    true,
				DefaultText: "specific gzfile date",
			},
		},

		Before: func(c *cli.Context) error {
			date := c.String("d")
			spotReader, err := reader.NewGzDepthReader(fmt.Sprintf("huobi.shibusdt-%s.csv.gz", date))
			if err != nil {
				return errors.WithMessage(err, "create spot reader fail")
			}

			swapReader, err := reader.NewGzDepthReader(fmt.Sprintf("huobi.SHIB-USD-%s.csv.gz", date))
			if err != nil {
				return errors.WithMessage(err, "create swap reader fail")
			}

			c.App.Metadata["spotReader"] = spotReader
			c.App.Metadata["swapReader"] = swapReader

			if err := config.Init("."); err != nil {
				return errors.WithMessage(err, "init config fail")
			}
			return nil
		},

		Commands: []*cli.Command{
			{
				Name: "raw",
				Action: func(c *cli.Context) error {
					spotReader := c.App.Metadata["spotReader"].(reader.GzDepthReader)
					swapReader := c.App.Metadata["swapReader"].(reader.GzDepthReader)

					config := config.Ins().LoadConfig()

					var ck Checker
					if err := ck.Check(config.Config.Queue.OpenThreshold, spotReader, swapReader); err != nil {
						fmt.Printf("error=%s\n", err.Error())
					}
					return nil
				},
			},
			{
				Name: "queue",
				Action: func(c *cli.Context) error {
					var (
						spotDepth *exchange.Depth
						swapDepth *exchange.Depth
						err       error
					)
					spotReader := c.App.Metadata["spotReader"].(reader.GzDepthReader)
					swapReader := c.App.Metadata["swapReader"].(reader.GzDepthReader)
					config := config.Ins().LoadConfig()
					q := rate.NewQueue(config.Config.Queue, false)

					spotDepth, err = spotReader.Read()
					if err != nil {
						return errors.WithMessage(err, "read spot fail")
					}

					swapDepth, err = swapReader.Read()
					if err != nil {
						return errors.WithMessage(err, "read swap fail")
					}

					q.PushSpot(spotDepth)
					q.PushSwap(swapDepth)

					for {
						if q.CanOpen() {
							fmt.Printf("can open spot=%s swap=%s\n", spotDepth, swapDepth)
						} else if q.CanClose() {
							fmt.Printf("can close spot=%s swap=%s\n", spotDepth, swapDepth)
						}

						if spotDepth.Created.Before(swapDepth.Created) {
							spotDepth, err = spotReader.Read()
							if err != nil {
								return errors.WithMessage(err, "read spot depth fail")
							}
							q.PushSpot(spotDepth)
						} else {
							swapDepth, err = swapReader.Read()
							if err != nil {
								return errors.WithMessage(err, "read swap depth fial")
							}
							q.PushSwap(swapDepth)
						}
					}
				},
			},
			{
				Name: "strategy",
				Action: func(c *cli.Context) error {
					spotReader := c.App.Metadata["spotReader"].(reader.GzDepthReader)
					swapReader := c.App.Metadata["swapReader"].(reader.GzDepthReader)

					tsw := testSwapSymbol{
						BaseSwapSymbol: *ccexgo.NewBaseSwapSymbolWithCfg("", decimal.NewFromFloat(10.0),
							ccexgo.SymbolConfig{}, nil),
					}
					config := config.Ins().LoadConfig()

					lg := level.NewFilter(logger.GetLogger(), level.AllowDebug())
					sg := logic.NewStrategy(config.Exchange, nil, nil, nil, nil, &tsw, nil, lg)

					good, bad, err := sg.Test(config.Config, spotReader, swapReader)
					for _, g := range good {
						fmt.Printf("open sr=%f spot_depth=%s swap_depth=%s\n", g.SR, g.Spot, g.Swap)
					}

					for _, b := range bad {
						fmt.Printf("close sr=%f spot_depth=%s swap_depth=%s\n", b.SR, b.Spot, b.Swap)
					}

					return err
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Printf("run app error=%s\n", err.Error())
	}
}
