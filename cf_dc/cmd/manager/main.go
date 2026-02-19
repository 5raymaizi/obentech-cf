package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cf_arbitrage/cli/app/cf"
	"cf_arbitrage/cli/manager"
	"cf_arbitrage/cmd/cf_arbitrage/config"
	"cf_arbitrage/message"

	common "go.common"
	"go.common/loader"
	config2 "go.common/loader/config"
	"go.common/loader/dao"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/natefinch/lumberjack"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/urfave/cli/v2"
)

type (
	Config struct {
		ExePath string    `mapstructure:"exe_path"`
		BaseDir string    `mapstructure:"base_dir"`
		Bind    string    `mapstructure:"bind"`
		Prefix  string    `mapstructure:"prefix"`
		CF      cf.Config `mapstructure:"cf"`
	}

	Response struct {
		Data  interface{} `json:"data"`
		Error string      `json:"error"`
	}
)

var (
	responseOK = Response{
		Data: "ok",
	}

	version string
	build   string
	commit  string
)

func init() {
	cli.VersionPrinter = func(c *cli.Context) {
		fmt.Printf("version=%s\tbuild=%s\tcommit=%s\n", version, build, commit)
	}
}

func main() {
	if version == "" {
		version = "unknown"
	}
	app := cli.App{
		Version: version,
		Action: func(c *cli.Context) error {
			viper.AddConfigPath("conf")
			if err := viper.ReadInConfig(); err != nil {
				return errors.WithMessage(err, "read config error")
			}

			var cfg Config
			if err := viper.Unmarshal(&cfg); err != nil {
				return errors.WithMessage(err, "unmarshal config fial")
			}
			viper.SetDefault(`redis`, viper.Get(`cf.redis`))
			config2.SetDefViper(viper.GetViper())
			config2.InitSecrets()

			viper.Set("acts.rest", cfg.CF.Acts.Rest)
			viper.Set("acts.ws", cfg.CF.Acts.Ws)
			viper.Set("acts.vault", cfg.CF.Acts.Vault)
			message.Init(cfg.CF.Account, cfg.CF.BotID, cfg.CF.ImBotID)
			message.InitBot2(cfg.CF.QImBotID, cfg.CF.P0BotID, cfg.CF.P3BotID)
			message.ReloadWarnAtRule(cfg.CF.WarnAt)
			loader.LoadAll(dao.RegistRedis)

			var encryptActs *config.EncryptActs
			if config2.SecretString("acts.encryptKey") != "" {
				encryptActs = &config.EncryptActs{
					Key: config2.SecretString("acts.encryptKey"),
					IV:  config2.SecretString("acts.encryptIV"),
				}
			}

			factory := cf.NewFactory(cfg.ExePath, cfg.BaseDir, cfg.CF, encryptActs)
			m := manager.NewManager(factory)
			if err := m.Reload(); err != nil {
				return errors.WithMessage(err, "realod config fail")
			}
			defer func() {
				if err := m.StopAll(); err != nil {
					fmt.Fprintf(os.Stderr, "stop all fail error=%s", err.Error())
				}
				m.Save()
			}()

			log.SetOutput(&lumberjack.Logger{
				Filename:   "cf_manager.log",
				MaxSize:    10,
				MaxAge:     28,
				LocalTime:  true,
				MaxBackups: 5,
			})

			go common.SetIntervalRandom([2]time.Duration{5 * time.Minute, 10 * time.Minute}, m.CheckMmrRisk)

			e := echo.New()
			e.Use(middleware.RequestLoggerWithConfig(middleware.RequestLoggerConfig{
				LogURI:    true,
				LogMethod: true,
				LogStatus: true,
				LogValuesFunc: func(c echo.Context, v middleware.RequestLoggerValues) error {
					log.Printf("time=%s method=%s uri=%s status=%d", v.StartTime.Local(), v.Method, v.URI, v.Status)
					return nil
				},
			}))

			group := e.Group(fmt.Sprintf("%s/api/cf_arbitrage/", cfg.Prefix))
			group.GET("keys", func(c echo.Context) error {
				keys := m.Keys()
				return c.JSON(200, Response{
					Data: keys,
				})
			})

			// 返回每个交易对的状态
			group.GET("keysStatus", func(c echo.Context) error {
				keys := m.KeysStatus()
				return c.JSON(200, Response{
					Data: keys,
				})
			})

			group.POST(":id", func(c echo.Context) error {
				request := c.Request()
				raw, err := io.ReadAll(request.Body)
				if err != nil {
					return c.JSON(500, Response{
						Error: fmt.Sprintf("read request body failed error=%s", err.Error()),
					})
				}
				if err := m.Add(c.Param("id"), raw); err != nil {
					return c.JSON(500, Response{
						Error: fmt.Sprintf("add id=%s failed error=%s", c.Param("id"), err.Error()),
					})
				}

				return c.JSON(200, responseOK)
			})

			group.POST(":id/addOrder", func(c echo.Context) error {
				request := c.Request()
				raw, err := io.ReadAll(request.Body)
				if err != nil {
					return c.JSON(500, Response{
						Error: fmt.Sprintf("read request body failed error=%s", err.Error()),
					})
				}

				operator := request.Header.Get(`operator`)
				if err := m.AddOrder(c.Param("id"), raw, operator); err != nil {
					return c.JSON(500, Response{
						Error: fmt.Sprintf("add order id=%s operator=%s failed error=%s", c.Param("id"), operator, err.Error()),
					})
				}

				return c.JSON(200, responseOK)
			})

			group.POST(":id/transfer", func(c echo.Context) error {
				request := c.Request()
				raw, err := io.ReadAll(request.Body)
				if err != nil {
					return c.JSON(500, Response{
						Error: fmt.Sprintf("read request body failed error=%s", err.Error()),
					})
				}

				operator := request.Header.Get(`operator`)
				if err := m.Transfer(c.Param("id"), raw, operator); err != nil {
					return c.JSON(500, Response{
						Error: fmt.Sprintf("transfer id=%s operator=%s failed error=%s", c.Param("id"), operator, err.Error()),
					})
				}

				return c.JSON(200, responseOK)
			})

			group.POST("rawRequest", func(c echo.Context) error {
				request := c.Request()
				raw, err := io.ReadAll(request.Body)
				if err != nil {
					return c.JSON(500, Response{
						Error: fmt.Sprintf("read request body failed error=%s", err.Error()),
					})
				}

				operator := request.Header.Get(`operator`)
				data, err := m.RawRequest(raw, operator)
				if err != nil {
					return c.JSON(500, Response{
						Error: fmt.Sprintf("raw request operator=%s failed error=%s", operator, err.Error()),
					})
				}

				var res any
				if err = json.Unmarshal([]byte(data), &res); err != nil {
					return c.JSON(500, Response{
						Error: fmt.Sprintf("raw request operator=%s unmarshal failed error=%s", operator, err.Error()),
					})
				}

				return c.JSON(200, Response{
					Data: res,
				})
			})

			group.POST(":id/clearExposure", func(c echo.Context) error {
				id := c.Param("id")
				request := c.Request()
				raw, err := io.ReadAll(request.Body)
				if err != nil {
					return c.JSON(500, Response{
						Error: fmt.Sprintf("read request body failed error=%s", err.Error()),
					})
				}

				if err := m.ClearExposure(id, raw); err != nil {
					return c.JSON(500, Response{
						Error: fmt.Sprintf("clear exposure fail id=%s error=%s", id, err.Error()),
					})
				}
				return c.JSON(200, responseOK)
			})

			group.GET(":id", func(c echo.Context) error {
				id := c.Param("id")
				resp, err := m.Get(id)
				if err != nil {
					return c.JSON(500, Response{
						Error: fmt.Sprintf("get id=%s failed error=%s", id, err.Error()),
					})
				}
				return c.JSON(200, Response{
					Data: resp,
				})
			})

			group.PUT(":id", func(c echo.Context) error {
				request := c.Request()
				raw, err := io.ReadAll(request.Body)
				if err != nil {
					return c.JSON(500, Response{
						Error: fmt.Sprintf("read request body failed error=%s", err.Error()),
					})
				}
				if err := m.Update(c.Param("id"), raw); err != nil {
					return c.JSON(500, Response{
						Error: fmt.Sprintf("update id=%s failed error=%s", c.Param("id"), err.Error()),
					})
				}

				return c.JSON(200, responseOK)
			})

			group.PUT(":id/start", func(c echo.Context) error {
				id := c.Param("id")
				if err := m.Start(id); err != nil {
					return c.JSON(500, Response{
						Error: fmt.Sprintf("start fail id=%s error=%s", id, err.Error()),
					})
				}
				return c.JSON(200, responseOK)
			})

			group.PUT(":id/stop", func(c echo.Context) error {
				id := c.Param("id")
				if err := m.Stop(id); err != nil {
					return c.JSON(500, Response{
						Error: fmt.Sprintf("stop fail id=%s error=%s", id, err.Error()),
					})
				}
				return c.JSON(200, responseOK)
			})

			group.PUT("stopall", func(c echo.Context) error {
				if err := m.StopAll(); err != nil {
					return c.JSON(500, Response{
						Error: fmt.Sprintf("stopall fail error=%s", err.Error()),
					})
				}

				return c.JSON(200, responseOK)
			})

			group.PUT("startall", func(c echo.Context) error {
				if err := m.StartAll(); err != nil {
					return c.JSON(500, Response{
						Error: fmt.Sprintf("startall fail error=%s", err.Error()),
					})
				}
				return c.JSON(200, responseOK)
			})

			group.PUT("refreshall", func(c echo.Context) error {
				if err := m.UpdateAll(); err != nil {
					return c.JSON(500, Response{
						Error: fmt.Sprintf("refreshall fail error=%s", err.Error()),
					})
				}
				return c.JSON(200, responseOK)
			})

			group.PUT(":id/clear", func(c echo.Context) error {
				id := c.Param("id")
				if err := m.Clear(id); err != nil {
					return c.JSON(500, Response{
						Error: fmt.Sprintf("clear fail id=%s error=%s", id, err.Error()),
					})
				}
				return c.JSON(200, responseOK)
			})

			group.PUT(":id/setADLEnd", func(c echo.Context) error {
				id := c.Param("id")
				if err := m.SetADLEnd(id); err != nil {
					return c.JSON(500, Response{
						Error: fmt.Sprintf("set adl end fail id=%s error=%s", id, err.Error()),
					})
				}
				return c.JSON(200, responseOK)
			})

			group.DELETE(":id/del", func(c echo.Context) error {
				id := c.Param("id")
				if err := m.Del(id); err != nil {
					return c.JSON(500, Response{
						Error: fmt.Sprintf("del fail id=%s error=%s", id, err.Error()),
					})
				}
				return c.JSON(200, responseOK)
			})

			go func() {
				e.Logger.Info(e.Start(cfg.Bind))
			}()

			sig := make(chan os.Signal, 1)
			signal.Notify(sig, syscall.SIGINT, syscall.SIGABRT, syscall.SIGTERM)

			<-sig

			e.Shutdown(context.Background())
			return nil
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "run error error=%s", err.Error())
		return
	}
}
