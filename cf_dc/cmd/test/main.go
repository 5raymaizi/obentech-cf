package main

import (
	"fmt"
	"sync"
	"time"

	"runtime/debug"

	"github.com/pkg/errors"
	common "go.common"
	"go.common/apis"
	"go.common/apis/apiFactory"
	"go.common/loader/config"
)

var commit = func() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				return setting.Value
			}
		}
	}

	return ""
}()

type baseMessage struct {
	title string
	body  string
}

func (bm *baseMessage) Title() string {
	return bm.title
}

func (bm *baseMessage) Body() string {
	return bm.body
}

var depthCh chan *apis.DepthEvent

func main() {
	fmt.Println(commit)
	fmt.Println(debug.ReadBuildInfo())
	return
	// ctx := context.Background()
	// title := "ETC_USDT open position"
	// body := "pos_id=20220804_891, start=2022-08-04 10:15:32.81360638 +0800 CST m=+235.348037212, sr_open=0.000520, open_trade_unit=50, open_usdt=500.004, spot_order_id=2713657802, spot_avg_price=36.550000000000004, spot_deal_amount=13.68, spot_open_time=1970-01-01 08:00:00 +0800 CST, spot_done_time=2022-08-04 10:15:32.828 +0800 CST, swap_order_id=4172430849, swap_avg_price=36.58, swap_deal_amount=50, swap_open_time=2022-08-04 10:15:32.891 +0800 CST, swap_done_time=2022-08-04 10:15:32.878 +0800 CST, spot_to_swap_amount=10.9396224, spot_keep=2.7349056, spot_to_swap_duration=162.571915ms, spot_depth_id=2012, spot_depth_bid0_price=36.54, spot_depth_ask0_price=36.55, spot_depth_bid1_price=36.53, spot_depth_ask1_price=36.56, spot_depth_timestamp=2022-08-04 10:15:32.737615868 +0800 CST m=+235.272046727, swap_depth_id=1200, swap_depth_bid0_price=36.58, swap_depth_ask0_price=36.581, swap_depth_bid1_price=36.579, swap_depth_ask1_price=36.583, swap_depth_timestamp=2022-08-04 10:15:32.81338322 +0800 CST m=+235.347814077, sr_open_real=0.0008207934336524, open_real_usdt=500.00400000000005472, currency_diff=-0.00244040317056319762574247851136, real_currency_diff=-0.00244040317056319762574247851136, end=2022-08-04 10:15:33.131699571 +0800 CST m=+235.666130403, open_limit=100"
	// msg := &baseMessage{
	// 	title: title,
	// 	body:  body,
	// }
	// message.Init("f4db793e-935d-4550-b78b-a22f51cd1223")
	// err := message.Send(ctx, msg)
	// if err != nil {
	// panic(err)
	// }
	// s := []byte{}
	// s := []byte(title)
	// fmt.Println(string(s))
	depthCh = make(chan *apis.DepthEvent, 10)
	logStr := fmt.Sprintf(`acts,%s,%s,%s`, `binance`, ``, ``)
	config.Init()
	api, err := apiFactory.GetApiAndLogin(logStr)
	if err != nil {
		common.PanicWithTime(errors.WithMessage(err, "acts swap api init fail"))
	}
	err = api.SubscribeDepths(OnDepths, apis.ProductTypeSwap, 5, `BTCUSDT`)
	if err != nil {
		panic(err)
	}
	// go selectC()
	var w sync.WaitGroup
	w.Add(1)
	w.Wait()
}

func OnDepths(depths *apis.DepthEvent) {
	fmt.Println(depths)
	// depthCh <- depths
	// fmt.Println(len(depthCh))
	// newDs := &ccexgo.OrderBook{
	// 	Symbol:  hsc.symbol,
	// 	Bids:    make([]ccexgo.OrderElem, 0),
	// 	Asks:    make([]ccexgo.OrderElem, 0),
	// 	Created: time.Now(),
	// }
	// for _, ask := range depths.Asks {
	// 	newDs.Asks = append(newDs.Asks, ccexgo.OrderElem{
	// 		Price:  ask.Price,
	// 		Amount: ask.Amount,
	// 	})
	// }
	// for _, bid := range depths.Bids {
	// 	newDs.Bids = append(newDs.Bids, ccexgo.OrderElem{
	// 		Price:  bid.Price,
	// 		Amount: bid.Amount,
	// 	})
	// }
	// hsc.orderbookCH <- &exchange.Depth{OrderBook: newDs, ID: hsc.depthID}
	// hsc.depthID++
}

//title="ETC_USDT open position" body="pos_id=20220804_891, start=2022-08-04 10:15:32.81360638 +0800 CST m=+235.348037212, sr_open=0.000520, open_trade_unit=50, open_usdt=500.004, spot_order_id=2713657802, spot_avg_price=36.550000000000004, spot_deal_amount=13.68, spot_open_time=1970-01-01 08:00:00 +0800 CST, spot_done_time=2022-08-04 10:15:32.828 +0800 CST, swap_order_id=4172430849, swap_avg_price=36.58, swap_deal_amount=50, swap_open_time=2022-08-04 10:15:32.891 +0800 CST, swap_done_time=2022-08-04 10:15:32.878 +0800 CST, spot_to_swap_amount=10.9396224, spot_keep=2.7349056, spot_to_swap_duration=162.571915ms, spot_depth_id=2012, spot_depth_bid0_price=36.54, spot_depth_ask0_price=36.55, spot_depth_bid1_price=36.53, spot_depth_ask1_price=36.56, spot_depth_timestamp=2022-08-04 10:15:32.737615868 +0800 CST m=+235.272046727, swap_depth_id=1200, swap_depth_bid0_price=36.58, swap_depth_ask0_price=36.581, swap_depth_bid1_price=36.579, swap_depth_ask1_price=36.583, swap_depth_timestamp=2022-08-04 10:15:32.81338322 +0800 CST m=+235.347814077, sr_open_real=0.0008207934336524, open_real_usdt=500.00400000000005472, currency_diff=-0.00244040317056319762574247851136, real_currency_diff=-0.00244040317056319762574247851136, end=2022-08-04 10:15:33.131699571 +0800 CST m=+235.666130403, open_limit=100"

func selectC() {
	var n int
	for {
		depths := <-depthCh
		fmt.Println("[SELECT]", depths)
		n++
		if (n % 30) == 0 {
			time.Sleep(10 * time.Second)
		}
	}
}

func OnTrades(trades []*apis.TradeEvent) {
	for _, t := range trades {
		fmt.Println(t)
	}
	// depthCh <- depths
	// fmt.Println(len(depthCh))
}
