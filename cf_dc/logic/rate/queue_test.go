package rate

import (
	"cf_arbitrage/exchange"
	"testing"
	"time"

	ccexgo "github.com/NadiaSama/ccexgo/exchange"
)

func TestRing(t *testing.T) {
	rr := newRateRing(10)
	minDuration := time.Millisecond * 100
	maxDuration := time.Millisecond * 200

	t.Run("test empty not pass", func(t *testing.T) {
		if p := rr.pass(minDuration, maxDuration); p {
			t.Errorf("unexpect pass")
		}
	})

	t.Run("test only 1 elem", func(t *testing.T) {
		now := time.Now()
		rr.push(now.Add(maxDuration*-1 - 10*time.Millisecond))

		if p := rr.pass(minDuration, maxDuration); p {
			t.Errorf("unexpect pass")
		}

		rr.push(now.Add(minDuration*-1 - 10*time.Millisecond))
		if p := rr.pass(minDuration, maxDuration); !p {
			t.Errorf("unexpect not pass")
		}

		rr.push(now.Add(minDuration + 10*time.Millisecond))
		if p := rr.pass(minDuration, maxDuration); p {
			t.Errorf("unexpect pass")
		}
	})

	t.Run("test a lot elem", func(t *testing.T) {
		//rr.reset()
		now := time.Now()
		for i := 100; i > 10; i-- {
			ts := now.Add(maxDuration*-1 - time.Duration(i)*time.Millisecond)
			rr.push(ts)
		}

		rr.push(now.Add(-1*maxDuration + 50*time.Millisecond))
		rr.push(now.Add(-1*maxDuration + 51*time.Millisecond))

		rr.push(now.Add(-1*minDuration + 10*time.Millisecond))

		for i := 0; i < 2; i++ {
			if p := rr.pass(minDuration, maxDuration); !p {
				t.Errorf("expect pass")
			}
		}

		if p := rr.pass(minDuration, maxDuration); p {
			t.Errorf("unexpect pass")
		}
	})

}

func TestQueue(t *testing.T) {
	q := NewQueue(QueueConfig{
		MSecs:          10000,
		OpenThreshold:  0.1,
		CloseThreshold: 0.111,
	}, 0, 0, 0, 0, 0.001, 0.001)

	spotB := exchange.Depth{
		OrderBook: &ccexgo.OrderBook{
			Bids: []ccexgo.OrderElem{
				{Price: 1.0},
				{Price: 1.0},
			},
			Asks: []ccexgo.OrderElem{
				{Price: 1.0},
				{Price: 1.0},
			},
			Created: time.Now().Add(time.Second * -8),
		},
		ID: 1,
	}

	swapB := exchange.Depth{
		OrderBook: &ccexgo.OrderBook{
			Bids: []ccexgo.OrderElem{
				{Price: 1.3},
				{Price: 1.2},
			},
			Asks: []ccexgo.OrderElem{
				{Price: 1.11},
				{Price: 1.22},
			},
			Created: time.Now().Add(time.Millisecond * -8500),
		},
		ID: 1,
	}
	q.PushSpot(&spotB)
	q.PushSwap1(&swapB)
	q.PushSwap2(&swapB)

	t.Run("test open calc", func(t *testing.T) {
		goods := q.bookIsGoodForOpen(0)
		if !goods.tt.Ok {
			t.Errorf("expect good for open")
		}
		q.srOpenThreshold = 0.21
		goods = q.bookIsGoodForOpen(0)
		if goods.tt.Ok {
			t.Errorf("expect not good")
		}
		q.srOpenThreshold = 0.1
	})

	t.Run("test close calc", func(t *testing.T) {
		goods := q.bookIsGoodForClose(0)
		// sr_00_tt 是负数 -0.146，小于阈值 0.111，所以应该是 Ok=true (因为 -0.146 < 0.111)
		if !goods.tt.Ok {
			t.Errorf("expect good for close, got sr_tt=%v, threshold=%v", goods.tt.Spread, q.srCloseThreshold)
		}

		q.srCloseThreshold = 0.22
		goods = q.bookIsGoodForClose(0)
		// 当阈值改为 0.22 时，-0.146 < 0.22，所以应该是 Ok=true
		if !goods.tt.Ok {
			t.Errorf("expect good, got sr_tt=%v, threshold=%v", goods.tt.Spread, q.srCloseThreshold)
		}
		q.srCloseThreshold = 0.111
	})

}
