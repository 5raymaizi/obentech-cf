package output

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cf_arbitrage/util/logger"

	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"go.common/redis_service"
)

type (
	Data struct {
		Exchange string
		Chan     string
		Data     map[string]string
	}
	Manager struct {
		lockHeld    atomic.Bool // 是否持有锁的标志
		lockKey     string
		lockValue   string
		streams     map[string]*CSVStream
		redisStream *RedisStream
		odir        string
		bakDir      string
		src         chan *Data
		done        chan struct{}
		// 用于等待Loop协程完成
		loopWg sync.WaitGroup
	}
)

func NewManager(src chan *Data, odir, exchangeName, symbol, redisUniqueName string) (*Manager, error) {
	if err := os.MkdirAll(odir, os.ModePerm); err != nil {
		return nil, errors.WithMessagef(err, "create dir '%s' fail", odir)
	}
	os.Chmod(odir, 0777)
	bakDir := path.Join(odir, "rotate")
	if err := os.MkdirAll(bakDir, os.ModePerm); err != nil {
		return nil, errors.WithMessagef(err, "create bkdir '%s' fail", bakDir)
	}
	os.Chmod(bakDir, 0777)

	hostname, _ := os.Hostname()

	return &Manager{
		lockKey:   fmt.Sprintf("cf_dc:%s:%s", exchangeName, symbol),
		lockValue: fmt.Sprintf("host:%s-pid:%d", hostname, os.Getpid()),
		streams:   make(map[string]*CSVStream),
		redisStream: &RedisStream{
			uniqueName: redisUniqueName,
			rds:        redis_service.GetGoRedis(0),
		},
		odir:   odir,
		bakDir: bakDir,
		src:    src,
		done:   make(chan struct{}),
	}, nil
}

func (m *Manager) Loop(ctx context.Context) {
	m.loopWg.Add(1)
	defer m.loopWg.Done()
	defer close(m.done)
	defer func() {
		for _, s := range m.streams {
			s.Close()
		}
	}()

	timer := tomorrow()
	lockTicker := time.NewTicker(10 * time.Second)

	m.acquireLock()
	defer func() {
		m.releaseLock()
		lockTicker.Stop()
	}()

	for {
		select {
		case n := <-m.src:
			m.redisStream.checkWrite(&n.Data)
			if err := m.writeNotify(n); err != nil {
			}

		case <-timer.C:
			m.doRotate()
			timer = tomorrow()

		case <-lockTicker.C:
			if m.lockHeld.Load() {
				// 刷新锁
				m.refreshLock()
			} else {
				// 尝试持有锁
				if m.acquireLock() {
					level.Warn(logger.GetLogger()).Log("message", "acquire lock success, previous owner released it")
				}
			}

		case <-ctx.Done():
			return
		}
	}
}

func (m *Manager) Done() chan struct{} {
	return m.done
}

// Wait 等待Loop协程完成，确保所有数据都被刷新到磁盘
func (m *Manager) Wait() {
	m.loopWg.Wait()
	level.Info(logger.GetLogger()).Log("message", "manager loop wait done")
}

func (m *Manager) writeNotify(n *Data) error {
	// 有一个交易对在写文件了就不写了
	if !m.lockHeld.Load() {
		return nil
	}
	name := strings.Join([]string{n.Exchange, n.Chan}, ".")
	stream, ok := m.streams[name]
	if !ok {
		p := path.Join(m.odir, fmt.Sprintf("%s.csv", name))
		var fields []string
		for k := range n.Data {
			fields = append(fields, k)
		}
		sort.Strings(fields)
		stream = NewStream(fields, p)
		if err := stream.Open(); err != nil {
			return err
		}
		m.streams[name] = stream
	}

	if err := stream.Write(n.Data); err != nil {
		return err
	}

	return nil
}

// Rotate 异步进行转储压缩
//
//	func (m *Manager) Rotate() error {
//		timeout := time.NewTimer(time.Second)
//		select {
//		case m.rch <- struct{}{}:
//			return nil
//		case <-timeout.C:
//			return errors.New("timeout")
//		}
//	}
func (m *Manager) doRotate() {
	streams := m.streams
	for sn, s := range streams {
		if err := s.Close(); err != nil {
			return
		}

		//重命名文件，避免异步压缩新时新的stream覆盖已有数据
		dir, _ := path.Split(s.Path)
		name := path.Join(dir, csvFileName(sn, s.Create))
		if err := os.Rename(s.Path, name); err != nil {
			return
		}
		s.Path = name
	}
	go func() {
		src := rand.NewSource(generateUniqueSeed()) // 设置随机数种子
		r := rand.New(src)
		minSleep, maxSleep := 0, 7200
		sleepTime := r.Intn(maxSleep-minSleep+1) + minSleep
		time.Sleep(time.Duration(sleepTime) * time.Second) // 随机休眠0-7200s,避免多任务cpu占用高
		for name, s := range streams {
			if err := m.compress(name, s); err != nil {
			} else {
				if err := os.Remove(s.Path); err != nil {
				}
			}
		}
	}()

	m.streams = make(map[string]*CSVStream)
}

func (m *Manager) compress(name string, s *CSVStream) error {
	cfile := csvFileName(name, s.Create)
	r := path.Join(m.bakDir, fmt.Sprintf("%s.gz", cfile))
	dst, err := os.Create(r)
	if err != nil {
		return errors.WithMessagef(err, "create gz file")
	}
	os.Chmod(r, 0766) // -rwxrw-rw-
	defer dst.Close()
	src, err := os.Open(s.Path)
	if err != nil {
		return errors.WithMessagef(err, "open source fail")
	}
	defer src.Close()

	if err := Compress(src, dst); err != nil {
		return errors.WithMessagef(err, "compress fail")
	}
	return nil
}

func (m *Manager) acquireLock() bool {
	success, err := m.redisStream.rds.SetNX(m.lockKey, m.lockValue, 30*time.Second).Result()
	if err != nil {
		level.Warn(logger.GetLogger()).Log("message", "acquire lock err", "err", err)
		return false
	}
	if success {
		m.lockHeld.Store(true)
		level.Info(logger.GetLogger()).Log("message", "acquire lock success", "value", m.lockValue)
	}
	return success
}

func (m *Manager) refreshLock() {
	if !m.lockHeld.Load() {
		return
	}
	if err := m.redisStream.rds.Expire(m.lockKey, 30*time.Second).Err(); err != nil {
		level.Error(logger.GetLogger()).Log("message", "refresh lock err", "err", err)
	}

}

func (m *Manager) releaseLock() {
	if m.lockHeld.Load() {
		if err := m.redisStream.rds.Del(m.lockKey).Err(); err != nil {
			level.Error(logger.GetLogger()).Log("message", "release lock err", "err", err)
		}
		m.lockHeld.Store(false)
	}
}

func csvFileName(name string, t ...time.Time) string {
	if len(t) != 0 {
		return fmt.Sprintf("%s-%s.csv", name, t[0].Format("20060102"))
	}
	return fmt.Sprintf("%s.csv.gz", name)
}

func generateUniqueSeed() int64 {
	now := time.Now().UnixNano()
	pid := int64(os.Getpid())

	// 将时间戳和PID组合，增加唯一性
	seed := now ^ pid
	return seed
}

func tomorrow() *time.Timer {
	next := time.Now().Add(time.Hour * 24)
	return time.NewTimer(time.Until(time.Date(next.Year(), next.Month(), next.Day(), 0, 0, 0, 0, next.Location())))
}
