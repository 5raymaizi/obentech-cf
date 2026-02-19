package logger

/*import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
)

type (
	rotateFile struct {
		path  string
		mu    sync.Mutex
		file  *os.File
		done  chan struct{}
		close chan struct{}
	}

	Config struct {
		Path  string `mapstructure:"path" json:"path"`
		Level string `mapstructure:"level" json:"level"`
	}
)

var (
	logger log.Logger
	rf     *rotateFile
)

func newRotateFile(path string) (*rotateFile, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, errors.WithMessage(err, "open file fail")
	}

	ret := &rotateFile{
		path:  path,
		file:  file,
		done:  make(chan struct{}),
		close: make(chan struct{}),
	}

	go ret.loop()
	return ret, nil
}

func (rf *rotateFile) loop() {
	timer := nextTimer()
	defer close(rf.done)
	for {
		select {
		case <-rf.close:
			level.Info(logger).Log("message", "logger quit")
			return
		case <-timer:
			rf.rotate()
			timer = nextTimer()
		}
	}
}

func (rf *rotateFile) rotate() error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if err := rf.file.Close(); err != nil {
		return errors.WithMessage(err, "close file fail")
	}

	dst := fmt.Sprintf("%s.%s", rf.path, time.Now().Add(-1*time.Hour*24).Format("20060102"))

	if err := os.Rename(rf.path, dst); err != nil {
		return errors.WithMessage(err, "rename file fail")
	}

	file, err := os.OpenFile(rf.path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return errors.WithMessage(err, "open file fail")
	}

	rf.file = file
	return nil
}

func nextTimer() <-chan time.Time {
	rotate := time.Now().Add(time.Hour * 24)
	ts := time.Date(rotate.Year(), rotate.Month(), rotate.Day(), 0, 0, 0, 0, rotate.Location())
	return time.After(time.Until(ts))
}

func (rf *rotateFile) Write(raw []byte) (int, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.file.Write(raw)
}

func (rf *rotateFile) Close() error {
	close(rf.close)
	select {
	case <-time.After(time.Second):
		return errors.Errorf("rotate file quit timeout")
	case <-rf.done:
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.file.Close()
	return nil
}

func NewLogger(cfg Config, isDebug bool) error {
	var path string
	if cfg.Path == "" {
		path = "cf_arbitrage.log"
	}
	file, err := newRotateFile(path)
	if err != nil {
		return errors.WithMessage(err, "create rotate file fail")
	}

	rf = file
	if isDebug {
		logger = newLogger(os.Stdout)
	} else {
		logger = newLogger(file)
	}

	var op level.Option
	lv := strings.ToLower(cfg.Level)
	switch lv {
	case "", "info":
		op = level.AllowInfo()

	case "debug":
		op = level.AllowDebug()

	case "warn":
		op = level.AllowWarn()

	case "error":
		op = level.AllowError()
	}
	level.Info(logger).Log("message", "NewLogger", "lv", lv)
	logger = level.NewFilter(logger, op)
	return nil
}

func Close() error {
	return rf.Close()
}

func GetLogger() log.Logger {
	if logger == nil {
		logger = newLogger(os.Stdout)
	}
	return logger
}

func newLogger(out io.Writer) log.Logger {
	ret := log.With(log.NewLogfmtLogger(out), "timestamp", log.DefaultTimestamp)
	return ret
}
*/
