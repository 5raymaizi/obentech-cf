package message

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"cf_arbitrage/util/logger"

	gocommon "go.common"

	"github.com/go-kit/log/level"
	"github.com/shopspring/decimal"
)

type (
	Message interface {
		Title() string
		Body() string
		AtUsers() []string
	}

	baseMessage struct {
		title     string
		body      string
		atUserIds []string
	}

	//PosOperate message
	PosOperate struct {
		baseMessage
	}

	OperateFail struct {
		baseMessage
	}

	ManualOperate struct {
		Operator string    // 操作人
		OPEvent  string    // 操作事件
		OPMsg    string    // 操作信息
		Result   string    // 操作结果
		Time     time.Time // 操作时间
	}
)

func (bm *baseMessage) Title() string {
	return bm.title
}

func (bm *baseMessage) Body() string {
	return bm.body
}

func (bm *baseMessage) AtUsers() []string {
	return bm.atUserIds
}

func (mo *ManualOperate) String() string {
	return fmt.Sprintf("事件:%s\n信息:%s\n操作人:%s\n操作时间:%s\n操作结果:%s", mo.OPEvent, mo.OPMsg, mo.Operator, mo.Time.Format("2006-01-02 15:04:05.000"), mo.Result)
}

func NewTimeOut(symbol string, minute int, info string) *baseMessage {
	return &baseMessage{
		title: fmt.Sprintf("%s 操作花费时间过久", Account),
		body:  fmt.Sprintf("symbol=%s minute=%d, info=%s, 联系开发定位", symbol, minute, info),
	}
}

func NewLogicStart(symbol string) *baseMessage {
	return &baseMessage{
		title: fmt.Sprintf("策略启动 symbol=%s", symbol),
		body:  "",
	}
}

func NewLogicStartFail(symbol string, err error) *baseMessage {
	return &baseMessage{
		title: fmt.Sprintf("%s 策略启动失败 symbol=%s", Account, symbol),
		body:  fmt.Sprintf("error=%s", err.Error()),
	}
}

func NewLogicExit(symbol string) *baseMessage {
	return &baseMessage{
		title: fmt.Sprintf("策略退出 symbol=%s", symbol),
		body:  "",
	}
}

func NewLogicExitError(symbol string, err error) *baseMessage {
	return &baseMessage{
		title:     fmt.Sprintf("%s 策略非正常退出 symbol=%s", Account, symbol),
		body:      fmt.Sprintf("error=%s", err.Error()),
		atUserIds: getWarnAtUserWithTime(),
	}
}

func NewPosOperate(symbol string, title string, data [][2]string) *PosOperate {
	fields := []string{}
	for _, d := range data {
		fields = append(fields, fmt.Sprintf("%s=%s", d[0], d[1]))
	}
	return &PosOperate{
		baseMessage: baseMessage{
			title: fmt.Sprintf("%s %s", symbol, title),
			body:  strings.Join(fields, ", "),
		},
	}
}

func NewOperateFail(title string, msg string) *OperateFail {
	return &OperateFail{
		baseMessage: baseMessage{
			title: fmt.Sprintf("%s %s", Account, title),
			body:  msg,
		},
	}
}

func NewBalanceMismatch(currency string, cachePos1Long, cachePos1Short, cachePos2Long, cachePos2Short, pos1Long, pos1Short, pos2Long, pos2Short decimal.Decimal) *baseMessage {
	return &baseMessage{
		title: fmt.Sprintf("%s balance 匹配不一致", currency),
		body: fmt.Sprintf("cache_swap1_long=%s cache_swap1_short=%s cache_swap2_long=%s cache_swap2_short=%s \nfetch_swap1_long=%s fetch_swap2_short=%s fetch_swap2_long=%s fetch_swap2_short=%s",
			cachePos1Long, cachePos1Short, cachePos2Long, cachePos2Short, pos1Long, pos1Short, pos2Long, pos2Short),
	}
}

func NewCheckBalanceFail(currency string, err error) *baseMessage {
	return &baseMessage{
		title: fmt.Sprintf("%s 检测balance失败", currency),
		body:  err.Error(),
	}
}

// 手动接口调用告警
func NewManualOperate(title string, mo *ManualOperate) *baseMessage {
	return &baseMessage{
		title: fmt.Sprintf("%s %s", Account, title),
		body:  mo.String(),
	}
}

func NewExposureOperate(symbol, currency, side string, amount, avgPrice, fee decimal.Decimal, feeCurrency string) *baseMessage {
	return &baseMessage{
		title: fmt.Sprintf("%s %s 处理敞口成功", Account, symbol),
		body:  fmt.Sprintf("currency=%s side=%s, amount=%s avg_price=%s fee=%s fee_currency=%s", currency, side, amount, avgPrice, fee, feeCurrency),
	}
}

// 通用信息发送
func NewCommonMsg(title, body string) *baseMessage {
	return &baseMessage{
		title: fmt.Sprintf("%s %s", Account, title),
		body:  body,
	}
}

// 通用信息发送-加【重要】告警前缀
func NewCommonMsgWithImport(title, body string) *baseMessage {
	return &baseMessage{
		title:     fmt.Sprintf("【重要】%s %s", Account, title),
		body:      body,
		atUserIds: getWarnAtUserWithTime(),
	}
}

// 通用信息发送-带@功能
func NewCommonMsgWithAt(title, body string) *baseMessage {
	return &baseMessage{
		title:     fmt.Sprintf("%s %s", Account, title),
		body:      body,
		atUserIds: getWarnAtUserWithTime(),
	}
}

func getWarnAtUserWithTime() []string {
	return getUserIDs()
}

// 解析时间区间
func parseTimeRange(timeRangeStr string) (time.Time, time.Time, error) {
	loc, _ := time.LoadLocation("Asia/Shanghai")
	times := strings.Split(timeRangeStr, "_")
	startHour, err1 := strconv.Atoi(times[0])
	endHour, err2 := strconv.Atoi(times[1])

	// 检查时间是否有效
	if err1 != nil || err2 != nil || startHour < 0 || endHour < 0 || startHour > 23 || endHour > 23 {
		return time.Time{}, time.Time{}, fmt.Errorf("无效的时间区间: %s", timeRangeStr)
	}

	now := time.Now().In(loc)
	start := time.Date(now.Year(), now.Month(), now.Day(), startHour, 0, 0, 0, loc)
	end := time.Date(now.Year(), now.Month(), now.Day(), endHour, 0, 0, 0, loc)

	if startHour == endHour {
		// 处理从某个时间点到第二天同一时间点的情况
		end = start.Add(24 * time.Hour)
	} else if endHour < startHour {
		end = end.Add(24 * time.Hour)
	}

	return start, end, nil
}

// 获取当前时间符合的用户ID列表
func getUserIDs() []string {
	defer func() {
		if e := recover(); e != nil {
			err := gocommon.NewErrorf(e, "")
			level.Error(logger.GetLogger()).Log("message", "get users id failed", "err", err)
		}
	}()
	loc, _ := time.LoadLocation("Asia/Shanghai")
	now := time.Now().In(loc)
	userIDs := make(map[string]bool)

	for timeRange, users := range WarnAt {
		start, end, err := parseTimeRange(timeRange)
		if err != nil {
			level.Warn(logger.GetLogger()).Log("message", "parse time range failed", "err", err)
			continue
		}

		// 使用包含起始时间和结束时间的比较方式
		if (now.After(start) || now.Equal(start)) && (now.Before(end) || now.Equal(end)) {
			for _, userID := range users {
				userIDs[userID] = true
			}
		}
	}

	var result []string
	for userID := range userIDs {
		result = append(result, userID)
	}

	return result
}
