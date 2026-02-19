package message

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"cf_arbitrage/util/logger"

	"go.common/helper"

	"github.com/go-kit/log/level"
	"github.com/go-lark/lark"
	"github.com/pkg/errors"
)

const larkDomain = "https://open.larksuite.com/open-apis/bot/v2/hook/"
const maxRetries = 2
const baseDelay = 5 * time.Second
const maxConcurrentRetries = 500 // 最大并发重试数

var (
	gBot            *lark.Bot
	importantGBot   *lark.Bot
	quantifyImGBots []*lark.Bot // 量化重要群
	p0ImGBot        *lark.Bot   // p0群
	p3ImGBot        *lark.Bot   // p3群
	Host            string
	Account         string
	WarnAt          map[string]map[string]string

	// 用于控制并发重试的信号量
	retrySemaphore chan struct{}
)

func Init(account, botID, importantBotId string) {
	gBot = lark.NewNotificationBot(botID)
	gBot.SetDomain(larkDomain)
	importantGBot = lark.NewNotificationBot(importantBotId)
	importantGBot.SetDomain(larkDomain)
	Host, _ = os.Hostname()
	Account = account

	// 初始化信号量，控制最大并发重试数
	retrySemaphore = make(chan struct{}, maxConcurrentRetries)
}

func InitBot2(quantifyImGBotIds string, p0BotID string, p3BotID string) {
	quantifyImGBotIdsList := strings.Split(quantifyImGBotIds, ",")
	for i, quantifyImGBotId := range quantifyImGBotIdsList {
		quantifyImGBots = append(quantifyImGBots, lark.NewNotificationBot(quantifyImGBotId))
		quantifyImGBots[i].SetDomain(larkDomain)
	}
	if p0BotID != `` {
		p0ImGBot = lark.NewNotificationBot(p0BotID)
		p0ImGBot.SetDomain(larkDomain)
	}
	if p3BotID != `` {
		p3ImGBot = lark.NewNotificationBot(p3BotID)
		p3ImGBot.SetDomain(larkDomain)
	}
}

// 加载告警@规则
func ReloadWarnAtRule(w map[string]map[string]string) {
	WarnAt = w
	level.Info(logger.GetLogger()).Log("message", "reload warn at rule", "new", fmt.Sprintf("%+v", WarnAt))
}

func SendRaw(ctx context.Context, bot *lark.Bot, msg Message) error {
	if bot == nil {
		return errors.Errorf("not init")
	}
	message := lark.NewMsgBuffer(lark.MsgPost)
	postContent := lark.NewPostBuilder().Title(fmt.Sprintf("%s (%s)", msg.Title(), Host)).TextTag(msg.Body(), 1, true)
	atUserIds := msg.AtUsers()
	if len(atUserIds) > 0 {
		for _, u := range atUserIds {
			postContent.AtTag("", u)
		}
	}

	resp, err := bot.PostNotificationV2(message.Post(postContent.Render()).Build())
	if err != nil {
		level.Warn(logger.GetLogger()).Log("message", "lark send raw", "resp", fmt.Sprintf("%+v", resp), "err", err.Error())
		return err
	}
	if resp.Code != 0 {
		level.Warn(logger.GetLogger()).Log("message", "lark send raw", "resp", fmt.Sprintf("%+v", resp))
		return fmt.Errorf("%+v", resp)
	}
	return nil
}

// sendWithRetry 带指数退避重试的消息发送函数
// 最多重试3次，每次重试间隔为5秒的指数退避，异步重试避免阻塞
func sendWithRetry(ctx context.Context, bot *lark.Bot, msg Message, functionName string) error {
	err := SendRaw(ctx, bot, msg)
	if err == nil {
		return nil
	}

	// 尝试获取信号量，如果获取失败说明并发重试数已达上限
	select {
	case retrySemaphore <- struct{}{}:
		// 成功获取信号量，启动异步重试
		go func() {
			defer func() { <-retrySemaphore }() // 完成后释放信号量
			sendWithRetryAsync(ctx, bot, msg, functionName, 0)
		}()
	default:
		// 并发重试数已达上限，记录警告但不阻塞
		level.Warn(logger.GetLogger()).Log(
			"message", fmt.Sprintf("%s retry rejected: concurrent limit reached", functionName),
			"max_concurrent", maxConcurrentRetries,
			"title", msg.Title(),
			"body", msg.Body(),
		)
	}

	return err
}

// sendWithRetryAsync 异步重试函数，避免阻塞主线程
func sendWithRetryAsync(ctx context.Context, bot *lark.Bot, msg Message, functionName string, attempt int) {
	// 检查context是否已经取消
	select {
	case <-ctx.Done():
		level.Warn(logger.GetLogger()).Log(
			"message", fmt.Sprintf("%s retry cancelled", functionName),
			"reason", ctx.Err().Error(),
		)
		return
	default:
	}

	// 如果已经达到最大重试次数，记录错误日志并退出
	if attempt >= maxRetries {
		level.Error(logger.GetLogger()).Log(
			"message", fmt.Sprintf("%s failed after %d attempts", functionName, maxRetries+1),
			"title", msg.Title(),
			"body", msg.Body(),
		)
		return
	}

	// 计算指数退避延迟时间: 5^1, 5^2, 25s, + 30s随机
	power := 1
	for i := 0; i < attempt; i++ {
		power *= 5
	}
	delay := baseDelay * time.Duration(power)
	delay = delay + time.Duration(helper.RandIntRange(0, 30))*time.Second

	level.Info(logger.GetLogger()).Log(
		"message", fmt.Sprintf("%s retry attempt %d/%d", functionName, attempt+1, maxRetries),
		"retry_in", delay.String(),
	)

	// 使用timer代替sleep，支持context取消
	timer := time.NewTimer(delay)
	defer timer.Stop() // 确保timer资源被释放

	select {
	case <-timer.C:
		// 延迟时间到，继续执行
	case <-ctx.Done():
		// context被取消，退出
		level.Warn(logger.GetLogger()).Log(
			"message", fmt.Sprintf("%s retry cancelled during sleep", functionName),
			"reason", ctx.Err().Error(),
		)
		return
	}

	// 尝试发送
	err := SendRaw(ctx, bot, msg)
	if err == nil {
		return
	}

	// 如果还是失败，继续下一次重试
	sendWithRetryAsync(ctx, bot, msg, functionName, attempt+1)
}

func Send(ctx context.Context, msg Message) error {
	return sendWithRetry(ctx, gBot, msg, "Send")
}

// SendImportant 发送重要告警
func SendImportant(ctx context.Context, msg Message) error {
	err := sendWithRetry(ctx, importantGBot, msg, "SendImportant")

	if strings.Contains(msg.Title(), `【重要】`) && quantifyImGBots != nil {
		go SendQuantifyImportant(ctx, msg)
		if strings.Contains(msg.Title(), `收到ADL或强平订单`) {
			go SendP0Important(ctx, msg)
		}
	}

	return err
}

func SendQuantifyImportant(ctx context.Context, msg Message) error {
	if len(quantifyImGBots) == 0 {
		return errors.Errorf("quantifyImGBots not init")
	}
	var err error
	for _, bot := range quantifyImGBots {
		e := sendWithRetry(ctx, bot, msg, "SendQuantifyImportant")
		if e != nil {
			err = e
		}
	}
	return err
}

func SendP0Important(ctx context.Context, msg Message) error {
	if p0ImGBot == nil {
		return errors.Errorf("p0ImGBot not init")
	}
	return sendWithRetry(ctx, p0ImGBot, msg, "SendP0Important")
}


func SendP3Important(ctx context.Context, msg Message) error {
	if p3ImGBot == nil {
		return errors.Errorf("p3ImGBot not init")
	}
	return sendWithRetry(ctx, p3ImGBot, msg, "SendP3Important")
}