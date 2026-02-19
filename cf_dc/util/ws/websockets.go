package ws

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	common "go.common"
	"go.common/log"

	"github.com/gorilla/websocket"
)

type WebSocketClient struct {
	url          string
	subscribeMsg []byte
	handler      func(raw []byte)

	conn        *websocket.Conn
	isConnected bool
	mu          sync.Mutex

	parentCtx context.Context    // 外部传入的父上下文
	ctx       context.Context    // 内部子上下文, 父上下文取消, 所有派生的子上下文都会取消
	cancel    context.CancelFunc // 用于控制子上下文
}

func NewWebSocketClient(ctx context.Context, url string, subscribeMsg []byte, handler func(raw []byte)) *WebSocketClient {
	childCtx, cancel := context.WithCancel(ctx)
	return &WebSocketClient{
		url:          url,
		subscribeMsg: subscribeMsg,
		handler:      handler,
		parentCtx:    ctx,
		ctx:          childCtx,
		cancel:       cancel,
	}
}

func (w *WebSocketClient) Connect() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.isConnected {
		return nil
	}

	c, _, err := websocket.DefaultDialer.Dial(w.url, nil)
	if err != nil {
		return err
	}

	w.conn = c
	if w.subscribeMsg != nil {
		if err := w.conn.WriteMessage(websocket.TextMessage, w.subscribeMsg); err != nil {
			c.Close()
			w.conn = nil
			// 确保重置连接状态
			w.isConnected = false
			return err
		}
	}
	w.isConnected = true
	log.Info("[WebSocketClient] 连接成功: %s %s", w.url, w.subscribeMsg)
	return nil
}

/*
func combineContexts(ctx1, ctx2 context.Context) (context.Context, context.CancelFunc) {
	combinedCtx, cancel := context.WithCancel(context.Background())

	go func() {
		select {
		case <-ctx1.Done():
			cancel()
		case <-ctx2.Done():
			cancel()
		}
	}()

	return combinedCtx, cancel
}
*/

func (w *WebSocketClient) resetConnection() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.cancel != nil {
		w.cancel() // 通知当前协程退出
	}

	if w.conn != nil {
		_ = w.conn.Close()
		w.conn = nil
	}
	// 重置连接状态
	w.isConnected = false

	// 从外部 ctx 派生一个新的子上下文
	childCtx, cancel := context.WithCancel(w.parentCtx)
	w.ctx = childCtx
	w.cancel = cancel
}

func (w *WebSocketClient) start(event *common.Event) {
	wg := sync.WaitGroup{}
	wg.Add(2)

	// 开启消息读取
	go func() {
		if err := w.readMessages(w.ctx); err != nil {
			event.Set() // 通知 Run
		}
		wg.Done()
	}()
	// 开启心跳管理
	go func() {
		if err := w.manageHeartbeat(w.ctx); err != nil {
			event.Set() // 通知 Run
		}
		wg.Done()
	}()

	go func() {
		wg.Wait()
	}()
}

func (w *WebSocketClient) readMessages(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			log.Info("[WebSocketClient] %s 接收到退出信号，停止读取消息", w.url)
			return nil

		default:
			// 在设置deadline前先检查连接是否存在
			w.mu.Lock()
			conn := w.conn
			w.mu.Unlock()

			if conn == nil {
				return fmt.Errorf("connection is nil")
			}

			// 设置读取超时时间
			conn.SetReadDeadline(time.Now().Add(30 * time.Second)) // 超时时间
			_, message, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					log.Warn("[WebSocketClient] %s %s 非预期关闭错误: %v", w.url, w.subscribeMsg, err)
				} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					log.Warn("[WebSocketClient] %s %s 读取超时", w.url, w.subscribeMsg)
				} else {
					log.Warn("[WebSocketClient] %s %s 读取消息失败: %v", w.url, w.subscribeMsg, err)
				}
				return err
			}
			w.handler(message)
		}
	}
}

func (w *WebSocketClient) manageHeartbeat(ctx context.Context) error {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			var conn *websocket.Conn
			w.mu.Lock()
			conn = w.conn
			w.mu.Unlock()

			if conn == nil {
				continue
			}

			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				log.Error("[WebSocketClient] %s %s 心跳失败: %v", w.url, w.subscribeMsg, err)
				return err
			}

		case <-ctx.Done():
			log.Info("[WebSocketClient] %s 心跳管理退出", w.url)
			return nil
		}
	}
}

func (w *WebSocketClient) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.cancel != nil {
		w.cancel()
	}

	if w.conn != nil {
		_ = w.conn.Close()
		w.conn = nil
		w.isConnected = false
	}
	log.Info("[WebSocketClient] %s %s 已停止运行", w.url, w.subscribeMsg)
}

func (w *WebSocketClient) Run(exit *common.Event) {
	retryInterval := time.Second
	defer w.Stop() // 确保退出时释放资源

	e := common.NewEvent() // 用于通知错误

	for {
		select {
		case <-w.parentCtx.Done():
			log.Info("[WebSocketClient] %s %s 接收到退出信号，停止运行", w.url, w.subscribeMsg)
			return

		case <-exit.Done():
			log.Info("[WebSocketClient] %s %s 接收到退出信号，停止运行", w.url, w.subscribeMsg)
			return

		case <-e.Done(): // 监听错误信号
			log.Warn("[WebSocketClient] %s %s 收到错误信号: 尝试重连", w.url, w.subscribeMsg)
			w.resetConnection() // 重置连接
			time.Sleep(retryInterval)
			e.Unset()

		default:
		}

		// 如果已经连接，避免重复连接
		w.mu.Lock()
		connected := w.isConnected
		w.mu.Unlock()

		if connected {
			time.Sleep(1 * time.Second) // 避免频繁循环
			continue
		}

		// 尝试连接
		if err := w.Connect(); err != nil {
			log.Error("[WebSocketClient] %s 连接失败，%s 后重试, err: %+v", w.url, retryInterval, err)
			time.Sleep(retryInterval)
			continue
		}

		// 连接成功后启动消息处理和心跳管理
		w.start(e)
	}
}
