package remoting

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"github.com/go-errors/errors"
)

type BaseRemotingAchieve struct {
	bootstrap               *netm.Bootstrap
	responseTable           map[int32]*ResponseFuture
	responseTableLock       sync.RWMutex
	rpcHook                 RPCHook
	defaultRequestProcessor RequestProcessor
	processorTable          map[int32]RequestProcessor // 注册的处理器
	processorTableLock      sync.RWMutex
	timeoutTimer            *time.Timer
	messageQueue            *messageQueue
	isRunning               bool
}

// RegisterProcessor register porcessor
func (ra *BaseRemotingAchieve) RegisterProcessor(requestCode int32, processor RequestProcessor) {
	if ra.processorTable == nil {
		ra.processorTable = make(map[int32]RequestProcessor)
	}

	// 注册业务处理器
	ra.processorTableLock.Lock()
	ra.processorTable[requestCode] = processor
	ra.processorTableLock.Unlock()
}

// RegisterDefaultProcessor register default porcessor
func (ra *BaseRemotingAchieve) RegisterDefaultProcessor(processor RequestProcessor) {
	ra.defaultRequestProcessor = processor
}

// RegisterRPCHook 注册rpc hook
func (ra *BaseRemotingAchieve) RegisterRPCHook(rpcHook RPCHook) {
	ra.rpcHook = rpcHook
}

func (ra *BaseRemotingAchieve) processReceived(buffer []byte, ctx netm.Context) {
	if ctx == nil {
		logger.Fatalf("processReceived context is nil")
		return
	}

	// 创建chan
	ch := ra.messageQueue.createQueueIfNotExist(ctx.Addr())

	// 拷贝buffer
	nbuf := make([]byte, len(buffer))
	copy(nbuf, buffer)

	// 数据放入队列
	ra.messageQueue.putMessage(ch, ctx, nbuf)
}

func (ra *BaseRemotingAchieve) processMessageFromQueue(ctx netm.Context, buffer []byte) {
	// 开启gorouting处理响应
	ra.startGoRoutine(func() {
		ra.processMessageReceived(ctx, buffer)
	})
}

func (ra *BaseRemotingAchieve) processMessageReceived(ctx netm.Context, buffer []byte) {
	// 解析报文
	buf := bytes.NewBuffer(buffer)
	remotingCommand, err := protocol.DecodeRemotingCommand(buf)
	if err != nil {
		logger.Fatalf("processMessageReceived deconde failed: %v", err)
		return
	}

	if remotingCommand == nil {
		return
	}

	// 报文分类处理
	switch remotingCommand.Type() {
	case protocol.REQUEST_COMMAND:
		ra.processRequestCommand(ctx, remotingCommand)
	case protocol.RESPONSE_COMMAND:
		ra.processResponseCommand(ctx, remotingCommand)
	default:
	}
}

func (ra *BaseRemotingAchieve) processRequestCommand(ctx netm.Context, remotingCommand *protocol.RemotingCommand) {
	// 获取业务处理器，没有注册使用默认处理器
	ra.processorTableLock.Lock()
	processor, ok := ra.processorTable[remotingCommand.Code]
	ra.processorTableLock.Unlock()
	if !ok {
		processor = ra.defaultRequestProcessor
	}

	// 没有处理器，错误处理。
	if processor == nil {
		errMsg := fmt.Sprintf("request type %d not supported", remotingCommand.Code)
		response := protocol.CreateResponseCommand(protocol.REQUEST_CODE_NOT_SUPPORTED, errMsg)
		response.Opaque = remotingCommand.Opaque
		ra.sendResponse(response, ctx)
		logger.Fatalf("processRequestCommand addr[%s] %s", ctx.Addr(), errMsg)
		return
	}

	// rpc hook before
	if ra.rpcHook != nil {
		ra.rpcHook.DoBeforeRequest(ctx, remotingCommand)
	}

	// 调用处理器
	response, err := processor.ProcessRequest(ctx, remotingCommand)

	// rpc hook after
	if ra.rpcHook != nil {
		ra.rpcHook.DoAfterResponse(ctx, remotingCommand, response)
	}

	// 错误处理
	if err != nil {
		response := protocol.CreateResponseCommand(protocol.SYSTEM_ERROR, err.Error())
		response.Opaque = remotingCommand.Opaque
		ra.sendResponse(response, ctx)
		logger.Fatalf("process request exception %v", err)
		return
	}

	// send oneway 不需要响应
	if remotingCommand.IsOnewayRPC() {
		return
	}

	if response == nil {
		// 收到请求，但是没有返回应答，可能是processRequest中进行了应答，忽略这种情况
		return
	}

	// 返回响应
	response.Opaque = remotingCommand.Opaque
	response.MarkResponseType()
	ra.sendResponse(response, ctx)
}

func (ra *BaseRemotingAchieve) processResponseCommand(ctx netm.Context, response *protocol.RemotingCommand) {
	// 获取响应
	ra.responseTableLock.RLock()
	responseFuture, ok := ra.responseTable[response.Opaque]
	ra.responseTableLock.RUnlock()
	if !ok {
		logger.Fatalf("receive response, but not matched any request, %s response Opaque: %d", ctx.Addr(), response.Opaque)
		return
	}

	// 从table中删除响应
	ra.responseTableLock.Lock()
	delete(ra.responseTable, response.Opaque)
	ra.responseTableLock.Unlock()

	// 取得响应体，执行回调函数。
	responseFuture.responseCommand = response
	if responseFuture.invokeCallback != nil {
		responseFuture.invokeCallback(responseFuture)
	}

	// 取得响应体，通知等待goroutine。
	if responseFuture.done != nil {
		responseFuture.done <- true
	}
}

func (ra *BaseRemotingAchieve) startGoRoutine(fn func()) {
	if ra.isRunning {
		go fn()
	}
}

func (ra *BaseRemotingAchieve) sendRequest(request *protocol.RemotingCommand, ctx netm.Context) error {
	return ra.send(request, ctx)
}

func (ra *BaseRemotingAchieve) sendResponse(response *protocol.RemotingCommand, ctx netm.Context) error {
	return ra.send(response, ctx)
}

// 发送报文
func (ra *BaseRemotingAchieve) send(remotingCommand *protocol.RemotingCommand, ctx netm.Context) error {
	//_, err = ra.bootstrap.Write(addr, header)
	// 发送报文
	_, err := ctx.Write(remotingCommand.Bytes())
	if err != nil {
		return errors.Wrap(err, 0)
	}

	return nil
}

func (ra *BaseRemotingAchieve) invokeSync(ctx netm.Context, request *protocol.RemotingCommand, timeoutMillis int64) (*protocol.RemotingCommand, error) {
	// 创建请求响应
	responseFuture := newResponseFuture(request.Opaque, timeoutMillis)
	responseFuture.done = make(chan bool)

	// 将创建的请求响应放到响应table
	ra.responseTableLock.Lock()
	ra.responseTable[request.Opaque] = responseFuture
	ra.responseTableLock.Unlock()

	// 发送请求
	err := ra.sendRequest(request, ctx)
	if err != nil {
		logger.Fatalf("invokeSync->sendRequest failed: %s %v", ctx.Addr(), err)
		return nil, err
	}
	responseFuture.sendRequestOK = true

	// 等待请求响应
	select {
	case <-responseFuture.done:
		return responseFuture.responseCommand, nil
	case <-time.After(time.Duration(timeoutMillis) * time.Millisecond):
		return nil, errors.Errorf("invoke sync timeout")
	}
}

func (ra *BaseRemotingAchieve) invokeAsync(ctx netm.Context, request *protocol.RemotingCommand, timeoutMillis int64, invokeCallback InvokeCallback) error {
	// 创建请求响应
	responseFuture := newResponseFuture(request.Opaque, timeoutMillis)
	responseFuture.invokeCallback = invokeCallback

	// 将创建的请求响应放到响应table
	ra.responseTableLock.Lock()
	ra.responseTable[request.Opaque] = responseFuture
	ra.responseTableLock.Unlock()

	// 发送请求
	err := ra.sendRequest(request, ctx)
	if err != nil {
		logger.Fatalf("invokeASync->sendRequest failed: %s %v", ctx.Addr(), err)
		return err
	}
	responseFuture.sendRequestOK = true

	return nil
}

func (ra *BaseRemotingAchieve) invokeOneway(ctx netm.Context, request *protocol.RemotingCommand, timeoutMillis int64) error {
	// 发送请求
	err := ra.sendRequest(request, ctx)
	if err != nil {
		logger.Fatalf("invokeOneway->sendRequest failed: %s %v", ctx.Addr(), err)
		return err
	}

	return nil
}

// 扫描发送请求响应报文是否超时
func (ra *BaseRemotingAchieve) scanResponseTable() {
	var (
		seqs []int32
	)

	// 检查超时响应，表更为对锁检查，过滤出超时响应后，写锁删除。
	// 通常情况下，超时连接没有那么多。Modify: jerrylou, <gunsluo@gmail.com> Since: 2017-09-01
	ra.responseTableLock.RLock()
	for seq, responseFuture := range ra.responseTable {
		// 超时判断
		if (responseFuture.beginTimestamp + responseFuture.timeoutMillis + 1000) <= time.Now().Unix()*1000 {
			seqs = append(seqs, seq)
			logger.Fatalf("remove time out request %v", responseFuture)
		}
	}
	ra.responseTableLock.RUnlock()

	// 没有超时连接
	if len(seqs) == 0 {
		return
	}

	ra.responseTableLock.Lock()
	for _, seq := range seqs {
		responseFuture, ok := ra.responseTable[seq]
		if !ok {
			continue
		}
		// 删除超时响应
		delete(ra.responseTable, seq)

		// 回调执行
		if responseFuture.invokeCallback != nil {
			ra.startGoRoutine(func() {
				responseFuture.invokeCallback(responseFuture)
			})
		}
	}
	ra.responseTableLock.Unlock()
}

// 定时扫描响应
func (ra *BaseRemotingAchieve) startScheduledTask() {
	ra.startGoRoutine(func() {
		ra.timeoutTimer = time.NewTimer(3 * time.Second)
		for {
			<-ra.timeoutTimer.C
			ra.scanResponseTable()
			ra.timeoutTimer.Reset(time.Second)
		}
	})
}

// RegisterContextListener 注册context listener
func (ra *BaseRemotingAchieve) RegisterContextListener(contextListener netm.ContextListener) {
	ra.bootstrap.RegisterContextListener(&innerContextListener{real: contextListener, ra: ra})
}

func (ra *BaseRemotingAchieve) OnContextConnect(ctx netm.Context) {
	if ra.messageQueue != nil {
		ra.messageQueue.createQueueIfNotExist(ctx.Addr())
	}
}

func (ra *BaseRemotingAchieve) OnContextClose(ctx netm.Context) {
	if ra.messageQueue != nil {
		ra.messageQueue.remove(ctx.Addr())
	}
}

func (ra *BaseRemotingAchieve) OnContextError(ctx netm.Context) {
	if ra.messageQueue != nil {
		ra.messageQueue.remove(ctx.Addr())
	}
}

func (ra *BaseRemotingAchieve) OnContextIdle(ctx netm.Context) {
	if ra.messageQueue != nil {
		ra.messageQueue.remove(ctx.Addr())
	}
}
