package remoting

import (
	"net"
	"sync"
	"sync/atomic"

	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

// DefalutRemotingClient default remoting client
type DefalutRemotingClient struct {
	bootstrap           *netm.Bootstrap
	namesrvAddrList     []string
	namesrvAddrListLock sync.RWMutex
	namesrvAddrChoosed  string
	namesrvIndex        uint32
	BaseRemotingAchieve
}

// NewDefalutRemotingClient return new default remoting client
func NewDefalutRemotingClient() *DefalutRemotingClient {
	remotingClient := &DefalutRemotingClient{}
	remotingClient.responseTable = make(map[int32]*ResponseFuture)
	remotingClient.framePacketActuator = NewLengthFieldFramePacket(FRAME_MAX_LENGTH, 0, 4, 0)
	remotingClient.bootstrap = netm.NewBootstrap()
	return remotingClient
}

// Start start client
func (rc *DefalutRemotingClient) Start() {
	rc.bootstrap.RegisterHandler(func(buffer []byte, addr string, conn net.Conn) {
		rc.processReceived(buffer, addr, conn)
	})

	rc.isRunning = true
	// 定时扫描响应
	rc.startScheduledTask()
}

// Shutdown shutdown client
func (rc *DefalutRemotingClient) Shutdown() {
	rc.timeoutTimer.Stop()
	rc.bootstrap.Shutdown()
	rc.isRunning = false
}

// GetNameServerAddressList return nameserver addr list
func (rc *DefalutRemotingClient) GetNameServerAddressList() []string {
	rc.namesrvAddrListLock.RLock()
	defer rc.namesrvAddrListLock.RUnlock()
	return rc.namesrvAddrList
}

// UpdateNameServerAddressList update nameserver addrs list
func (rc *DefalutRemotingClient) UpdateNameServerAddressList(addrs []string) {
	var (
		repeat bool
	)

	rc.namesrvAddrListLock.Lock()
	for _, addr := range addrs {
		// 去除重复地址
		for _, oaddr := range rc.namesrvAddrList {
			if addr == oaddr {
				repeat = true
				break
			}
		}

		if repeat == false {
			rc.namesrvAddrList = append(rc.namesrvAddrList, addr)
		}
	}
	rc.namesrvAddrListLock.Unlock()
}

// InvokeSync 同步调用并返回响应, addr为空字符串，则在namesrvAddrList中选择地址
func (rc *DefalutRemotingClient) InvokeSync(addr string, request *protocol.RemotingCommand, timeoutMillis int64) (*protocol.RemotingCommand, error) {
	// 创建连接，如果addr为空字符串，则在name server中选择一个地址。
	conn, err := rc.createConnectByAddr(&addr)
	if err != nil {
		return nil, err
	}

	// rpc hook before
	if rc.rpcHook != nil {
		rc.rpcHook.DoBeforeRequest(addr, conn, request)
	}

	response, err := rc.invokeSync(addr, conn, request, timeoutMillis)

	// rpc hook after
	if rc.rpcHook != nil {
		rc.rpcHook.DoAfterResponse(addr, conn, request, response)
	}

	return response, err
}

// InvokeAsync 异步调用
func (rc *DefalutRemotingClient) InvokeAsync(addr string, request *protocol.RemotingCommand, timeoutMillis int64, invokeCallback InvokeCallback) error {
	// 创建连接，如果addr为空字符串，则在name server中选择一个地址。
	conn, err := rc.createConnectByAddr(&addr)
	if err != nil {
		return err
	}

	// rpc hook before
	if rc.rpcHook != nil {
		rc.rpcHook.DoBeforeRequest(addr, conn, request)
	}

	return rc.invokeAsync(addr, conn, request, timeoutMillis, invokeCallback)
}

// InvokeSync 单向发送消息
func (rc *DefalutRemotingClient) InvokeOneway(addr string, request *protocol.RemotingCommand, timeoutMillis int64) error {
	// 创建连接，如果addr为空字符串，则在name server中选择一个地址。
	conn, err := rc.createConnectByAddr(&addr)
	if err != nil {
		return err
	}

	// rpc hook before
	if rc.rpcHook != nil {
		rc.rpcHook.DoBeforeRequest(addr, conn, request)
	}

	return rc.invokeOneway(addr, conn, request, timeoutMillis)
}

func (rc *DefalutRemotingClient) createConnectByAddr(addrPtr *string) (net.Conn, error) {
	if *addrPtr == "" {
		*addrPtr = rc.chooseNameseverAddr()
	}

	// 创建连接，如果连接存在，则不会创建。
	return rc.bootstrap.ConnectJoinAddrAndReturn(*addrPtr)
}

func (rc *DefalutRemotingClient) chooseNameseverAddr() string {
	if rc.namesrvAddrChoosed != "" {
		//return rc.namesrvAddrChoosed
		// 判断连接是否可用，不可用选取其它name server addr
		if rc.bootstrap.HasConnect(rc.namesrvAddrChoosed) {
			return rc.namesrvAddrChoosed
		}
	}

	var (
		caddr string
		nlen  uint32
		i     uint32
	)
	rc.namesrvAddrListLock.RLock()
	nlen = uint32(len(rc.namesrvAddrList))
	for ; i < nlen; i++ {
		atomic.AddUint32(&rc.namesrvIndex, 1)
		idx := rc.namesrvIndex % nlen

		newAddr := rc.namesrvAddrList[idx]
		rc.namesrvAddrChoosed = newAddr
		//caddr = rc.namesrvAddrChoosed
		//break
		if rc.bootstrap.HasConnect(newAddr) {
			caddr = rc.namesrvAddrChoosed
			break
		}
	}
	rc.namesrvAddrListLock.RUnlock()

	return caddr
}
