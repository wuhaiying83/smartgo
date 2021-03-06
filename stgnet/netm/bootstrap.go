package netm

import (
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/go-errors/errors"
)

// Bootstrap 启动器
type Bootstrap struct {
	listener          net.Listener
	contextTable      map[string]Context
	contextTableLock  sync.RWMutex
	contextListener   ContextListener
	opts              *Options
	optsMu            sync.RWMutex
	handlers          []Handler
	checkCtxIdleTimer *time.Timer
	keepalive         bool
	mu                sync.Mutex
	running           bool
	grRunning         bool
}

// NewBootstrap 创建启动器
func NewBootstrap() *Bootstrap {
	b := &Bootstrap{
		opts:      &Options{},
		grRunning: true,
	}
	b.contextTable = make(map[string]Context)
	return b
}

// Bind 监听地址、端口
func (bootstrap *Bootstrap) Bind(host string, port int) *Bootstrap {
	bootstrap.optsMu.Lock()
	bootstrap.opts.Host = host
	bootstrap.opts.Port = port
	bootstrap.optsMu.Unlock()
	return bootstrap
}

// Sync 启动服务
func (bootstrap *Bootstrap) Sync() {
	// check handlers if register
	if len(bootstrap.handlers) == 0 {
		bootstrap.Warnf("no handler register, data not process.")
	}

	opts := bootstrap.getOpts()
	addr := net.JoinHostPort(opts.Host, strconv.Itoa(opts.Port))

	listener, e := net.Listen("tcp", addr)
	if e != nil {
		bootstrap.Fatalf("Error listening on port: %s, %q", addr, e)
		return
	}
	bootstrap.Noticef("listening for connections on %s", net.JoinHostPort(opts.Host, strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)))
	bootstrap.Noticef("bootstrap is ready")

	bootstrap.mu.Lock()
	if opts.Port == 0 {
		// Write resolved port back to options.
		_, port, err := net.SplitHostPort(listener.Addr().String())
		if err != nil {
			bootstrap.Fatalf("Error parsing server address (%s): %s", listener.Addr().String(), err)
			bootstrap.mu.Unlock()
			return
		}
		portNum, err := strconv.Atoi(port)
		if err != nil {
			bootstrap.Fatalf("Error parsing server address (%s): %s", listener.Addr().String(), err)
			bootstrap.mu.Unlock()
			return
		}
		opts.Port = portNum
	}
	bootstrap.listener = listener
	bootstrap.running = true
	bootstrap.mu.Unlock()

	tmpDelay := ACCEPT_MIN_SLEEP
	for bootstrap.isRunning() {
		conn, err := listener.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				bootstrap.Debugf("Temporary Client Accept Error(%v), sleeping %dms",
					ne, tmpDelay/time.Millisecond)
				time.Sleep(tmpDelay)
				tmpDelay *= 2
				if tmpDelay > ACCEPT_MAX_SLEEP {
					tmpDelay = ACCEPT_MAX_SLEEP
				}
				continue
			} else if bootstrap.isRunning() {
				bootstrap.Errorf("Accept error: %v", err)
				continue
			} else {
				bootstrap.Errorf("Exiting: %v", err)
				bootstrap.LogFlush()
				break
			}
		}
		tmpDelay = ACCEPT_MIN_SLEEP

		// 配置连接
		err = bootstrap.setConnect(conn)
		if err != nil {
			bootstrap.Errorf("config connect error: %v", err)
			continue
		}

		// 以客户端ip,port管理连接
		remoteAddr := conn.RemoteAddr().String()
		ctx := newDefaultContext(remoteAddr, conn, bootstrap)
		bootstrap.contextTableLock.Lock()
		bootstrap.contextTable[remoteAddr] = ctx
		bootstrap.contextTableLock.Unlock()
		//bootstrap.Debugf("Client connection created %s", remoteAddr)

		bootstrap.startGoRoutine(func() {
			bootstrap.handleConn(ctx)
		})

		// 通知连接创建
		bootstrap.startGoRoutine(func() {
			bootstrap.onContextConnect(ctx)
		})
	}

	//bootstrap.Noticef("Bootstrap Exiting..")
}

// Connect 连接指定地址、端口(服务器地址管理连接)
func (bootstrap *Bootstrap) Connect(host string, port int) error {
	addr := net.JoinHostPort(host, strconv.Itoa(port))

	return bootstrap.ConnectJoinAddr(addr)
}

// Connect 使用指定地址、端口的连接字符串连接
func (bootstrap *Bootstrap) ConnectJoinAddr(addr string) error {
	_, err := bootstrap.ConnectJoinAddrAndReturn(addr)
	return err
}

// Connect 使用指定地址、端口的连接字符串进行连接并返回连接
func (bootstrap *Bootstrap) ConnectJoinAddrAndReturn(addr string) (Context, error) {
	// check handlers if register
	if len(bootstrap.handlers) == 0 {
		bootstrap.Warnf("no handler register, data not process.")
	}

	// 保证创建时，多线程安全
	bootstrap.contextTableLock.Lock()
	ctx, ok := bootstrap.contextTable[addr]
	if ok {
		bootstrap.contextTableLock.Unlock()
		return ctx, nil
	}

	nctx, e := bootstrap.connect(addr, "")
	if e != nil {
		bootstrap.contextTableLock.Unlock()
		bootstrap.Fatalf("Error Connect on port: %s, %q", addr, e)
		return nil, errors.Wrap(e, 0)
	}

	bootstrap.contextTable[addr] = nctx
	bootstrap.contextTableLock.Unlock()
	bootstrap.Noticef("connect listening on port: %s", addr)
	bootstrap.Noticef("client connections on %s", nctx.LocalAddr().String())

	bootstrap.startGoRoutine(func() {
		bootstrap.handleConn(nctx)
	})

	// 通知连接创建
	bootstrap.startGoRoutine(func() {
		bootstrap.onContextConnect(nctx)
	})

	return nctx, nil
}

// 创建新连接
func (bootstrap *Bootstrap) connect(sraddr, sladdr string) (Context, error) {
	raddr, e := net.ResolveTCPAddr("tcp", sraddr)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}

	var laddr *net.TCPAddr
	if sladdr != "" {
		laddr, e = net.ResolveTCPAddr("tcp", sladdr)
		if e != nil {
			return nil, errors.Wrap(e, 0)
		}
	}

	conn, e := net.DialTCP("tcp", laddr, raddr)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}

	e = bootstrap.setConnect(conn)
	if e != nil {
		return nil, e
	}

	ctx := newDefaultContext(sraddr, conn, bootstrap)
	return ctx, nil
}

// HasConnect find connect by addr, return bool
func (bootstrap *Bootstrap) HasConnect(addr string) bool {
	bootstrap.contextTableLock.RLock()
	_, ok := bootstrap.contextTable[addr]
	bootstrap.contextTableLock.RUnlock()
	if !ok {
		return false
	}

	return true
}

// Disconnect 关闭指定连接
func (bootstrap *Bootstrap) Disconnect(addr string) {
	bootstrap.contextTableLock.RLock()
	ctx, ok := bootstrap.contextTable[addr]
	bootstrap.contextTableLock.RUnlock()
	if ok {
		ctx.Close()
	}
}

// 移除连接
func (bootstrap *Bootstrap) remove(ctx Context) {
	addr := ctx.Addr()
	bootstrap.contextTableLock.Lock()
	if _, ok := bootstrap.contextTable[addr]; ok {
		delete(bootstrap.contextTable, addr)
	}
	bootstrap.contextTableLock.Unlock()
}

// Shutdown 关闭bootstrap
func (bootstrap *Bootstrap) Shutdown() {
	bootstrap.mu.Lock()
	bootstrap.running = false
	bootstrap.mu.Unlock()

	// 关闭listener
	if bootstrap.listener != nil {
		bootstrap.listener.Close()
	}

	// 关闭所有连接
	bootstrap.contextTableLock.Lock()
	contexts := make(map[string]Context, len(bootstrap.contextTable))
	for addr, ctx := range bootstrap.contextTable {
		contexts[addr] = ctx
		delete(bootstrap.contextTable, addr)
	}
	bootstrap.contextTableLock.Unlock()

	for _, ctx := range contexts {
		ctx.Close()
	}

	// 关闭定时器
	if bootstrap.checkCtxIdleTimer != nil {
		bootstrap.checkCtxIdleTimer.Stop()
		bootstrap.checkCtxIdleTimer = nil
	}
}

// Write 发送消息
func (bootstrap *Bootstrap) Write(addr string, buffer []byte) (n int, e error) {
	bootstrap.contextTableLock.RLock()
	ctx, ok := bootstrap.contextTable[addr]
	bootstrap.contextTableLock.RUnlock()
	if !ok {
		bootstrap.Fatalf("not found connect: %s", addr)
		e = errors.Errorf("not found connect %s", addr)
		return
	}

	n, e = ctx.Write(buffer)
	if e != nil {
		e = errors.Wrap(e, 0)
	}

	return
}

// RegisterHandler 注册连接接收数据时回调执行函数
func (bootstrap *Bootstrap) RegisterHandler(fns ...Handler) *Bootstrap {
	bootstrap.handlers = append(bootstrap.handlers, fns...)
	return bootstrap
}

// RegisterContextListener 注册连接的监听接口
func (bootstrap *Bootstrap) RegisterContextListener(contextListener ContextListener) *Bootstrap {
	bootstrap.contextListener = contextListener
	return bootstrap
}

// 接收数据
func (bootstrap *Bootstrap) handleConn(ctx Context) {
	b := make([]byte, 1024)
	for {
		n, err := ctx.Read(b)
		if err != nil {
			bootstrap.Fatalf("failed handle connect: %s %s", ctx.Addr(), err)
			break
		}

		for _, fn := range bootstrap.handlers {
			fn(b[:n], ctx)
		}
	}

	bootstrap.Debugf("connect[%s] Exiting..", ctx.Addr())
}

func (bootstrap *Bootstrap) startGoRoutine(fn func()) {
	if bootstrap.grRunning {
		go fn()
	}
}

func (bootstrap *Bootstrap) isRunning() bool {
	bootstrap.mu.Lock()
	defer bootstrap.mu.Unlock()
	return bootstrap.running
}

func (bootstrap *Bootstrap) getOpts() *Options {
	bootstrap.optsMu.RLock()
	opts := bootstrap.opts
	bootstrap.optsMu.RUnlock()
	return opts
}

// 设置空闲时间，单位秒
func (bootstrap *Bootstrap) SetIdle(idle int) *Bootstrap {
	bootstrap.optsMu.Lock()
	bootstrap.opts.Idle = idle
	bootstrap.optsMu.Unlock()
	bootstrap.startScheduledCheckContextIdle()

	return bootstrap
}

// Size 当前连接数
func (bootstrap *Bootstrap) Size() int {
	bootstrap.contextTableLock.RLock()
	defer bootstrap.contextTableLock.RUnlock()
	return len(bootstrap.contextTable)
}

// NewRandomConnect 连接指定本地和远程地址、端口(laddr端口为0为随机端口)。特殊业务使用
func (bootstrap *Bootstrap) NewRandomConnect(sraddr, sladdr string) (Context, error) {
	// check handlers if register
	if len(bootstrap.handlers) == 0 {
		bootstrap.Warnf("no handler register, data not process.")
	}

	nctx, e := bootstrap.connect(sraddr, sladdr)
	if e != nil {
		bootstrap.Fatalf("Error Connect on port: %s, %q", sraddr, e)
		return nil, errors.Wrap(e, 0)
	}

	localAddr := nctx.LocalAddr().String()
	bootstrap.contextTableLock.Lock()
	bootstrap.contextTable[localAddr] = nctx
	bootstrap.contextTableLock.Unlock()
	//bootstrap.Noticef("Connect listening on port: %s", sraddr)
	//bootstrap.Noticef("client connections on %s", localAddr)

	bootstrap.startGoRoutine(func() {
		bootstrap.handleConn(nctx)
	})

	// 通知连接创建
	bootstrap.startGoRoutine(func() {
		bootstrap.onContextConnect(nctx)
	})

	return nctx, nil
}

// SetKeepAlive 配置连接keepalive，default is false
func (bootstrap *Bootstrap) SetKeepAlive(keepalive bool) *Bootstrap {
	bootstrap.keepalive = keepalive
	return bootstrap
}

// 配置连接
func (bootstrap *Bootstrap) setConnect(conn net.Conn) error {
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if err := tcpConn.SetKeepAlive(bootstrap.keepalive); err != nil {
			return errors.Wrap(err, 0)
		}
	}

	return nil
}

// Contexts 返回指定context
func (bootstrap *Bootstrap) Context(addr string) Context {
	bootstrap.contextTableLock.RLock()
	ctx, ok := bootstrap.contextTable[addr]
	bootstrap.contextTableLock.RUnlock()
	if !ok {
		return nil
	}

	return ctx
}

// Contexts 返回context
func (bootstrap *Bootstrap) Contexts() []Context {
	var contexts []Context

	bootstrap.contextTableLock.RLock()
	for _, ctx := range bootstrap.contextTable {
		contexts = append(contexts, ctx)
	}
	bootstrap.contextTableLock.RUnlock()

	return contexts
}

// 创建连接时进行通知
func (bootstrap *Bootstrap) onContextConnect(ctx Context) {
	if bootstrap.contextListener != nil {
		bootstrap.contextListener.OnContextConnect(ctx)
	}
}

// 关闭连接时进行通知
func (bootstrap *Bootstrap) onContextClose(ctx Context) {
	// 删除连接
	bootstrap.remove(ctx)
	if bootstrap.contextListener != nil {
		bootstrap.contextListener.OnContextClose(ctx)
	}
}

// 连接异常时进行通知
func (bootstrap *Bootstrap) onContextError(ctx Context) {
	// 删除连接
	bootstrap.remove(ctx)
	if bootstrap.contextListener != nil {
		bootstrap.contextListener.OnContextError(ctx)
	}
}

// 连接空闲时进行通知
func (bootstrap *Bootstrap) onContextIdle(ctx Context) {
	// 删除连接
	bootstrap.remove(ctx)
	if bootstrap.contextListener != nil {
		bootstrap.contextListener.OnContextIdle(ctx)
	}
}

// 检查空闲连接
func (bootstrap *Bootstrap) startScheduledCheckContextIdle() {
	bootstrap.optsMu.RLock()
	idle := bootstrap.opts.Idle
	bootstrap.optsMu.RUnlock()

	if idle <= 0 {
		return
	}

	// 已经启动
	if bootstrap.checkCtxIdleTimer != nil {
		return
	}

	// 开启定时器检查
	bootstrap.startGoRoutine(func() {
		interval := idle / 2
		if interval == 0 {
			interval = idle
		}
		bootstrap.checkCtxIdleTimer = time.NewTimer(time.Duration(interval) * time.Second)
		for {
			<-bootstrap.checkCtxIdleTimer.C
			bootstrap.scanIdleContextTable(idle)
			bootstrap.checkCtxIdleTimer.Reset(time.Duration(interval) * time.Second)
		}
	})
}

// 扫描空闲连接
func (bootstrap *Bootstrap) scanIdleContextTable(idle int) {
	var contexts []Context

	bootstrap.contextTableLock.RLock()
	for _, ctx := range bootstrap.contextTable {
		// 超时判断
		ctxIdle := ctx.Idle().Seconds()
		if int(ctxIdle) >= idle {
			contexts = append(contexts, ctx)
		}
	}
	bootstrap.contextTableLock.RUnlock()

	for _, ctx := range contexts {
		bootstrap.onContextIdle(ctx)
		bootstrap.Fatalf("remove time out context %s, idle time: %s", ctx.Addr(), ctx.Idle())
	}
}
