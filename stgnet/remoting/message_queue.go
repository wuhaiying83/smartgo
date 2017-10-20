package remoting

import (
	"sync"

	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
)

const (
	DEFAULT_QUEUE_SIZE       = 10000
	DEFAULT_POOL_INIT_SIZE   = 1024
	DEFAULT_POOL_BUFFER_SIZE = 1024
)

type messageHandler func(netm.Context, []byte)

type message struct {
	cache *[]byte
	size  int
	ctx   netm.Context
}

type messageQueue struct {
	size     int                     // 接收的缓存队列大小
	chans    map[string]chan message // 接收的缓存队列,按接收ip:port区分
	rwMu     sync.RWMutex            // 读写锁
	handler  messageHandler          // 处理完成的执行函数
	actuator *fragmentationActuator  // 内置粘包处理器,减少锁的使用
	pool     *sync.Pool              // 临时对象池，用于产生cache
}

func newMessageQueue(size int, handler messageHandler) *messageQueue {
	return &messageQueue{
		size:    size,
		chans:   make(map[string]chan message),
		handler: handler,
	}
}

func (queue *messageQueue) setFragmentationActuator(actuator *fragmentationActuator) {
	queue.actuator = actuator
}

func (queue *messageQueue) usePool(initSize, bufferSize int) {
	queue.pool = &sync.Pool{
		New: func() interface{} {
			mem := make([]byte, bufferSize)
			return &mem
		},
	}

	for i := 0; i < initSize; i++ {
		queue.pool.Put(queue.pool.New())
	}
}

func (queue *messageQueue) createQueueIfNotExist(key string) chan message {
	var assembler PacketFragmentationAssembler

	ch, ok := queue.createQueueChanIfNotExist(key)
	if ok {
		// 创建队列时，同时创建粘包
		if queue.actuator != nil {
			assembler = queue.actuator.createAssemblerIfNotExist(key)
		}
		// 启动队列，开始接收数据
		queue.startReceiveMsgOnQueue(key, ch, assembler)
	}

	return ch
}

func (queue *messageQueue) createQueueChanIfNotExist(key string) (chan message, bool) {
	queue.rwMu.RLock()
	ch, ok := queue.chans[key]
	if ok {
		queue.rwMu.RUnlock()
		return ch, false
	}
	queue.rwMu.RUnlock()

	queue.rwMu.Lock()
	ch = make(chan message, queue.size)
	queue.chans[key] = ch
	queue.rwMu.Unlock()

	return ch, true
}

func (queue *messageQueue) getQueueChan(key string) chan message {
	queue.rwMu.RLock()
	defer queue.rwMu.RUnlock()
	ch, ok := queue.chans[key]
	if ok {
		return ch
	}

	return nil
}

func (queue *messageQueue) startReceiveMsgOnQueue(key string, ch chan message, assembler PacketFragmentationAssembler) {
	queue.startGoRoutine(func() {
		if assembler != nil {
			// 粘包处理
			for msg := range ch {
				fragmentation := (*msg.cache)[:msg.size]
				err := assembler.Pack(fragmentation, func(buffer []byte) {
					if queue.handler != nil {
						queue.handler(msg.ctx, buffer)
					}
				})
				if err != nil {
					logger.Fatalf("startReceiveMsgOnQueue Pack buffer failed: %v", err)
				}

				//回收内存
				queue.pool.Put(msg.cache)
			}
		} else {
			// 不使用粘包
			for msg := range ch {
				if queue.handler != nil {
					fragmentation := (*msg.cache)[:msg.size]
					queue.handler(msg.ctx, fragmentation)
				}
			}
		}

		logger.Infof("startReceiveMsgOnQueue queue goroutine exit: %s", key)
	})
}

func (queue *messageQueue) remove(key string) {
	queue.rwMu.Lock()
	if ch, ok := queue.chans[key]; ok {
		delete(queue.chans, key)
		close(ch)
	}
	queue.rwMu.Unlock()

	if queue.actuator != nil {
		queue.actuator.remove(key)
	}
}

func (queue *messageQueue) close() {
	queue.rwMu.Lock()
	for k, ch := range queue.chans {
		delete(queue.chans, k)
		close(ch)
	}
	queue.rwMu.Unlock()

	if queue.actuator != nil {
		queue.actuator.clean()
	}
}

func (queue *messageQueue) putMessage(ch chan message, ctx netm.Context, buffer []byte) {
	if queue.pool != nil {
		mem := queue.pool.Get().(*[]byte)
		copy(*mem, buffer)
		ch <- message{ctx: ctx, cache: mem, size: len(buffer)}
	} else {
		ch <- message{ctx: ctx, cache: &buffer, size: len(buffer)}
	}
}

func (queue *messageQueue) startGoRoutine(fn func()) {
	go fn()
}
