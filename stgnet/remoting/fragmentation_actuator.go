package remoting

import (
	"sync"
)

const (
	FRAME_MAX_LENGTH = 8388608
)

type fragmentationActuator struct {
	maxFrameLength      int // 最大帧的长度
	lengthFieldOffset   int // 长度属性的起始偏移量
	lengthFieldLength   int // 长度属性的长度
	initialBytesToStrip int // 业务数据需要跳过的长度
	rwmu                sync.RWMutex
	assemblers          map[string]PacketFragmentationAssembler
}

func newFragmentationActuator(maxFrameLength, lengthFieldOffset, lengthFieldLength, initialBytesToStrip int) *fragmentationActuator {
	return &fragmentationActuator{
		maxFrameLength:      maxFrameLength,
		lengthFieldOffset:   lengthFieldOffset,
		lengthFieldLength:   lengthFieldLength,
		initialBytesToStrip: initialBytesToStrip,
		assemblers:          make(map[string]PacketFragmentationAssembler),
	}
}

func (actutor *fragmentationActuator) createAssemblerIfNotExist(key string) PacketFragmentationAssembler {
	actutor.rwmu.RLock()
	one, ok := actutor.assemblers[key]
	if ok {
		actutor.rwmu.RUnlock()
		return one
	}
	actutor.rwmu.RUnlock()

	actutor.rwmu.Lock()
	one, ok = actutor.assemblers[key]
	if ok {
		actutor.rwmu.Unlock()
		return one
	}
	one = actutor.newAssembler()
	actutor.assemblers[key] = one
	actutor.rwmu.Unlock()

	return one
}

func (actutor *fragmentationActuator) newAssembler() PacketFragmentationAssembler {
	return NewLengthFieldFragmentationAssemblage(actutor.maxFrameLength,
		actutor.lengthFieldOffset, actutor.lengthFieldLength, actutor.initialBytesToStrip)
}

func (actutor *fragmentationActuator) getAssembler(key string) PacketFragmentationAssembler {
	actutor.rwmu.RLock()
	defer actutor.rwmu.RUnlock()
	one, ok := actutor.assemblers[key]
	if ok {
		return one
	}

	return nil
}
