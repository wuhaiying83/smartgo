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

func (actuator *fragmentationActuator) createAssemblerIfNotExist(key string) PacketFragmentationAssembler {
	actuator.rwmu.Lock()
	one, ok := actuator.assemblers[key]
	if ok {
		actuator.rwmu.Unlock()
		return one
	}
	one = actuator.newAssembler()
	actuator.assemblers[key] = one
	actuator.rwmu.Unlock()

	return one
}

func (actuator *fragmentationActuator) newAssembler() PacketFragmentationAssembler {
	return NewLengthFieldFragmentationAssemblage(actuator.maxFrameLength,
		actuator.lengthFieldOffset, actuator.lengthFieldLength, actuator.initialBytesToStrip)
}

func (actuator *fragmentationActuator) getAssembler(key string) PacketFragmentationAssembler {
	actuator.rwmu.RLock()
	defer actuator.rwmu.RUnlock()
	one, ok := actuator.assemblers[key]
	if ok {
		return one
	}

	return nil
}

func (actuator *fragmentationActuator) remove(key string) PacketFragmentationAssembler {
	var assembler PacketFragmentationAssembler

	actuator.rwmu.Lock()
	one, ok := actuator.assemblers[key]
	if ok {
		delete(actuator.assemblers, key)
		assembler = one
	}
	actuator.rwmu.Unlock()

	return assembler
}

func (actuator *fragmentationActuator) clean() {
	actuator.rwmu.Lock()
	for k, _ := range actuator.assemblers {
		delete(actuator.assemblers, k)
	}
	actuator.rwmu.Unlock()
}
