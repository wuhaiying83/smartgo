package remoting

import (
	"bytes"
	"encoding/binary"

	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"github.com/go-errors/errors"
)

type LengthFieldFragmentationAssemblage struct {
	maxFrameLength      int           // 最大帧的长度
	lengthFieldOffset   int           // 长度属性的起始偏移量
	lengthFieldLength   int           // 长度属性的长度
	initialBytesToStrip int           // 业务数据需要跳过的长度
	cache               *bytes.Buffer // 碎片存储
}

func NewLengthFieldFragmentationAssemblage(maxFrameLength, lengthFieldOffset, lengthFieldLength, initialBytesToStrip int) *LengthFieldFragmentationAssemblage {
	return &LengthFieldFragmentationAssemblage{
		maxFrameLength:      maxFrameLength,
		lengthFieldOffset:   lengthFieldOffset,
		lengthFieldLength:   lengthFieldLength,
		initialBytesToStrip: initialBytesToStrip,
		cache:               &bytes.Buffer{},
	}
}

func (lfpfa *LengthFieldFragmentationAssemblage) Pack(buffer []byte, fn func([]byte)) (e error) {
	var (
		length = len(buffer)
	)

	if length > lfpfa.maxFrameLength {
		// 报文长度大于设置最大长度，丢弃报文（之后考虑其它方式）
		logger.Errorf("buffer length[%d] > maxFrameLength[%d], discard.", length, lfpfa.maxFrameLength)
		e = errors.Errorf("buffer length[%d] > maxFrameLength[%d], discard.", length, lfpfa.maxFrameLength)
		return
	}

	// 缓存报文
	_, e = lfpfa.cache.Write(buffer)
	if e != nil {
		e = errors.Wrap(e, 0)
		return
	}

	return lfpfa.pack(fn)
}

func (lfpfa *LengthFieldFragmentationAssemblage) pack(fn func([]byte)) (e error) {
	var (
		start        int
		end          int
		length       int
		packetLength int
	)

	start = lfpfa.lengthFieldOffset
	end = lfpfa.lengthFieldOffset + lfpfa.lengthFieldLength

	for {
		length = lfpfa.cache.Len()
		if length <= end {
			// 长度不够，等待下个报文。
			break
		}

		// 读取报文长度
		lengthFieldBytes := lfpfa.cache.Bytes()[start:end]
		packetLength, e = lfpfa.readLengthFieldLength(lengthFieldBytes)
		if e != nil {
			break
		}

		// 报文传输出错或报文到达顺序与发送顺序不一致，顺序问题之后考虑。
		if packetLength > lfpfa.maxFrameLength {
			// 丢弃报文
			lfpfa.cache.Reset()
			logger.Errorf("frame length[%d] > maxFrameLength[%d], discard.", packetLength, lfpfa.maxFrameLength)
			e = errors.Errorf("frame length[%d] > maxFrameLength[%d], discard.", packetLength, lfpfa.maxFrameLength)
			break
		}

		// 长度小于报文长度，等待下个报文
		if length-end < packetLength {
			break
		}

		// 报文长度足够，读取报文并调整buffer
		nbuf, e := lfpfa.adjustBuffer(packetLength + end)
		if e != nil {
			break
		}
		//bufs = append(bufs, nbuf)
		fn(nbuf)
	}

	return
}

func (lfpfa *LengthFieldFragmentationAssemblage) adjustBuffer(packetLength int) ([]byte, error) {
	// buffer中报文长度
	distance := lfpfa.cache.Len() - packetLength

	// 读取报文掉过的长度
	if lfpfa.initialBytesToStrip > 0 {
		lfpfa.cache.Next(lfpfa.initialBytesToStrip)
		packetLength -= lfpfa.initialBytesToStrip
	}

	// 读取报文
	buffer := lfpfa.cache.Next(packetLength)
	if distance == 0 {
		// buffer数据已经读取完
		lfpfa.cache.Reset()
	}

	nbuffer := make([]byte, len(buffer))
	copy(nbuffer, buffer)

	return nbuffer, nil
}

func (lfpfa *LengthFieldFragmentationAssemblage) readLengthFieldLength(lengthFieldBytes []byte) (int, error) {
	var (
		packetLength int
	)
	switch lfpfa.lengthFieldLength {
	case 1:
		packetLength = int(lengthFieldBytes[0])
	case 2:
		lengthField := binary.BigEndian.Uint16(lengthFieldBytes)
		packetLength = int(lengthField)
	case 4:
		lengthField := binary.BigEndian.Uint32(lengthFieldBytes)
		packetLength = int(lengthField)
	case 8:
		lengthField := binary.BigEndian.Uint64(lengthFieldBytes)
		packetLength = int(lengthField)
	default:
		logger.Warnf("not support lengthFieldLength[%d].", lfpfa.lengthFieldLength)
		return 0, errors.Errorf("not support lengthFieldLength[%d].", lfpfa.lengthFieldLength)
	}

	return packetLength, nil
}
