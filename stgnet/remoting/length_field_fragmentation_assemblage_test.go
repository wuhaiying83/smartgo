package remoting

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"
)

func preparePackFullBuffer() []byte {
	var (
		buf          = bytes.NewBuffer([]byte{})
		length int32 = 10
	)
	binary.Write(buf, binary.BigEndian, length)
	buf.Write([]byte("abcdefghij"))

	return buf.Bytes()
}

func TestPackFull(t *testing.T) {
	var (
		size                               int
		lengthFieldFragmentationAssemblage = NewLengthFieldFragmentationAssemblage(8388608, 0, 4, 0)
	)

	buffer := preparePackFullBuffer()

	e := lengthFieldFragmentationAssemblage.Pack(buffer, func(msg []byte) {
		size++
		if !reflect.DeepEqual(msg, buffer) {
			t.Errorf("Test failed: return buf%v incorrect, expect%v", msg, buffer)
		}
	})
	if e != nil {
		t.Errorf("Test failed: %s", e)
	}

	if size != 1 {
		t.Errorf("Test failed: return buf size[%d] incorrect, expect[%d]", size, 1)
	}
}

func TestPackOffsetFull(t *testing.T) {
	var (
		size                               int
		lengthFieldFragmentationAssemblage = NewLengthFieldFragmentationAssemblage(8388608, 0, 4, 4)
	)

	buffer := preparePackFullBuffer()

	e := lengthFieldFragmentationAssemblage.Pack(buffer, func(msg []byte) {
		size++
		if !reflect.DeepEqual(msg, buffer[4:]) {
			t.Errorf("Test failed: return buf%v incorrect, expect%v", msg, buffer[4:])
		}
	})
	if e != nil {
		t.Errorf("Test failed: %s", e)
	}

	if size != 1 {
		t.Errorf("Test failed: return buf size[%d] incorrect, expect[%d]", size, 1)
	}
}

func preparePackPartOfOneBuffer() []byte {
	var (
		buf          = bytes.NewBuffer([]byte{})
		length int32 = 10
	)
	binary.Write(buf, binary.BigEndian, length)

	return buf.Bytes()
}

func preparePackPartOfTwoBuffer() []byte {
	var (
		buf = bytes.NewBuffer([]byte{})
	)
	buf.Write([]byte("abcdefghij"))

	return buf.Bytes()
}

func TestPackPartOf(t *testing.T) {
	var (
		size                               int
		lengthFieldFragmentationAssemblage = NewLengthFieldFragmentationAssemblage(8388608, 0, 4, 0)
	)

	buffer := preparePackPartOfOneBuffer()
	e := lengthFieldFragmentationAssemblage.Pack(buffer, func(msg []byte) {
		size++
	})
	if e != nil {
		t.Errorf("Test failed: %s", e)
	}

	if size != 0 {
		t.Errorf("Test failed: return buf size[%d] incorrect, expect[%d]", size, 0)
	}
	bufferHeader := buffer

	buffer = preparePackPartOfTwoBuffer()
	e = lengthFieldFragmentationAssemblage.Pack(buffer, func(msg []byte) {
		size++
		allBytes := append(bufferHeader, buffer...)
		if !reflect.DeepEqual(msg, allBytes) {
			t.Errorf("Test failed: return buf%v incorrect, expect%v", msg, allBytes)
		}
	})
	if e != nil {
		t.Errorf("Test failed: %s", e)
	}

	if size != 1 {
		t.Errorf("Test failed: return buf size[%d] incorrect, expect[%d]", size, 1)
	}
}

func TestPackOffsetPartOf(t *testing.T) {
	var (
		size                               int
		lengthFieldFragmentationAssemblage = NewLengthFieldFragmentationAssemblage(8388608, 0, 4, 4)
	)

	buffer := preparePackPartOfOneBuffer()
	e := lengthFieldFragmentationAssemblage.Pack(buffer, func(msg []byte) {
		size++
	})
	if e != nil {
		t.Errorf("Test failed: %s", e)
	}

	if size != 0 {
		t.Errorf("Test failed: return buf size[%d] incorrect, expect[%d]", size, 0)
	}
	bufferHeader := buffer[4:]

	buffer = preparePackPartOfTwoBuffer()
	e = lengthFieldFragmentationAssemblage.Pack(buffer, func(msg []byte) {
		size++
		allBytes := append(bufferHeader, buffer...)
		if !reflect.DeepEqual(msg, allBytes) {
			t.Errorf("Test failed: return buf%v incorrect, expect%v", msg, allBytes)
		}
	})
	if e != nil {
		t.Errorf("Test failed: %s", e)
	}

	if size != 1 {
		t.Errorf("Test failed: return buf size[%d] incorrect, expect[%d]", size, 1)
	}
}

func preparePackPartOfOneBuffer2() []byte {
	var (
		buf          = bytes.NewBuffer([]byte{})
		length int32 = 10
	)
	binary.Write(buf, binary.BigEndian, length)
	buf.Write([]byte("abcd"))

	return buf.Bytes()
}

func preparePackPartOfTwoBuffer2() []byte {
	var (
		buf          = bytes.NewBuffer([]byte{})
		length int32 = 10
	)
	buf.Write([]byte("efghij"))
	binary.Write(buf, binary.BigEndian, length)
	buf.Write([]byte("abc"))

	return buf.Bytes()
}

func TestPackPartOf2(t *testing.T) {
	var (
		size                               int
		lengthFieldFragmentationAssemblage = NewLengthFieldFragmentationAssemblage(8388608, 0, 4, 0)
	)

	buffer := preparePackPartOfOneBuffer2()
	e := lengthFieldFragmentationAssemblage.Pack(buffer, func(msg []byte) {
		size++
	})
	if e != nil {
		t.Errorf("Test failed: %s", e)
	}

	if size != 0 {
		t.Errorf("Test failed: return buf size[%d] incorrect, expect[%d]", size, 0)
	}
	bufferHeader := buffer

	buffer = preparePackPartOfTwoBuffer2()
	e = lengthFieldFragmentationAssemblage.Pack(buffer, func(msg []byte) {
		size++
		allBytes := append(bufferHeader, buffer...)
		allBytes = allBytes[:len(msg)]
		if !reflect.DeepEqual(msg, allBytes) {
			t.Errorf("Test failed: return buf%v incorrect, expect%v", msg, allBytes)
		}
	})
	if e != nil {
		t.Errorf("Test failed: %s", e)
	}

	if size != 1 {
		t.Errorf("Test failed: return buf size[%d] incorrect, expect[%d]", size, 1)
	}
}

func TestPackOffsetPartOf2(t *testing.T) {
	var (
		size                               int
		lengthFieldFragmentationAssemblage = NewLengthFieldFragmentationAssemblage(8388608, 0, 4, 4)
	)

	buffer := preparePackPartOfOneBuffer2()
	e := lengthFieldFragmentationAssemblage.Pack(buffer, func(msg []byte) {
		size++
	})
	if e != nil {
		t.Errorf("Test failed: %s", e)
	}

	if size != 0 {
		t.Errorf("Test failed: return buf size[%d] incorrect, expect[%d]", size, 0)
	}
	bufferHeader := buffer[4:]

	buffer = preparePackPartOfTwoBuffer2()
	e = lengthFieldFragmentationAssemblage.Pack(buffer, func(msg []byte) {
		size++
		allBytes := append(bufferHeader, buffer...)
		allBytes = allBytes[:len(msg)]
		if !reflect.DeepEqual(msg, allBytes) {
			t.Errorf("Test failed: return buf%v incorrect, expect%v", msg, allBytes)
		}
	})
	if e != nil {
		t.Errorf("Test failed: %s", e)
	}

	if size != 1 {
		t.Errorf("Test failed: return buf size[%d] incorrect, expect[%d]", size, 1)
	}
}

func preparePackPartOfOneDiscardBuffer() []byte {
	var (
		buf = bytes.NewBuffer([]byte{})
	)
	buf.Write([]byte("abcdefghij"))

	return buf.Bytes()
}

func preparePackPartOfTwoDiscardBuffer() []byte {
	var (
		buf          = bytes.NewBuffer([]byte{})
		length int32 = 10
	)
	binary.Write(buf, binary.BigEndian, length)

	return buf.Bytes()
}

func TestPackDiscardOffsetPartOf(t *testing.T) {
	var (
		size                               int
		lengthFieldFragmentationAssemblage = NewLengthFieldFragmentationAssemblage(8388608, 0, 4, 0)
	)

	buffer := preparePackPartOfOneDiscardBuffer()
	e := lengthFieldFragmentationAssemblage.Pack(buffer, func(msg []byte) {
		size++
	})
	if e != nil {
		t.Logf("Pack failed: %s", e)
	}

	if size != 0 {
		t.Errorf("Test failed: return buf size[%d] incorrect, expect[%d]", size, 0)
	}

	buffer = preparePackPartOfTwoDiscardBuffer()
	e = lengthFieldFragmentationAssemblage.Pack(buffer, func(msg []byte) {
		size++
	})
	if e != nil {
		t.Errorf("Test failed: %s", e)
	}

	if size != 0 {
		t.Errorf("Test failed: return buf size[%d] incorrect, expect[%d]", size, 0)
	}
}

func TestPackDiscardPartOf(t *testing.T) {
	var (
		size                               int
		lengthFieldFragmentationAssemblage = NewLengthFieldFragmentationAssemblage(8388608, 0, 4, 4)
	)

	buffer := preparePackPartOfOneDiscardBuffer()
	e := lengthFieldFragmentationAssemblage.Pack(buffer, func(msg []byte) {
		size++
	})
	if e != nil {
		t.Logf("UnPack failed: %s", e)
	}

	if size != 0 {
		t.Errorf("Test failed: return buf size[%d] incorrect, expect[%d]", size, 0)
	}

	buffer = preparePackPartOfTwoDiscardBuffer()
	e = lengthFieldFragmentationAssemblage.Pack(buffer, func(msg []byte) {
		size++
	})
	if e != nil {
		t.Errorf("Test failed: %s", e)
	}

	if size != 0 {
		t.Errorf("Test failed: return buf size[%d] incorrect, expect[%d]", size, 0)
	}
}
