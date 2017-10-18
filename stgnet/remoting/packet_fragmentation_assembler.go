package remoting

import "bytes"

type PacketFragmentationAssembler interface {
	Pack(addr string, buffer []byte, fn func(*bytes.Buffer)) error
}
