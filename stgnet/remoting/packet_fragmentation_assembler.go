package remoting

type PacketFragmentationAssembler interface {
	Pack(addr string, buffer []byte, fn func([]byte)) error
}
