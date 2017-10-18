package remoting

type PacketFragmentationAssembler interface {
	Pack(buffer []byte, fn func([]byte)) error
}
