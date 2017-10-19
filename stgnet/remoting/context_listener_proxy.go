package remoting

import (
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
)

type innerContextListener struct {
	real netm.ContextListener
	ra   *BaseRemotingAchieve
}

func (proxy *innerContextListener) OnContextConnect(ctx netm.Context) {
	proxy.ra.OnContextConnect(ctx)
	proxy.real.OnContextConnect(ctx)
}

func (proxy *innerContextListener) OnContextClose(ctx netm.Context) {
	proxy.ra.OnContextClose(ctx)
	proxy.real.OnContextClose(ctx)
}

func (proxy *innerContextListener) OnContextError(ctx netm.Context) {
	proxy.ra.OnContextError(ctx)
	proxy.real.OnContextError(ctx)
}

func (proxy *innerContextListener) OnContextIdle(ctx netm.Context) {
	proxy.ra.OnContextIdle(ctx)
	proxy.real.OnContextIdle(ctx)
}
