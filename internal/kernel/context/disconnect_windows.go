//go:build windows

package context

// WatchDisconnect returns ctx.Done on Windows where non-blocking MSG_PEEK via
// syscall.Recvfrom is not supported.
func (ctx *Context) WatchDisconnect() (<-chan struct{}, func()) {
	return ctx.Done, func() {}
}
