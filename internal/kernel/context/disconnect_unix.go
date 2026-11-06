//go:build !windows

package context

import (
	"syscall"
	"time"
)

// WatchDisconnect monitors ctx.Conn for peer closure while a command is blocked
// without consuming any bytes from the socket buffer.
func (ctx *Context) WatchDisconnect() (<-chan struct{}, func()) {
	if ctx.Conn == nil {
		return ctx.Done, func() {}
	}
	sc, ok := ctx.Conn.(syscall.Conn)
	if !ok {
		return ctx.Done, func() {}
	}
	rawConn, err := sc.SyscallConn()
	if err != nil {
		return ctx.Done, func() {}
	}

	discCh := make(chan struct{})
	stopCh := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		var buf [1]byte
		for {
			var disconnected bool
			ctrlErr := rawConn.Control(func(fd uintptr) {
				n, _, recvErr := syscall.Recvfrom(int(fd), buf[:], syscall.MSG_PEEK|syscall.MSG_DONTWAIT)
				if n == 0 && recvErr == nil {
					disconnected = true
				} else if recvErr != nil && recvErr != syscall.EAGAIN && recvErr != syscall.EWOULDBLOCK && recvErr != syscall.EINTR {
					disconnected = true
				}
			})
			if ctrlErr != nil || disconnected {
				close(discCh)
				return
			}
			select {
			case <-stopCh:
				return
			case <-ctx.Done:
				close(discCh)
				return
			case <-ticker.C:
			}
		}
	}()
	return discCh, func() {
		close(stopCh)
	}
}
