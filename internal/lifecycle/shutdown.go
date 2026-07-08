// Package lifecycle provides graceful shutdown coordination for FrostDB.
// It listens for OS signals and orchestrates orderly teardown of components.
package lifecycle

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"frostdb/internal/logger"
)

// ShutdownFunc is a named cleanup function.
type ShutdownFunc struct {
	Name string
	Fn   func() error
}

// Manager coordinates graceful shutdown.
type Manager struct {
	ctx    context.Context
	cancel context.CancelFunc
	funcs  []ShutdownFunc
	mu     sync.Mutex
	done   chan struct{}
}

// NewManager creates a shutdown manager that cancels on SIGINT/SIGTERM.
func NewManager() *Manager {
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
	}

	go m.listenSignals()
	return m
}

// Context returns a context that is cancelled on shutdown signal.
func (m *Manager) Context() context.Context {
	return m.ctx
}

// Register adds a cleanup function to run during shutdown (LIFO order).
func (m *Manager) Register(name string, fn func() error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.funcs = append(m.funcs, ShutdownFunc{Name: name, Fn: fn})
}

// Wait blocks until shutdown is complete.
func (m *Manager) Wait() {
	<-m.done
}

func (m *Manager) listenSignals() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		logger.Info("shutdown signal received", "signal", sig.String())
	case <-m.ctx.Done():
	}

	m.cancel()
	m.runCleanup()
	close(m.done)
}

func (m *Manager) runCleanup() {
	m.mu.Lock()
	funcs := make([]ShutdownFunc, len(m.funcs))
	copy(funcs, m.funcs)
	m.mu.Unlock()

	// Run in reverse order (LIFO)
	for i := len(funcs) - 1; i >= 0; i-- {
		sf := funcs[i]
		logger.Info("shutting down component", "name", sf.Name)
		if err := sf.Fn(); err != nil {
			logger.Error("shutdown error", "name", sf.Name, "error", err)
		}
	}
	logger.Info("shutdown complete")
}
