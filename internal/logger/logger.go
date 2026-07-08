// Package logger provides structured, leveled logging for FrostDB.
// Built on log/slog for zero-allocation hot paths and structured fields.
package logger

import (
	"io"
	"log/slog"
	"os"
	"sync/atomic"
)

// Level aliases for convenience.
const (
	LevelDebug = slog.LevelDebug
	LevelInfo  = slog.LevelInfo
	LevelWarn  = slog.LevelWarn
	LevelError = slog.LevelError
)

var defaultLogger atomic.Pointer[slog.Logger]

func init() {
	// Default: text handler, info level, stderr
	Set(New(Options{}))
}

// Options configures the logger.
type Options struct {
	Level  slog.Level // minimum level (default: Info)
	JSON   bool       // use JSON format (default: text)
	Output io.Writer  // output destination (default: stderr)
}

// New creates a configured slog.Logger.
func New(opts Options) *slog.Logger {
	if opts.Output == nil {
		opts.Output = os.Stderr
	}

	handlerOpts := &slog.HandlerOptions{
		Level: opts.Level,
	}

	var handler slog.Handler
	if opts.JSON {
		handler = slog.NewJSONHandler(opts.Output, handlerOpts)
	} else {
		handler = slog.NewTextHandler(opts.Output, handlerOpts)
	}

	return slog.New(handler)
}

// Set replaces the global logger.
func Set(l *slog.Logger) {
	defaultLogger.Store(l)
	slog.SetDefault(l)
}

// L returns the current global logger.
func L() *slog.Logger {
	return defaultLogger.Load()
}

// Convenience functions that use the global logger.

func Debug(msg string, args ...any) { L().Debug(msg, args...) }
func Info(msg string, args ...any)  { L().Info(msg, args...) }
func Warn(msg string, args ...any)  { L().Warn(msg, args...) }
func Error(msg string, args ...any) { L().Error(msg, args...) }
