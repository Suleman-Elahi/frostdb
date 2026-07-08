package server

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"frostdb/internal/blocking"
	"frostdb/internal/command/generic"
	"frostdb/internal/command/registry"
	"frostdb/internal/command/tx"
	"frostdb/internal/engine/api"
	"frostdb/internal/engine/keyevent"
	kctx "frostdb/internal/kernel/context"
	"frostdb/internal/logger"
	"frostdb/internal/resp/encoder"
	"frostdb/internal/resp/parser"
	"frostdb/internal/resp/types"
	"frostdb/internal/stats"
)

type Server struct {
	addr     string
	engine   api.Engine
	multiDB  api.MultiDB // non-nil if engine supports multi-DB
	listener net.Listener
	wg       sync.WaitGroup
	closed   atomic.Bool
	mu       sync.Mutex
	conns    map[net.Conn]struct{}
	nextID   atomic.Int64 // counter for client IDs
}

func New(addr string, engine api.Engine) *Server {
	blocking.Reset()
	stats.Init() // Initialize stats tracking
	s := &Server{
		addr:   addr,
		engine: engine,
		conns:  make(map[net.Conn]struct{}),
	}
	if mdb, ok := engine.(api.MultiDB); ok {
		s.multiDB = mdb
	}
	engine.OnTouch(tx.GlobalWatchManager.Touch)
	return s
}

// Start begins accepting connections. It blocks until the listener is closed.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.listener = ln
	logger.Info("FrostDB listening", "addr", s.addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			if s.closed.Load() {
				return nil // graceful shutdown
			}
			logger.Warn("accept error", "error", err)
			continue
		}
		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// Stop gracefully shuts down the server: stops accepting, waits for in-flight connections.
func (s *Server) Stop() error {
	s.closed.Store(true)
	blocking.TriggerShutdown()
	if s.listener != nil {
		s.listener.Close()
	}
	s.mu.Lock()
	for c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return nil
}

// writeBufferSize is the per-connection write buffer.
// 64 KB amortises syscalls for bulk responses and pipelined commands.
const writeBufferSize = 64 * 1024

func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()

	// Track connection stats
	st := stats.Get()
	st.TotalConnectionsReceived.Add(1)
	st.ConnectedClients.Add(1)
	defer st.ConnectedClients.Add(-1)

	s.mu.Lock()
	s.conns[conn] = struct{}{}
	s.mu.Unlock()
	defer s.recoverPanic(conn)

	// Disable Nagle's algorithm: send responses immediately without waiting
	// to coalesce small packets. Critical for low-latency Redis workloads.
	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}

	cctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientID := int(s.nextID.Add(1))
	ctx := &kctx.Context{
		Engine:     s.engine,
		DB:         0,
		DBSelector: s.selectDB,
		Conn:       conn,
		Done:       cctx.Done(),
		ClientID:   clientID,
	}

	generic.RegisterClient(ctx)

	defer func() {
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()

		// Clean up any active PubSub subscriptions
		if ctx != nil {
			for _, cleanup := range ctx.Subscriptions {
				cleanup()
			}
			for _, cleanup := range ctx.PatternSubs {
				cleanup()
			}
		}

		generic.UnregisterClient(ctx)
		_ = conn.Close()
	}()

	p := parser.New(conn)
	w := bufio.NewWriterSize(conn, writeBufferSize)

	cmdCh := make(chan []types.Value)
	errCh := make(chan error, 1)

	// Read loop
	go func() {
		for {
			val, err := p.Parse()
			if err != nil {
				cancel()
				errCh <- err
				return
			}
			if val.Type == types.TypeArray {
				cmdCh <- val.Array
			} else {
				// Send a nil slice to represent a format error
				cmdCh <- nil
			}
		}
	}()

	// Execution loop
	for {
		select {
		case err := <-errCh:
			if err != io.EOF && !isConnectionClosed(err) {
				logger.Warn("parse error",
					"remote", conn.RemoteAddr().String(),
					"error", err,
				)
			}
			return

		case cmdArgs := <-cmdCh:
			var result types.Value
			var cmdName string
			startTime := time.Now()

			if len(cmdArgs) > 0 {
				cmdName = strings.ToUpper(string(cmdArgs[0].Str))

				ctx.LastCmd = strings.ToLower(cmdName)
				generic.UpdateClientLastCmd(ctx, ctx.LastCmd)

				// Recover from panics in command handlers to keep the connection alive.
				func() {
					defer func() {
						if r := recover(); r != nil {
							logger.Error("panic in command handler",
								"command", cmdName,
								"panic", r,
							)
							result = types.NewError("ERR Internal server error")
						}
					}()
					result = registry.Dispatch(ctx, cmdArgs)
				}()

				if ctx.Engine != nil && result.Type != types.TypeError && len(cmdArgs) > 1 {
					// Only publish if the command mutated state.
					// Most commands return an integer > 0 if they mutated state.
					mutated := true
					if result.Type == types.TypeInteger && result.Num == 0 {
						// 0 means no mutation ONLY for specific commands.
						if cmdName == "SADD" || cmdName == "SREM" || cmdName == "ZADD" || cmdName == "ZREM" || cmdName == "EXPIRE" {
							mutated = false
						}
					} else if result.IsNull || result.Type == types.TypeNull || result.Type == types.TypeError {
						mutated = false
					} else if result.Type == types.TypeSimpleString && string(result.Str) == "QUEUED" {
						mutated = false
					}

					if mutated {
						key := string(cmdArgs[1].Str)
						db := ctx.DB
						logger.Debug("notifyKeyEvent", "cmd", cmdName, "key", key, "db", db, "resultType", result.Type)
						switch cmdName {
						case "LPUSH":
							ctx.Engine.NotifyKeyEvent(db, uint32(keyevent.FlagList), "lpush", key)
						case "RPUSH":
							ctx.Engine.NotifyKeyEvent(db, uint32(keyevent.FlagList), "rpush", key)
						case "RPOP":
							ctx.Engine.NotifyKeyEvent(db, uint32(keyevent.FlagList), "rpop", key)
						case "EXPIRE":
							ctx.Engine.NotifyKeyEvent(db, uint32(keyevent.FlagGeneric), "expire", key)
						case "HSET":
							ctx.Engine.NotifyKeyEvent(db, uint32(keyevent.FlagHash), "hset", key)
						case "HMSET":
							ctx.Engine.NotifyKeyEvent(db, uint32(keyevent.FlagHash), "hset", key)
						case "HINCRBY":
							ctx.Engine.NotifyKeyEvent(db, uint32(keyevent.FlagHash), "hincrby", key)
						case "SADD":
							ctx.Engine.NotifyKeyEvent(db, uint32(keyevent.FlagSet), "sadd", key)
						case "SREM":
							ctx.Engine.NotifyKeyEvent(db, uint32(keyevent.FlagSet), "srem", key)
						case "ZADD":
							ctx.Engine.NotifyKeyEvent(db, uint32(keyevent.FlagZSet), "zadd", key)
						case "ZREM":
							ctx.Engine.NotifyKeyEvent(db, uint32(keyevent.FlagZSet), "zrem", key)
						case "XADD":
							ctx.Engine.NotifyKeyEvent(db, uint32(keyevent.FlagStream), "xadd", key)
						case "XGROUP":
							if len(cmdArgs) > 2 {
								subCmd := strings.ToLower(string(cmdArgs[1].Str))
								xkey := string(cmdArgs[2].Str)
								switch subCmd {
								case "create":
									ctx.Engine.NotifyKeyEvent(db, uint32(keyevent.FlagStream), "xgroup-create", xkey)
								case "createconsumer":
									ctx.Engine.NotifyKeyEvent(db, uint32(keyevent.FlagStream), "xgroup-createconsumer", xkey)
								case "delconsumer":
									ctx.Engine.NotifyKeyEvent(db, uint32(keyevent.FlagStream), "xgroup-delconsumer", xkey)
								}
							}
						case "XREADGROUP":
							if len(cmdArgs) > 4 {
								for i, arg := range cmdArgs {
									if strings.ToUpper(string(arg.Str)) == "STREAMS" && i+1 < len(cmdArgs) {
										xkey := string(cmdArgs[i+1].Str)
										ctx.Engine.NotifyKeyEvent(db, uint32(keyevent.FlagStream), "xgroup-createconsumer", xkey)
										break
									}
								}
							}
						case "XCLAIM", "XAUTOCLAIM":
							if len(cmdArgs) > 3 {
								xkey := string(cmdArgs[1].Str)
								ctx.Engine.NotifyKeyEvent(db, uint32(keyevent.FlagStream), "xgroup-createconsumer", xkey)
							}
						}
					}
				}

			} else if cmdArgs != nil {
				result = types.NewError("ERR wrong number of arguments")
			} else {
				result = types.NewError("ERR unknown request format")
			}

			// Record command stats
			elapsed := time.Since(startTime).Microseconds()
			failed := result.Type == types.TypeError
			if cmdName != "" {
				st.RecordCommand(cmdName, uint64(elapsed), failed)
			}
			if failed {
				st.TotalErrorReplies.Add(1)
				// Track per-error-type stats (first word of error message)
				errMsg := string(result.Str)
				errPrefix := errMsg
				if spaceIdx := strings.IndexByte(errMsg, ' '); spaceIdx > 0 {
					errPrefix = errMsg[:spaceIdx]
				}
				if errPrefix != "" {
					st.RecordErrorType(errPrefix)
				}
			}

			if result.Type != types.TypeNoReply {
				if err := encoder.Encode(w, result); err != nil {
					if !isConnectionClosed(err) {
						logger.Warn("encode error",
							"remote", conn.RemoteAddr().String(),
							"error", err,
						)
					}
					return
				}
				if err := w.Flush(); err != nil {
					if !isConnectionClosed(err) {
						logger.Warn("flush error",
							"remote", conn.RemoteAddr().String(),
							"error", err,
						)
					}
				}
			}

		case msg := <-ctx.PubSubCh:
			// Handle PubSub messages asynchronously
			var reply types.Value
			if msg.Pattern != "" {
				reply = types.NewArray([]types.Value{
					types.NewString("pmessage"),
					types.NewString(msg.Pattern),
					types.NewString(msg.Channel),
					types.NewString(msg.Message),
				})
			} else {
				reply = types.NewArray([]types.Value{
					types.NewString("message"),
					types.NewString(msg.Channel),
					types.NewString(msg.Message),
				})
			}
			if err := encoder.Encode(w, reply); err != nil {
				return
			}
			if err := w.Flush(); err != nil {
				return
			}
		}
	}
}

// selectDB switches the connection's active database.
func (s *Server) selectDB(ctx *kctx.Context, db int) string {
	if s.multiDB == nil {
		if db != 0 {
			return "ERR DB index is out of range"
		}
		return ""
	}
	eng := s.multiDB.SelectDB(db)
	if eng == nil {
		return "ERR DB index is out of range"
	}
	ctx.Engine = eng
	ctx.DB = db
	return ""
}

// recoverPanic catches panics in connection handlers so one bad request
// doesn't crash the server. Logs a full stack trace at error level.
func (s *Server) recoverPanic(conn net.Conn) {
	if r := recover(); r != nil {
		buf := make([]byte, 4096)
		n := runtime.Stack(buf, false)
		logger.Error("panic in connection handler",
			"remote", conn.RemoteAddr().String(),
			"panic", r,
			"stack", string(buf[:n]),
		)
	}
}

// isConnectionClosed returns true if the error indicates the client
// disconnected (connection reset, broken pipe, etc.).
func isConnectionClosed(err error) bool {
	if errors.Is(err, net.ErrClosed) || errors.Is(err, context.Canceled) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}
	return false
}
