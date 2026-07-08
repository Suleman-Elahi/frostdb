package context

import (
	"time"
	
	"frostdb/internal/engine/api"
	"frostdb/internal/resp/types"
	"net"
)

// Context holds per-connection state and gives access to the engine.
type Context struct {
	// Engine is the DB-scoped view for the current connection.
	// Commands should only access data through this interface.
	Engine api.Engine

	// DBSelector allows SELECT to switch the active database.
	// It is set by the server and must not be nil.
	DBSelector DBSelectorFunc

	// DB is the currently selected database index (0-15).
	DB int

	Conn net.Conn
	
	// Done is closed when the connection is dropped.
	Done <-chan struct{}

	// Protocol version (2 or 3)
	Protocol int

	// Client name (set via CLIENT SETNAME)
	ClientName string

	// Username (set via AUTH)
	Username string

	// Transaction state
	InMulti        bool
	ExecutingMulti bool
	TxQueue        [][]types.Value
	TxErrors       bool
	WatchAborted   bool

	// Deferred signals: SignalKey calls during MULTI/EXEC are buffered here
	// and flushed after the transaction completes, matching Redis atomicity.
	DeferredSignals []string

	// PubSub state
	Subscriptions map[string]func()
	PatternSubs   map[string]func()
	PubSubMode    bool
	PubSubCh      chan api.PubSubMessage

	// WATCH state
	WatchedKeys map[string]struct{}

	// Scripting state — suppresses WAL logging for sub-commands inside EVAL
	InScript bool

	ClientTxID      uint64           // Client ID matching the transaction owner
	ReadOnly        bool             // True if execution is strictly read-only (e.g., EvalRO)
	AllowOOM        bool             // True if scripts are allowed to execute write commands under OOM
	ScriptTimeMs    int64            // Deterministic timestamp for script execution

	// Replication rewrite support
	SkipReplication bool
	ReplicateAs     [][]byte

	// Track last executed command for CLIENT LIST
	LastCmd string

	// Blocked indicates the client is currently blocked on a blocking command (BLPOP, etc.).
	// Used by CLIENT LIST to show the "b" flag.
	Blocked bool

	// ClientID is a unique identifier assigned to this connection.
	ClientID int
}

// DBSelectorFunc switches the context to the given database index.
// Returns an error string if the index is out of range, or "" on success.
type DBSelectorFunc func(ctx *Context, db int) string

func (ctx *Context) NowMs() int64 {
	if ctx.ScriptTimeMs > 0 {
		return ctx.ScriptTimeMs
	}
	return time.Now().UnixMilli()
}
