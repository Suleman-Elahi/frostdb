package api

import (
	"frostdb/internal/engine/object"
	"frostdb/internal/engine/shard"
)

// BlockingEngine is implemented by engines that support synchronous FIFO
// signal handoff for blocking commands.
type BlockingEngine interface {
	SubscribeWaiter(keys []string) (*shard.Waiter, func())
	FlushSignals(keys []string)
}

// PubSubOOMEngine is implemented by engines that track pubsub memory and shard channels.
type PubSubOOMEngine interface {
	CheckPubSubOOM(argBytes int64) bool
	PubSubChannels(pattern string, shard bool) []string
}

// PubSubMessage represents a message published to a channel
type PubSubMessage struct {
	Channel string
	Message string
	Pattern string
}

type EvictionConfig struct {
	MaxMemory             int64
	Policy                string
	MaxSamples            int
	LazyfreeLazyServerDel bool
}

// Engine defines the core storage interface that commands interact with.
// Commands should never touch storage directly, only through this interface.
type Engine interface {
	Get(key string) (*object.Object, bool)
	GetWithTime(key string, nowMs int64) (*object.Object, bool)
	GetRaw(key string) (*object.Object, bool)

	// GetCheckExpired returns the object if not expired, without side effects.
	// Used by blocking commands to avoid premature passive expiry in replication stream.
	GetCheckExpired(key string) (*object.Object, bool)

	// GetBytes is like Get but takes a byte slice to avoid allocation
	GetBytes(key []byte) (*object.Object, bool)
	GetBytesWithTime(key []byte, nowMs int64) (*object.Object, bool)
	Set(key string, obj *object.Object)
	Del(key string) bool

	SetBytes(key []byte, obj *object.Object)
	DelBytes(key []byte) bool

	// WAL logging for persistence
	LogCommand(args [][]byte) error

	// Blocking operations support
	Subscribe(keys []string) (<-chan struct{}, func())
	SignalKey(key string)

	PubSubSubscribe(channel string, ch chan PubSubMessage) func()
	PubSubPublish(channel, message string) int64
	PubSubNumSub(channels []string) []int64
	PubSubNumPat() int64
	NotifyKeyspaceEvent(event, key string)

	// Key scanning
	Keys(pattern string) []string
	KeysAll(pattern string) []string

	// Eviction: returns true if write should be rejected (noeviction + OOM)
	TryEvict() bool

	// Config: get/set eviction parameters at runtime
	GetEvictionConfig() EvictionConfig
	SetEvictionConfig(cfg EvictionConfig)

	// RDB export from live state
	SaveRDB(path string) (int, error)

	// RDB import: load keys from an RDB file into the current database.
	LoadRDB(path string) (int, error)

	// AOF rewrite (checkpoint + WAL truncation)
	RewriteAOF() error
	RewriteAOFAsync() error
	AOFRewriteInProgress() int

	// LoadAOF reloads state from checkpoint + WAL files (DEBUG LOADAOF).
	LoadAOF() error

	// Keyspace notifications
	NotifyKeyEvent(dbID int, eventType uint32, event, key string)
	NotifySubkey(dbID int, eventType uint32, event, key string, fields []string)
	SetNotifyFlags(flags string)
	GetNotifyFlags() string

	// Touch notifies the engine that a key has been modified.
	// This is primarily used to abort WATCH transactions.
	OnTouch(cb func(key string))

	// Replication testing support
	AddReplica(conn interface{})
	PropagateToReplicas(args [][]byte)

	// Dirty counter for changes since last save
	GetDirty() int64
	AddDirty(n int64)
	ResetDirty()
}

// MultiDB extends Engine with multi-database support.
// Implemented by the concrete engine; used by SELECT and MOVE.
type MultiDB interface {
	Engine

	// SelectDB returns the Engine view for the given database index.
	// Returns nil if db is out of range.
	SelectDB(db int) Engine

	// NumDBs returns the total number of logical databases.
	NumDBs() int

	// MoveKey moves a key from one DB to another.
	// Returns true if the key was moved, false if src key doesn't exist or dst key already exists.
	MoveKey(key string, srcDB, dstDB int) bool

	// SwapDB swaps the contents of two databases atomically.
	SwapDB(db1, db2 int) error

	// FlushDB removes all keys from the given database.
	FlushDB(db int)

	// DBSize returns the number of keys in the given database.
	DBSize(db int) int
}
