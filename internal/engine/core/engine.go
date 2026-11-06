package core

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"frostdb/internal/concurrency"
	"frostdb/internal/engine/api"
	"frostdb/internal/engine/eviction"
	"frostdb/internal/engine/keyevent"
	"frostdb/internal/engine/object"
	"frostdb/internal/engine/persistence/checkpoint"
	"frostdb/internal/engine/persistence/recovery"
	"frostdb/internal/engine/persistence/wal"
	"frostdb/internal/engine/rdb"
	"frostdb/internal/engine/shard"
	"frostdb/internal/logger"
)

// WALFailurePolicy controls write admission while WAL I/O is unhealthy.
type WALFailurePolicy string

const (
	// WALFailureStopWrites preserves durability guarantees by rejecting writes
	// after a WAL failure until a subsequent sync succeeds.
	WALFailureStopWrites WALFailurePolicy = "stop-writes"
	// WALFailureContinue keeps accepting writes during WAL failures. It is
	// intended only for operators who explicitly prefer availability over
	// durability while storage is impaired.
	WALFailureContinue WALFailurePolicy = "continue"
)

// ParseWALFailurePolicy validates the wal-error-policy setting. The empty
// value defaults to the production-safe stop-writes behavior.
func ParseWALFailurePolicy(value string) (WALFailurePolicy, error) {
	policy := WALFailurePolicy(strings.ToLower(strings.TrimSpace(value)))
	if policy == "" {
		return WALFailureStopWrites, nil
	}
	switch policy {
	case WALFailureStopWrites, WALFailureContinue:
		return policy, nil
	default:
		return "", fmt.Errorf("invalid wal error policy %q (expected stop-writes or continue)", value)
	}
}

// PersistOptions configures persistence for the engine
type PersistOptions struct {
	DataDir string
	// WALFsyncMode controls WAL durability: always, everysec (default), or no.
	WALFsyncMode wal.FsyncMode
	// WALFailurePolicy controls whether unhealthy WAL storage blocks future
	// writes. The empty value defaults to stop-writes.
	WALFailurePolicy WALFailurePolicy
	// WALSyncOnWrite is retained for source compatibility; it maps to always
	// when WALFsyncMode is not explicitly configured.
	WALSyncOnWrite     bool
	CheckpointInterval time.Duration // 0 = no auto checkpoint
	Eviction           eviction.Config
}

type CheckpointStatus struct {
	InProgress           bool
	LastSuccessUnixMilli int64
	LastError            string
	SuccessCount         uint64
	FailureCount         uint64
}

var errEngineClosed = errors.New("engine is closed")

type Engine struct {
	// dbs holds NumDBs logical databases, each with its own shard set.
	dbs [NumDBs]*DBView

	// PubSub (engine-wide, not DB-scoped)
	pubsubMu     sync.RWMutex
	pubsubSubs   map[string][]chan api.PubSubMessage
	pubsubMemory atomic.Int64

	// Persistence
	wal              *wal.WAL
	dataDir          string
	walFailurePolicy WALFailurePolicy

	checkpointMu        sync.Mutex
	checkpointStatusMu  sync.RWMutex
	checkpointInFlight  int32
	lastCheckpointAt    time.Time
	lastCheckpointErr   string
	checkpointSuccesses uint64
	checkpointFailures  uint64

	persistenceMu     sync.Mutex
	persistenceStopCh chan struct{}
	persistenceWg     sync.WaitGroup
	closed            int32
	closeOnce         sync.Once
	closeErr          error

	// Eviction
	Evictor *eviction.Evictor

	Notifier *keyevent.Notifier

	notifyFlags atomic.Value // stores the raw notify-keyspace-events string

	onTouchCb func(key string)

	replMu   sync.RWMutex
	replicas []*replicaInfo

	dirty                int64 // rdb_changes_since_last_save counter
	aofRewriteInProgress int32 // 1 when background AOF rewrite is running

	// Active expire
	expireStopCh chan struct{}
	expireWg     sync.WaitGroup
}

// DB returns the DBView for the given database index.
// Panics if db is out of range — callers must validate first.
func (e *Engine) DB(db int) *DBView {
	return e.dbs[db]
}

// Shards returns the shard array for DB 0 (used by legacy RDB import/export).
func (e *Engine) Shards() [256]*shard.Shard {
	return e.dbs[0].shards
}

// New creates an in-memory-only engine (no persistence). Used by tests.
func New() api.Engine {
	e := &Engine{
		pubsubSubs:       make(map[string][]chan api.PubSubMessage),
		walFailurePolicy: WALFailureStopWrites,
	}
	for i := 0; i < NumDBs; i++ {
		e.dbs[i] = newDBView(e, i)
	}
	allShards := make([][256]*shard.Shard, NumDBs)
	for i := 0; i < NumDBs; i++ {
		allShards[i] = e.dbs[i].Shards()
	}
	e.Evictor = eviction.NewEvictor(allShards, eviction.DefaultConfig())
	e.Evictor.OnEvict = func(dbIdx int, key string) {
		e.Notifier.NotifyEvicted(dbIdx, key)
	}
	e.Notifier = keyevent.NewNotifier(e.dbs[0])
	e.startActiveExpireLoop()
	return e.dbs[0]
}

// NewWithPersistence creates an engine with WAL and checkpoint support.
// It recovers state from disk on startup.
func NewWithPersistence(opts PersistOptions) (api.Engine, error) {
	failurePolicy, err := ParseWALFailurePolicy(string(opts.WALFailurePolicy))
	if err != nil {
		return nil, err
	}
	e := &Engine{
		pubsubSubs:        make(map[string][]chan api.PubSubMessage),
		dataDir:           opts.DataDir,
		walFailurePolicy:  failurePolicy,
		persistenceStopCh: make(chan struct{}),
	}
	for i := 0; i < NumDBs; i++ {
		e.dbs[i] = newDBView(e, i)
	}
	allShards := make([][256]*shard.Shard, NumDBs)
	for i := 0; i < NumDBs; i++ {
		allShards[i] = e.dbs[i].Shards()
	}
	e.Evictor = eviction.NewEvictor(allShards, opts.Eviction)
	e.Evictor.OnEvict = func(dbIdx int, key string) {
		e.Notifier.NotifyEvicted(dbIdx, key)
	}
	e.Notifier = keyevent.NewNotifier(e.dbs[0])

	// Recover existing state
	state, err := recovery.Recover(recovery.Options{DataDir: opts.DataDir})
	if err != nil {
		return nil, err
	}

	// Load checkpoint records into the database encoded in each record.
	// Legacy FROSTCK1 records omit DB and therefore correctly default to DB 0.
	var skipped int
	for _, rec := range state.Records {
		if int(rec.DB) >= NumDBs {
			skipped++
			logger.Warn("recovery: skip key with invalid database", "key", rec.Key, "db", rec.DB)
			continue
		}
		obj, err := deserializeObject(rec)
		if err != nil {
			skipped++
			logger.Warn("recovery: skip key", "key", rec.Key, "error", err)
			continue
		}
		s := e.dbs[rec.DB].getShard(rec.Key)
		s.Set(rec.Key, obj)
	}
	if skipped > 0 {
		logger.Warn("recovery: skipped corrupted keys", "count", skipped)
	}

	// Replay WAL entries
	for _, entry := range state.WALEntries {
		e.replayEntry(entry)
	}

	// Open WAL for new writes
	walOpts := wal.Options{
		Dir:            filepath.Join(opts.DataDir, "wal"),
		MaxFileSize:    64 * 1024 * 1024,
		InitialSegment: state.CheckpointMetadata.WALCutoff + 1,
		FsyncMode:      opts.WALFsyncMode,
		SyncOnWrite:    opts.WALSyncOnWrite,
	}
	w, err := wal.Open(walOpts)
	if err != nil {
		return nil, err
	}
	e.wal = w

	// Start auto-checkpoint if configured. It is tracked so Close waits for it
	// before closing the WAL.
	if opts.CheckpointInterval > 0 {
		e.startPersistenceWorker(func() {
			e.checkpointLoop(opts.CheckpointInterval)
		})
	}

	e.startActiveExpireLoop()

	return e.dbs[0], nil
}

func (e *Engine) getShard(key string) *shard.Shard {
	return e.dbs[0].getShard(key)
}

func (e *Engine) getShardByBytes(key []byte) *shard.Shard {
	return e.dbs[0].getShardByBytes(key)
}

// Get/Set/Del on Engine operate on DB 0 (used by WAL replay and internal ops).
func (e *Engine) Get(key string) (*object.Object, bool) {
	return e.dbs[0].Get(key)
}

func (e *Engine) GetWithTime(key string, nowMs int64) (*object.Object, bool) {
	return e.dbs[0].GetWithTime(key, nowMs)
}

func (e *Engine) GetRaw(key string) (*object.Object, bool) {
	return e.dbs[0].GetRaw(key)
}

func (e *Engine) GetBytes(key []byte) (*object.Object, bool) {
	return e.dbs[0].GetBytes(key)
}

func (e *Engine) GetBytesWithTime(key []byte, nowMs int64) (*object.Object, bool) {
	return e.dbs[0].GetBytesWithTime(key, nowMs)
}

func (e *Engine) GetCheckExpired(key string) (*object.Object, bool) {
	return e.dbs[0].GetCheckExpired(key)
}

func (e *Engine) SetBytes(key []byte, obj *object.Object) {
	e.dbs[0].SetBytes(key, obj)
}

func (e *Engine) Set(key string, obj *object.Object) {
	e.dbs[0].Set(key, obj)
}

func (e *Engine) Del(key string) bool {
	return e.dbs[0].Del(key)
}

func (e *Engine) DelBytes(key []byte) bool {
	return e.dbs[0].DelBytes(key)
}

// GetDirty returns the rdb_changes_since_last_save counter.
func (e *Engine) GetDirty() int64 {
	return atomic.LoadInt64(&e.dirty)
}

func (e *Engine) AddDirty(n int64) {
	atomic.AddInt64(&e.dirty, n)
}

// ResetDirty resets the dirty counter (called after save/checkpoint).
func (e *Engine) ResetDirty() {
	atomic.StoreInt64(&e.dirty, 0)
}

var walBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 256)
		return &b
	},
}

var walCommandMagic = [...]byte{'F', 'D', 'B', 'W'}

const walCommandVersion byte = 1

type replicaInfo struct {
	conn   net.Conn
	lastDB int
}

// LogCommand writes a raw command to the WAL (called by mutating commands).
// Returns an error if the WAL write fails.
func (e *Engine) LogCommand(db int, args [][]byte) error {
	if db < 0 || db >= NumDBs {
		return fmt.Errorf("wal: invalid database %d", db)
	}
	var err error
	if e.wal != nil {
		bp := walBufPool.Get().(*[]byte)
		buf := (*bp)[:0]

		// Versioned command framing keeps the original DB alongside the command.
		// Legacy records begin with the uint16 argument count and remain readable
		// as DB 0 during recovery.
		buf = append(buf, walCommandMagic[:]...)
		buf = append(buf, walCommandVersion, byte(db))
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(args)))
		for _, arg := range args {
			buf = binary.LittleEndian.AppendUint32(buf, uint32(len(arg)))
			buf = append(buf, arg...)
		}

		err = e.wal.Append(buf)
		if err != nil {
			logger.Error("wal: append failed", "error", err)
		}
		*bp = buf
		walBufPool.Put(bp)
	}

	// Propagate to attached test replicas even in in-memory mode (e.wal == nil).
	e.PropagateToReplicas(db, args)

	return err
}

// PropagateToReplicas sends a RESP-encoded command to all attached replicas
// without WAL logging. Used for commands like SWAPDB, SELECT, and key expiry
// DEL that need to be replicated but aren't normal write commands.
func (e *Engine) PropagateToReplicas(db int, args [][]byte) {
	e.replMu.RLock()
	hasReplicas := len(e.replicas) > 0
	e.replMu.RUnlock()
	if !hasReplicas {
		return
	}

	e.replMu.Lock()
	defer e.replMu.Unlock()
	if len(e.replicas) == 0 {
		return
	}
	// Build RESP array
	var replBuf []byte
	replBuf = append(replBuf, '*')
	replBuf = append(replBuf, []byte(fmt.Sprint(len(args)))...)
	replBuf = append(replBuf, '\r', '\n')
	for _, arg := range args {
		replBuf = append(replBuf, '$')
		replBuf = append(replBuf, []byte(fmt.Sprint(len(arg)))...)
		replBuf = append(replBuf, '\r', '\n')
		replBuf = append(replBuf, arg...)
		replBuf = append(replBuf, '\r', '\n')
	}
	validReplicas := make([]*replicaInfo, 0, len(e.replicas))
	for _, r := range e.replicas {
		var werr error
		if r.lastDB != db {
			selectBuf := []byte(fmt.Sprintf("*2\r\n$6\r\nSELECT\r\n$%d\r\n%d\r\n", len(fmt.Sprint(db)), db))
			_, werr = r.conn.Write(selectBuf)
			if werr == nil {
				r.lastDB = db
			}
		}
		if werr == nil {
			_, werr = r.conn.Write(replBuf)
		}
		if werr == nil {
			validReplicas = append(validReplicas, r)
		}
	}
	e.replicas = validReplicas
}

func (e *Engine) Subscribe(keys []string) (<-chan struct{}, func()) {
	return e.dbs[0].Subscribe(keys)
}

func (e *Engine) SubscribeWaiter(keys []string) (*shard.Waiter, func()) {
	return e.dbs[0].SubscribeWaiter(keys)
}

func (e *Engine) FlushSignals(keys []string) {
	e.dbs[0].FlushSignals(keys)
}

func (e *Engine) SignalKey(key string) {
	e.dbs[0].SignalKey(key)
}

func (e *Engine) Keys(pattern string) []string {
	return e.dbs[0].Keys(pattern)
}

func (e *Engine) KeysAll(pattern string) []string {
	return e.dbs[0].KeysAll(pattern)
}

// PubSubSubscribe subscribes the given channel to a pubsub channel and returns a cleanup function
func (e *Engine) PubSubSubscribe(channel string, ch chan api.PubSubMessage) func() {
	e.pubsubMu.Lock()
	defer e.pubsubMu.Unlock()

	if e.pubsubSubs == nil {
		e.pubsubSubs = make(map[string][]chan api.PubSubMessage)
	}

	e.pubsubSubs[channel] = append(e.pubsubSubs[channel], ch)
	memBytes := int64(len(channel)*2 + 256)
	e.pubsubMemory.Add(memBytes)

	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			e.pubsubMemory.Add(-memBytes)
			e.pubsubMu.Lock()
			defer e.pubsubMu.Unlock()
			subs := e.pubsubSubs[channel]
			for i, sub := range subs {
				if sub == ch {
					e.pubsubSubs[channel] = append(subs[:i], subs[i+1:]...)
					break
				}
			}
			if len(e.pubsubSubs[channel]) == 0 {
				delete(e.pubsubSubs, channel)
			}
		})
	}

	return cleanup
}

func (e *Engine) CheckPubSubOOM(argBytes int64) bool {
	cfg := e.Evictor.GetConfig()
	if cfg.MaxMemory <= 0 {
		return false
	}
	if e.TryEvict() {
		return true
	}
	used := e.Evictor.UsedMemory() + e.pubsubMemory.Load() + argBytes
	if used > cfg.MaxMemory {
		if cfg.Policy == eviction.PolicyNoEviction {
			return true
		}
		e.Evictor.Evict()
		used = e.Evictor.UsedMemory() + e.pubsubMemory.Load() + argBytes
		if used > cfg.MaxMemory {
			return true
		}
	}
	return false
}

func (e *Engine) PubSubChannels(pattern string, shard bool) []string {
	e.pubsubMu.RLock()
	defer e.pubsubMu.RUnlock()

	var channels []string
	for subChannel, subs := range e.pubsubSubs {
		if len(subs) == 0 {
			continue
		}
		if shard {
			if strings.HasPrefix(subChannel, "__shard:") {
				ch := subChannel[len("__shard:"):]
				if pattern == "" || pattern == "*" {
					channels = append(channels, ch)
				} else if matched, _ := filepath.Match(pattern, ch); matched {
					channels = append(channels, ch)
				}
			}
		} else {
			if strings.HasPrefix(subChannel, "__pattern:") || strings.HasPrefix(subChannel, "__shard:") {
				continue
			}
			if pattern == "" || pattern == "*" {
				channels = append(channels, subChannel)
			} else if matched, _ := filepath.Match(pattern, subChannel); matched {
				channels = append(channels, subChannel)
			}
		}
	}
	return channels
}

func (e *Engine) PubSubPublish(channel, message string) int64 {
	e.pubsubMu.RLock()
	defer e.pubsubMu.RUnlock()

	count := int64(0)

	if strings.HasPrefix(channel, "__shard:") {
		realCh := channel[len("__shard:"):]
		if subs, ok := e.pubsubSubs[channel]; ok {
			for _, ch := range subs {
				select {
				case ch <- api.PubSubMessage{Channel: realCh, Message: message, Pattern: "__shard"}:
					count++
				default:
				}
			}
		}
		return count
	}

	// Exact matches
	if subs, ok := e.pubsubSubs[channel]; ok {
		for _, ch := range subs {
			select {
			case ch <- api.PubSubMessage{Channel: channel, Message: message}:
				count++
			default:
			}
		}
	}

	// Pattern matches
	for subChannel, subs := range e.pubsubSubs {
		if len(subChannel) > 10 && subChannel[:10] == "__pattern:" {
			pattern := subChannel[10:]
			matched, _ := filepath.Match(pattern, channel)
			if matched {
				for _, ch := range subs {
					select {
					// For pattern subscriptions, we send the message.
					// Note: the command handler maps this to pmessage.
					case ch <- api.PubSubMessage{Channel: channel, Message: message, Pattern: pattern}:
						count++
					default:
					}
				}
			}
		}
	}

	return count
}

func (e *Engine) PubSubNumSub(channels []string) []int64 {
	e.pubsubMu.RLock()
	defer e.pubsubMu.RUnlock()

	res := make([]int64, len(channels))
	for i, ch := range channels {
		res[i] = int64(len(e.pubsubSubs[ch]))
	}
	return res
}

func (e *Engine) PubSubNumPat() int64 {
	e.pubsubMu.RLock()
	defer e.pubsubMu.RUnlock()

	var count int64
	for channel := range e.pubsubSubs {
		if len(channel) > 10 && channel[:10] == "__pattern:" {
			count++
		}
	}
	return count
}

func (e *Engine) notifyKeyspaceEventWithDB(dbID int, event, key string) {
	// Map event names to their flag types
	var eventType keyevent.Flags
	switch event {
	case "del":
		eventType = keyevent.FlagGeneric
	case "expire":
		eventType = keyevent.FlagGeneric
	case "expired":
		eventType = keyevent.FlagExpired
	case "evicted":
		eventType = keyevent.FlagEvicted
	case "set", "setbit", "incrby", "incrbyfloat", "append":
		eventType = keyevent.FlagString
	case "lpush", "rpush", "rpop", "lpop", "linsert", "lset", "lrem", "ltrim":
		eventType = keyevent.FlagList
	case "sadd", "srem", "spop":
		eventType = keyevent.FlagSet
	case "hset", "hincrby", "hincrbyfloat", "hdel", "hexpire", "hexpired", "hpersist":
		eventType = keyevent.FlagHash
	case "zadd", "zrem", "zincrby", "zpopmax", "zpopmin":
		eventType = keyevent.FlagZSet
	case "xadd", "xdel", "xtrim", "xgroup-create", "xgroup-createconsumer", "xgroup-delconsumer":
		eventType = keyevent.FlagStream
	case "new":
		eventType = keyevent.FlagNew
	case "overwritten":
		eventType = keyevent.FlagOverwrite
	case "type_changed":
		eventType = keyevent.FlagTypeChanged
	case "restore":
		eventType = keyevent.FlagGeneric
	case "rename_from", "rename_to":
		eventType = keyevent.FlagGeneric
	case "copy_to":
		eventType = keyevent.FlagGeneric
	default:
		eventType = keyevent.FlagGeneric
	}

	// Use the Notifier to respect notify-keyspace-events flags
	e.Notifier.Notify(dbID, eventType, event, key)
}

func (e *Engine) NotifyKeyspaceEvent(event, key string) {
	e.notifyKeyspaceEventWithDB(0, event, key)
}

// Checkpoint creates a consistent snapshot of all databases and records the
// sealed WAL segment included in it. Recovery replays only newer segments.
func (e *Engine) Checkpoint() (err error) {
	if e.isClosed() {
		return errEngineClosed
	}

	// Serialize snapshots so that manual, scheduled, and rewrite checkpoints
	// cannot rotate the WAL concurrently.
	e.checkpointMu.Lock()
	defer e.checkpointMu.Unlock()
	if e.isClosed() {
		return errEngineClosed
	}

	atomic.StoreInt32(&e.checkpointInFlight, 1)
	defer func() {
		atomic.StoreInt32(&e.checkpointInFlight, 0)
		e.recordCheckpointOutcome(err)
	}()

	return e.checkpointLocked()
}

func (e *Engine) checkpointLocked() error {
	cpDir := filepath.Join(e.dataDir, "checkpoint")

	// Writers take striped write locks, so this prevents every mutation while
	// the WAL boundary is sealed and the snapshot is serialized.
	concurrency.GlobalLocker.LockAll(false)
	defer concurrency.GlobalLocker.UnlockAll(false)

	var cutoff uint64
	if e.wal != nil {
		var err error
		cutoff, err = e.wal.Rotate()
		if err != nil {
			return fmt.Errorf("checkpoint: rotate WAL: %w", err)
		}
	}

	w, err := checkpoint.NewWriterWithMetadata(cpDir, checkpoint.Metadata{WALCutoff: cutoff})
	if err != nil {
		return err
	}

	var skipped int
	var writeErr error
	for dbIdx := 0; dbIdx < NumDBs; dbIdx++ {
		for i := 0; i < 256; i++ {
			e.dbs[dbIdx].shards[i].ForEach(func(key string, obj *object.Object) {
				if writeErr != nil {
					return
				}
				rec, serErr := serializeObject(key, obj)
				if serErr != nil {
					skipped++
					logger.Warn("checkpoint: skip key", "key", key, "error", serErr)
					return
				}
				rec.DB = uint8(dbIdx)
				if err := w.WriteRecord(rec); err != nil {
					writeErr = err
				}
			})
		}
	}
	if writeErr != nil {
		_ = w.Abort()
		return fmt.Errorf("checkpoint: write record: %w", writeErr)
	}
	if skipped > 0 {
		logger.Warn("checkpoint: skipped keys", "count", skipped)
	}
	if err := w.Close(); err != nil {
		return err
	}
	if e.wal != nil {
		if err := e.wal.PruneThrough(cutoff); err != nil {
			// The published cutoff makes retained files harmless. Report cleanup
			// failure without invalidating an otherwise durable checkpoint.
			logger.Warn("checkpoint: prune WAL", "error", err)
		}
	}
	return nil
}

func (e *Engine) checkpointLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-e.persistenceStopCh:
			return
		case <-ticker.C:
			if e.isClosed() {
				return
			}
			if err := e.Checkpoint(); err != nil && !e.isClosed() {
				logger.Error("auto-checkpoint failed", "error", err)
			}
		}
	}
}

// Close stops all persistence workers before closing the WAL. It is safe to
// call repeatedly; every caller receives the result of the first close.
func (e *Engine) Close() error {
	e.closeOnce.Do(func() {
		// Prevent new background workers from being registered, signal existing
		// ones, then wait for active checkpoints or rewrites to finish.
		e.persistenceMu.Lock()
		atomic.StoreInt32(&e.closed, 1)
		if e.persistenceStopCh != nil {
			close(e.persistenceStopCh)
		}
		e.persistenceMu.Unlock()
		e.persistenceWg.Wait()

		// A direct/manual checkpoint may not be a tracked worker. Waiting on
		// this mutex guarantees it cannot still rotate the WAL during close.
		e.checkpointMu.Lock()
		e.checkpointMu.Unlock()

		if e.expireStopCh != nil {
			close(e.expireStopCh)
			e.expireWg.Wait()
		}
		if e.wal != nil {
			e.closeErr = e.wal.Close()
		}
	})
	return e.closeErr
}

func (e *Engine) isClosed() bool {
	return atomic.LoadInt32(&e.closed) != 0
}

func (e *Engine) startPersistenceWorker(fn func()) bool {
	e.persistenceMu.Lock()
	defer e.persistenceMu.Unlock()
	if e.isClosed() || e.persistenceStopCh == nil {
		return false
	}
	e.persistenceWg.Add(1)
	go func() {
		defer e.persistenceWg.Done()
		fn()
	}()
	return true
}

func (e *Engine) recordCheckpointOutcome(err error) {
	e.checkpointStatusMu.Lock()
	defer e.checkpointStatusMu.Unlock()
	if err != nil {
		e.lastCheckpointErr = err.Error()
		e.checkpointFailures++
		return
	}
	e.lastCheckpointAt = time.Now()
	e.lastCheckpointErr = ""
	e.checkpointSuccesses++
}

// CheckpointStatus returns the most recent checkpoint result and live state.
func (e *Engine) CheckpointStatus() CheckpointStatus {
	e.checkpointStatusMu.RLock()
	defer e.checkpointStatusMu.RUnlock()
	status := CheckpointStatus{
		InProgress:   atomic.LoadInt32(&e.checkpointInFlight) != 0,
		LastError:    e.lastCheckpointErr,
		SuccessCount: e.checkpointSuccesses,
		FailureCount: e.checkpointFailures,
	}
	if !e.lastCheckpointAt.IsZero() {
		status.LastSuccessUnixMilli = e.lastCheckpointAt.UnixMilli()
	}
	return status
}

// startActiveExpireLoop starts a background goroutine that periodically
// scans hash keys for expired fields and emits hexpired notifications.
func (e *Engine) startActiveExpireLoop() {
	e.expireStopCh = make(chan struct{})
	e.expireWg.Add(1)
	go func() {
		defer e.expireWg.Done()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-e.expireStopCh:
				return
			case <-ticker.C:
				e.runActiveExpireCycle()
			}
		}
	}()
}

// runActiveExpireCycle scans all databases for hash keys with expired fields.
func (e *Engine) runActiveExpireCycle() {
	// Check if active expire is enabled (controlled by DEBUG SET-ACTIVE-EXPIRE)
	if !keyevent.ActiveExpireEnabled.Load() {
		return
	}

	nowMs := time.Now().UnixMilli()
	checked := 0
	const maxCheckPerCycle = 100

	for dbIdx := 0; dbIdx < NumDBs && checked < maxCheckPerCycle; dbIdx++ {
		for shardIdx := 0; shardIdx < 256 && checked < maxCheckPerCycle; shardIdx++ {
			shard := e.dbs[dbIdx].shards[shardIdx]

			sampleKeys := shard.SampleKeys(20)
			if len(sampleKeys) == 0 {
				continue
			}

			// Expire whole keys first.
			for _, key := range sampleKeys {
				if checked >= maxCheckPerCycle {
					break
				}
				unlock := concurrency.GlobalLocker.LockKeys([]string{key}, true)
				obj, ok := shard.GetRaw(key)
				if !ok || obj == nil {
					unlock()
					continue
				}
				if obj.ExpireAt <= 0 || obj.ExpireAt > nowMs {
					unlock()
					continue
				}
				if shard.Del(key) {
					e.Notifier.Notify(dbIdx, keyevent.FlagExpired, "expired", key)
				}
				checked++
				unlock()
			}
			if checked >= maxCheckPerCycle {
				break
			}

			// Then scan hash-field expirations.
			var hashKeys []string
			for _, key := range sampleKeys {
				obj, ok := shard.GetRaw(key)
				if ok && obj != nil && obj.Type == object.TypeHash {
					hashKeys = append(hashKeys, key)
				}
			}

			for _, key := range hashKeys {
				if checked >= maxCheckPerCycle {
					break
				}
				unlock := concurrency.GlobalLocker.LockKeys([]string{key}, true)
				obj, ok := shard.GetRaw(key)
				if !ok || obj == nil || obj.Type != object.TypeHash {
					unlock()
					continue
				}
				h, ok := obj.Value.(*object.Hash)
				if !ok || h.TTLs == nil || len(h.TTLs) == 0 {
					unlock()
					continue
				}

				var expired []string
				for field, ttl := range h.TTLs {
					if ttl > 0 && ttl <= nowMs {
						expired = append(expired, field)
					}
				}
				if len(expired) == 0 {
					unlock()
					continue
				}

				for _, field := range expired {
					h.Del(field)
				}
				checked++

				// Send hexpired notification (at most one per hash per cycle).
				e.Notifier.Notify(dbIdx, keyevent.FlagHash, "hexpired", key)
				e.Notifier.NotifySubkey(dbIdx, keyevent.FlagHash, "hexpired", key, expired)

				// If hash is empty, delete it and send del notification
				if h.Len() == 0 {
					shard.Del(key)
					e.Notifier.Notify(dbIdx, keyevent.FlagGeneric, "del", key)
				}
				unlock()
			}
		}
	}
}

// replayEntry replays a single WAL entry into the database encoded by its
// versioned command frame. Legacy frames implicitly target DB 0.
func (e *Engine) replayEntry(entry wal.Entry) {
	db, args, ok := decodeWALCommand(entry.Command)
	if !ok || db < 0 || db >= NumDBs || len(args) == 0 {
		return
	}
	applyCommand(e.dbs[db], args, entry.Timestamp)
}

func decodeWALCommand(buf []byte) (int, [][]byte, bool) {
	db := 0
	if len(buf) >= 7 && string(buf[:4]) == string(walCommandMagic[:]) {
		if buf[4] != walCommandVersion {
			logger.Warn("recovery: unsupported WAL command version", "version", buf[4])
			return 0, nil, false
		}
		db = int(buf[5])
		buf = buf[6:]
	}
	if len(buf) < 2 {
		logger.Warn("recovery: truncated WAL entry")
		return 0, nil, false
	}
	nargs := binary.LittleEndian.Uint16(buf[:2])
	buf = buf[2:]

	args := make([][]byte, 0, nargs)
	for i := 0; i < int(nargs); i++ {
		if len(buf) < 4 {
			logger.Warn("recovery: truncated WAL entry")
			return 0, nil, false
		}
		length := binary.LittleEndian.Uint32(buf[:4])
		buf = buf[4:]
		if uint32(len(buf)) < length {
			logger.Warn("recovery: truncated WAL entry")
			return 0, nil, false
		}
		args = append(args, buf[:length])
		buf = buf[length:]
	}
	return db, args, true
}

// touchObject updates access tracking based on eviction policy.
func (e *Engine) touchObject(obj *object.Object) {
	cfg := e.Evictor.GetConfig()
	switch cfg.Policy {
	case eviction.PolicyAllKeysLFU, eviction.PolicyVolatileLFU:
		eviction.TouchLFU(obj, cfg.LFULogFactor, cfg.LFUDecayTime)
	case eviction.PolicyNoEviction:
		// no-op
	default:
		eviction.TouchLRU(obj)
	}
}

// initObjectAccess sets the initial access tracking on a new object.
func (e *Engine) initObjectAccess(obj *object.Object) {
	cfg := e.Evictor.GetConfig()
	switch cfg.Policy {
	case eviction.PolicyAllKeysLFU, eviction.PolicyVolatileLFU:
		eviction.InitLFU(obj)
	case eviction.PolicyNoEviction:
		// no-op
	default:
		eviction.InitLRU(obj)
	}
}

// estimateSize returns a rough byte estimate for memory tracking.
func estimateSize(obj *object.Object) int64 {
	base := int64(64) // Object struct overhead
	switch obj.Type {
	case object.TypeString:
		if s, ok := obj.Value.([]byte); ok {
			return base + int64(len(s))
		}
	case object.TypeList:
		if l, ok := obj.Value.(*object.List); ok {
			return base + int64(l.Len)*80 // node + value estimate
		}
	case object.TypeHash:
		if h, ok := obj.Value.(*object.Hash); ok {
			return base + int64(h.Len())*100
		}
	case object.TypeSet:
		if s, ok := obj.Value.(*object.Set); ok {
			return base + int64(s.Len())*50
		}
	case object.TypeZSet:
		if z, ok := obj.Value.(*object.ZSet); ok {
			return base + int64(z.Len())*120
		}
	}
	return base + 128
}

// TryEvict runs eviction if memory exceeds the limit. Returns true if eviction
// was needed but policy is noeviction (caller should reject the write).
func (e *Engine) TryEvict() bool {
	if e.Evictor.IsNoEviction() {
		return true // reject write
	}
	if e.Evictor.NeedsEviction() {
		e.Evictor.Evict()
	}
	return false
}

// GetEvictionConfig returns the current eviction configuration.
func (e *Engine) GetEvictionConfig() api.EvictionConfig {
	cfg := e.Evictor.GetConfig()
	return api.EvictionConfig{
		MaxMemory:             cfg.MaxMemory,
		Policy:                cfg.Policy.String(),
		MaxSamples:            cfg.MaxSamples,
		LazyfreeLazyServerDel: cfg.LazyfreeLazyServerDel,
	}
}

// SetEvictionConfig updates eviction parameters at runtime.
func (e *Engine) SetEvictionConfig(ecfg api.EvictionConfig) {
	cfg := e.Evictor.GetConfig()
	if ecfg.MaxMemory >= 0 {
		cfg.MaxMemory = ecfg.MaxMemory
	}
	if ecfg.Policy != "" {
		cfg.Policy = eviction.ParsePolicy(ecfg.Policy)
	}
	if ecfg.MaxSamples > 0 {
		cfg.MaxSamples = ecfg.MaxSamples
	}
	cfg.LazyfreeLazyServerDel = ecfg.LazyfreeLazyServerDel
	e.Evictor.SetConfig(cfg)
}

// SaveRDB exports every logical database to an atomic RDB snapshot.
func (e *Engine) SaveRDB(path string) (int, error) {
	concurrency.GlobalLocker.LockAll(false)
	defer concurrency.GlobalLocker.UnlockAll(false)

	databases := make([][256]*shard.Shard, NumDBs)
	for dbIdx := 0; dbIdx < NumDBs; dbIdx++ {
		databases[dbIdx] = e.dbs[dbIdx].shards
	}
	return rdb.NewMultiExporter(databases).ExportFile(path)
}

// LoadRDB imports every database encoded in an RDB file.
func (e *Engine) LoadRDB(path string) (int, error) {
	concurrency.GlobalLocker.LockAll(true)
	defer concurrency.GlobalLocker.UnlockAll(true)

	databases := make([][256]*shard.Shard, NumDBs)
	for dbIdx := 0; dbIdx < NumDBs; dbIdx++ {
		databases[dbIdx] = e.dbs[dbIdx].shards
	}
	return rdb.NewMultiImporter(databases).ImportFile(path)
}

// RewriteAOF creates a fresh checkpoint and removes superseded snapshots.
// Checkpoint itself seals and prunes WAL segments with a durable cutoff; do not
// truncate current.wal here because it may already contain post-snapshot writes.
func (e *Engine) RewriteAOF() error {
	if e.isClosed() {
		return errEngineClosed
	}
	if e.wal == nil {
		return nil
	}
	if err := e.Checkpoint(); err != nil {
		return err
	}
	cpDir := filepath.Join(e.dataDir, "checkpoint")
	_, _ = checkpoint.CleanupOldCheckpoints(cpDir)
	return nil
}

// RewriteAOFAsync performs an asynchronous AOF rewrite in the background.
func (e *Engine) RewriteAOFAsync() error {
	if e.isClosed() {
		return errEngineClosed
	}
	if e.wal == nil {
		return nil
	}
	if !e.startPersistenceWorker(func() {
		atomic.StoreInt32(&e.aofRewriteInProgress, 1)
		defer atomic.StoreInt32(&e.aofRewriteInProgress, 0)
		if err := e.RewriteAOF(); err != nil {
			if !e.isClosed() {
				logger.Error("background AOF rewrite failed", "error", err)
			}
			return
		}
		logger.Info("background AOF rewrite completed")
	}) {
		return errEngineClosed
	}
	return nil
}

// AOFRewriteInProgress returns 1 if a background AOF rewrite is running.
func (e *Engine) AOFRewriteInProgress() int {
	return int(atomic.LoadInt32(&e.aofRewriteInProgress))
}

// LoadAOF reloads all databases from checkpoint plus WAL files (DEBUG LOADAOF).
func (e *Engine) LoadAOF() error {
	if e.dataDir == "" {
		return nil
	}

	// Block all mutations for the complete replacement. The recovery read is
	// included so a concurrent write cannot be lost between recovery and flush.
	concurrency.GlobalLocker.LockAll(true)
	defer concurrency.GlobalLocker.UnlockAll(true)
	if e.wal != nil {
		if err := e.wal.Sync(); err != nil {
			return fmt.Errorf("load AOF: sync WAL: %w", err)
		}
	}

	state, err := recovery.Recover(recovery.Options{DataDir: e.dataDir})
	if err != nil {
		return err
	}
	for dbIdx := 0; dbIdx < NumDBs; dbIdx++ {
		for i := 0; i < 256; i++ {
			e.dbs[dbIdx].shards[i].Flush()
		}
	}

	for _, rec := range state.Records {
		if int(rec.DB) >= NumDBs {
			logger.Warn("recovery: skip key with invalid database", "key", rec.Key, "db", rec.DB)
			continue
		}
		obj, err := deserializeObject(rec)
		if err != nil {
			logger.Warn("recovery: skip key", "key", rec.Key, "error", err)
			continue
		}
		e.dbs[rec.DB].getShard(rec.Key).Set(rec.Key, obj)
	}
	for _, entry := range state.WALEntries {
		e.replayEntry(entry)
	}
	return nil
}

// notifyKeyEventWithDB sends a keyspace notification if enabled, with dbID.
func (e *Engine) notifyKeyEventWithDB(dbID int, eventType uint32, event, key string) {
	e.Notifier.Notify(dbID, keyevent.Flags(eventType), event, key)
	if e.onTouchCb != nil {
		e.onTouchCb(key)
	}
}

// NotifyKeyEvent sends a keyspace notification if enabled.
func (e *Engine) NotifyKeyEvent(dbID int, eventType uint32, event, key string) {
	e.notifyKeyEventWithDB(dbID, eventType, event, key)
}

// NotifySubkey sends a subkey notification if enabled.
func (e *Engine) NotifySubkey(dbID int, eventType uint32, event, key string, fields []string) {
	e.Notifier.NotifySubkey(dbID, keyevent.Flags(eventType), event, key, fields)
}

// SetNotifyFlags configures which keyspace notifications are active.
func (e *Engine) SetNotifyFlags(flags string) {
	e.notifyFlags.Store(flags)
	e.Notifier.SetFlags(keyevent.ParseFlags(flags))
}

// GetNotifyFlags returns the current notification flags as a string.
func (e *Engine) GetNotifyFlags() string {
	s, ok := e.notifyFlags.Load().(string)
	if ok && s != "" {
		if allUpper(s) {
			return sortFlags(s)
		}
		return s
	}
	return flagsToString(e.Notifier.GetFlags())
}

func allUpper(s string) bool {
	for _, c := range s {
		if c < 'A' || c > 'Z' {
			return false
		}
	}
	return true
}

func sortFlags(s string) string {
	b := []byte(s)
	for i := 0; i < len(b); i++ {
		for j := i + 1; j < len(b); j++ {
			if b[i] > b[j] {
				b[i], b[j] = b[j], b[i]
			}
		}
	}
	return string(b)
}

func (e *Engine) OnTouch(cb func(key string)) {
	e.onTouchCb = cb
}

func (e *Engine) AddReplica(conn interface{}) {
	e.replMu.Lock()
	defer e.replMu.Unlock()
	c, ok := conn.(net.Conn)
	if !ok {
		return
	}
	e.replicas = append(e.replicas, &replicaInfo{conn: c, lastDB: -1})
}

func flagsToString(f keyevent.Flags) string {
	var s []byte
	if f&keyevent.FlagKeyspace != 0 {
		s = append(s, 'K')
	}
	if f&keyevent.FlagKeyevent != 0 {
		s = append(s, 'E')
	}
	if f&keyevent.FlagGeneric != 0 {
		s = append(s, 'g')
	}
	if f&keyevent.FlagString != 0 {
		s = append(s, '$')
	}
	if f&keyevent.FlagList != 0 {
		s = append(s, 'l')
	}
	if f&keyevent.FlagSet != 0 {
		s = append(s, 's')
	}
	if f&keyevent.FlagHash != 0 {
		s = append(s, 'h')
	}
	if f&keyevent.FlagZSet != 0 {
		s = append(s, 'z')
	}
	if f&keyevent.FlagExpired != 0 {
		s = append(s, 'x')
	}
	if f&keyevent.FlagEvicted != 0 {
		s = append(s, 'e')
	}
	if f&keyevent.FlagStream != 0 {
		s = append(s, 't')
	}
	if f&keyevent.FlagOverwrite != 0 {
		s = append(s, 'o')
	}
	if f&keyevent.FlagTypeChanged != 0 {
		s = append(s, 'c')
	}
	if f&keyevent.FlagNew != 0 {
		s = append(s, 'n')
	}
	if f&keyevent.FlagSubkey != 0 {
		s = append(s, 'S')
	}
	if f&keyevent.FlagSubkeyEvent != 0 {
		s = append(s, 'T')
	}
	if f&keyevent.FlagSubkeyItem != 0 {
		s = append(s, 'I')
	}
	if f&keyevent.FlagSubkeyValue != 0 {
		s = append(s, 'V')
	}
	return string(s)
}

// --- MultiDB interface implementation ---

// SelectDB returns the api.Engine view for the given database index.
// Returns nil if db is out of range.
func (e *Engine) SelectDB(db int) api.Engine {
	if db < 0 || db >= NumDBs {
		return nil
	}
	return e.dbs[db]
}

// NumDBs returns the total number of logical databases.
func (e *Engine) NumDBs() int {
	return NumDBs
}

// MoveKey moves a key from srcDB to dstDB.
// Returns false if the key doesn't exist in src or already exists in dst.
func (e *Engine) MoveKey(key string, srcDB, dstDB int) bool {
	if srcDB < 0 || srcDB >= NumDBs || dstDB < 0 || dstDB >= NumDBs {
		return false
	}
	src := e.dbs[srcDB]
	dst := e.dbs[dstDB]

	obj, ok := src.Get(key)
	if !ok {
		return false
	}
	if _, exists := dst.Get(key); exists {
		return false
	}
	dst.Set(key, obj)
	src.Del(key)
	return true
}

// SwapDB atomically swaps the contents of two databases by exchanging their
// shard arrays. This is safer than swapping DBView pointers because existing
// references (e.g. ctx.Engine in active connections) continue to work correctly.
// After swapping, blocked clients on both databases are woken up so they can
// re-evaluate whether the keys they're waiting on now have data.
func (e *Engine) SwapDB(db1, db2 int) error {
	if db1 < 0 || db1 >= NumDBs || db2 < 0 || db2 >= NumDBs || db1 == db2 {
		return nil
	}
	v1 := e.dbs[db1]
	v2 := e.dbs[db2]
	// Swap the shard arrays under both views' locks.
	// We acquire all 512 shard locks in index order to avoid deadlock.
	for i := 0; i < 256; i++ {
		v1.shards[i].LockForSwap()
		v2.shards[i].LockForSwap()
	}
	for i := 0; i < 256; i++ {
		v1.shards[i].SwapData(v2.shards[i])
	}
	for i := 255; i >= 0; i-- {
		v2.shards[i].UnlockForSwap()
		v1.shards[i].UnlockForSwap()
	}

	// Wake blocked clients on both databases — the swap may have made
	// data available in keys they were waiting on.
	var keys1, keys2 []string
	for i := 0; i < 256; i++ {
		keys1 = append(keys1, v1.shards[i].WaiterKeys()...)
		keys2 = append(keys2, v2.shards[i].WaiterKeys()...)
	}
	v1.FlushSignals(keys1)
	v2.FlushSignals(keys2)

	return nil
}

// FlushDB removes all keys from the given database.
func (e *Engine) FlushDB(db int) {
	if db < 0 || db >= NumDBs {
		return
	}
	v := e.dbs[db]
	for i := 0; i < 256; i++ {
		keys := v.shards[i].Keys()
		for _, k := range keys {
			v.shards[i].Del(k)
		}
	}
	// Reset memory tracking — all keys in this DB are gone.
	e.Evictor.ResetMemory()
}

// DBSize returns the number of keys (including expired) in the given database.
func (e *Engine) DBSize(db int) int {
	if db < 0 || db >= NumDBs {
		return 0
	}
	v := e.dbs[db]
	total := 0
	for i := 0; i < 256; i++ {
		total += v.shards[i].Len()
	}
	return total
}

// PersistenceStatus exposes live WAL durability and I/O health to operational
// surfaces such as INFO. In-memory engines report persistence as disabled.
func (e *Engine) PersistenceStatus() wal.Status {
	if e.wal == nil {
		return wal.Status{FsyncMode: wal.FsyncEverySec, Healthy: true}
	}
	return e.wal.Status()
}

// PersistenceEnabled reports whether this engine currently writes a WAL.
func (e *Engine) PersistenceEnabled() bool {
	return e.wal != nil
}

// WALWriteAdmission reports whether mutating commands may execute under the
// active WAL failure policy. Read-only commands are intentionally unaffected.
func (e *Engine) WALWriteAdmission() (bool, string) {
	policy := e.walFailurePolicy
	if policy == "" {
		policy = WALFailureStopWrites
	}
	if e.wal == nil || policy == WALFailureContinue {
		return true, string(policy)
	}
	return e.wal.Status().Healthy, string(policy)
}
