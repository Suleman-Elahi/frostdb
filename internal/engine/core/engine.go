package core

import (
	"encoding/binary"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

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

// PersistOptions configures persistence for the engine
type PersistOptions struct {
	DataDir            string
	WALSyncOnWrite     bool
	CheckpointInterval time.Duration // 0 = no auto checkpoint
	Eviction           eviction.Config
}

type Engine struct {
	// dbs holds NumDBs logical databases, each with its own shard set.
	dbs [NumDBs]*DBView

	// PubSub (engine-wide, not DB-scoped)
	pubsubMu   sync.RWMutex
	pubsubSubs map[string][]chan api.PubSubMessage

	// Persistence
	wal     *wal.WAL
	dataDir string

	// Eviction
	Evictor *eviction.Evictor

	Notifier *keyevent.Notifier

	notifyFlags atomic.Value // stores the raw notify-keyspace-events string

	onTouchCb func(key string)

	replMu   sync.Mutex
	replicas []*replicaInfo

	dirty               int64 // rdb_changes_since_last_save counter
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
		pubsubSubs: make(map[string][]chan api.PubSubMessage),
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
	e := &Engine{
		pubsubSubs: make(map[string][]chan api.PubSubMessage),
		dataDir:    opts.DataDir,
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

	// Load checkpoint records
	var skipped int
	for _, rec := range state.Records {
		obj, err := deserializeObject(rec)
		if err != nil {
			skipped++
			logger.Warn("recovery: skip key", "key", rec.Key, "error", err)
			continue
		}
		s := e.dbs[0].getShard(rec.Key)
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
		Dir:         filepath.Join(opts.DataDir, "wal"),
		MaxFileSize: 64 * 1024 * 1024,
		SyncOnWrite: opts.WALSyncOnWrite,
	}
	w, err := wal.Open(walOpts)
	if err != nil {
		return nil, err
	}
	e.wal = w

	// Start auto-checkpoint if configured
	if opts.CheckpointInterval > 0 {
		go e.checkpointLoop(opts.CheckpointInterval)
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

type replicaInfo struct {
	conn   net.Conn
	lastDB int
}

// LogCommand writes a raw command to the WAL (called by mutating commands).
// Returns an error if the WAL write fails.
func (e *Engine) LogCommand(db int, args [][]byte) error {
	if e.wal == nil {
		return nil
	}
	bp := walBufPool.Get().(*[]byte)
	buf := (*bp)[:0]

	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(args)))
	for _, arg := range args {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(arg)))
		buf = append(buf, arg...)
	}

	err := e.wal.Append(buf)
	if err != nil {
		logger.Error("wal: append failed", "error", err)
	}
	*bp = buf
	walBufPool.Put(bp)

	// Propagate to attached test replicas
	e.replMu.Lock()
	if len(e.replicas) > 0 {
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
	e.replMu.Unlock()

	return err
}

// PropagateToReplicas sends a RESP-encoded command to all attached replicas
// without WAL logging. Used for commands like SWAPDB, SELECT, and key expiry
// DEL that need to be replicated but aren't normal write commands.
func (e *Engine) PropagateToReplicas(db int, args [][]byte) {
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

	cleanup := func() {
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
	}

	return cleanup
}

func (e *Engine) PubSubPublish(channel, message string) int64 {
	e.pubsubMu.RLock()
	defer e.pubsubMu.RUnlock()

	count := int64(0)
	
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

// Checkpoint creates a full snapshot of all data across all DBs.
func (e *Engine) Checkpoint() error {
	cpDir := filepath.Join(e.dataDir, "checkpoint")
	w, err := checkpoint.NewWriter(cpDir)
	if err != nil {
		return err
	}

	var skipped int
	for dbIdx := 0; dbIdx < NumDBs; dbIdx++ {
		for i := 0; i < 256; i++ {
			e.dbs[dbIdx].shards[i].ForEach(func(key string, obj *object.Object) {
				rec, serErr := serializeObject(key, obj)
				if serErr != nil {
					skipped++
					logger.Warn("checkpoint: skip key", "key", key, "error", serErr)
					return
				}
				w.WriteRecord(rec)
			})
		}
	}
	if skipped > 0 {
		logger.Warn("checkpoint: skipped keys", "count", skipped)
	}

	return w.Close()
}

func (e *Engine) checkpointLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		if err := e.Checkpoint(); err != nil {
			logger.Error("auto-checkpoint failed", "error", err)
		}
	}
}

// Close flushes the WAL
func (e *Engine) Close() error {
	// Stop active expire loop
	if e.expireStopCh != nil {
		close(e.expireStopCh)
		e.expireWg.Wait()
	}
	if e.wal != nil {
		return e.wal.Close()
	}
	return nil
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

			// Expire whole keys first.
			for _, key := range shard.KeysAll() {
				if checked >= maxCheckPerCycle {
					break
				}
				obj, ok := shard.GetRaw(key)
				if !ok || obj == nil {
					continue
				}
				if obj.ExpireAt <= 0 || obj.ExpireAt > nowMs {
					continue
				}
				if shard.Del(key) {
					e.Notifier.Notify(dbIdx, keyevent.FlagExpired, "expired", key)
				}
				checked++
			}
			if checked >= maxCheckPerCycle {
				break
			}

			// Then scan hash-field expirations.
			shard.ForEach(func(key string, obj *object.Object) {
				if checked >= maxCheckPerCycle {
					return
				}
				if obj.Type != object.TypeHash {
					return
				}
				h, ok := obj.Value.(*object.Hash)
				if !ok || h.TTLs == nil || len(h.TTLs) == 0 {
					return
				}

				var expired []string
				for field, ttl := range h.TTLs {
					if ttl > 0 && ttl <= nowMs {
						expired = append(expired, field)
					}
				}
				if len(expired) == 0 {
					return
				}

				for _, field := range expired {
					h.Del(field)
				}
				checked++

				// Send hexpired notification (at most one per hash per cycle).
				e.Notifier.Notify(dbIdx, keyevent.FlagHash, "hexpired", key)

				// If hash is empty, delete it and send del notification
				if h.Len() == 0 {
					e.Del(key)
					e.Notifier.Notify(dbIdx, keyevent.FlagGeneric, "del", key)
				}
			})
		}
	}
}

// replayEntry replays a single WAL entry (re-applies the command to memory)
func (e *Engine) replayEntry(entry wal.Entry) {
	buf := entry.Command
	if len(buf) < 2 {
		return
	}
	nargs := binary.LittleEndian.Uint16(buf[:2])
	buf = buf[2:]

	args := make([][]byte, 0, nargs)
	for i := 0; i < int(nargs); i++ {
		if len(buf) < 4 {
			logger.Warn("recovery: truncated WAL entry")
			return
		}
		length := binary.LittleEndian.Uint32(buf[:4])
		buf = buf[4:]
		if uint32(len(buf)) < length {
			logger.Warn("recovery: truncated WAL entry")
			return
		}
		args = append(args, buf[:length])
		buf = buf[length:]
	}

	if len(args) == 0 {
		return
	}
	applyCommand(e, args)
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
		if s, ok := obj.Value.(string); ok {
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

// SaveRDB exports the live in-memory state of DB 0 to an RDB file.
func (e *Engine) SaveRDB(path string) (int, error) {
	exp := rdb.NewExporter(e.dbs[0].shards)
	return exp.ExportFile(path)
}

// LoadRDB imports keys from an RDB file into DB 0.
func (e *Engine) LoadRDB(path string) (int, error) {
	imp := rdb.NewImporter(e.dbs[0].shards)
	return imp.ImportFile(path)
}

// RewriteAOF performs a synchronous AOF rewrite:
// 1. Creates a checkpoint of all data
// 2. Truncates the WAL
// 3. Cleans up old checkpoint files
// This is equivalent to Redis's BGREWRITEAOF but synchronous.
func (e *Engine) RewriteAOF() error {
	if e.wal == nil {
		return nil // persistence not enabled
	}

	// Step 1: Create checkpoint
	if err := e.Checkpoint(); err != nil {
		return err
	}

	// Step 2: Truncate WAL (entries are now in checkpoint)
	if err := e.wal.Truncate(); err != nil {
		return err
	}

	// Step 3: Clean up old checkpoints
	cpDir := filepath.Join(e.dataDir, "checkpoint")
	checkpoint.CleanupOldCheckpoints(cpDir)

	return nil
}

// RewriteAOFAsync performs an asynchronous AOF rewrite in the background.
func (e *Engine) RewriteAOFAsync() error {
	if e.wal == nil {
		return nil
	}
	go func() {
		atomic.StoreInt32(&e.aofRewriteInProgress, 1)
		defer atomic.StoreInt32(&e.aofRewriteInProgress, 0)
		if err := e.RewriteAOF(); err != nil {
			logger.Error("background AOF rewrite failed", "error", err)
		} else {
			logger.Info("background AOF rewrite completed")
		}
	}()
	return nil
}

// AOFRewriteInProgress returns 1 if a background AOF rewrite is running.
func (e *Engine) AOFRewriteInProgress() int {
	return int(atomic.LoadInt32(&e.aofRewriteInProgress))
}

// LoadAOF reloads state from checkpoint + WAL files (DEBUG LOADAOF).
func (e *Engine) LoadAOF() error {
	if e.dataDir == "" {
		return nil
	}

	// Recover from checkpoint + WAL
	state, err := recovery.Recover(recovery.Options{DataDir: e.dataDir})
	if err != nil {
		return err
	}

	// Flush current DB 0
	for i := 0; i < 256; i++ {
		e.dbs[0].shards[i].Flush()
	}

	// Load checkpoint records
	for _, rec := range state.Records {
		obj, err := deserializeObject(rec)
		if err != nil {
			continue
		}
		s := e.dbs[0].getShard(rec.Key)
		s.Set(rec.Key, obj)
	}

	// Replay WAL entries
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
		v1.shards[i], v2.shards[i] = v2.shards[i], v1.shards[i]
	}
	for i := 255; i >= 0; i-- {
		v2.shards[i].UnlockForSwap()
		v1.shards[i].UnlockForSwap()
	}

	// Wake blocked clients on both databases — the swap may have made
	// data available in keys they were waiting on.
	for i := 0; i < 256; i++ {
		e.dbs[db1].shards[i].SignalAllBlocked()
		e.dbs[db2].shards[i].SignalAllBlocked()
	}

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
