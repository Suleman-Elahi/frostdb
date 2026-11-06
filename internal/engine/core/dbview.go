package core

import (
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"frostdb/internal/engine/api"
	"frostdb/internal/engine/object"
	"frostdb/internal/engine/persistence/wal"
	"frostdb/internal/engine/shard"
	"frostdb/internal/stats"
)

// Compile-time assertion: *DBView must implement api.MultiDB.
// If this line fails to compile, the server's type assertion will silently fail at runtime.
var _ api.MultiDB = (*DBView)(nil)

// NumDBs is the number of logical databases, matching Redis default.
const NumDBs = 16

// DBView is a DB-scoped view of the Engine that implements api.Engine.
// All key operations are routed to the shards of the selected database.
// Non-key operations (WAL, pubsub, eviction, notifications) delegate to the parent.
type DBView struct {
	engine *Engine
	db     int
	shards [256]*shard.Shard
}

// newDBView creates a DBView for the given database index.
func newDBView(e *Engine, db int) *DBView {
	v := &DBView{engine: e, db: db}
	for i := 0; i < 256; i++ {
		v.shards[i] = shard.New()
	}
	return v
}

func (v *DBView) getShard(key string) *shard.Shard {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return v.shards[h%256]
}

func (v *DBView) getShardByBytes(key []byte) *shard.Shard {
	h := uint32(2166136261)
	for _, b := range key {
		h ^= uint32(b)
		h *= 16777619
	}
	return v.shards[h%256]
}

// --- Key operations (DB-scoped) ---

func (v *DBView) Get(key string) (*object.Object, bool) {
	s := v.getShard(key)
	obj, ok := s.Get(key)
	if !ok {
		// Check for passive expiry: key exists but is expired
		raw, rawOk := s.GetRaw(key)
		if rawOk && raw.ExpireAt > 0 && raw.ExpireAt < time.Now().UnixMilli() {
			s.Del(key)
			v.engine.PropagateToReplicas(v.db, [][]byte{[]byte("DEL"), []byte(key)})
			v.NotifyKeyspaceEvent("expired", key)
		}
		stats.Get().KeyspaceMisses.Add(1)
		return nil, false
	}
	v.engine.touchObject(obj)
	stats.Get().KeyspaceHits.Add(1)
	return obj, true
}

func (v *DBView) GetWithTime(key string, nowMs int64) (*object.Object, bool) {
	s := v.getShard(key)
	obj, ok := s.GetWithTime(key, nowMs)
	if !ok {
		// Check for passive expiry: key exists but is expired
		raw, rawOk := s.GetRaw(key)
		if rawOk && raw.ExpireAt > 0 && raw.ExpireAt < nowMs {
			s.Del(key)
			v.engine.PropagateToReplicas(v.db, [][]byte{[]byte("DEL"), []byte(key)})
			v.NotifyKeyspaceEvent("expired", key)
		}
		stats.Get().KeyspaceMisses.Add(1)
		return nil, false
	}
	v.engine.touchObject(obj)
	stats.Get().KeyspaceHits.Add(1)
	return obj, true
}

func (v *DBView) GetRaw(key string) (*object.Object, bool) {
	s := v.getShard(key)
	obj, ok := s.GetRaw(key)
	if !ok {
		stats.Get().KeyspaceMisses.Add(1)
		return nil, false
	}
	v.engine.touchObject(obj)
	stats.Get().KeyspaceHits.Add(1)
	return obj, true
}

// GetCheckExpired returns the object only if it exists and is not expired,
// without triggering passive expiry (no key deletion, no replication).
// Used by blocking commands (BLPOP etc.) to check key availability without
// side effects on the replication stream.
func (v *DBView) GetCheckExpired(key string) (*object.Object, bool) {
	s := v.getShard(key)
	obj, ok := s.GetCheckExpired(key)
	if !ok {
		return nil, false
	}
	return obj, true
}

func (v *DBView) GetBytes(key []byte) (*object.Object, bool) {
	s := v.getShardByBytes(key)
	obj, ok := s.GetBytes(key)
	if !ok {
		stats.Get().KeyspaceMisses.Add(1)
		return nil, false
	}
	v.engine.touchObject(obj)
	stats.Get().KeyspaceHits.Add(1)
	return obj, true
}

func (v *DBView) GetBytesWithTime(key []byte, nowMs int64) (*object.Object, bool) {
	s := v.getShardByBytes(key)
	obj, ok := s.GetBytesWithTime(key, nowMs)
	if !ok {
		// Check for passive expiry: key exists but is expired
		raw, rawOk := s.GetRaw(string(key))
		if rawOk && raw.ExpireAt > 0 && raw.ExpireAt < nowMs {
			s.Del(string(key))
			v.engine.PropagateToReplicas(v.db, [][]byte{[]byte("DEL"), key})
			v.NotifyKeyspaceEvent("expired", string(key))
		}
		stats.Get().KeyspaceMisses.Add(1)
		return nil, false
	}
	v.engine.touchObject(obj)
	stats.Get().KeyspaceHits.Add(1)
	return obj, true
}

func (v *DBView) Set(key string, obj *object.Object) {
	s := v.getShard(key)
	v.engine.initObjectAccess(obj)
	s.Set(key, obj)
	v.engine.Evictor.TrackAlloc(estimateSize(obj))
	atomic.AddInt64(&v.engine.dirty, 1)
	if v.engine.onTouchCb != nil {
		v.engine.onTouchCb(key)
	}
}

func (v *DBView) SetBytes(key []byte, obj *object.Object) {
	s := v.getShardByBytes(key)
	v.engine.initObjectAccess(obj)
	s.Set(string(key), obj)
	v.engine.Evictor.TrackAlloc(estimateSize(obj))
	atomic.AddInt64(&v.engine.dirty, 1)
	if v.engine.onTouchCb != nil {
		v.engine.onTouchCb(string(key))
	}
}

func (v *DBView) Del(key string) bool {
	s := v.getShard(key)
	deleted := s.Del(key)
	if deleted {
		atomic.AddInt64(&v.engine.dirty, 1)
		if v.engine.onTouchCb != nil {
			v.engine.onTouchCb(key)
		}
	}
	return deleted
}

func (v *DBView) DelBytes(key []byte) bool {
	s := v.getShardByBytes(key)
	deleted := s.Del(string(key))
	if deleted {
		atomic.AddInt64(&v.engine.dirty, 1)
		if v.engine.onTouchCb != nil {
			v.engine.onTouchCb(string(key))
		}
	}
	return deleted
}

func (v *DBView) Keys(pattern string) []string {
	var result []string
	for i := 0; i < 256; i++ {
		keys := v.shards[i].Keys()
		for _, k := range keys {
			if pattern == "*" {
				result = append(result, k)
			} else {
				matched, _ := filepath.Match(pattern, k)
				if matched {
					result = append(result, k)
				}
			}
		}
	}
	return result
}

// KeysAll returns all keys matching the pattern, including expired ones.
func (v *DBView) KeysAll(pattern string) []string {
	var result []string
	for i := 0; i < 256; i++ {
		keys := v.shards[i].KeysAll()
		for _, k := range keys {
			if pattern == "*" {
				result = append(result, k)
			} else {
				matched, _ := filepath.Match(pattern, k)
				if matched {
					result = append(result, k)
				}
			}
		}
	}
	return result
}

func (v *DBView) Subscribe(keys []string) (<-chan struct{}, func()) {
	w, cleanup := v.SubscribeWaiter(keys)
	ch := make(chan struct{}, 1)
	go func() {
		for {
			select {
			case ackCh := <-w.WakeCh:
				select {
				case ch <- struct{}{}:
				default:
				}
				ackCh <- shard.SignalAck{Consumed: true}
			case <-w.DoneCh:
				return
			}
		}
	}()
	return ch, cleanup
}

func (v *DBView) SubscribeWaiter(keys []string) (*shard.Waiter, func()) {
	w := shard.NewWaiter()
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		s := v.getShard(key)
		s.SubscribeWaiter(key, w)
	}
	var unsubOnce sync.Once
	cleanup := func() {
		unsubOnce.Do(func() {
			w.CloseDone()
			for key := range seen {
				s := v.getShard(key)
				s.UnsubscribeWaiter(key, w)
			}
		})
	}
	return w, cleanup
}

func (v *DBView) FlushSignals(initialKeys []string) {
	if len(initialKeys) == 0 {
		return
	}
	queue := make([]string, len(initialKeys))
	copy(queue, initialKeys)

	for len(queue) > 0 {
		key := queue[0]
		queue = queue[1:]

		if v.engine.onTouchCb != nil {
			v.engine.onTouchCb(key)
		}

		s := v.getShard(key)
		ws := s.WaitersSnapshot(key)
		for _, w := range ws {
			ackCh := make(chan shard.SignalAck, 1)
			select {
			case w.WakeCh <- ackCh:
				var ack shard.SignalAck
				var gotAck bool
				select {
				case ack = <-ackCh:
					gotAck = true
				case <-w.DoneCh:
					select {
					case ack = <-ackCh:
						gotAck = true
					default:
					}
				}
				if gotAck {
					if len(ack.Cascaded) > 0 {
						queue = append(queue, ack.Cascaded...)
					}
					if !ack.Consumed || !s.HasKey(key) {
						goto nextKey
					}
				}
			case <-w.DoneCh:
			}
		}
	nextKey:
	}
}

func (v *DBView) SignalKey(key string) {
	v.FlushSignals([]string{key})
}

// Shards returns the shard array for this DB (used by RDB export/import, checkpoint).
func (v *DBView) Shards() [256]*shard.Shard {
	return v.shards
}

// --- Delegated operations (engine-wide, not DB-scoped) ---

func (v *DBView) LogCommand(args [][]byte) error {
	return v.engine.LogCommand(v.db, args)
}

func (v *DBView) PubSubSubscribe(channel string, ch chan api.PubSubMessage) func() {
	return v.engine.PubSubSubscribe(channel, ch)
}

func (v *DBView) PubSubPublish(channel, message string) int64 {
	return v.engine.PubSubPublish(channel, message)
}

func (v *DBView) PubSubNumSub(channels []string) []int64 {
	return v.engine.PubSubNumSub(channels)
}

func (v *DBView) PubSubNumPat() int64 {
	return v.engine.PubSubNumPat()
}

func (v *DBView) CheckPubSubOOM(argBytes int64) bool {
	return v.engine.CheckPubSubOOM(argBytes)
}

func (v *DBView) PubSubChannels(pattern string, shard bool) []string {
	return v.engine.PubSubChannels(pattern, shard)
}

func (v *DBView) NotifyKeyspaceEvent(event, key string) {
	v.engine.notifyKeyspaceEventWithDB(v.db, event, key)
}

func (v *DBView) TryEvict() bool {
	return v.engine.TryEvict()
}

func (v *DBView) GetEvictionConfig() api.EvictionConfig {
	return v.engine.GetEvictionConfig()
}

func (v *DBView) SetEvictionConfig(cfg api.EvictionConfig) {
	v.engine.SetEvictionConfig(cfg)
}

func (v *DBView) SaveRDB(path string) (int, error) {
	return v.engine.SaveRDB(path)
}

func (v *DBView) LoadRDB(path string) (int, error) {
	return v.engine.LoadRDB(path)
}

func (v *DBView) RewriteAOF() error {
	return v.engine.RewriteAOF()
}

func (v *DBView) RewriteAOFAsync() error {
	return v.engine.RewriteAOFAsync()
}

func (v *DBView) AOFRewriteInProgress() int {
	return v.engine.AOFRewriteInProgress()
}

func (v *DBView) LoadAOF() error {
	return v.engine.LoadAOF()
}

func (v *DBView) NotifyKeyEvent(dbID int, eventType uint32, event, key string) {
	_ = dbID // Use the view's own db
	v.engine.notifyKeyEventWithDB(v.db, eventType, event, key)
}

func (v *DBView) NotifySubkey(dbID int, eventType uint32, event, key string, fields []string) {
	_ = dbID // Use the view's own db
	v.engine.NotifySubkey(v.db, eventType, event, key, fields)
}

func (v *DBView) SetNotifyFlags(flags string) {
	v.engine.SetNotifyFlags(flags)
}

func (v *DBView) GetNotifyFlags() string {
	return v.engine.GetNotifyFlags()
}

func (v *DBView) OnTouch(cb func(key string)) {
	v.engine.OnTouch(cb)
}

func (v *DBView) AddReplica(conn interface{}) {
	v.engine.AddReplica(conn)
}

func (v *DBView) PropagateToReplicas(args [][]byte) {
	v.engine.PropagateToReplicas(v.db, args)
}

func (v *DBView) GetDirty() int64 {
	return v.engine.GetDirty()
}

func (v *DBView) AddDirty(n int64) {
	v.engine.AddDirty(n)
}

func (v *DBView) ResetDirty() {
	v.engine.ResetDirty()
}

// ParentEngine returns the underlying Engine (used for admin operations like RDB import/export).
func (v *DBView) ParentEngine() *Engine {
	return v.engine
}

// --- api.MultiDB implementation on DBView ---
// DBView is the public-facing type returned by New/NewWithPersistence.
// It must implement MultiDB so the server can use SELECT, MOVE, SWAPDB, etc.

// SelectDB returns the Engine view for the given database index.
func (v *DBView) SelectDB(db int) api.Engine {
	return v.engine.SelectDB(db)
}

// NumDBs returns the total number of logical databases.
func (v *DBView) NumDBs() int {
	return NumDBs
}

// MoveKey moves a key from srcDB to dstDB.
func (v *DBView) MoveKey(key string, srcDB, dstDB int) bool {
	return v.engine.MoveKey(key, srcDB, dstDB)
}

// SwapDB swaps the contents of two databases atomically.
func (v *DBView) SwapDB(db1, db2 int) error {
	return v.engine.SwapDB(db1, db2)
}

// FlushDB removes all keys from the given database.
func (v *DBView) FlushDB(db int) {
	v.engine.FlushDB(db)
}

// DBSize returns the number of keys in the given database.
func (v *DBView) DBSize(db int) int {
	return v.engine.DBSize(db)
}

// PersistenceStatus exposes the parent engine's WAL health without expanding
// the command-facing Engine interface used by lightweight test engines.
func (v *DBView) PersistenceStatus() wal.Status {
	return v.engine.PersistenceStatus()
}

// PersistenceEnabled reports whether the parent engine has an active WAL.
func (v *DBView) PersistenceEnabled() bool {
	return v.engine.PersistenceEnabled()
}

// WALWriteAdmission delegates the active WAL write policy and admission state.
func (v *DBView) WALWriteAdmission() (bool, string) {
	return v.engine.WALWriteAdmission()
}

// CheckpointStatus exposes the parent engine's latest checkpoint outcome.
func (v *DBView) CheckpointStatus() CheckpointStatus {
	return v.engine.CheckpointStatus()
}
