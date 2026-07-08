// Package stats provides server-wide statistics tracking.
// All counters are atomic and safe for concurrent access.
package stats

import (
	"sync"
	"sync/atomic"
	"time"
)

// Stats holds server-wide statistics.
// Use the global instance via Get().
type Stats struct {
	// Server start time
	startTime time.Time

	// Connection stats
	TotalConnectionsReceived atomic.Uint64
	ConnectedClients         atomic.Int64

	// Command stats
	TotalCommandsProcessed atomic.Uint64

	// Network stats
	TotalNetInputBytes  atomic.Uint64
	TotalNetOutputBytes atomic.Uint64

	// Key stats
	KeyspaceHits   atomic.Uint64
	KeyspaceMisses atomic.Uint64
	ExpiredKeys    atomic.Uint64
	EvictedKeys    atomic.Uint64

	// Error stats
	RejectedConnections atomic.Uint64
	TotalErrorReplies   atomic.Uint64

	// Per-command stats
	cmdStatsMu sync.RWMutex
	cmdStats   map[string]*CommandStat

	// Per-error-type stats (e.g., "ERR", "OOM", "WRONGTYPE")
	errStatsMu sync.RWMutex
	errStats   map[string]*atomic.Uint64
}

// CommandStat tracks statistics for a single command.
type CommandStat struct {
	Calls          atomic.Uint64
	Usec           atomic.Uint64 // total microseconds spent
	RejectedCalls  atomic.Uint64
	FailedCalls    atomic.Uint64
}

var (
	globalStats *Stats
	once        sync.Once
)

// Init initializes the global stats instance. Call once at server startup.
func Init() {
	once.Do(func() {
		globalStats = &Stats{
			startTime: time.Now(),
			cmdStats:  make(map[string]*CommandStat),
			errStats:  make(map[string]*atomic.Uint64),
		}
	})
}

// Get returns the global stats instance.
func Get() *Stats {
	if globalStats == nil {
		Init()
	}
	return globalStats
}

// StartTime returns when the server started.
func (s *Stats) StartTime() time.Time {
	return s.startTime
}

// UptimeSeconds returns seconds since server start.
func (s *Stats) UptimeSeconds() int64 {
	return int64(time.Since(s.startTime).Seconds())
}

// UptimeDays returns days since server start.
func (s *Stats) UptimeDays() int64 {
	return int64(time.Since(s.startTime).Hours() / 24)
}

// RecordCommand records a command execution with timing.
func (s *Stats) RecordCommand(name string, usec uint64, failed bool) {
	s.TotalCommandsProcessed.Add(1)

	s.cmdStatsMu.RLock()
	stat, ok := s.cmdStats[name]
	s.cmdStatsMu.RUnlock()

	if !ok {
		s.cmdStatsMu.Lock()
		// Double-check after acquiring write lock
		if stat, ok = s.cmdStats[name]; !ok {
			stat = &CommandStat{}
			s.cmdStats[name] = stat
		}
		s.cmdStatsMu.Unlock()
	}

	stat.Calls.Add(1)
	stat.Usec.Add(usec)
	if failed {
		stat.FailedCalls.Add(1)
	}
}

// RecordRejectedCommand records a command that was rejected before execution.
func (s *Stats) RecordRejectedCommand(name string) {
	s.cmdStatsMu.RLock()
	stat, ok := s.cmdStats[name]
	s.cmdStatsMu.RUnlock()

	if !ok {
		s.cmdStatsMu.Lock()
		if stat, ok = s.cmdStats[name]; !ok {
			stat = &CommandStat{}
			s.cmdStats[name] = stat
		}
		s.cmdStatsMu.Unlock()
	}

	stat.RejectedCalls.Add(1)
}

// GetCommandStats returns a snapshot of per-command statistics.
func (s *Stats) GetCommandStats() map[string]CommandStatSnapshot {
	s.cmdStatsMu.RLock()
	defer s.cmdStatsMu.RUnlock()

	result := make(map[string]CommandStatSnapshot, len(s.cmdStats))
	for name, stat := range s.cmdStats {
		calls := stat.Calls.Load()
		usec := stat.Usec.Load()
		usecPerCall := float64(0)
		if calls > 0 {
			usecPerCall = float64(usec) / float64(calls)
		}
		result[name] = CommandStatSnapshot{
			Calls:         calls,
			Usec:          usec,
			UsecPerCall:   usecPerCall,
			RejectedCalls: stat.RejectedCalls.Load(),
			FailedCalls:   stat.FailedCalls.Load(),
		}
	}
	return result
}

// CommandStatSnapshot is an immutable snapshot of command stats.
type CommandStatSnapshot struct {
	Calls         uint64
	Usec          uint64
	UsecPerCall   float64
	RejectedCalls uint64
	FailedCalls   uint64
}

// Reset resets all statistics (for CONFIG RESETSTAT).
func (s *Stats) Reset() {
	s.TotalConnectionsReceived.Store(0)
	s.TotalCommandsProcessed.Store(0)
	s.TotalNetInputBytes.Store(0)
	s.TotalNetOutputBytes.Store(0)
	s.KeyspaceHits.Store(0)
	s.KeyspaceMisses.Store(0)
	s.ExpiredKeys.Store(0)
	s.EvictedKeys.Store(0)
	s.RejectedConnections.Store(0)
	s.TotalErrorReplies.Store(0)

	s.cmdStatsMu.Lock()
	s.cmdStats = make(map[string]*CommandStat)
	s.cmdStatsMu.Unlock()

	s.errStatsMu.Lock()
	s.errStats = make(map[string]*atomic.Uint64)
	s.errStatsMu.Unlock()
}

// RecordErrorType increments the counter for a specific error type prefix.
// The errPrefix should be the first word of the error (e.g., "ERR", "OOM", "WRONGTYPE").
func (s *Stats) RecordErrorType(errPrefix string) {
	s.errStatsMu.RLock()
	counter, ok := s.errStats[errPrefix]
	s.errStatsMu.RUnlock()

	if !ok {
		s.errStatsMu.Lock()
		if counter, ok = s.errStats[errPrefix]; !ok {
			counter = &atomic.Uint64{}
			s.errStats[errPrefix] = counter
		}
		s.errStatsMu.Unlock()
	}

	counter.Add(1)
}

// GetErrorStats returns a snapshot of per-error-type counts.
func (s *Stats) GetErrorStats() map[string]uint64 {
	s.errStatsMu.RLock()
	defer s.errStatsMu.RUnlock()

	result := make(map[string]uint64, len(s.errStats))
	for prefix, counter := range s.errStats {
		result[prefix] = counter.Load()
	}
	return result
}
