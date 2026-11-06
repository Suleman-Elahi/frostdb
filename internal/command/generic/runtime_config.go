package generic

import (
	"sync/atomic"

	"frostdb/internal/engine/object"
)

var listMaxListpackSize = &object.ListMaxListpackSize
var listCompressDepth = &object.ListCompressDepth
var debugQuicklistPackedThresholdBytes = &object.QuicklistPackedThresholdBytes

var setMaxIntsetEntries atomic.Int64
var setMaxListpackEntries atomic.Int64
var setMaxListpackValue atomic.Int64

var zsetMaxListpackEntries atomic.Int64
var zsetMaxListpackValue atomic.Int64

var hashMaxListpackEntries atomic.Int64
var hashMaxListpackValue atomic.Int64

// AllowAccessExpired controls whether KEYS commands return expired keys.
// This is a debug feature used by the Redis test suite.
var AllowAccessExpired atomic.Bool

// MinReplicasToWrite controls the min-replicas-to-write config.
// Since FrostDB has no replicas, any value > 0 will cause NOREPLICAS errors.
var MinReplicasToWrite atomic.Int64

func init() {
	listMaxListpackSize.Store(-2)
	listCompressDepth.Store(0)
	debugQuicklistPackedThresholdBytes.Store(0)
	
	setMaxIntsetEntries.Store(512)
	setMaxListpackEntries.Store(128)
	setMaxListpackValue.Store(64)
	
	zsetMaxListpackEntries.Store(128)
	zsetMaxListpackValue.Store(64)

	hashMaxListpackEntries.Store(128)
	hashMaxListpackValue.Store(64)
}

