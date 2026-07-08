// Package keyevent implements Redis-compatible keyspace notifications.
//
// Two types of notifications:
//   - Keyspace: __keyspace@<db>__:<key>  → event name (e.g. "set", "del")
//   - Keyevent: __keyevent@<db>__:<event> → key name
//
// Notifications are delivered via the existing PubSub system.
package keyevent

import (
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	"frostdb/internal/engine/api"
)

// ActiveExpireEnabled controls whether the active hash-field expiry loop runs.
// Toggled by DEBUG SET-ACTIVE-EXPIRE. Defaults to true (enabled) like Redis.
var ActiveExpireEnabled atomic.Bool

func init() {
	ActiveExpireEnabled.Store(true)
}

// Flags control which notifications are enabled (matches Redis notify-keyspace-events).
type Flags uint32

const (
	FlagKeyspace      Flags = 1 << iota // K — keyspace notifications
	FlagKeyevent                        // E — keyevent notifications
	FlagGeneric                         // g — generic commands (DEL, EXPIRE, RENAME, etc.)
	FlagString                          // $ — string commands
	FlagList                            // l — list commands
	FlagSet                             // s — set commands
	FlagHash                            // h — hash commands
	FlagZSet                            // z — sorted set commands
	FlagExpired                         // x — expired events
	FlagEvicted                         // e — evicted events
	FlagStream                          // t — stream commands
	FlagOverwrite                       // o — overwritten events
	FlagTypeChanged                     // c — type_changed events
	FlagNew                             // n — new key events
	FlagSubkey                          // S — subkey notifications
	FlagSubkeyEvent                     // T — subkeyevent notifications
	FlagSubkeyItem                      // I — subkeyspaceitem notifications
	FlagSubkeyValue                     // V — subkeyspaceevent notifications
	FlagAll = FlagGeneric | FlagString | FlagList | FlagSet |
		FlagHash | FlagZSet | FlagExpired | FlagEvicted | FlagStream
)

// ParseFlags converts a Redis notify-keyspace-events string to Flags.
func ParseFlags(s string) Flags {
	var f Flags
	for _, c := range s {
		switch c {
		case 'K':
			f |= FlagKeyspace
		case 'E':
			f |= FlagKeyevent
		case 'g':
			f |= FlagGeneric
		case '$':
			f |= FlagString
		case 'l':
			f |= FlagList
		case 's':
			f |= FlagSet
		case 'h':
			f |= FlagHash
		case 'z':
			f |= FlagZSet
		case 'x':
			f |= FlagExpired
		case 'e':
			f |= FlagEvicted
		case 't':
			f |= FlagStream
		case 'o':
			f |= FlagOverwrite
		case 'c':
			f |= FlagTypeChanged
		case 'n':
			f |= FlagNew
		case 'S':
			f |= FlagSubkey
		case 'T':
			f |= FlagSubkeyEvent
		case 'I':
			f |= FlagSubkeyItem
		case 'V':
			f |= FlagSubkeyValue
		case 'A':
			f |= FlagAll
		}
	}
	return f
}

// Notifier publishes keyspace/keyevent notifications via PubSub.
type Notifier struct {
	flags  atomic.Uint32
	engine api.Engine
	raw    string
}

// NewNotifier creates a notifier attached to the engine's PubSub system.
func NewNotifier(engine api.Engine) *Notifier {
	return &Notifier{engine: engine}
}

// SetFlags updates which notifications are active.
func (n *Notifier) SetFlags(f Flags) {
	n.flags.Store(uint32(f))
}

// GetFlags returns the current notification flags.
func (n *Notifier) GetFlags() Flags {
	return Flags(n.flags.Load())
}

// Enabled returns true if notifications are configured at all.
func (n *Notifier) Enabled() bool {
	f := n.GetFlags()
	return f&(FlagKeyspace|FlagKeyevent) != 0
}

// Notify sends a keyspace notification if the relevant flags are enabled.
// eventType is the flag category (FlagString, FlagGeneric, etc.)
// event is the event name ("set", "del", "expire", etc.)
// key is the affected key.
func (n *Notifier) Notify(dbID int, eventType Flags, event, key string) {
	f := n.GetFlags()
	if f == 0 {
		return
	}

	// Must have at least K or E, plus the event type flag
	if f&eventType == 0 {
		return
	}

	// __keyspace@<db>__:<key> → event
	if f&FlagKeyspace != 0 {
		channel := fmt.Sprintf("__keyspace@%d__:%s", dbID, key)
		n.engine.PubSubPublish(channel, event)
	}

	// __keyevent@<db>__:<event> → key
	if f&FlagKeyevent != 0 {
		channel := fmt.Sprintf("__keyevent@%d__:%s", dbID, event)
		n.engine.PubSubPublish(channel, key)
	}
}

// NotifyExpired is a convenience for expired key events.
func (n *Notifier) NotifyExpired(dbID int, key string) {
	n.Notify(dbID, FlagExpired, "expired", key)
}

// NotifyEvicted is a convenience for evicted key events.
func (n *Notifier) NotifyEvicted(dbID int, key string) {
	n.Notify(dbID, FlagEvicted, "evicted", key)
}

// NotifySubkey sends a subkey notification if the relevant flags are enabled.
// eventType is the flag category (FlagHash, etc.)
// event is the event name ("hset", "hdel", etc.)
// key is the affected key.
// fields lists the subkey field names affected.
func (n *Notifier) NotifySubkey(dbID int, eventType Flags, event, key string, fields []string) {
	f := n.GetFlags()
	if f == 0 {
		return
	}
	if f&eventType == 0 {
		return
	}

	// Build field payload: "len:field,len:field,..."
	payload := event
	if len(fields) > 0 {
		var sb strings.Builder
		sb.WriteString(event)
		sb.WriteByte('|')
		for i, field := range fields {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(strconv.Itoa(len(field)))
			sb.WriteByte(':')
			sb.WriteString(field)
		}
		payload = sb.String()
	}

	// S: __subkeyspace@<db>__:<key> → "event|len:field,..."
	if f&FlagSubkey != 0 {
		channel := fmt.Sprintf("__subkeyspace@%d__:%s", dbID, key)
		n.engine.PubSubPublish(channel, payload)
	}

	// T: __subkeyevent@<db>__:<event> → "len:key|len:field,..."
	if f&FlagSubkeyEvent != 0 {
		channel := fmt.Sprintf("__subkeyevent@%d__:%s", dbID, event)
		msg := fmt.Sprintf("%d:%s|%s", len(key), key, payload[len(event)+1:])
		n.engine.PubSubPublish(channel, msg)
	}

	// V: __subkeyspaceevent@<db>__:<event>|<key> → "len:field,..."
	if f&FlagSubkeyValue != 0 {
		channel := fmt.Sprintf("__subkeyspaceevent@%d__:%s|%s", dbID, event, key)
		n.engine.PubSubPublish(channel, payload[len(event)+1:])
	}

	// I: __subkeyspaceitem@<db>__:<key>\n<field> → event (one per field)
	if f&FlagSubkeyItem != 0 {
		for _, field := range fields {
			// Skip keys that contain newlines (as Redis does)
			if strings.ContainsRune(key, '\n') {
				continue
			}
			channel := fmt.Sprintf("__subkeyspaceitem@%d__:%s\n%s", dbID, key, field)
			n.engine.PubSubPublish(channel, event)
		}
	}
}
