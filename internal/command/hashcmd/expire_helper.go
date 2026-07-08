package hashcmd

import (
	"frostdb/internal/engine/keyevent"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
)

// ExpireIfNeeded checks a list of fields (or all fields if nil) and expires them if needed.
// If notifyHexpired is true, it emits hexpired when one or more fields are removed.
// If notifyDel is true, it emits del when the hash becomes empty.
func ExpireIfNeeded(ctx *context.Context, key string, h *object.Hash, fields []string, nowMs int64, notifyHexpired bool, notifyDel bool) int {
	if h.TTLs == nil || len(h.TTLs) == 0 {
		return 0
	}

	if fields == nil {
		fields = h.Keys() // check all fields
	}

	var expiredFields []string
	for _, field := range fields {
		ttl := h.GetTTL(field)
		if ttl > 0 && ttl <= nowMs {
			expiredFields = append(expiredFields, field)
		}
	}

	if len(expiredFields) == 0 {
		return 0
	}

	for _, field := range expiredFields {
		h.Del(field)
	}

	hashBecameEmpty := h.Len() == 0

	// Redis emits `hexpired` for commands that surface lazy field expiry.
	if notifyHexpired {
		ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagHash), "hexpired", key)
		ctx.Engine.NotifySubkey(ctx.DB, uint32(keyevent.FlagHash), "hexpired", key, fields)
	}
	if hashBecameEmpty && notifyDel {
		ctx.Engine.Del(key)
		ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagGeneric), "del", key)
		ctx.Engine.LogCommand([][]byte{[]byte("DEL"), []byte(key)})
	}

	// Log HDEL for the expired fields
	replArgs := make([][]byte, 0, 2+len(expiredFields))
	replArgs = append(replArgs, []byte("HDEL"), []byte(key))
	for _, f := range expiredFields {
		replArgs = append(replArgs, []byte(f))
	}
	ctx.Engine.LogCommand(replArgs)

	return len(expiredFields)
}
