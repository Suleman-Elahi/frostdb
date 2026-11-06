package generic

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"hash/crc64"
	"log"
	"strconv"
	"strings"

	"github.com/hdt3213/rdb/parser"

	"frostdb/internal/engine/keyevent"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// RestoreCommand implements RESTORE key ttl serialized-value [REPLACE].
type RestoreCommand struct{}

func (c RestoreCommand) Name() string { return "RESTORE" }

func (c RestoreCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 3 {
		return types.NewError("ERR wrong number of arguments for 'restore' command")
	}

	key := string(args[0].Str)
	ttlMs, err := strconv.ParseInt(string(args[1].Str), 10, 64)
	if err != nil {
		return types.NewError("ERR value is not an integer or out of range")
	}
	payload := args[2].Str

	replace := false
	absttl := false
	for i := 3; i < len(args); i++ {
		switch strings.ToUpper(string(args[i].Str)) {
		case "REPLACE":
			replace = true
		case "ABSTTL":
			absttl = true
		case "IDLETIME", "FREQ":
			i++ // skip value
		}
	}

	// Check if key exists
	if _, exists := ctx.Engine.Get(key); exists && !replace {
		return types.NewError("BUSYKEY Target key name already exists.")
	}

	// Validate: minimum size is version(2) + crc(8) = 10 bytes of trailer
	if len(payload) < 10 {
		return types.NewError("ERR DUMP payload version or checksum are wrong")
	}

	// Verify CRC64 checksum (last 8 bytes)
	dataLen := len(payload) - 8
	storedCRC := binary.LittleEndian.Uint64(payload[dataLen:])
	computedCRC := crc64.Checksum(payload[:dataLen], crc64Table)
	if storedCRC != computedCRC {
		return types.NewError("ERR DUMP payload version or checksum are wrong")
	}

	// Strip the 2-byte version and 8-byte CRC trailer to get the RDB data
	rdbData := payload[:dataLen-2]
	var obj *object.Object

	if bytes.HasPrefix(rdbData, streamDumpMagic) {
		var snap object.StreamSnapshot
		if err := json.Unmarshal(rdbData[len(streamDumpMagic):], &snap); err != nil {
			return types.NewError("ERR Bad data format")
		}
		obj = &object.Object{
			Type:  object.TypeStream,
			Value: object.StreamFromSnapshot(snap),
		}
	} else {
		// Parse the RDB data using the library
		decoder := parser.NewDecoder(bytes.NewReader(rdbData))
		var parseErr error

		parseErr = decoder.Parse(func(o parser.RedisObject) bool {
			obj = convertParsedObject(o)
			return false // only need the first object
		})

		if parseErr != nil || obj == nil {
			log.Printf("RESTORE failed: parseErr=%v, obj_is_nil=%v, payload_b64=%s, rdb_b64=%s",
				parseErr, obj == nil,
				base64.StdEncoding.EncodeToString(payload),
				base64.StdEncoding.EncodeToString(rdbData))
			return types.NewError("ERR Bad data format")
		}
	}

	// Apply TTL
	if ttlMs > 0 {
		if absttl {
			obj.ExpireAt = ttlMs
		} else {
			obj.ExpireAt = ctx.NowMs() + ttlMs
		}
	}

	if replace {
		oldObj, existed := ctx.Engine.Get(key)
		if existed {
			ctx.Engine.Del(key)
			ctx.Engine.Set(key, obj)
			ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagGeneric), "restore", key)
			ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagOverwrite), "overwritten", key)
			if oldObj.Type != obj.Type {
				ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagTypeChanged), "type_changed", key)
			}
			ctx.SignalKey(key)
			return types.RespOK
		}
		ctx.Engine.Del(key)
	}
	ctx.Engine.Set(key, obj)
	ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagGeneric), "restore", key)
	ctx.SignalKey(key)
	return types.RespOK
}

// convertParsedObject converts a parser.RedisObject to our internal object.
func convertParsedObject(o parser.RedisObject) *object.Object {
	switch o.GetType() {
	case parser.StringType:
		str := o.(*parser.StringObject)
		// Defensive copy — see rdb.go's convertObject for rationale (vendored
		// decoder buffer-reuse guarantees not audited for this migration).
		return &object.Object{
			Type:  object.TypeString,
			Value: append([]byte(nil), str.Value...),
		}
	case parser.ListType:
		list := o.(*parser.ListObject)
		l := object.NewList()
		for _, val := range list.Values {
			l.PushBack(val)
		}
		l.RecomputeEncodingAuto()
		return &object.Object{
			Type:  object.TypeList,
			Value: l,
		}
	case parser.SetType:
		set := o.(*parser.SetObject)
		s := object.NewSet()
		for _, member := range set.Members {
			s.Add(string(member))
		}
		return &object.Object{
			Type:  object.TypeSet,
			Value: s,
		}
	case parser.HashType:
		hash := o.(*parser.HashObject)
		h := object.NewHash()
		for field, val := range hash.Hash {
			h.Set(field, string(val))
		}
		return &object.Object{
			Type:  object.TypeHash,
			Value: h,
		}
	case parser.ZSetType:
		zset := o.(*parser.ZSetObject)
		z := object.NewZSet()
		for _, entry := range zset.Entries {
			z.Add(entry.Score, entry.Member)
		}
		return &object.Object{
			Type:  object.TypeZSet,
			Value: z,
		}
	default:
		return nil
	}
}
