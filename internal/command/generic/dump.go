package generic

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"hash/crc64"

	"github.com/hdt3213/rdb/encoder"
	"github.com/hdt3213/rdb/model"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// DumpCommand implements DUMP key — serializes a key's value in RDB format.
type DumpCommand struct{}

func (c DumpCommand) Name() string { return "DUMP" }

var crc64Table = crc64.MakeTable(crc64.ECMA)
var streamDumpMagic = []byte("FROSTSTRM1\n")

func (c DumpCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewError("ERR wrong number of arguments for 'dump' command")
	}

	key := string(args[0].Str)
	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.RespNil
	}

	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf)

	// Write a minimal RDB: header + db header + single object + end
	enc.WriteHeader()
	enc.WriteDBHeader(0, 1, 0)

	var err error
	switch obj.Type {
	case object.TypeString:
		val, _ := obj.Value.([]byte)
		err = enc.WriteStringObject(key, val)
	case object.TypeList:
		l := obj.Value.(*object.List)
		values := l.Snapshot()
		err = enc.WriteListObject(key, values)
	case object.TypeSet:
		s := obj.Value.(*object.Set)
		members := s.Members()
		data := make([][]byte, len(members))
		for i, m := range members {
			data[i] = []byte(m)
		}
		err = enc.WriteSetObject(key, data)
	case object.TypeHash:
		h := obj.Value.(*object.Hash)
		hashMap := make(map[string][]byte, h.Len())
		for field, val := range h.GetAll() {
			hashMap[field] = []byte(val)
		}
		err = enc.WriteHashMapObject(key, hashMap)
	case object.TypeZSet:
		z := obj.Value.(*object.ZSet)
		entries := make([]*model.ZSetEntry, 0, z.Len())
		node := z.Skiplist.Head.Level[0].Forward
		for node != nil {
			entries = append(entries, &model.ZSetEntry{
				Score:  node.Score,
				Member: node.Member,
			})
			node = node.Level[0].Forward
		}
		err = enc.WriteZSetObject(key, entries)
	case object.TypeStream:
		stream := obj.Value.(*object.Stream)
		snap := stream.Snapshot()
		js, jerr := json.Marshal(snap)
		if jerr != nil {
			return types.NewError("ERR serialization failed: " + jerr.Error())
		}
		payload := append([]byte{}, streamDumpMagic...)
		payload = append(payload, js...)
		var ver [2]byte
		binary.LittleEndian.PutUint16(ver[:], 10)
		payload = append(payload, ver[:]...)
		checksum := crc64.Checksum(payload, crc64Table)
		var crc [8]byte
		binary.LittleEndian.PutUint64(crc[:], checksum)
		payload = append(payload, crc[:]...)
		return types.Value{Type: types.TypeBulkString, Str: payload}
	case object.TypeArray:
		arr := obj.Value.(*object.Array)
		hashMap := arr.ToRDBHash()
		err = enc.WriteHashMapObject(key, hashMap)
	default:
		return types.NewError("ERR DUMP is not supported for this type")
	}

	if err != nil {
		return types.NewError("ERR serialization failed: " + err.Error())
	}
	enc.WriteEnd()

	// The payload is: [rdb_data][rdb_version:2 LE][crc64:8 LE]
	payload := buf.Bytes()

	// Append RDB version (10) as 2 bytes LE
	var ver [2]byte
	binary.LittleEndian.PutUint16(ver[:], 10)
	payload = append(payload, ver[:]...)

	// Append CRC64 checksum
	checksum := crc64.Checksum(payload, crc64Table)
	var crc [8]byte
	binary.LittleEndian.PutUint64(crc[:], checksum)
	payload = append(payload, crc[:]...)

	return types.Value{Type: types.TypeBulkString, Str: payload}
}
