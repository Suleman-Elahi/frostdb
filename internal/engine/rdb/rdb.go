// Package rdb provides RDB file import and export for FrostDB.
// Import parses a Redis .rdb file and loads keys into their source databases.
// Export dumps all logical FrostDB databases as a valid .rdb file.
package rdb

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/hdt3213/rdb/parser"

	"frostdb/internal/engine/object"
	"frostdb/internal/engine/shard"
)

// Importer loads an RDB file into FrostDB shard sets.
type Importer struct {
	databases [][256]*shard.Shard
}

// NewImporter creates an importer targeting a single database. Source database
// indexes are intentionally collapsed to that target for legacy callers.
func NewImporter(shards [256]*shard.Shard) *Importer {
	return NewMultiImporter([][256]*shard.Shard{shards})
}

// NewMultiImporter creates an importer that preserves source RDB database
// indexes. Source databases outside databases are skipped.
func NewMultiImporter(databases [][256]*shard.Shard) *Importer {
	return &Importer{databases: databases}
}

func (imp *Importer) targetDB(source int) (int, bool) {
	if len(imp.databases) == 1 {
		return 0, true
	}
	if source < 0 || source >= len(imp.databases) {
		return 0, false
	}
	return source, true
}

func (imp *Importer) getShard(db int, key string) *shard.Shard {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return imp.databases[db][h%256]
}

// ImportFile reads an RDB file from disk and loads all supported keys.
func (imp *Importer) ImportFile(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("rdb import: open: %w", err)
	}
	defer f.Close()
	return imp.Import(f)
}

// Import reads an RDB stream. A multi-database importer preserves each source
// object's DB index; a legacy single-database importer retains prior behavior.
func (imp *Importer) Import(r io.Reader) (int, error) {
	decoder := parser.NewDecoder(r)
	count := 0

	err := decoder.Parse(func(o parser.RedisObject) bool {
		db, ok := imp.targetDB(o.GetDBIndex())
		if !ok {
			return true
		}
		key := o.GetKey()
		obj := imp.convertObject(o)
		if obj == nil {
			return true
		}
		if expiration := o.GetExpiration(); expiration != nil {
			obj.ExpireAt = expiration.UnixMilli()
		}
		imp.getShard(db, key).Set(key, obj)
		count++
		return true
	})
	if err != nil {
		return count, fmt.Errorf("rdb import: parse: %w", err)
	}
	return count, nil
}

func (imp *Importer) convertObject(o parser.RedisObject) *object.Object {
	switch o.GetType() {
	case parser.StringType:
		str := o.(*parser.StringObject)
		// Defensive copy: the RDB decoder is a vendored third-party module
		// (third_party/rdb) and its buffer-reuse guarantees across decoded
		// objects are not something this migration audited, so don't store
		// str.Value directly.
		return &object.Object{Type: object.TypeString, Value: append([]byte(nil), str.Value...)}
	case parser.ListType:
		list := o.(*parser.ListObject)
		value := object.NewList()
		for _, item := range list.Values {
			value.PushBack(item)
		}
		value.RecomputeEncodingAuto()
		return &object.Object{Type: object.TypeList, Value: value}
	case parser.SetType:
		set := o.(*parser.SetObject)
		value := object.NewSet()
		for _, member := range set.Members {
			value.Add(string(member))
		}
		return &object.Object{Type: object.TypeSet, Value: value}
	case parser.HashType:
		hash := o.(*parser.HashObject)
		if array := object.FromRDBHash(hash.Hash); array != nil {
			return &object.Object{Type: object.TypeArray, Value: array}
		}
		value := object.NewHash()
		for field, item := range hash.Hash {
			value.Set(field, string(item))
		}
		return &object.Object{Type: object.TypeHash, Value: value}
	case parser.ZSetType:
		zset := o.(*parser.ZSetObject)
		value := object.NewZSet()
		for _, entry := range zset.Entries {
			value.Add(entry.Score, entry.Member)
		}
		return &object.Object{Type: object.TypeZSet, Value: value}
	case parser.StreamType:
		return imp.convertStream(o.(*parser.StreamObject))
	default:
		return nil
	}
}

func (imp *Importer) convertStream(source *parser.StreamObject) *object.Object {
	if len(source.Groups) > 0 && strings.HasPrefix(source.Groups[0].Name, streamSnapshotGroupPrefix) {
		raw := strings.TrimPrefix(source.Groups[0].Name, streamSnapshotGroupPrefix)
		var snap object.StreamSnapshot
		if err := json.Unmarshal([]byte(raw), &snap); err == nil {
			return &object.Object{Type: object.TypeStream, Value: object.StreamFromSnapshot(snap)}
		}
	}

	stream := object.NewStream()
	for _, entry := range source.Entries {
		for _, message := range entry.Msgs {
			if message.Deleted {
				continue
			}
			fields := make([]object.StreamField, 0, len(message.Fields))
			for key, value := range message.Fields {
				fields = append(fields, object.StreamField{Name: key, Value: value})
			}
			id := strconv.FormatUint(message.Id.Ms, 10) + "-" + strconv.FormatUint(message.Id.Sequence, 10)
			if _, err := stream.Add(id, fields); err != nil {
				log.Printf("rdb import: stream entry %s: %v", id, err)
			}
		}
	}

	for _, groupSource := range source.Groups {
		lastID := strconv.FormatUint(groupSource.LastId.Ms, 10) + "-" + strconv.FormatUint(groupSource.LastId.Sequence, 10)
		if err := stream.CreateGroup(groupSource.Name, lastID); err != nil {
			log.Printf("rdb import: stream group %s: %v", groupSource.Name, err)
			continue
		}
		group := stream.Groups[groupSource.Name]
		for _, pending := range groupSource.Pending {
			idText := strconv.FormatUint(pending.Id.Ms, 10) + "-" + strconv.FormatUint(pending.Id.Sequence, 10)
			id, err := object.ParseStreamID(idText)
			if err != nil {
				continue
			}
			group.Pending[id] = &object.StreamPendingEntry{
				Entry:          object.StreamEntry{ID: id},
				DeliveryCount:  int(pending.DeliveryCount),
				LastDeliveryMs: int64(pending.DeliveryTime),
			}
		}
		for _, consumer := range groupSource.Consumers {
			group.Consumers[consumer.Name] = len(consumer.Pending)
			for _, pendingID := range consumer.Pending {
				idText := strconv.FormatUint(pendingID.Ms, 10) + "-" + strconv.FormatUint(pendingID.Sequence, 10)
				id, err := object.ParseStreamID(idText)
				if err != nil {
					continue
				}
				if pending, ok := group.Pending[id]; ok {
					pending.Consumer = consumer.Name
				}
			}
		}
	}
	return &object.Object{Type: object.TypeStream, Value: stream}
}
