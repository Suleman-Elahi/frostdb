// Package rdb provides RDB file import and export for FrostDB.
// Import parses a Redis .rdb file and loads all keys into the engine.
// Export dumps the current engine state as a valid .rdb file.
package rdb

import (
	"fmt"
	"io"
	"log"
	"os"
	"strconv"

	"github.com/hdt3213/rdb/parser"

	"frostdb/internal/engine/object"
	"frostdb/internal/engine/shard"
)

// Importer loads an RDB file into FrostDB shards.
type Importer struct {
	shards [256]*shard.Shard
}

// NewImporter creates an importer targeting the given shards.
func NewImporter(shards [256]*shard.Shard) *Importer {
	return &Importer{shards: shards}
}

func (imp *Importer) getShard(key string) *shard.Shard {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return imp.shards[h%256]
}

// ImportFile reads an RDB file from disk and loads all keys.
// Returns the number of keys imported and any error.
func (imp *Importer) ImportFile(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("rdb import: open: %w", err)
	}
	defer f.Close()
	return imp.Import(f)
}

// Import reads an RDB stream and loads all keys into the engine shards.
func (imp *Importer) Import(r io.Reader) (int, error) {
	decoder := parser.NewDecoder(r)
	count := 0

	err := decoder.Parse(func(o parser.RedisObject) bool {
		key := o.GetKey()
		obj := imp.convertObject(o)
		if obj == nil {
			return true // skip unsupported types
		}

		// Handle expiration
		exp := o.GetExpiration()
		if exp != nil {
			obj.ExpireAt = exp.UnixMilli()
		}

		imp.getShard(key).Set(key, obj)
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
		return &object.Object{
			Type:  object.TypeString,
			Value: string(str.Value),
		}

	case parser.ListType:
		list := o.(*parser.ListObject)
		l := object.NewList()
		for _, val := range list.Values {
			l.PushBack(val)
		}
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
		// Check if this hash is a serialized array
		arr := object.FromRDBHash(hash.Hash)
		if arr != nil {
			return &object.Object{
				Type:  object.TypeArray,
				Value: arr,
			}
		}
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

	case parser.StreamType:
		stream := o.(*parser.StreamObject)
		return imp.convertStream(stream)

	default:
		return nil
	}
}

func (imp *Importer) convertStream(so *parser.StreamObject) *object.Object {
	s := object.NewStream()

	// Add all entries from all stream entries (listpack nodes)
	for _, entry := range so.Entries {
		for _, msg := range entry.Msgs {
			if msg.Deleted {
				continue
			}
			fields := make([]object.StreamField, 0, len(msg.Fields))
			for k, v := range msg.Fields {
				fields = append(fields, object.StreamField{Name: k, Value: v})
			}
			idStr := strconv.FormatUint(msg.Id.Ms, 10) + "-" + strconv.FormatUint(msg.Id.Sequence, 10)
			_, err := s.Add(idStr, fields)
			if err != nil {
				log.Printf("rdb import: stream entry %s: %v", idStr, err)
			}
		}
	}

	// Recreate consumer groups
	for _, g := range so.Groups {
		lastIDStr := strconv.FormatUint(g.LastId.Ms, 10) + "-" + strconv.FormatUint(g.LastId.Sequence, 10)
		err := s.CreateGroup(g.Name, lastIDStr)
		if err != nil {
			log.Printf("rdb import: stream group %s: %v", g.Name, err)
			continue
		}
		// Restore pending entries
		group := s.Groups[g.Name]
		for _, pending := range g.Pending {
			idStr := strconv.FormatUint(pending.Id.Ms, 10) + "-" + strconv.FormatUint(pending.Id.Sequence, 10)
			id, perr := object.ParseStreamID(idStr)
			if perr != nil {
				continue
			}
			group.Pending[id] = &object.StreamPendingEntry{
				Entry:          object.StreamEntry{ID: id},
				DeliveryCount:  int(pending.DeliveryCount),
				LastDeliveryMs: int64(pending.DeliveryTime),
			}
		}
		// Map consumers
		for _, consumer := range g.Consumers {
			group.Consumers[consumer.Name] = len(consumer.Pending)
			// Assign consumer name to their pending entries
			for _, pid := range consumer.Pending {
				pidStr := strconv.FormatUint(pid.Ms, 10) + "-" + strconv.FormatUint(pid.Sequence, 10)
				parsedID, perr := object.ParseStreamID(pidStr)
				if perr != nil {
					continue
				}
				if pe, ok := group.Pending[parsedID]; ok {
					pe.Consumer = consumer.Name
				}
			}
		}
	}

	return &object.Object{
		Type:  object.TypeStream,
		Value: s,
	}
}
