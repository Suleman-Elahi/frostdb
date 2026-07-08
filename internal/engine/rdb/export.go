package rdb

import (
	"fmt"
	"io"
	"os"

	"github.com/hdt3213/rdb/encoder"
	"github.com/hdt3213/rdb/model"

	"frostdb/internal/engine/object"
	"frostdb/internal/engine/shard"
)

// Exporter dumps FrostDB state as an RDB file.
type Exporter struct {
	shards [256]*shard.Shard
	writer io.Writer
}

// NewExporter creates an exporter from the given shards.
func NewExporter(shards [256]*shard.Shard) *Exporter {
	return &Exporter{shards: shards}
}

// NewExporterToWriter creates an exporter that writes to the given writer.
func NewExporterToWriter(shards [256]*shard.Shard, w io.Writer) *Exporter {
	return &Exporter{shards: shards, writer: w}
}

// ExportFile writes the current state to an RDB file at the given path.
// Returns the number of keys exported.
func (exp *Exporter) ExportFile(path string) (int, error) {
	f, err := os.Create(path)
	if err != nil {
		return 0, fmt.Errorf("rdb export: create: %w", err)
	}
	defer f.Close()
	exp.writer = f
	return exp.Export()
}

// Export writes RDB data to the configured writer.
// Returns the number of keys exported.
func (exp *Exporter) Export() (int, error) {
	if exp.writer == nil {
		return 0, fmt.Errorf("rdb export: no writer configured")
	}

	enc := encoder.NewEncoder(exp.writer)
	if err := enc.WriteHeader(); err != nil {
		return 0, fmt.Errorf("rdb export: header: %w", err)
	}

	// Write aux fields
	enc.WriteAux("redis-ver", "7.0.0")
	enc.WriteAux("redis-bits", "64")
	enc.WriteAux("aof-preamble", "0")

	// Count total keys for DB header
	totalKeys := 0
	totalExpires := 0
	for i := 0; i < 256; i++ {
		exp.shards[i].ForEach(func(key string, obj *object.Object) {
			totalKeys++
			if obj.ExpireAt > 0 {
				totalExpires++
			}
		})
	}

	// Only write DB header if there are keys
	if totalKeys > 0 {
		if err := enc.WriteDBHeader(0, uint64(totalKeys), uint64(totalExpires)); err != nil {
			return 0, fmt.Errorf("rdb export: db header: %w", err)
		}
	}

	count := 0
	var writeErr error

	for i := 0; i < 256; i++ {
		exp.shards[i].ForEach(func(key string, obj *object.Object) {
			if writeErr != nil {
				return
			}
			writeErr = exp.writeObject(enc, key, obj)
			if writeErr == nil {
				count++
			}
		})
		if writeErr != nil {
			return count, writeErr
		}
	}

	if err := enc.WriteEnd(); err != nil {
		return count, fmt.Errorf("rdb export: end: %w", err)
	}
	return count, nil
}

func (exp *Exporter) writeObject(enc *encoder.Encoder, key string, obj *object.Object) error {
	opts := exp.ttlOpts(obj)

	switch obj.Type {
	case object.TypeString:
		val, ok := obj.Value.(string)
		if !ok {
			return nil
		}
		return enc.WriteStringObject(key, []byte(val), opts...)

	case object.TypeList:
		l, ok := obj.Value.(*object.List)
		if !ok {
			return nil
		}
		values := l.Snapshot()
		return enc.WriteListObject(key, values, opts...)

	case object.TypeSet:
		s, ok := obj.Value.(*object.Set)
		if !ok {
			return nil
		}
		members := s.Members()
		data := make([][]byte, len(members))
		for i, m := range members {
			data[i] = []byte(m)
		}
		return enc.WriteSetObject(key, data, opts...)

	case object.TypeHash:
		h, ok := obj.Value.(*object.Hash)
		if !ok {
			return nil
		}
		hashMap := make(map[string][]byte, h.Len())
		for field, val := range h.GetAll() {
			hashMap[field] = []byte(val)
		}
		return enc.WriteHashMapObject(key, hashMap, opts...)

	case object.TypeZSet:
		z, ok := obj.Value.(*object.ZSet)
		if !ok {
			return nil
		}
		entries := make([]*model.ZSetEntry, 0, z.Len())
		node := z.Skiplist.Head.Level[0].Forward
		for node != nil {
			entries = append(entries, &model.ZSetEntry{
				Score:  node.Score,
				Member: node.Member,
			})
			node = node.Level[0].Forward
		}
		return enc.WriteZSetObject(key, entries, opts...)

	case object.TypeStream:
		s, ok := obj.Value.(*object.Stream)
		if !ok {
			return nil
		}
		return exp.writeStream(enc, key, s, opts...)

	case object.TypeArray:
		arr, ok := obj.Value.(*object.Array)
		if !ok {
			return nil
		}
		hashMap := arr.ToRDBHash()
		return enc.WriteHashMapObject(key, hashMap, opts...)

	default:
		return nil
	}
}

func (exp *Exporter) ttlOpts(obj *object.Object) []interface{} {
	if obj.ExpireAt > 0 {
		return []interface{}{encoder.WithTTL(uint64(obj.ExpireAt))}
	}
	return nil
}

func (exp *Exporter) writeStream(enc *encoder.Encoder, key string, s *object.Stream, opts ...interface{}) error {
	// Build a model.StreamObject for the encoder
	so := &model.StreamObject{}

	// Collect all entries
	var entries []*model.StreamEntry
	s.Blocks.Ascend(func(b object.StreamBlock) bool {
		it := b.Node.Iterator()
		streamEntry := &model.StreamEntry{
			FirstMsgId: &model.StreamId{Ms: uint64(b.FirstID.MS), Sequence: uint64(b.FirstID.Seq)},
		}
		for it.HasNext() {
			entry := it.Next()
			msg := &model.StreamMessage{
				Id:     &model.StreamId{Ms: uint64(entry.ID.MS), Sequence: uint64(entry.ID.Seq)},
				Fields: make(map[string]string, len(entry.Fields)),
			}
			for _, f := range entry.Fields {
				msg.Fields[f.Name] = f.Value
			}
			streamEntry.Msgs = append(streamEntry.Msgs, msg)
		}
		entries = append(entries, streamEntry)
		return true
	})
	so.Entries = entries
	so.Length = uint64(s.Len())

	// Last ID
	if s.Blocks.Len() > 0 {
		lastBlock, _ := s.Blocks.Max()
		it := lastBlock.Node.Iterator()
		var lastEntry object.StreamEntry
		for it.HasNext() {
			lastEntry = it.Next()
		}
		so.LastId = &model.StreamId{Ms: uint64(lastEntry.ID.MS), Sequence: uint64(lastEntry.ID.Seq)}
		// FirstId
		firstBlock, _ := s.Blocks.Min()
		firstIt := firstBlock.Node.Iterator()
		if firstIt.HasNext() {
			first := firstIt.Next()
			so.FirstId = &model.StreamId{Ms: uint64(first.ID.MS), Sequence: uint64(first.ID.Seq)}
		}
	}

	// Groups
	for _, g := range s.Groups {
		sg := &model.StreamGroup{
			Name:   g.Name,
			LastId: &model.StreamId{Ms: uint64(g.LastDeliveredID.MS), Sequence: uint64(g.LastDeliveredID.Seq)},
		}
		for id, pe := range g.Pending {
			sg.Pending = append(sg.Pending, &model.StreamNAck{
				Id:            &model.StreamId{Ms: uint64(id.MS), Sequence: uint64(id.Seq)},
				DeliveryTime:  uint64(pe.LastDeliveryMs),
				DeliveryCount: uint64(pe.DeliveryCount),
			})
		}
		// Build consumers from pending entries
		consumerPending := make(map[string][]*model.StreamId)
		for id, pe := range g.Pending {
			consumerPending[pe.Consumer] = append(consumerPending[pe.Consumer],
				&model.StreamId{Ms: uint64(id.MS), Sequence: uint64(id.Seq)})
		}
		for name, pids := range consumerPending {
			sg.Consumers = append(sg.Consumers, &model.StreamConsumer{
				Name:    name,
				Pending: pids,
			})
		}
		so.Groups = append(so.Groups, sg)
	}

	return enc.WriteStreamObject(key, so, opts...)
}
