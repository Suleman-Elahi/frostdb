package rdb

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/hdt3213/rdb/encoder"
	"github.com/hdt3213/rdb/model"

	"frostdb/internal/engine/object"
	"frostdb/internal/engine/shard"
)

// Exporter dumps FrostDB state as an RDB file.
type Exporter struct {
	databases [][256]*shard.Shard
	writer    io.Writer
}

// NewExporter creates an exporter for one database, retaining the original API.
func NewExporter(shards [256]*shard.Shard) *Exporter {
	return NewMultiExporter([][256]*shard.Shard{shards})
}

// NewMultiExporter creates an exporter that writes every supplied logical DB.
func NewMultiExporter(databases [][256]*shard.Shard) *Exporter {
	return &Exporter{databases: databases}
}

// NewExporterToWriter creates a single-database exporter for a writer.
func NewExporterToWriter(shards [256]*shard.Shard, writer io.Writer) *Exporter {
	return NewMultiExporterToWriter([][256]*shard.Shard{shards}, writer)
}

// NewMultiExporterToWriter creates a multi-database exporter for a writer.
func NewMultiExporterToWriter(databases [][256]*shard.Shard, writer io.Writer) *Exporter {
	return &Exporter{databases: databases, writer: writer}
}

// ExportFile atomically writes the current state to an RDB file.
func (exp *Exporter) ExportFile(path string) (int, error) {
	dir := filepath.Dir(path)
	file, err := os.CreateTemp(dir, ".dump-*.rdb.tmp")
	if err != nil {
		return 0, fmt.Errorf("rdb export: create temporary file: %w", err)
	}
	tempPath := file.Name()
	exp.writer = file

	count, exportErr := exp.Export()
	if exportErr != nil {
		_ = file.Close()
		_ = os.Remove(tempPath)
		return count, exportErr
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(tempPath)
		return count, fmt.Errorf("rdb export: fsync: %w", err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tempPath)
		return count, fmt.Errorf("rdb export: close: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		_ = os.Remove(tempPath)
		return count, fmt.Errorf("rdb export: publish: %w", err)
	}
	if err := syncDirectory(dir); err != nil {
		return count, fmt.Errorf("rdb export: sync directory: %w", err)
	}
	return count, nil
}

func syncDirectory(dir string) error {
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

// Export writes RDB data to the configured writer.
func (exp *Exporter) Export() (int, error) {
	if exp.writer == nil {
		return 0, fmt.Errorf("rdb export: no writer configured")
	}

	enc := encoder.NewEncoder(exp.writer)
	if err := enc.WriteHeader(); err != nil {
		return 0, fmt.Errorf("rdb export: header: %w", err)
	}
	enc.WriteAux("redis-ver", "7.0.0")
	enc.WriteAux("redis-bits", "64")
	enc.WriteAux("aof-preamble", "0")

	count := 0
	for dbIndex, shards := range exp.databases {
		keys, expires := countKeys(shards)
		if keys == 0 {
			continue
		}
		if err := enc.WriteDBHeader(uint(dbIndex), uint64(keys), uint64(expires)); err != nil {
			return count, fmt.Errorf("rdb export: db %d header: %w", dbIndex, err)
		}
		for shardIndex := 0; shardIndex < len(shards); shardIndex++ {
			var writeErr error
			shards[shardIndex].ForEach(func(key string, obj *object.Object) {
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
	}

	if err := enc.WriteEnd(); err != nil {
		return count, fmt.Errorf("rdb export: end: %w", err)
	}
	return count, nil
}

func countKeys(shards [256]*shard.Shard) (keys, expires int) {
	for i := 0; i < len(shards); i++ {
		shards[i].ForEach(func(_ string, obj *object.Object) {
			keys++
			if obj.ExpireAt > 0 {
				expires++
			}
		})
	}
	return keys, expires
}

func (exp *Exporter) writeObject(enc *encoder.Encoder, key string, obj *object.Object) error {
	opts := exp.ttlOpts(obj)
	switch obj.Type {
	case object.TypeString:
		value, ok := obj.Value.([]byte)
		if !ok {
			return nil
		}
		return enc.WriteStringObject(key, value, opts...)
	case object.TypeList:
		value, ok := obj.Value.(*object.List)
		if !ok {
			return nil
		}
		return enc.WriteListObject(key, value.Snapshot(), opts...)
	case object.TypeSet:
		value, ok := obj.Value.(*object.Set)
		if !ok {
			return nil
		}
		members := value.Members()
		data := make([][]byte, len(members))
		for i, member := range members {
			data[i] = []byte(member)
		}
		return enc.WriteSetObject(key, data, opts...)
	case object.TypeHash:
		value, ok := obj.Value.(*object.Hash)
		if !ok {
			return nil
		}
		hash := make(map[string][]byte, value.Len())
		for field, item := range value.GetAll() {
			hash[field] = []byte(item)
		}
		return enc.WriteHashMapObject(key, hash, opts...)
	case object.TypeZSet:
		value, ok := obj.Value.(*object.ZSet)
		if !ok {
			return nil
		}
		entries := make([]*model.ZSetEntry, 0, value.Len())
		for node := value.Skiplist.Head.Level[0].Forward; node != nil; node = node.Level[0].Forward {
			entries = append(entries, &model.ZSetEntry{Score: node.Score, Member: node.Member})
		}
		return enc.WriteZSetObject(key, entries, opts...)
	case object.TypeStream:
		value, ok := obj.Value.(*object.Stream)
		if !ok {
			return nil
		}
		return exp.writeStream(enc, key, value, opts...)
	case object.TypeArray:
		value, ok := obj.Value.(*object.Array)
		if !ok {
			return nil
		}
		return enc.WriteHashMapObject(key, value.ToRDBHash(), opts...)
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

const streamSnapshotGroupPrefix = "__frostdb_stream_snapshot__:"

func (exp *Exporter) writeStream(enc *encoder.Encoder, key string, stream *object.Stream, opts ...interface{}) error {
	lastID := stream.LastGeneratedStreamID()
	snapJSON, err := json.Marshal(stream.Snapshot())
	if err != nil {
		return err
	}
	streamObject := &model.StreamObject{
		Length: uint64(stream.Len()),
		LastId: &model.StreamId{Ms: lastID.MS, Sequence: lastID.Seq},
		Groups: []*model.StreamGroup{
			{
				Name:   streamSnapshotGroupPrefix + string(snapJSON),
				LastId: &model.StreamId{},
			},
		},
	}
	return enc.WriteStreamObject(key, streamObject, opts...)
}
