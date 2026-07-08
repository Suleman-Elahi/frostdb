package core

import (
	"encoding/json"
	"fmt"
	"sort"

	"frostdb/internal/engine/object"
	"frostdb/internal/engine/persistence/checkpoint"
)

type zsetEntry struct {
	Member string  `json:"m"`
	Score  float64 `json:"s"`
}

// serializeObject converts an Object to a checkpoint Record
func serializeObject(key string, obj *object.Object) (checkpoint.Record, error) {
	var data []byte
	var err error

	switch obj.Type {
	case object.TypeString:
		s, ok := obj.Value.(string)
		if !ok {
			return checkpoint.Record{}, fmt.Errorf("invalid string value")
		}
		data = []byte(s)
	case object.TypeList:
		l, ok := obj.Value.(*object.List)
		if !ok {
			return checkpoint.Record{}, fmt.Errorf("invalid list value")
		}
		// Snapshot flattens linked nodes and avoids cyclic pointers.
		data, err = json.Marshal(l.Snapshot())
		if err != nil {
			return checkpoint.Record{}, err
		}
	case object.TypeHash:
		h, ok := obj.Value.(*object.Hash)
		if !ok {
			return checkpoint.Record{}, fmt.Errorf("invalid hash value")
		}
		data, err = json.Marshal(h.GetAll())
		if err != nil {
			return checkpoint.Record{}, err
		}
	case object.TypeSet:
		s, ok := obj.Value.(*object.Set)
		if !ok {
			return checkpoint.Record{}, fmt.Errorf("invalid set value")
		}
		members := s.Members()
		sort.Strings(members)
		data, err = json.Marshal(members)
		if err != nil {
			return checkpoint.Record{}, err
		}
	case object.TypeZSet:
		z, ok := obj.Value.(*object.ZSet)
		if !ok {
			return checkpoint.Record{}, fmt.Errorf("invalid zset value")
		}
		entries := make([]zsetEntry, 0, z.Len())
		for _, member := range z.Members() {
			score, _ := z.Score(member)
			entries = append(entries, zsetEntry{Member: member, Score: score})
		}
		data, err = json.Marshal(entries)
		if err != nil {
			return checkpoint.Record{}, err
		}
	case object.TypeStream:
		s, ok := obj.Value.(*object.Stream)
		if !ok {
			return checkpoint.Record{}, fmt.Errorf("invalid stream value")
		}
		// Snapshot converts map[StreamID]*StreamPendingEntry to []StreamPendingEntry,
		// which is JSON-safe (struct-keyed maps are not).
		data, err = json.Marshal(s.Snapshot())
		if err != nil {
			return checkpoint.Record{}, err
		}
	case object.TypeArray:
		arr, ok := obj.Value.(*object.Array)
		if !ok {
			return checkpoint.Record{}, fmt.Errorf("invalid array value")
		}
		data, err = arr.MarshalJSON()
		if err != nil {
			return checkpoint.Record{}, err
		}
	default:
		// Fallback for less-common/experimental types.
		data, err = json.Marshal(obj.Value)
		if err != nil {
			return checkpoint.Record{}, err
		}
	}

	return checkpoint.Record{
		Key:      key,
		Type:     uint8(obj.Type),
		Encoding: uint8(obj.Encoding),
		ExpireAt: obj.ExpireAt,
		Data:     data,
	}, nil
}

// deserializeObject converts a checkpoint Record back to an Object
func deserializeObject(rec checkpoint.Record) (*object.Object, error) {
	obj := &object.Object{
		Type:     object.Type(rec.Type),
		Encoding: object.Encoding(rec.Encoding),
		ExpireAt: rec.ExpireAt,
	}

	switch obj.Type {
	case object.TypeString:
		obj.Value = string(rec.Data)
	case object.TypeList:
		var values [][]byte
		if err := json.Unmarshal(rec.Data, &values); err != nil {
			return nil, err
		}
		l := object.NewList()
		for _, v := range values {
			l.PushBack(v)
		}
		obj.Value = l
	case object.TypeHash:
		var m map[string]string
		if err := json.Unmarshal(rec.Data, &m); err != nil {
			return nil, err
		}
		h := object.NewHash()
		for k, v := range m {
			h.Set(k, v)
		}
		obj.Value = h
	case object.TypeSet:
		var members []string
		if err := json.Unmarshal(rec.Data, &members); err != nil {
			return nil, err
		}
		s := object.NewSet()
		s.Add(members...)
		obj.Value = s
	case object.TypeZSet:
		var entries []zsetEntry
		if err := json.Unmarshal(rec.Data, &entries); err != nil {
			return nil, err
		}
		z := object.NewZSet()
		for _, e := range entries {
			z.Add(e.Score, e.Member)
		}
		obj.Value = z
	case object.TypeStream:
		var snap object.StreamSnapshot
		if err := json.Unmarshal(rec.Data, &snap); err != nil {
			return nil, err
		}
		obj.Value = object.StreamFromSnapshot(snap)
	case object.TypeArray:
		arr, err := object.ParseArrayJSON(rec.Data)
		if err != nil {
			return nil, err
		}
		obj.Value = arr
	default:
		// Fallback preserves prior behavior for unsupported types.
		obj.Value = rec.Data
	}

	return obj, nil
}
