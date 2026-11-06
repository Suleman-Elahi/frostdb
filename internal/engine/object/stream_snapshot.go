package object

import "container/list"

type StreamSnapshot struct {
	Entries              []StreamEntry
	Blocks               [][]StreamEntry
	Groups               []StreamGroupSnapshot
	LastMS               uint64
	LastSeq              uint64
	IDMPDuration         int64
	IDMPMaxSize          int
	IDMPAdded            uint64
	IDMPDuplicates       uint64
	IDMP                 map[string][]IDMPRecord
	EntriesAdded         uint64
	RecordedFirstEntryID StreamID
	MaxDeletedEntryID    StreamID
}

type StreamGroupSnapshot struct {
	Name            string
	LastDeliveredID StreamID
	Pending         []StreamPendingEntry
	Consumers       map[string]int
}

type IDMPRecord struct {
	IID       string
	ID        StreamID
	ExpiresAt int64
}

func (s *Stream) Snapshot() StreamSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensureIDMPStateLocked()
	snap := StreamSnapshot{
		LastMS:               s.lastMS,
		LastSeq:              s.lastSeq,
		IDMPDuration:         s.idmpDurationSec,
		IDMPMaxSize:          s.idmpMaxSize,
		IDMPAdded:            s.idmpAdded,
		IDMPDuplicates:       s.idmpDuplicates,
		IDMP:                 make(map[string][]IDMPRecord, len(s.idmpByProducer)),
		EntriesAdded:         s.entriesAdded,
		RecordedFirstEntryID: s.recordedFirstEntryID,
		MaxDeletedEntryID:    s.maxDeletedEntryID,
	}

	s.Blocks.Ascend(func(b StreamBlock) bool {
		it := b.Node.Iterator()
		var blockEntries []StreamEntry
		for it.HasNext() {
			e := it.Next()
			fieldsCopy := make([]StreamField, len(e.Fields))
			copy(fieldsCopy, e.Fields)
			entryCopy := StreamEntry{ID: e.ID, Fields: fieldsCopy}
			snap.Entries = append(snap.Entries, entryCopy)
			blockEntries = append(blockEntries, entryCopy)
		}
		if len(blockEntries) > 0 {
			snap.Blocks = append(snap.Blocks, blockEntries)
		}
		return true
	})

	for _, g := range s.Groups {
		consumers := make(map[string]int, len(g.Consumers))
		for k, v := range g.Consumers {
			consumers[k] = v
		}
		gs := StreamGroupSnapshot{
			Name:            g.Name,
			LastDeliveredID: g.LastDeliveredID,
			Pending:         make([]StreamPendingEntry, 0, len(g.Pending)),
			Consumers:       consumers,
		}
		for _, pe := range g.Pending {
			gs.Pending = append(gs.Pending, *pe)
		}
		snap.Groups = append(snap.Groups, gs)
	}

	for producer, ps := range s.idmpByProducer {
		records := make([]IDMPRecord, 0, len(ps.entries))
		for e := ps.order.Front(); e != nil; e = e.Next() {
			iid := e.Value.(string)
			rec := ps.entries[iid]
			if rec == nil {
				continue
			}
			records = append(records, IDMPRecord{
				IID:       iid,
				ID:        rec.id,
				ExpiresAt: rec.expiresAt,
			})
		}
		snap.IDMP[producer] = records
	}

	return snap
}

func StreamFromSnapshot(snap StreamSnapshot) *Stream {
	s := NewStream()
	if snap.IDMPDuration > 0 {
		s.idmpDurationSec = snap.IDMPDuration
	}
	if snap.IDMPMaxSize > 0 {
		s.idmpMaxSize = snap.IDMPMaxSize
	}
	s.idmpAdded = snap.IDMPAdded
	s.idmpDuplicates = snap.IDMPDuplicates

	if len(snap.Blocks) > 0 {
		for _, blockEntries := range snap.Blocks {
			if len(blockEntries) == 0 {
				continue
			}
			node := NewStreamNode()
			for _, e := range blockEntries {
				fieldsCopy := make([]StreamField, len(e.Fields))
				copy(fieldsCopy, e.Fields)
				node.AppendRaw(StreamEntry{ID: e.ID, Fields: fieldsCopy})
			}
			s.Blocks.ReplaceOrInsert(StreamBlock{
				FirstID: blockEntries[0].ID,
				Node:    node,
			})
			s.length += len(blockEntries)
		}
	} else {
		for _, e := range snap.Entries {
			_, _ = s.Add(e.ID.String(), e.Fields)
		}
	}

	// Restore metadata AFTER populating blocks/entries so Add() cannot overwrite them.
	s.lastMS = snap.LastMS
	s.lastSeq = snap.LastSeq
	s.entriesAdded = snap.EntriesAdded
	s.recordedFirstEntryID = snap.RecordedFirstEntryID
	s.maxDeletedEntryID = snap.MaxDeletedEntryID

	for _, g := range snap.Groups {
		group := &StreamGroup{
			Name:            g.Name,
			LastDeliveredID: g.LastDeliveredID,
			Pending:         make(map[StreamID]*StreamPendingEntry, len(g.Pending)),
			Consumers:       make(map[string]int, len(g.Consumers)),
		}
		for k, v := range g.Consumers {
			group.Consumers[k] = v
		}
		for _, pe := range g.Pending {
			entry := pe
			group.Pending[entry.Entry.ID] = &entry
			if entry.Consumer != "" {
				if _, ok := group.Consumers[entry.Consumer]; !ok {
					group.Consumers[entry.Consumer]++
				}
			}
		}
		s.Groups[g.Name] = group
	}
	if s.idmpByProducer == nil {
		s.idmpByProducer = make(map[string]*idmpProducerState)
	}
	for producer, records := range snap.IDMP {
		ps := &idmpProducerState{
			entries: make(map[string]*idmpEntry, len(records)),
			order:   list.New(),
		}
		for _, r := range records {
			elem := ps.order.PushBack(r.IID)
			ps.entries[r.IID] = &idmpEntry{
				id:        r.ID,
				expiresAt: r.ExpiresAt,
				elem:      elem,
			}
		}
		s.idmpByProducer[producer] = ps
	}
	return s
}
