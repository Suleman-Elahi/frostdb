package object

import "container/list"

type StreamSnapshot struct {
	Entries              []StreamEntry
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
		for it.HasNext() {
			snap.Entries = append(snap.Entries, it.Next())
		}
		return true
	})

	for _, g := range s.Groups {
		gs := StreamGroupSnapshot{
			Name:            g.Name,
			LastDeliveredID: g.LastDeliveredID,
			Pending:         make([]StreamPendingEntry, 0, len(g.Pending)),
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
	s.idmpDurationSec = snap.IDMPDuration
	s.idmpMaxSize = snap.IDMPMaxSize
	s.idmpAdded = snap.IDMPAdded
	s.idmpDuplicates = snap.IDMPDuplicates
	s.lastMS = snap.LastMS
	s.lastSeq = snap.LastSeq
	s.entriesAdded = snap.EntriesAdded
	s.recordedFirstEntryID = snap.RecordedFirstEntryID
	s.maxDeletedEntryID = snap.MaxDeletedEntryID

	for _, e := range snap.Entries {
		_, _ = s.Add(e.ID.String(), e.Fields)
	}
	// Restore entriesAdded from snapshot (Add increments it, but we want the snapshot value)
	s.entriesAdded = snap.EntriesAdded
	// Restore recordedFirstEntryID (Add may have set it, but we want snapshot value)
	s.recordedFirstEntryID = snap.RecordedFirstEntryID

	for _, g := range snap.Groups {
		_ = s.CreateGroup(g.Name, g.LastDeliveredID.String())
		group := s.Groups[g.Name]
		if group == nil {
			continue
		}
		for _, pe := range g.Pending {
			entry := pe
			group.Pending[entry.Entry.ID] = &entry
			if entry.Consumer != "" {
				group.Consumers[entry.Consumer]++
			}
		}
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

