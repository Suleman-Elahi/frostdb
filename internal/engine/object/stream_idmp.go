package object

import (
	"container/list"
	"sync/atomic"
	"time"
)

var (
	defaultIDMPDurationSec atomic.Int64
	defaultIDMPMaxSize     atomic.Int64
)

func init() {
	defaultIDMPDurationSec.Store(100)
	defaultIDMPMaxSize.Store(100)
}

func DefaultIDMPDurationSec() int64 { return defaultIDMPDurationSec.Load() }
func DefaultIDMPMaxSize() int64     { return defaultIDMPMaxSize.Load() }

func SetDefaultIDMPDurationSec(v int64) { defaultIDMPDurationSec.Store(v) }
func SetDefaultIDMPMaxSize(v int64)     { defaultIDMPMaxSize.Store(v) }

type idmpEntry struct {
	id        StreamID
	expiresAt int64
	elem      *list.Element
}

type idmpProducerState struct {
	entries map[string]*idmpEntry
	order   *list.List // iid insertion order (oldest at front)
}

func (s *Stream) ensureIDMPStateLocked() {
	if s.idmpByProducer == nil {
		s.idmpByProducer = make(map[string]*idmpProducerState)
	}
	if s.idmpDurationSec == 0 {
		s.idmpDurationSec = DefaultIDMPDurationSec()
	}
	if s.idmpMaxSize == 0 {
		s.idmpMaxSize = int(DefaultIDMPMaxSize())
	}
}

func (s *Stream) getProducerStateLocked(producer string) *idmpProducerState {
	ps, ok := s.idmpByProducer[producer]
	if ok {
		return ps
	}
	ps = &idmpProducerState{
		entries: make(map[string]*idmpEntry),
		order:   list.New(),
	}
	s.idmpByProducer[producer] = ps
	return ps
}

func (s *Stream) sweepProducerLocked(producer string, nowMs int64) {
	ps, ok := s.idmpByProducer[producer]
	if !ok {
		return
	}
	for e := ps.order.Front(); e != nil; {
		next := e.Next()
		iid := e.Value.(string)
		rec, exists := ps.entries[iid]
		if !exists {
			ps.order.Remove(e)
		} else if rec.expiresAt <= nowMs {
			delete(ps.entries, iid)
			ps.order.Remove(e)
		}
		e = next
	}
	if len(ps.entries) == 0 {
		delete(s.idmpByProducer, producer)
	}
}

func (s *Stream) evictProducerLocked(producer string) {
	ps, ok := s.idmpByProducer[producer]
	if !ok {
		return
	}
	for len(ps.entries) > s.idmpMaxSize {
		front := ps.order.Front()
		if front == nil {
			break
		}
		iid := front.Value.(string)
		delete(ps.entries, iid)
		ps.order.Remove(front)
	}
	if len(ps.entries) == 0 {
		delete(s.idmpByProducer, producer)
	}
}

func (s *Stream) addOrGetIDMPLocked(producer, iid string, candidateID StreamID, nowMs int64) (StreamID, bool) {
	s.ensureIDMPStateLocked()
	s.sweepProducerLocked(producer, nowMs)
	ps := s.getProducerStateLocked(producer)

	if rec, ok := ps.entries[iid]; ok {
		s.idmpDuplicates++
		return rec.id, true
	}

	expiresAt := nowMs + s.idmpDurationSec*1000
	elem := ps.order.PushBack(iid)
	ps.entries[iid] = &idmpEntry{
		id:        candidateID,
		expiresAt: expiresAt,
		elem:      elem,
	}
	s.idmpAdded++
	s.evictProducerLocked(producer)
	return candidateID, false
}

func (s *Stream) recordIDMPLocked(producer, iid string, id StreamID, nowMs int64) error {
	s.ensureIDMPStateLocked()
	s.sweepProducerLocked(producer, nowMs)
	ps := s.getProducerStateLocked(producer)

	if rec, ok := ps.entries[iid]; ok {
		if rec.id.Compare(id) != 0 {
			return errIDMPConflict
		}
		return nil
	}

	expiresAt := nowMs + s.idmpDurationSec*1000
	elem := ps.order.PushBack(iid)
	ps.entries[iid] = &idmpEntry{
		id:        id,
		expiresAt: expiresAt,
		elem:      elem,
	}
	s.evictProducerLocked(producer)
	return nil
}

func (s *Stream) clearIDMPHistoryLocked() {
	s.idmpByProducer = make(map[string]*idmpProducerState)
}

func (s *Stream) IDMPStats() (durationSec int64, maxSize int64, pidsTracked int64, iidsTracked int64, iidsAdded int64, iidsDuplicates int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensureIDMPStateLocked()
	nowMs := time.Now().UnixMilli()
	for producer := range s.idmpByProducer {
		s.sweepProducerLocked(producer, nowMs)
	}

	pidsTracked = int64(len(s.idmpByProducer))
	for _, ps := range s.idmpByProducer {
		iidsTracked += int64(len(ps.entries))
	}

	return s.idmpDurationSec, int64(s.idmpMaxSize), pidsTracked, iidsTracked, int64(s.idmpAdded), int64(s.idmpDuplicates)
}

func (s *Stream) SetIDMPConfig(durationSec int64, maxSize int64, hasDuration bool, hasMaxSize bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensureIDMPStateLocked()
	changed := false
	if hasDuration && s.idmpDurationSec != durationSec {
		s.idmpDurationSec = durationSec
		changed = true
	}
	if hasMaxSize && int64(s.idmpMaxSize) != maxSize {
		s.idmpMaxSize = int(maxSize)
		changed = true
	}
	if changed {
		s.clearIDMPHistoryLocked()
	}
	return changed
}

var errIDMPConflict = &idmpConflictError{}

type idmpConflictError struct{}

func (e *idmpConflictError) Error() string {
	return "ERR IID already exists for this producer with a different stream ID"
}

