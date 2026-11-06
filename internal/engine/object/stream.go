package object

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/btree"
)

type StreamID struct {
	MS  uint64
	Seq uint64
}

func (id StreamID) String() string {
	return strconv.FormatUint(id.MS, 10) + "-" + strconv.FormatUint(id.Seq, 10)
}

func (id StreamID) Compare(other StreamID) int {
	if id.MS < other.MS {
		return -1
	}
	if id.MS > other.MS {
		return 1
	}
	if id.Seq < other.Seq {
		return -1
	}
	if id.Seq > other.Seq {
		return 1
	}
	return 0
}

func ParseStreamID(s string) (StreamID, error) {
	return ParseStreamIDWithDefaultSeq(s, 0)
}

func ParseStreamIDWithDefaultSeq(s string, defaultSeq uint64) (StreamID, error) {
	idx := strings.IndexByte(s, '-')
	if idx < 0 {
		ms, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return StreamID{}, fmt.Errorf("ERR Invalid stream ID")
		}
		return StreamID{MS: ms, Seq: defaultSeq}, nil
	}
	ms, err := strconv.ParseUint(s[:idx], 10, 64)
	if err != nil {
		return StreamID{}, fmt.Errorf("ERR Invalid stream ID")
	}
	seq, err := strconv.ParseUint(s[idx+1:], 10, 64)
	if err != nil {
		return StreamID{}, fmt.Errorf("ERR Invalid stream ID")
	}
	return StreamID{MS: ms, Seq: seq}, nil
}

func withinIDRange(id StreamID, start, end string) bool {
	if start != "-" {
		startID, err := ParseStreamID(start)
		if err == nil && id.Compare(startID) < 0 {
			return false
		}
	}
	if end != "+" {
		endID, err := ParseStreamID(end)
		if err == nil && id.Compare(endID) > 0 {
			return false
		}
	}
	return true
}

type StreamField struct {
	Name  string
	Value string
}

type StreamEntry struct {
	ID     StreamID
	Fields []StreamField
}

type StreamPendingEntry struct {
	Entry          StreamEntry
	Consumer       string
	DeliveryCount  int
	LastDeliveryMs int64
}

type StreamGroup struct {
	Name            string
	LastDeliveredID StreamID
	Pending         map[StreamID]*StreamPendingEntry
	Consumers       map[string]int
}

type StreamBlock struct {
	FirstID StreamID
	Node    *StreamNode
}

type Stream struct {
	mu              sync.Mutex
	Blocks          *btree.BTreeG[StreamBlock]
	Groups          map[string]*StreamGroup
	idmpByProducer  map[string]*idmpProducerState
	idmpDurationSec int64
	idmpMaxSize     int
	idmpAdded       uint64
	idmpDuplicates  uint64
	lastMS          uint64
	lastSeq         uint64
	length          int // total number of entries

	// Redis 7.0+ stream metadata
	entriesAdded         uint64   // total entries ever added (survives deletions)
	recordedFirstEntryID StreamID // first entry ID when created (survives trim if entries exist)
	maxDeletedEntryID    StreamID // maximum ID among deleted entries
}

type StreamTrimRefPolicy int

const (
	StreamTrimKeepRef StreamTrimRefPolicy = iota
	StreamTrimDelRef
	StreamTrimAcked
)

func NewStream() *Stream {
	return &Stream{
		Blocks: btree.NewG(32, func(a, b StreamBlock) bool {
			return a.FirstID.Compare(b.FirstID) < 0
		}),
		Groups:          make(map[string]*StreamGroup),
		idmpByProducer:  make(map[string]*idmpProducerState),
		idmpDurationSec: DefaultIDMPDurationSec(),
		idmpMaxSize:     int(DefaultIDMPMaxSize()),
	}
}

func (s *Stream) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.length
}

// BlockCount returns the number of BTree blocks (analogous to radix-tree-keys in Redis).
func (s *Stream) BlockCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Blocks.Len()
}

func (s *Stream) LastGeneratedID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return StreamID{MS: s.lastMS, Seq: s.lastSeq}.String()
}

func (s *Stream) LastGeneratedStreamID() StreamID {
	s.mu.Lock()
	defer s.mu.Unlock()
	return StreamID{MS: s.lastMS, Seq: s.lastSeq}
}

// SetLastID sets the last generated ID for the stream.
// The new ID must be >= the highest ID currently in the stream (if any entries exist).
func (s *Stream) SetLastID(id StreamID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check against entries currently in the stream, not historical max
	if s.Blocks.Len() > 0 {
		lastBlock, _ := s.Blocks.Max()
		lastEntry := lastBlock.Node.LastEntry()
		if id.Compare(lastEntry.ID) < 0 {
			return fmt.Errorf("ERR The ID specified in XSETID is smaller than the target stream top item")
		}
	}

	s.lastMS = id.MS
	s.lastSeq = id.Seq
	return nil
}

// SetLastIDWithOptions sets the last generated ID along with optional entries-added and max-deleted-id.
// This is used by XSETID command with ENTRIESADDED and MAXDELETEDID options.
func (s *Stream) SetLastIDWithOptions(id StreamID, entriesAdded int64, maxDeletedID *StreamID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check against entries currently in the stream
	if s.Blocks.Len() > 0 {
		lastBlock, _ := s.Blocks.Max()
		lastEntry := lastBlock.Node.LastEntry()
		if id.Compare(lastEntry.ID) < 0 {
			return fmt.Errorf("ERR The ID specified in XSETID is smaller than the target stream top item")
		}
	}

	s.lastMS = id.MS
	s.lastSeq = id.Seq

	if entriesAdded >= 0 {
		s.entriesAdded = uint64(entriesAdded)
	}

	if maxDeletedID != nil {
		s.maxDeletedEntryID = *maxDeletedID
	}

	return nil
}

// EntriesAdded returns total count of entries ever added to the stream.
func (s *Stream) EntriesAdded() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.entriesAdded
}

// RecordedFirstEntryID returns the first entry ID recorded when stream was created.
func (s *Stream) RecordedFirstEntryID() StreamID {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recordedFirstEntryID
}

// MaxDeletedEntryID returns the maximum ID among deleted entries.
func (s *Stream) MaxDeletedEntryID() StreamID {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxDeletedEntryID
}

func (s *Stream) Add(id string, fields []StreamField) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.addLocked(id, fields)
}

func (s *Stream) AddIDMP(id string, fields []StreamField, producer, iid string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	nowMs := time.Now().UnixMilli()
	s.sweepProducerLocked(producer, nowMs)
	ps := s.getProducerStateLocked(producer)
	if rec, ok := ps.entries[iid]; ok {
		s.idmpDuplicates++
		return rec.id.String(), true, nil
	}

	actualIDStr, err := s.addLocked(id, fields)
	if err != nil {
		return "", false, err
	}
	parsed, err := ParseStreamID(actualIDStr)
	if err != nil {
		return "", false, err
	}
	_, _ = s.addOrGetIDMPLocked(producer, iid, parsed, nowMs)
	return actualIDStr, false, nil
}

func (s *Stream) RecordIDMP(producer, iid, idStr string) error {
	id, err := ParseStreamID(idStr)
	if err != nil {
		return fmt.Errorf("ERR Invalid stream ID specified as stream")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.hasIDLocked(id) {
		return fmt.Errorf("ERR No such message in stream")
	}

	if err := s.recordIDMPLocked(producer, iid, id, time.Now().UnixMilli()); err != nil {
		return err
	}
	return nil
}

func (s *Stream) addLocked(id string, fields []StreamField) (string, error) {
	actualID, err := s.nextIDLocked(id)
	if err != nil {
		return "", err
	}

	entry := StreamEntry{
		ID:     actualID,
		Fields: fields,
	}

	// Try to append to the last block
	var lastBlock StreamBlock
	var hasLast bool
	if s.Blocks.Len() > 0 {
		lastBlock, hasLast = s.Blocks.Max()
	}

	appended := false
	if hasLast {
		appended = lastBlock.Node.Append(entry)
	}

	if !appended {
		// Create a new block
		newNode := NewStreamNode()
		newNode.Append(entry)
		s.Blocks.ReplaceOrInsert(StreamBlock{
			FirstID: actualID,
			Node:    newNode,
		})
	}

	s.length++
	s.entriesAdded++

	// Track recorded first entry ID (set whenever stream transitions from empty to 1 entry, matching Redis t_stream.c)
	if s.length == 1 {
		s.recordedFirstEntryID = actualID
	}

	return actualID.String(), nil
}

func (s *Stream) hasIDLocked(target StreamID) bool {
	found := false
	s.Blocks.Ascend(func(b StreamBlock) bool {
		it := b.Node.Iterator()
		for it.HasNext() {
			entry := it.Next()
			if entry.ID.Compare(target) == 0 {
				found = true
				return false
			}
		}
		return true
	})
	return found
}

func (s *Stream) CreateGroup(name, startID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.Groups[name]; exists {
		return fmt.Errorf("BUSYGROUP Consumer Group name already exists")
	}

	var resolved StreamID
	if startID == "$" {
		resolved = StreamID{MS: s.lastMS, Seq: s.lastSeq}
	} else {
		parsed, err := ParseStreamID(startID)
		if err != nil {
			return fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
		}
		resolved = parsed
	}

	s.Groups[name] = &StreamGroup{
		Name:            name,
		LastDeliveredID: resolved,
		Pending:         make(map[StreamID]*StreamPendingEntry),
		Consumers:       make(map[string]int),
	}

	return nil
}

func (s *Stream) SetGroupID(name, startID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	g, exists := s.Groups[name]
	if !exists {
		return fmt.Errorf("NOGROUP No such consumer group '%s' for key name", name)
	}

	var resolved StreamID
	if startID == "$" {
		resolved = StreamID{MS: s.lastMS, Seq: s.lastSeq}
	} else {
		parsed, err := ParseStreamID(startID)
		if err != nil {
			return fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
		}
		resolved = parsed
	}
	g.LastDeliveredID = resolved
	return nil
}

func (s *Stream) DestroyGroup(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.Groups[name]; !exists {
		return false
	}
	delete(s.Groups, name)
	return true
}

func (s *Stream) HasConsumer(groupName, consumerName string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	g, exists := s.Groups[groupName]
	if !exists {
		return false
	}
	_, ok := g.Consumers[consumerName]
	return ok
}

func (s *Stream) CreateConsumer(groupName, consumerName string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	g, exists := s.Groups[groupName]
	if !exists {
		return 0, fmt.Errorf("NOGROUP No such consumer group '%s' for key name", groupName)
	}
	if _, ok := g.Consumers[consumerName]; ok {
		return 0, nil
	}
	g.Consumers[consumerName] = 0
	return 1, nil
}

func (s *Stream) DelConsumer(groupName, consumerName string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	g, exists := s.Groups[groupName]
	if !exists {
		return 0, fmt.Errorf("NOGROUP No such consumer group '%s' for key name", groupName)
	}
	removed := 0
	for id, pe := range g.Pending {
		if pe.Consumer == consumerName {
			delete(g.Pending, id)
			removed++
		}
	}
	delete(g.Consumers, consumerName)
	return removed, nil
}

func (s *Stream) ReadGroup(groupName, consumer string, count int, id string, noAck bool) ([]StreamEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, ok := s.Groups[groupName]
	if !ok {
		return nil, fmt.Errorf("NOGROUP No such key or consumer group")
	}

	if _, exists := group.Consumers[consumer]; !exists {
		group.Consumers[consumer] = 0
	}

	if id != ">" {
		return s.readPendingLocked(group, consumer, count, id, noAck), nil
	}

	entries := s.entriesAfterLocked(group, count)
	if len(entries) == 0 {
		return nil, nil
	}

	for _, entry := range entries {
		if !noAck {
			group.Pending[entry.ID] = &StreamPendingEntry{
				Entry:          entry,
				Consumer:       consumer,
				DeliveryCount:  1,
				LastDeliveryMs: time.Now().UnixMilli(),
			}
			group.Consumers[consumer]++
		}
		group.LastDeliveredID = entry.ID
	}

	return entries, nil
}

func (s *Stream) Ack(groupName string, ids ...string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, ok := s.Groups[groupName]
	if !ok {
		return 0
	}

	acked := int64(0)
	for _, idStr := range ids {
		id, err := ParseStreamID(idStr)
		if err != nil {
			continue
		}
		if _, exists := group.Pending[id]; exists {
			delete(group.Pending, id)
			acked++
		}
	}
	return acked
}

func (s *Stream) PendingSummary(groupName string) (int64, string, string, []PendingConsumerSummary) {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, ok := s.Groups[groupName]
	if !ok || len(group.Pending) == 0 {
		return 0, "", "", nil
	}

	ids := make([]StreamID, 0, len(group.Pending))
	for id := range group.Pending {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i].Compare(ids[j]) < 0
	})

	consumerCounts := make(map[string]int64)
	for _, pending := range group.Pending {
		consumerCounts[pending.Consumer]++
	}

	consumers := make([]PendingConsumerSummary, 0, len(consumerCounts))
	for name, count := range consumerCounts {
		consumers = append(consumers, PendingConsumerSummary{Name: name, Pending: count})
	}
	sort.Slice(consumers, func(i, j int) bool {
		return consumers[i].Name < consumers[j].Name
	})

	return int64(len(ids)), ids[0].String(), ids[len(ids)-1].String(), consumers
}

func (s *Stream) PendingRange(groupName, start, end string, count int, consumer string) []StreamPendingEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, ok := s.Groups[groupName]
	if !ok {
		return nil
	}

	ids := make([]StreamID, 0, len(group.Pending))
	for id := range group.Pending {
		if withinIDRange(id, start, end) {
			if consumer == "" || group.Pending[id].Consumer == consumer {
				ids = append(ids, id)
			}
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i].Compare(ids[j]) < 0
	})

	if count > 0 && len(ids) > count {
		ids = ids[:count]
	}

	result := make([]StreamPendingEntry, 0, len(ids))
	for _, id := range ids {
		p := group.Pending[id]
		result = append(result, *p)
	}
	return result
}

func (s *Stream) AutoClaim(groupName, consumer string, minIdleMs int64, start string, count int, justID bool) (string, []StreamPendingEntry, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, ok := s.Groups[groupName]
	if !ok {
		return "0-0", nil, nil
	}

	startID, err := ParseStreamID(start)
	if err != nil {
		startID = StreamID{}
	}

	ids := make([]StreamID, 0, len(group.Pending))
	for id, pending := range group.Pending {
		if id.Compare(startID) > 0 && time.Since(time.UnixMilli(pending.LastDeliveryMs)).Milliseconds() >= minIdleMs {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i].Compare(ids[j]) < 0
	})

	if count > 0 && len(ids) > count {
		ids = ids[:count]
	}

	claimed := make([]StreamPendingEntry, 0, len(ids))
	for _, id := range ids {
		p := group.Pending[id]
		p.Consumer = consumer
		p.DeliveryCount++
		p.LastDeliveryMs = time.Now().UnixMilli()
		group.Consumers[consumer]++
		claimed = append(claimed, *p)
	}

	next := "0-0"
	if len(ids) > 0 {
		next = ids[len(ids)-1].String()
	}

	deleted := make([]string, 0)
	if justID {
		claimedIDs := make([]string, 0, len(claimed))
		for _, entry := range claimed {
			claimedIDs = append(claimedIDs, entry.Entry.ID.String())
		}
		return next, claimed, claimedIDs
	}

	return next, claimed, deleted
}

func (s *Stream) entriesAfterLocked(group *StreamGroup, count int) []StreamEntry {
	resultCap := 16
	if count > 0 && count < resultCap {
		resultCap = count
	}
	result := make([]StreamEntry, 0, resultCap)

	// Find the block that might contain or be directly after LastDeliveredID
	// We use DescendLessOrEqual to find the highest block <= LastDeliveredID
	var startBlock StreamBlock
	foundBlock := false

	s.Blocks.DescendLessOrEqual(StreamBlock{FirstID: group.LastDeliveredID}, func(b StreamBlock) bool {
		startBlock = b
		foundBlock = true
		return false // only need the first one
	})

	searchID := group.LastDeliveredID

	if foundBlock {
		// Iterate from this block onwards
		s.Blocks.AscendGreaterOrEqual(startBlock, func(b StreamBlock) bool {
			it := b.Node.Iterator()
			for it.HasNext() {
				entry := it.Next()
				if entry.ID.Compare(searchID) > 0 {
					result = append(result, entry)
					if count > 0 && len(result) >= count {
						return false
					}
				}
			}
			return true
		})
	} else {
		// Start from the beginning if no block is <= searchID
		s.Blocks.Ascend(func(b StreamBlock) bool {
			it := b.Node.Iterator()
			for it.HasNext() {
				entry := it.Next()
				if entry.ID.Compare(searchID) > 0 {
					result = append(result, entry)
					if count > 0 && len(result) >= count {
						return false
					}
				}
			}
			return true
		})
	}

	return result
}

func (s *Stream) readPendingLocked(group *StreamGroup, consumer string, count int, startIDStr string, noAck bool) []StreamEntry {
	startID, _ := ParseStreamID(startIDStr)
	if _, exists := group.Consumers[consumer]; !exists {
		group.Consumers[consumer] = 0
	}
	ids := make([]StreamID, 0, len(group.Pending))
	for id, pending := range group.Pending {
		if pending.Consumer == consumer && id.Compare(startID) > 0 {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i].Compare(ids[j]) < 0
	})

	if count > 0 && len(ids) > count {
		ids = ids[:count]
	}

	result := make([]StreamEntry, 0, len(ids))
	for _, id := range ids {
		pending := group.Pending[id]
		if !s.hasIDLocked(id) {
			result = append(result, StreamEntry{ID: id, Fields: nil})
			continue
		}
		if pending.Entry.Fields == nil {
			result = append(result, StreamEntry{ID: id, Fields: []StreamField{}})
		} else {
			result = append(result, pending.Entry)
		}
	}
	return result
}

func (s *Stream) nextIDLocked(id string) (StreamID, error) {
	if strings.HasSuffix(id, "-*") {
		msStr := strings.TrimSuffix(id, "-*")
		ms, err := strconv.ParseUint(msStr, 10, 64)
		if err != nil {
			return StreamID{}, fmt.Errorf("ERR Invalid stream ID")
		}

		var seq uint64
		lastID := StreamID{MS: s.lastMS, Seq: s.lastSeq}
		if lastID.MS > 0 || lastID.Seq > 0 {
			if ms < lastID.MS {
				return StreamID{}, fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
			}
			if ms == lastID.MS {
				if lastID.Seq == math.MaxUint64 {
					// For partial IDs, Redis returns "equal or smaller" error when sequence is exhausted
					return StreamID{}, fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
				}
				seq = lastID.Seq + 1
			}
		}
		if ms == 0 && seq == 0 {
			seq = 1
		}
		parsed := StreamID{MS: ms, Seq: seq}
		s.lastMS, s.lastSeq = parsed.MS, parsed.Seq
		return parsed, nil
	}

	if id != "*" {
		parsed, err := ParseStreamID(id)
		if err != nil {
			return StreamID{}, err
		}
		if parsed.MS == 0 && parsed.Seq == 0 {
			return StreamID{}, fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
		}
		// Check against the last generated ID (which persists even if entries are deleted)
		lastID := StreamID{MS: s.lastMS, Seq: s.lastSeq}
		if lastID.MS > 0 || lastID.Seq > 0 {
			if parsed.Compare(lastID) <= 0 {
				return StreamID{}, fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
			}
		}
		s.lastMS, s.lastSeq = parsed.MS, parsed.Seq
		return parsed, nil
	}

	now := uint64(time.Now().UnixMilli())
	if now < s.lastMS {
		now = s.lastMS
	}
	if now == s.lastMS {
		if s.lastSeq == math.MaxUint64 {
			// Sequence exhausted for this millisecond, move to next millisecond
			if s.lastMS == math.MaxUint64 {
				return StreamID{}, fmt.Errorf("ERR The stream has exhausted the last possible ID, unable to add more items")
			}
			s.lastMS = now + 1
			s.lastSeq = 0
		} else {
			s.lastSeq++
		}
	} else {
		s.lastSeq = 0
		s.lastMS = now
	}
	return StreamID{MS: s.lastMS, Seq: s.lastSeq}, nil
}

type PendingConsumerSummary struct {
	Name    string
	Pending int64
}

// Delete removes entries by ID. Returns the number of entries actually deleted.
func (s *Stream) Delete(ids ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	deleted := 0
	for _, idStr := range ids {
		id, err := ParseStreamID(idStr)
		if err != nil {
			continue
		}
		if s.deleteIDLocked(id, false) {
			deleted++
		}
	}
	return deleted
}

// updateRecordedFirstEntryIDLocked updates recordedFirstEntryID to the current first entry.
// When the stream becomes empty, recordedFirstEntryID is reset to 0-0 (matching Redis t_stream.c).
// Must be called with s.mu held.
func (s *Stream) updateRecordedFirstEntryIDLocked() {
	if s.Blocks.Len() == 0 {
		s.recordedFirstEntryID = StreamID{}
		return
	}
	firstBlock, ok := s.Blocks.Min()
	if !ok {
		s.recordedFirstEntryID = StreamID{}
		return
	}
	it := firstBlock.Node.Iterator()
	if it.HasNext() {
		entry := it.Next()
		s.recordedFirstEntryID = entry.ID
	} else {
		s.recordedFirstEntryID = StreamID{}
	}
}

func (s *Stream) deleteIDLocked(id StreamID, delRef bool) bool {
	var targetBlock StreamBlock
	found := false
	s.Blocks.DescendLessOrEqual(StreamBlock{FirstID: id}, func(b StreamBlock) bool {
		targetBlock = b
		found = true
		return false
	})
	if !found {
		return false
	}
	if !targetBlock.Node.DeleteEntry(id) {
		return false
	}
	s.length--
	// Track the maximum deleted entry ID
	if id.Compare(s.maxDeletedEntryID) > 0 {
		s.maxDeletedEntryID = id
	}
	if targetBlock.Node.Count == 0 {
		s.Blocks.Delete(targetBlock)
	} else {
		newFirst := targetBlock.Node.FirstEntry().ID
		if newFirst.Compare(targetBlock.FirstID) != 0 {
			s.Blocks.Delete(targetBlock)
			s.Blocks.ReplaceOrInsert(StreamBlock{
				FirstID: newFirst,
				Node:    targetBlock.Node,
			})
		}
	}
	if delRef {
		s.cleanupEntryRefsLocked(id)
	}
	// Update recordedFirstEntryID if we deleted the first entry or emptied the stream
	if s.length == 0 || id.Compare(s.recordedFirstEntryID) == 0 {
		s.updateRecordedFirstEntryIDLocked()
	}
	return true
}

// cleanupEntryRefsLocked removes an ID from every consumer group's pending
// entries list. This is used by DELREF-style deletions, including cases where
// the stream entry itself no longer exists but dangling references remain.
// Must be called with s.mu held.
func (s *Stream) cleanupEntryRefsLocked(id StreamID) {
	for _, g := range s.Groups {
		delete(g.Pending, id)
	}
}

// CleanupEntryRefs removes an ID from every consumer group's pending entries
// list. It is used by DELREF-style commands.
func (s *Stream) CleanupEntryRefs(id StreamID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupEntryRefsLocked(id)
}

// IsAckedByAllGroups reports whether an entry has been fully acknowledged by
// all consumer groups, including groups that have not delivered it yet.
func (s *Stream) IsAckedByAllGroups(id StreamID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.isAckedByAllGroupsLocked(id)
}

// DeleteWithPolicy removes a single entry using the stream deletion policy
// semantics required by XDELEX.
// Returns -1 if the ID is not present, 1 if deleted, or 2 if kept because it
// is still referenced and ACKED was requested.
func (s *Stream) DeleteWithPolicy(id StreamID, policy StreamTrimRefPolicy) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch policy {
	case StreamTrimAcked:
		if !s.isAckedByAllGroupsLocked(id) {
			if !s.hasIDLocked(id) {
				return -1
			}
			return 2
		}
		if s.deleteIDLocked(id, false) {
			return 1
		}
		return -1
	case StreamTrimDelRef:
		s.cleanupEntryRefsLocked(id)
		if s.deleteIDLocked(id, false) {
			return 1
		}
		return -1
	default:
		if s.deleteIDLocked(id, false) {
			return 1
		}
		return -1
	}
}

// AckDelete acknowledges the entry in the specified consumer group and then
// applies the requested delete policy.
// Returns -1 if the ID is not pending in the group, 1 if the command
// acknowledged and deleted the entry, or 2 if the entry was acknowledged but
// kept because it is still referenced.
func (s *Stream) AckDelete(groupName string, id StreamID, policy StreamTrimRefPolicy) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, ok := s.Groups[groupName]
	if !ok {
		return -1
	}

	if _, pending := group.Pending[id]; !pending {
		return -1
	}
	delete(group.Pending, id)

	canDelete := true
	switch policy {
	case StreamTrimAcked:
		if !s.isAckedByAllGroupsLocked(id) {
			canDelete = false
		}
	case StreamTrimDelRef:
		s.cleanupEntryRefsLocked(id)
	}

	if canDelete {
		if s.deleteIDLocked(id, false) {
			return 1
		}
		return 1
	}
	return 2
}

func (s *Stream) isPendingLocked(id StreamID) bool {
	for _, g := range s.Groups {
		if _, ok := g.Pending[id]; ok {
			return true
		}
	}
	return false
}

func (s *Stream) isAckedByAllGroupsLocked(id StreamID) bool {
	for _, g := range s.Groups {
		// Never delivered to this group yet => not acked for this group.
		if id.Compare(g.LastDeliveredID) > 0 {
			return false
		}
		// Delivered but still pending => not acked.
		if _, ok := g.Pending[id]; ok {
			return false
		}
	}
	return true
}

// streamTrimLocked implements Redis streamTrim (t_stream.c) for MAXLEN and MINID
// with exact/approx (~), limit, and reference policy (KEEPREF, DELREF, ACKED).
// Note: Trimming does NOT update maxDeletedEntryID (only XDEL/XDELEX/XACKDEL/XSETID do).
func (s *Stream) streamTrimLocked(strategy string, maxlen int, minID StreamID, approx bool, limit int, policy StreamTrimRefPolicy) int {
	if strategy == "MAXLEN" && s.length <= maxlen {
		return 0
	}
	if s.Blocks.Len() == 0 {
		return 0
	}

	effectiveLimit := 0
	if approx {
		if limit < 0 {
			maxEntries := int(GetStreamNodeMaxEntries())
			if maxEntries > 0 {
				effectiveLimit = 100 * maxEntries
			} else {
				effectiveLimit = 10000
			}
			if effectiveLimit <= 0 {
				effectiveLimit = 10000
			}
			if effectiveLimit > 1000000 {
				effectiveLimit = 1000000
			}
		} else {
			effectiveLimit = limit
		}
	}

	blocks := make([]StreamBlock, 0, s.Blocks.Len())
	s.Blocks.Ascend(func(b StreamBlock) bool {
		blocks = append(blocks, b)
		return true
	})

	deleted := 0
	for _, b := range blocks {
		if strategy == "MAXLEN" && s.length <= maxlen {
			break
		}

		blockLen := b.Node.Len()
		if blockLen == 0 {
			s.Blocks.Delete(b)
			continue
		}

		if effectiveLimit > 0 && (deleted+blockLen) > effectiveLimit {
			break
		}

		var nodeEligible bool
		if strategy == "MAXLEN" {
			nodeEligible = (s.length - blockLen) >= maxlen
		} else {
			lastEntry := b.Node.LastEntry()
			nodeEligible = lastEntry.ID.Compare(minID) < 0
		}

		if policy == StreamTrimKeepRef {
			if nodeEligible {
				s.Blocks.Delete(b)
				s.length -= blockLen
				deleted += blockLen
				continue
			}
			if approx {
				break
			}

			// Exact partial trim
			if strategy == "MAXLEN" {
				toRemove := s.length - maxlen
				if toRemove > 0 {
					b.Node.TrimFront(toRemove)
					s.length -= toRemove
					deleted += toRemove
				}
			} else {
				trimmed := b.Node.TrimBeforeID(minID)
				s.length -= trimmed
				deleted += trimmed
			}
			s.Blocks.Delete(b)
			if b.Node.Len() > 0 {
				s.Blocks.ReplaceOrInsert(StreamBlock{
					FirstID: b.Node.FirstEntry().ID,
					Node:    b.Node,
				})
			}
			break
		}

		// DELREF or ACKED policy
		if approx && !nodeEligible {
			break
		}

		it := b.Node.Iterator()
		var kept []StreamEntry
		modified := false
		stopDeleting := false
		for it.HasNext() {
			entry := it.Next()
			if stopDeleting {
				kept = append(kept, entry)
				continue
			}
			if strategy == "MAXLEN" && s.length <= maxlen {
				stopDeleting = true
				kept = append(kept, entry)
				continue
			}
			if strategy == "MINID" && entry.ID.Compare(minID) >= 0 {
				stopDeleting = true
				kept = append(kept, entry)
				continue
			}

			canDelete := true
			if policy == StreamTrimAcked {
				if !s.isAckedByAllGroupsLocked(entry.ID) {
					canDelete = false
				}
			} else if policy == StreamTrimDelRef {
				s.cleanupEntryRefsLocked(entry.ID)
			}

			if canDelete {
				s.length--
				deleted++
				modified = true
			} else {
				kept = append(kept, entry)
			}
		}

		if modified {
			s.Blocks.Delete(b)
			if len(kept) > 0 {
				newNode := NewStreamNode()
				for _, e := range kept {
					newNode.AppendRaw(e)
				}
				s.Blocks.ReplaceOrInsert(StreamBlock{
					FirstID: kept[0].ID,
					Node:    newNode,
				})
			}
		}

		if nodeEligible {
			continue
		}
		break
	}

	if deleted > 0 {
		s.updateRecordedFirstEntryIDLocked()
	}
	return deleted
}

func (s *Stream) TrimMaxLenFull(maxlen int, approx bool, limit int, policy StreamTrimRefPolicy) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.streamTrimLocked("MAXLEN", maxlen, StreamID{}, approx, limit, policy)
}

func (s *Stream) TrimMinIDFull(threshold string, approx bool, limit int, policy StreamTrimRefPolicy) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	minID, err := ParseStreamID(threshold)
	if err != nil {
		return 0
	}
	return s.streamTrimLocked("MINID", 0, minID, approx, limit, policy)
}

func (s *Stream) TrimMaxLenWithPolicy(maxlen int, approx bool, policy StreamTrimRefPolicy) int {
	return s.TrimMaxLenFull(maxlen, approx, -1, policy)
}

func (s *Stream) TrimMinIDWithPolicy(threshold string, approx bool, policy StreamTrimRefPolicy) int {
	return s.TrimMinIDFull(threshold, approx, -1, policy)
}

// Range returns entries with IDs between startStr and endStr (inclusive).
// Use "-" for minimum and "+" for maximum. count=0 means unlimited.
// Prefix with '(' for exclusive range (e.g., "(1234-0" excludes 1234-0).
func (s *Stream) Range(startStr, endStr string, count int) []StreamEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.Blocks.Len() == 0 {
		return nil
	}

	var startID, endID StreamID
	exclusiveStart := false
	exclusiveEnd := false

	if startStr == "-" {
		startID = StreamID{}
	} else {
		if len(startStr) > 0 && startStr[0] == '(' {
			exclusiveStart = true
			startStr = startStr[1:]
		}
		parsed, err := ParseStreamIDWithDefaultSeq(startStr, 0)
		if err != nil {
			return nil
		}
		startID = parsed
	}

	if endStr == "+" {
		endID = StreamID{MS: math.MaxUint64, Seq: math.MaxUint64}
	} else {
		if len(endStr) > 0 && endStr[0] == '(' {
			exclusiveEnd = true
			endStr = endStr[1:]
		}
		parsed, err := ParseStreamIDWithDefaultSeq(endStr, math.MaxUint64)
		if err != nil {
			return nil
		}
		endID = parsed
	}

	var startBlock StreamBlock
	foundStartBlock := false
	s.Blocks.DescendLessOrEqual(StreamBlock{FirstID: startID}, func(b StreamBlock) bool {
		startBlock = b
		foundStartBlock = true
		return false
	})

	var result []StreamEntry
	visitBlock := func(b StreamBlock) bool {
		if b.FirstID.Compare(endID) > 0 {
			return false
		}
		it := b.Node.Iterator()
		for it.HasNext() {
			entry := it.Next()
			cmpStart := entry.ID.Compare(startID)
			if exclusiveStart && cmpStart == 0 {
				continue
			}
			if cmpStart < 0 {
				continue
			}
			cmpEnd := entry.ID.Compare(endID)
			if cmpEnd > 0 || (exclusiveEnd && cmpEnd == 0) {
				return false
			}
			result = append(result, entry)
			if count > 0 && len(result) >= count {
				return false
			}
		}
		return count == 0 || len(result) < count
	}

	if foundStartBlock {
		s.Blocks.AscendGreaterOrEqual(startBlock, visitBlock)
	} else {
		s.Blocks.Ascend(visitBlock)
	}

	return result
}

// TrimMaxLenApprox removes whole blocks until stream length is approximately <= maxlen.
func (s *Stream) TrimMaxLenApprox(maxlen int, limit int) int {
	return s.TrimMaxLenFull(maxlen, true, limit, StreamTrimKeepRef)
}

// TrimMinIDApprox removes whole blocks with IDs less than threshold.
func (s *Stream) TrimMinIDApprox(threshold string, limit int) int {
	return s.TrimMinIDFull(threshold, true, limit, StreamTrimKeepRef)
}

// TrimMaxLen removes oldest entries until stream length <= maxlen. Returns count removed.
func (s *Stream) TrimMaxLen(maxlen int) int {
	return s.TrimMaxLenFull(maxlen, false, 0, StreamTrimKeepRef)
}

// TrimMinID removes entries with IDs less than the given threshold. Returns count removed.
func (s *Stream) TrimMinID(threshold string) int {
	return s.TrimMinIDFull(threshold, false, 0, StreamTrimKeepRef)
}

func (s *Stream) RevRange(startStr, endStr string, count int) []StreamEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	var result []StreamEntry
	if s.Blocks.Len() == 0 {
		return result
	}

	startExclusive := false
	endExclusive := false
	if strings.HasPrefix(startStr, "(") {
		startExclusive = true
		startStr = strings.TrimPrefix(startStr, "(")
	}
	if strings.HasPrefix(endStr, "(") {
		endExclusive = true
		endStr = strings.TrimPrefix(endStr, "(")
	}

	startID := StreamID{MS: math.MaxUint64, Seq: math.MaxUint64}
	if startStr != "+" {
		if parsed, err := ParseStreamIDWithDefaultSeq(startStr, math.MaxUint64); err == nil {
			startID = parsed
		}
	}
	endID := StreamID{}
	if endStr != "-" {
		if parsed, err := ParseStreamIDWithDefaultSeq(endStr, 0); err == nil {
			endID = parsed
		}
	}

	s.Blocks.DescendLessOrEqual(StreamBlock{FirstID: startID}, func(b StreamBlock) bool {
		// Because a block's FirstID might be <= startID, its elements might be > startID.
		// We collect valid elements from this block and reverse them.
		var blockEntries []StreamEntry
		it := b.Node.Iterator()
		for it.HasNext() {
			entry := it.Next()
			cmpStart := entry.ID.Compare(startID)
			cmpEnd := entry.ID.Compare(endID)
			startOK := cmpStart < 0 || (!startExclusive && cmpStart == 0)
			endOK := cmpEnd > 0 || (!endExclusive && cmpEnd == 0)
			if startOK && endOK {
				blockEntries = append(blockEntries, entry)
			}
		}

		// Append in reverse order
		for i := len(blockEntries) - 1; i >= 0; i-- {
			result = append(result, blockEntries[i])
			if count > 0 && len(result) >= count {
				return false
			}
		}
		if b.FirstID.Compare(endID) < 0 {
			return false
		}
		return true
	})

	return result
}
