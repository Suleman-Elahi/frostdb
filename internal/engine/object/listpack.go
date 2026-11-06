package object

import (
	"encoding/binary"
	"math"
	"sync/atomic"
)

var streamNodeMaxEntries atomic.Int64

func init() {
	streamNodeMaxEntries.Store(100) // Default like Redis
}

// GetStreamNodeMaxEntries returns the max entries per stream node.
func GetStreamNodeMaxEntries() int64 {
	return streamNodeMaxEntries.Load()
}

// SetStreamNodeMaxEntries sets the max entries per stream node.
func SetStreamNodeMaxEntries(n int64) {
	streamNodeMaxEntries.Store(n)
}

// StreamNode represents a block of packed stream entries
type StreamNode struct {
	Data  []byte
	Count uint16
}

func NewStreamNode() *StreamNode {
	return &StreamNode{
		Data:  make([]byte, 0, 4096),
		Count: 0,
	}
}

// Append encodes a StreamEntry and appends it to the node.
// Returns true if appended, false if the node is full.
func (n *StreamNode) Append(entry StreamEntry) bool {
	maxEntries := GetStreamNodeMaxEntries()
	if maxEntries > 0 && int64(n.Count) >= maxEntries {
		return false
	}
	if maxEntries <= 0 && len(n.Data) > 4096 && n.Count > 0 {
		return false
	}
	n.AppendRaw(entry)
	return true
}

// AppendRaw encodes and appends a StreamEntry unconditionally without checking node capacity.
func (n *StreamNode) AppendRaw(entry StreamEntry) {
	var header [18]byte
	binary.BigEndian.PutUint64(header[0:8], uint64(entry.ID.MS))
	binary.BigEndian.PutUint64(header[8:16], uint64(entry.ID.Seq))
	
	fieldsLen := len(entry.Fields)
	if fieldsLen > math.MaxUint16 {
		fieldsLen = math.MaxUint16
	}
	binary.BigEndian.PutUint16(header[16:18], uint16(fieldsLen))
	
	n.Data = append(n.Data, header[:]...)

	var lenBuf [4]byte
	for i := 0; i < fieldsLen; i++ {
		f := entry.Fields[i]
		
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(f.Name)))
		n.Data = append(n.Data, lenBuf[:]...)
		n.Data = append(n.Data, f.Name...)

		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(f.Value)))
		n.Data = append(n.Data, lenBuf[:]...)
		n.Data = append(n.Data, f.Value...)
	}

	n.Count++
}

// Iterator for StreamNode
type StreamNodeIterator struct {
	node   *StreamNode
	offset int
	count  uint16
}

func (n *StreamNode) Iterator() *StreamNodeIterator {
	return &StreamNodeIterator{
		node:   n,
		offset: 0,
		count:  0,
	}
}

func (it *StreamNodeIterator) HasNext() bool {
	return it.count < it.node.Count && it.offset < len(it.node.Data)
}

func (it *StreamNodeIterator) Next() StreamEntry {
	data := it.node.Data
	ms := binary.BigEndian.Uint64(data[it.offset : it.offset+8])
	seq := binary.BigEndian.Uint64(data[it.offset+8 : it.offset+16])
	fieldsLen := int(binary.BigEndian.Uint16(data[it.offset+16 : it.offset+18]))
	it.offset += 18

	fields := make([]StreamField, fieldsLen)
	for i := 0; i < fieldsLen; i++ {
		nameLen := int(binary.BigEndian.Uint32(data[it.offset : it.offset+4]))
		it.offset += 4
		name := string(data[it.offset : it.offset+nameLen])
		it.offset += nameLen

		valLen := int(binary.BigEndian.Uint32(data[it.offset : it.offset+4]))
		it.offset += 4
		val := string(data[it.offset : it.offset+valLen])
		it.offset += valLen

		fields[i] = StreamField{Name: name, Value: val}
	}

	it.count++
	return StreamEntry{
		ID:     StreamID{MS: ms, Seq: seq},
		Fields: fields,
	}
}

// Len returns the number of entries in this node.
func (n *StreamNode) Len() int {
	return int(n.Count)
}

// FirstEntry returns the first entry in this node.
func (n *StreamNode) FirstEntry() StreamEntry {
	it := n.Iterator()
	if it.HasNext() {
		return it.Next()
	}
	return StreamEntry{}
}

// LastEntry returns the last entry in this node.
func (n *StreamNode) LastEntry() StreamEntry {
	it := n.Iterator()
	var last StreamEntry
	for it.HasNext() {
		last = it.Next()
	}
	return last
}

// TrimFront removes the first `count` entries from the node by rebuilding the data.
func (n *StreamNode) TrimFront(count int) {
	if count <= 0 || count >= int(n.Count) {
		return
	}
	it := n.Iterator()
	// Skip `count` entries
	for i := 0; i < count && it.HasNext(); i++ {
		it.Next()
	}
	// Remaining data starts at it.offset
	remaining := n.Data[it.offset:]
	n.Data = make([]byte, len(remaining))
	copy(n.Data, remaining)
	n.Count -= uint16(count)
}

// TrimBeforeID removes entries with ID < minID. Returns number removed.
func (n *StreamNode) TrimBeforeID(minID StreamID) int {
	it := n.Iterator()
	removed := 0
	for it.HasNext() {
		entry := it.Next()
		if entry.ID.Compare(minID) >= 0 {
			// Keep from previous entry's end offset
			keepOffset := it.offset
			// We need to recalculate: the iterator already advanced past this entry
			// So we rebuild by skipping `removed` entries
			n.TrimFront(removed)
			_ = keepOffset
			return removed
		}
		removed++
	}
	// All entries are below minID
	n.Data = n.Data[:0]
	n.Count = 0
	return removed
}

// DeleteEntry removes a single entry by ID. Returns true if found and removed.
func (n *StreamNode) DeleteEntry(id StreamID) bool {
	it := n.Iterator()
	idx := 0
	for it.HasNext() {
		entry := it.Next()
		if entry.ID.Compare(id) == 0 {
			// Rebuild data without this entry
			n.rebuildWithout(idx)
			return true
		}
		idx++
	}
	return false
}

// rebuildWithout rebuilds the node data excluding the entry at the given index.
func (n *StreamNode) rebuildWithout(skipIdx int) {
	it := &StreamNodeIterator{node: n, offset: 0, count: 0}
	var newData []byte
	idx := 0
	for it.HasNext() {
		startOff := it.offset
		it.Next()
		endOff := it.offset
		if idx != skipIdx {
			newData = append(newData, n.Data[startOff:endOff]...)
		}
		idx++
	}
	n.Data = newData
	n.Count--
}
