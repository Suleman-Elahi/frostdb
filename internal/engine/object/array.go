package object

import (
	"encoding/json"
	"sync"
)

var (
	DefaultSliceSize  int64 = 4096
	DefaultSparseKmax int   = 10
	DefaultSparseKmin int   = 5
)

const (
	SuperdirThreshold int64 = 2048 // slices >= this use superdir map
	EmptySlotSentinel string = "\x00\x00_FROST_EMPTY_SLOT_\x00\x00"
)

// Array implements a slice-based array type (Redis Array).
type Array struct {
	Mu        sync.RWMutex
	sliceSize int64
	sparseKmax int
	sparseKmin int
	count     int64  // number of populated slots
	len       int64  // max index + 1
	insertIdx int64  // cursor for ARINSERT/ARRING (-1 = terminal)
	explicitCursor bool // true if insertIdx was explicitly set by ARSEEK
	arrag     int64  // ring buffer size (0 = not ring mode)
	slices    []*Slice
	// For slices beyond the flat directory, use a map (superdir)
	dirMap    map[int64]*Slice
}

// Slice represents one slice of the array covering indices [sliceID*sliceSize, (sliceID+1)*sliceSize)
type Slice struct {
	mu      sync.RWMutex
	dense   []string // dense representation (nil if sparse or empty)
	sparse  map[int64]string // sparse representation
	active  int64 // number of populated slots in this slice
}

// NewArray creates a new Array with the given slice size and sparsity limits.
func NewArray(sliceSize int64, kmax, kmin int) *Array {
	return &Array{
		sliceSize:  sliceSize,
		sparseKmax: kmax,
		sparseKmin: kmin,
		slices:     make([]*Slice, 0),
		dirMap:     make(map[int64]*Slice),
	}
}

func (a *Array) getSlice(sliceID int64) *Slice {
	if sliceID < SuperdirThreshold {
		if sliceID >= int64(len(a.slices)) {
			return nil
		}
		return a.slices[sliceID]
	}
	return a.dirMap[sliceID]
}

func (a *Array) getOrCreateSlice(sliceID int64) *Slice {
	if sliceID < SuperdirThreshold {
		if sliceID >= int64(len(a.slices)) {
			newSlices := make([]*Slice, sliceID+1)
			copy(newSlices, a.slices)
			a.slices = newSlices
		}
		if a.slices[sliceID] == nil {
			a.slices[sliceID] = &Slice{sparse: make(map[int64]string)}
		}
		return a.slices[sliceID]
	}
	s, ok := a.dirMap[sliceID]
	if !ok {
		s = &Slice{sparse: make(map[int64]string)}
		a.dirMap[sliceID] = s
	}
	return s
}

// Set sets a value at a given index.
func (a *Array) Set(index int64, value string) int64 {
	sliceID := index / a.sliceSize
	off := index % a.sliceSize

	s := a.getOrCreateSlice(sliceID)

	needsPromote := false
	if s.dense == nil && s.active >= int64(a.sparseKmax-1) {
		_, sparseExists := s.sparse[off]
		if s.active >= int64(a.sparseKmax) || (!sparseExists && s.active >= int64(a.sparseKmax-1)) {
			needsPromote = true
		}
	}

	s.mu.Lock()

	oldExists := s.hasLocked(off)

	if needsPromote && s.dense == nil {
		sz := a.sliceSize
		dense := make([]string, sz)
		for i := range dense {
			dense[i] = EmptySlotSentinel
		}
		for o, v := range s.sparse {
			dense[o] = v
		}
		s.sparse = nil
		s.dense = dense
	}

	if s.dense != nil {
		if !oldExists {
			a.count++
			s.active++
		}
		s.dense[off] = value
	} else {
		if oldExists {
			s.sparse[off] = value
		} else {
			s.sparse[off] = value
			a.count++
			s.active++
		}
	}
	s.mu.Unlock()

	if index >= a.len {
		a.len = index + 1
	}

	if oldExists {
		return 0
	}
	return 1
}

func (s *Slice) hasLocked(off int64) bool {
	if s.dense != nil {
		return s.dense[off] != EmptySlotSentinel
	}
	_, ok := s.sparse[off]
	return ok
}

// SetContiguous writes multiple consecutive values starting at index.
func (a *Array) SetContiguous(start int64, values []string) int64 {
	sliceID := start / a.sliceSize
	off := start % a.sliceSize

	s := a.getOrCreateSlice(sliceID)

	// Check if we need to promote to dense (must do before locking)
	needsPromote := false
	if s.dense == nil && int(s.active)+len(values) >= a.sparseKmax {
		needsPromote = true
	}

	s.mu.Lock()

	if needsPromote {
		if s.dense == nil {
			sz := a.sliceSize
			dense := make([]string, sz)
			for i := range dense {
				dense[i] = EmptySlotSentinel
			}
			for off, v := range s.sparse {
				dense[off] = v
			}
			s.sparse = nil
			s.dense = dense
		}
	}

	var newlyFilled int64
	for i, v := range values {
		targetOff := off + int64(i)
		if targetOff >= a.sliceSize {
			a.count += newlyFilled
			s.active += newlyFilled
			s.mu.Unlock()
			var extraFilled int64
			for j := i; j < len(values); j++ {
				idx := start + int64(j)
				extraFilled += a.Set(idx, values[j])
			}
			end := start + int64(len(values))
			if end > a.len {
				a.len = end
			}
			return newlyFilled + extraFilled
		}

		if !s.hasLocked(targetOff) {
			newlyFilled++
		}
		if s.dense != nil {
			s.dense[targetOff] = v
		} else {
			s.sparse[targetOff] = v
		}
	}

	a.count += newlyFilled
	s.active += newlyFilled
	s.mu.Unlock()

	end := start + int64(len(values))
	if end > a.len {
		a.len = end
	}
	return newlyFilled
}



// Get returns the value at the given index, and whether it exists.
func (a *Array) Get(index int64) (string, bool) {
	sliceID := index / a.sliceSize
	off := index % a.sliceSize

	s := a.getSlice(sliceID)
	if s == nil {
		return "", false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.dense != nil {
		v := s.dense[off]
		if v == EmptySlotSentinel {
			return "", false
		}
		return v, true
	}
	v, ok := s.sparse[off]
	return v, ok
}

// Del removes the value at the given index. Returns true if deleted.
func (a *Array) Del(index int64) bool {
	sliceID := index / a.sliceSize
	off := index % a.sliceSize

	s := a.getSlice(sliceID)
	if s == nil {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.dense != nil {
		if s.dense[off] == EmptySlotSentinel {
			return false
		}
		s.dense[off] = EmptySlotSentinel
		s.active--
		a.count--
		if s.active == 0 {
			a.removeSlice(sliceID)
		} else if s.active < int64(a.sparseKmin) && s.active > 0 {
			a.demoteToSparse(s)
		}
		return true
	}

	if _, ok := s.sparse[off]; !ok {
		return false
	}
	delete(s.sparse, off)
	s.active--
	a.count--
	if s.active == 0 {
		a.removeSlice(sliceID)
		if a.count == 0 {
			a.len = 0
		}
	}
	return true
}

func (a *Array) removeSlice(sliceID int64) {
	if sliceID < SuperdirThreshold {
		if sliceID < int64(len(a.slices)) {
			a.slices[sliceID] = nil
		}
	} else {
		delete(a.dirMap, sliceID)
	}
}

func (a *Array) demoteToSparse(s *Slice) {
	sparse := make(map[int64]string, s.active)
	for i, v := range s.dense {
		if v != EmptySlotSentinel {
			sparse[int64(i)] = v
		}
	}
	s.dense = nil
	s.sparse = sparse
}

// Count returns the number of populated slots.
func (a *Array) Count() int64 { return a.count }

// Len returns the max populated index + 1.
func (a *Array) Len() int64 { return a.len }

// InsertIdx returns the current insert cursor.
func (a *Array) InsertIdx() int64 { return a.insertIdx }

// SetInsertIdx sets the insert cursor.
func (a *Array) SetInsertIdx(idx int64) {
	a.insertIdx = idx
	a.explicitCursor = true
}

// RingSize returns the ring buffer size (0 = not ring mode).
func (a *Array) RingSize() int64 { return a.arrag }

// SetRingSize sets the ring buffer size.
func (a *Array) SetRingSize(n int64) {
	if a.arrag == n && a.count <= n {
		return
	}
	a.reshapeRing(n)
}

func (a *Array) reshapeRing(newSize int64) {
	if a.count == 0 {
		a.arrag = newSize
		return
	}
	
	if a.explicitCursor && a.len <= newSize {
		a.arrag = newSize
		return
	}
	
	capacity := a.arrag
	if capacity <= 0 {
		capacity = a.len
	}
	
	var collected []string
	idx := a.insertIdx - 1
	slotsChecked := int64(0)
	
	for slotsChecked < capacity {
		mappedIdx := idx
		if a.arrag > 0 {
			mappedIdx = mappedIdx % a.arrag
			if mappedIdx < 0 {
				mappedIdx += a.arrag
			}
		} else if mappedIdx < 0 {
			break
		}
		
		v, ok := a.Get(mappedIdx)
		if !ok {
			break
		}
		collected = append(collected, v)
		idx--
		slotsChecked++
	}
	
	keep := int64(len(collected))
	if keep > newSize {
		keep = newSize
	}
	collected = collected[:keep]
	
	reverseSlice(collected)
	
	a.slices = nil
	a.dirMap = make(map[int64]*Slice)
	a.count = 0
	a.len = 0
	
	for i, v := range collected {
		a.Set(int64(i), v)
	}
	a.insertIdx = int64(len(collected))
	a.arrag = newSize
}

// Scan returns elements in [start, end] range with optional limit.
// Returns a slice of [2]string{index_str, value}.
func (a *Array) Scan(start, end int64, limit int64) [][2]string {
	reverse := start > end
	lo, hi := start, end
	if reverse {
		lo, hi = end, start
	}

	var result [][2]string
	var emitted int64

	if limit <= 0 {
		limit = 1<<63 - 1
	}

	// Collect only existing slice IDs
	var existingSlices []int64
	flatLimit := int64(len(a.slices))
	for sid := int64(0); sid < flatLimit; sid++ {
		if a.slices[sid] != nil {
			existingSlices = append(existingSlices, sid)
		}
	}
	for sid := range a.dirMap {
		existingSlices = append(existingSlices, sid)
	}

	if reverse {
		sortReverse(existingSlices)
	} else {
		sortForward(existingSlices)
	}

	for _, sid := range existingSlices {
		sliceStart := sid * a.sliceSize
		sliceEnd := sliceStart + a.sliceSize - 1

		if sliceEnd < lo || sliceStart > hi {
			continue
		}

		s := a.getSlice(sid)
		if s == nil {
			continue
		}

		rangeLo := lo
		if rangeLo < sliceStart {
			rangeLo = sliceStart
		}
		rangeHi := hi
		if rangeHi > sliceEnd {
			rangeHi = sliceEnd
		}

		s.mu.RLock()
		if s.dense != nil {
			if reverse {
				for i := rangeHi - sliceStart; i >= rangeLo-sliceStart && emitted < limit; i-- {
					if s.dense[i] != EmptySlotSentinel {
						result = append(result, [2]string{formatIdx(sliceStart + i), s.dense[i]})
						emitted++
					}
				}
			} else {
				for i := rangeLo - sliceStart; i <= rangeHi-sliceStart && emitted < limit; i++ {
					if s.dense[i] != EmptySlotSentinel {
						result = append(result, [2]string{formatIdx(sliceStart + i), s.dense[i]})
						emitted++
					}
				}
			}
		} else {
			indices := make([]int64, 0, len(s.sparse))
			for off := range s.sparse {
				idx := sliceStart + off
				if idx >= rangeLo && idx <= rangeHi {
					indices = append(indices, idx)
				}
			}
			if reverse {
				sortReverse(indices)
			} else {
				sortForward(indices)
			}
			for _, idx := range indices {
				if emitted >= limit {
					break
				}
				result = append(result, [2]string{formatIdx(idx), s.sparse[idx-sliceStart]})
				emitted++
			}
		}
		s.mu.RUnlock()

		if emitted >= limit {
			break
		}
	}

	return result
}

// Grep performs a pattern search in [start, end] range.
// matchType: "EXACT" or "MATCH" (substring)
func (a *Array) Grep(start, end int64, matchType, pattern string, nocase bool, withValues bool, limit int64) [][2]string {
	_ = start > end // handled by Scan sorting

	var result [][2]string
	var emitted int64

	if limit <= 0 {
		limit = 1<<63 - 1
	}

	items := a.Scan(start, end, 0)
	for _, item := range items {
		if emitted >= limit {
			break
		}
		val := item[1]
		match := false
		if matchType == "EXACT" {
			if nocase {
				match = stringsEqualFold(val, pattern)
			} else {
				match = val == pattern
			}
		} else {
			if nocase {
				match = containsFold(val, pattern)
			} else {
				match = contains(val, pattern)
			}
		}
		if match {
			if withValues {
				result = append(result, item)
			} else {
				result = append(result, [2]string{item[0], ""})
			}
			emitted++
		}
	}
	return result
}

// GrepRE performs a regex search (delegated to commands for now).
// Arop performs aggregate operations.
func (a *Array) Arop(start, end int64, op string, arg string) int64 {
	reverse := start > end
	lo, hi := start, end
	if reverse {
		lo, hi = end, start
	}

	items := a.Scan(lo, hi, 0)
	switch op {
	case "USED":
		return int64(len(items))
	case "MATCH":
		count := int64(0)
		for _, item := range items {
			if item[1] == arg {
				count++
			}
		}
		return count
	}
	return 0
}

// DelRange removes elements in [lo, hi].
func (a *Array) DelRange(lo, hi int64) int64 {
	if lo > hi {
		lo, hi = hi, lo
	}
	var deleted int64
	items := a.Scan(lo, hi, 0)
	for _, item := range items {
		idx, ok := parseIdx(item[0])
		if !ok {
			continue
		}
		if a.Del(idx) {
			deleted++
		}
	}
	return deleted
}

// RingInsert inserts a value at the next ring position, wrapping around.
func (a *Array) RingInsert(maxlen int64, value string) int64 {
	a.explicitCursor = false
	pos := a.insertIdx % maxlen
	_ = a.Del(pos)
	a.insertIdx = pos
	result := a.insertIdx
	_ = a.Set(pos, value)
	a.insertIdx = pos + 1
	return result
}

// Insert inserts at the next free position.
func (a *Array) Insert(value string) int64 {
	a.explicitCursor = false
	pos := a.insertIdx
	a.insertIdx = pos + 1
	_ = a.Set(pos, value)
	return pos
}

// Info returns array metadata as a map.
func (a *Array) Info() map[string]int64 {
	a.Mu.RLock()
	defer a.Mu.RUnlock()

	denseSlices := int64(0)
	sparseSlices := int64(0)
	dirSize := int64(0)

	for _, s := range a.slices {
		if s != nil {
			dirSize++
			s.mu.RLock()
			if s.dense != nil {
				denseSlices++
			} else {
				sparseSlices++
			}
			s.mu.RUnlock()
		}
	}
	for _, s := range a.dirMap {
		if s != nil {
			dirSize++
			s.mu.RLock()
			if s.dense != nil {
				denseSlices++
			} else {
				sparseSlices++
			}
			s.mu.RUnlock()
		}
	}

	avgDense := int64(0)
	if denseSlices > 0 {
		avgDense = a.sliceSize
	}

	return map[string]int64{
		"count":         a.count,
		"len":           a.len,
		"insert-idx":    a.insertIdx,
		"dense-slices":  denseSlices,
		"sparse-slices": sparseSlices,
		"directory-size": dirSize,
		"avg-dense-size": avgDense,
	}
}

// LastItems returns the N most recently inserted items.
func (a *Array) LastItems(n int64, rev bool) []string {
	if a.count == 0 {
		return nil
	}

	capacity := a.arrag
	if capacity <= 0 {
		capacity = a.len
	}

	if n > a.count {
		n = a.count
	}

	var result []string
	
	idx := a.insertIdx - 1
	slotsChecked := int64(0)
	
	for int64(len(result)) < n && slotsChecked < capacity {
		mappedIdx := idx
		if capacity > 0 {
			mappedIdx = mappedIdx % capacity
			if mappedIdx < 0 {
				mappedIdx += capacity
			}
		} else if mappedIdx < 0 {
			break
		}
		
		if v, ok := a.Get(mappedIdx); ok {
			result = append(result, v)
		}
		idx--
		slotsChecked++
	}

	if !rev {
		reverseSlice(result)
	}

	return result
}

// RDBKeyPrefix is the hash field prefix used to mark an RDB hash as a serialized array.
const RDBKeyPrefix = "__frost_array__"

// arrayRDBMeta holds the metadata for array RDB serialization.
type arrayRDBMeta struct {
	InsertIdx int64 `json:"i"`
	Explicit  bool  `json:"e"`
	RingSize  int64 `json:"r"`
}

// ToRDBHash converts the array to a hash map suitable for RDB export.
// The metadata is stored under a special key with the RDBKeyPrefix.
func (a *Array) ToRDBHash() map[string][]byte {
	a.Mu.RLock()
	defer a.Mu.RUnlock()

	meta := arrayRDBMeta{
		InsertIdx: a.insertIdx,
		Explicit:  a.explicitCursor,
		RingSize:  a.arrag,
	}
	metaJSON, _ := json.Marshal(meta)

	hash := make(map[string][]byte, a.count+1)
	hash[RDBKeyPrefix] = metaJSON

	// Collect all values
	for sid := int64(0); sid < int64(len(a.slices)); sid++ {
		s := a.slices[sid]
		if s == nil {
			continue
		}
		sliceStart := sid * a.sliceSize
		s.mu.RLock()
		if s.dense != nil {
			for i, v := range s.dense {
				if v != EmptySlotSentinel {
					hash[formatIdx(sliceStart+int64(i))] = []byte(v)
				}
			}
		} else {
			for off, v := range s.sparse {
				hash[formatIdx(sliceStart+off)] = []byte(v)
			}
		}
		s.mu.RUnlock()
	}
	for sid, s := range a.dirMap {
		if s == nil {
			continue
		}
		sliceStart := sid * a.sliceSize
		s.mu.RLock()
		if s.dense != nil {
			for i, v := range s.dense {
				if v != EmptySlotSentinel {
					hash[formatIdx(sliceStart+int64(i))] = []byte(v)
				}
			}
		} else {
			for off, v := range s.sparse {
				hash[formatIdx(sliceStart+off)] = []byte(v)
			}
		}
		s.mu.RUnlock()
	}

	return hash
}

// FromRDBHash reconstructs an Array from a hash loaded from RDB.
// Returns nil if the hash does not contain the array metadata marker.
func FromRDBHash(hash map[string][]byte) *Array {
	metaBytes, ok := hash[RDBKeyPrefix]
	if !ok {
		return nil
	}

	var meta arrayRDBMeta
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return nil
	}

	arr := NewArray(DefaultSliceSize, DefaultSparseKmax, DefaultSparseKmin)
	arr.insertIdx = meta.InsertIdx
	arr.explicitCursor = meta.Explicit
	arr.arrag = meta.RingSize

	for k, v := range hash {
		if k == RDBKeyPrefix {
			continue
		}
		idx, ok := parseIdx(k)
		if !ok {
			continue // skip malformed index keys
		}
		arr.Set(idx, string(v))
	}

	return arr
}

// arrayJSONData is the JSON structure for full array serialization (used by checkpoint/MarshalJSON).
type arrayJSONData struct {
	InsertIdx int64            `json:"insertIdx"`
	Explicit  bool             `json:"explicit"`
	RingSize  int64            `json:"ringSize"`
	Values    map[int64]string `json:"values"`
}

// MarshalJSON serializes the array to JSON for checkpoint persistence.
func (a *Array) MarshalJSON() ([]byte, error) {
	a.Mu.RLock()
	defer a.Mu.RUnlock()

	data := arrayJSONData{
		InsertIdx: a.insertIdx,
		Explicit:  a.explicitCursor,
		RingSize:  a.arrag,
		Values:    make(map[int64]string, a.count),
	}

	for sid := int64(0); sid < int64(len(a.slices)); sid++ {
		s := a.slices[sid]
		if s == nil {
			continue
		}
		sliceStart := sid * a.sliceSize
		s.mu.RLock()
		if s.dense != nil {
			for i, v := range s.dense {
				if v != EmptySlotSentinel {
					data.Values[sliceStart+int64(i)] = v
				}
			}
		} else {
			for off, v := range s.sparse {
				data.Values[sliceStart+off] = v
			}
		}
		s.mu.RUnlock()
	}
	for sid, s := range a.dirMap {
		if s == nil {
			continue
		}
		sliceStart := sid * a.sliceSize
		s.mu.RLock()
		if s.dense != nil {
			for i, v := range s.dense {
				if v != EmptySlotSentinel {
					data.Values[sliceStart+int64(i)] = v
				}
			}
		} else {
			for off, v := range s.sparse {
				data.Values[sliceStart+off] = v
			}
		}
		s.mu.RUnlock()
	}

	return json.Marshal(data)
}

// ParseArrayJSON creates an Array from JSON data (used by checkpoint deserialization).
func ParseArrayJSON(b []byte) (*Array, error) {
	var data arrayJSONData
	if err := json.Unmarshal(b, &data); err != nil {
		return nil, err
	}

	arr := NewArray(DefaultSliceSize, DefaultSparseKmax, DefaultSparseKmin)
	arr.insertIdx = data.InsertIdx
	arr.explicitCursor = data.Explicit
	arr.arrag = data.RingSize

	for idx, val := range data.Values {
		arr.Set(idx, val)
	}

	return arr, nil
}

// SetRawInsertIdx sets the insert index (used during deserialization).
func (a *Array) SetRawInsertIdx(idx int64) {
	a.insertIdx = idx
}

// SetRawRingSize sets the ring buffer size directly (used during deserialization).
func (a *Array) SetRawRingSize(sz int64) {
	a.arrag = sz
}
