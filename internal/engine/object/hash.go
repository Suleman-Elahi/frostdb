package object

type Hash struct {
	Dict  map[string]string
	order []string // insertion-order tracking
	TTLs  map[string]int64 // field -> expiration time in Unix milliseconds

	// scanSnap caches a sorted (hash, field) snapshot for HSCAN cursor
	// iteration, invalidated (via scanSnapVer) when fields are added/removed.
	scanSnap    []scanEntry
	scanSnapVer uint64
	snapVer     uint64
}

func NewHash() *Hash {
	return &Hash{
		Dict: make(map[string]string),
	}
}

// Set adds or updates a field in the hash.
// Returns 1 if the field is a new field in the hash and value was set.
// Returns 0 if the field already exists in the hash and the value was updated.
func (h *Hash) Set(field, value string) int {
	_, exists := h.Dict[field]
	h.Dict[field] = value
	if exists {
		// Overwriting clears the TTL
		if h.TTLs != nil {
			delete(h.TTLs, field)
		}
		return 0
	}
	h.order = append(h.order, field)
	h.scanSnapVer++ // new field; invalidate scan snapshot
	return 1
}

// Get returns the value associated with field in the hash.
func (h *Hash) Get(field string) (string, bool) {
	val, ok := h.Dict[field]
	return val, ok
}

// Del removes the specified fields from the hash.
// Returns the number of fields that were removed from the hash, not including specified but non existing fields.
func (h *Hash) Del(fields ...string) int {
	count := 0
	for _, field := range fields {
		if _, exists := h.Dict[field]; exists {
			delete(h.Dict, field)
			if h.TTLs != nil {
				delete(h.TTLs, field)
			}
			count++
		}
	}
	if count > 0 {
		h.rebuildOrder()
		h.scanSnapVer++ // fields removed; invalidate scan snapshot
	}
	return count
}

// Len returns the number of fields contained in the hash.
func (h *Hash) Len() int {
	return len(h.Dict)
}

// GetAll returns all fields and values of the hash as an unordered map.
func (h *Hash) GetAll() map[string]string {
	return h.Dict
}

// Keys returns all field names in insertion order.
func (h *Hash) Keys() []string {
	if len(h.order) == 0 && len(h.Dict) > 0 {
		// Lazy migration for hashes loaded without order tracking (e.g. RDB import).
		h.order = make([]string, 0, len(h.Dict))
		for k := range h.Dict {
			h.order = append(h.order, k)
		}
	}
	return h.order
}

// Scan iterates hash fields using a value-based (hash) cursor over a cached
// sorted snapshot, returning up to count fields whose hash exceeds the cursor
// and the next cursor (0 when complete). This preserves the SCAN guarantee
// under concurrent modification while keeping a full scan at O(n log n) total.
func (h *Hash) Scan(cursor uint64, count int) ([]string, uint64) {
	if h.scanSnap == nil || h.snapVer != h.scanSnapVer {
		fields := make([]string, 0, len(h.Dict))
		for f := range h.Dict {
			fields = append(fields, f)
		}
		h.scanSnap = buildScanSnapshot(fields)
		h.snapVer = h.scanSnapVer
	}
	return scanFromSnapshot(h.scanSnap, cursor, count)
}

// GetTTL returns the expiration time in Unix milliseconds for the given field, or 0 if it has no TTL.
func (h *Hash) GetTTL(field string) int64 {
	if h.TTLs != nil {
		if ttl, exists := h.TTLs[field]; exists {
			return ttl
		}
	}
	return 0
}

// SetTTL sets the expiration time in Unix milliseconds for the given field.
func (h *Hash) SetTTL(field string, ttl int64) {
	if h.TTLs == nil {
		h.TTLs = make(map[string]int64)
	}
	h.TTLs[field] = ttl
}

// DelTTL removes the expiration time for the given field.
func (h *Hash) DelTTL(field string) {
	if h.TTLs != nil {
		delete(h.TTLs, field)
	}
}

// rebuildOrder removes deleted entries from the order slice.
func (h *Hash) rebuildOrder() {
	j := 0
	for _, k := range h.order {
		if _, ok := h.Dict[k]; ok {
			h.order[j] = k
			j++
		}
	}
	h.order = h.order[:j]
}

