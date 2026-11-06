package object

// Type represents the Redis object type
type Type uint8

const (
	TypeString Type = iota
	TypeList
	TypeSet
	TypeZSet
	TypeHash
	TypeStream
	TypeArray
)

// Encoding represents how the object is stored internally
type Encoding uint8

const (
	EncodingRaw Encoding = iota
	EncodingInt
	EncodingListpack
	EncodingRawForce // Explicitly forces "raw" output in OBJECT ENCODING even if len <= 44
)

// Object represents a stored value in FrostDB
type Object struct {
	Type       Type
	Encoding   Encoding
	Value      any
	ExpireAt   int64
	Version    uint64
	LastAccess uint32 // LRU: 24-bit clock. LFU: 16-bit decaytime | 8-bit counter
}

// StringBytes returns a read-only byte view of a binary-safe string object.
// Callers must never mutate the returned slice — Object.Value for TypeString
// is stored as an immutable-by-convention []byte owned by the Object.
func StringBytes(o *Object) ([]byte, bool) {
	if o == nil || o.Type != TypeString {
		return nil, false
	}
	if v, ok := o.Value.([]byte); ok {
		return v, true
	}
	return nil, false
}

// StringValue returns the string stored in a string object. This allocates
// because a Go string must own an immutable copy of the bytes. Prefer
// StringBytes on hot paths (GET, MGET, ...) where the RESP encoder can
// consume a []byte directly without allocating.
func StringValue(o *Object) (string, bool) {
	if o == nil || o.Type != TypeString {
		return "", false
	}
	if v, ok := o.Value.([]byte); ok {
		return string(v), true
	}
	return "", false
}

// DeepCopy creates a deep copy of the object.
func (o *Object) DeepCopy() *Object {
	cp := &Object{
		Type:       o.Type,
		Encoding:   o.Encoding,
		ExpireAt:   o.ExpireAt,
		Version:    o.Version,
		LastAccess: o.LastAccess,
	}

	switch o.Type {
	case TypeString:
		switch v := o.Value.(type) {
		case []byte:
			// Defensive copy: []byte is mutable, so COPY/DEBUG RELOAD-style
			// duplication must not let the two Objects alias the same
			// backing array.
			cp.Value = append([]byte(nil), v...)
		default:
			cp.Value = o.Value
		}
	case TypeList:
		src := o.Value.(*List)
		dst := NewList()
		for _, val := range src.Snapshot() {
			dst.PushBack(val)
		}
		dst.Quicklist = src.IsQuicklist()
		cp.Value = dst
	case TypeHash:
		src := o.Value.(*Hash)
		dst := NewHash()
		for field, value := range src.GetAll() {
			dst.Set(field, value)
		}
		cp.Value = dst
	case TypeSet:
		src := o.Value.(*Set)
		dst := NewSet()
		for _, m := range src.Members() {
			dst.Add(m)
		}
		cp.Value = dst
	case TypeZSet:
		src := o.Value.(*ZSet)
		dst := NewZSet()
		node := src.Skiplist.Head.Level[0].Forward
		for node != nil {
			dst.Add(node.Score, node.Member)
			node = node.Level[0].Forward
		}
		cp.Value = dst
	case TypeStream:
		src := o.Value.(*Stream)
		cp.Value = StreamFromSnapshot(src.Snapshot())
	case TypeArray:
		src := o.Value.(*Array)
		if b, err := src.MarshalJSON(); err == nil {
			if dst, err := ParseArrayJSON(b); err == nil {
				cp.Value = dst
				break
			}
		}
		cp.Value = o.Value
	default:
		cp.Value = o.Value
	}

	return cp
}
