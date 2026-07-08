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
		// Strings are immutable in Go, safe to share
		cp.Value = o.Value
	case TypeList:
		src := o.Value.(*List)
		dst := NewList()
		for _, val := range src.Snapshot() {
			dst.PushBack(val)
		}
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
		// Streams are complex — share reference (COPY on streams is rare)
		cp.Value = o.Value
	case TypeArray:
		// Arrays are safe to share reference for COPY
		cp.Value = o.Value
	default:
		cp.Value = o.Value
	}

	return cp
}
