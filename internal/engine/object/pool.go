package object

import (
	"strconv"
	"sync"
)

var stringObjectPool = sync.Pool{
	New: func() any {
		return &Object{Type: TypeString, Encoding: EncodingRaw}
	},
}

// AcquireStringObject returns a pooled Object pre-configured as a string.
// value should be a []byte the caller will not mutate afterward — it is
// stored directly with no copy, which is safe because callers on the write
// path (SET, MSET, ...) always pass a freshly-owned buffer (the RESP parser
// never hands back a slice into a reused buffer; see parser.go's bulkPool
// usage) or a freshly-formatted numeric result (FormatInt/FormatFloat).
// A string argument is accepted for callers that produce values via
// strconv.Format* and is converted to []byte internally.
// If value can be parsed as a 64-bit signed integer, the encoding is set to
// EncodingInt to match Redis integer-encoding behaviour.
func AcquireStringObject(value any) *Object {
	obj := stringObjectPool.Get().(*Object)
	obj.Type = TypeString
	obj.ExpireAt = 0
	obj.Version = 0

	switch v := value.(type) {
	case []byte:
		obj.Value = v
		obj.Encoding = detectStringEncodingBytes(v)
	case string:
		// Callers that produce values via strconv.FormatInt/FormatFloat pass
		// a string; convert to []byte for uniform storage. This conversion
		// is unavoidable since a string's backing array cannot be reused.
		b := []byte(v)
		obj.Value = b
		obj.Encoding = detectStringEncodingBytes(b)
	default:
		obj.Value = value
		obj.Encoding = EncodingRaw
	}
	return obj
}

// AcquireRawStringObject is like AcquireStringObject but always forces
// EncodingRawForce — used by commands like APPEND, SETBIT, SETRANGE where Redis
// always returns a raw string (not embstr) even if the value could be integer-encoded.
// value should be a freshly-owned []byte the caller will not mutate afterward.
func AcquireRawStringObject(value []byte) *Object {
	obj := stringObjectPool.Get().(*Object)
	obj.Type = TypeString
	obj.Encoding = EncodingRawForce
	obj.Value = value
	obj.ExpireAt = 0
	obj.Version = 0
	return obj
}

// AcquireRawStringObjectBytes is retained as an alias for callers that
// already hold a []byte, matching the pre-migration API name.
func AcquireRawStringObjectBytes(value []byte) *Object {
	return AcquireRawStringObject(value)
}

// detectStringEncodingBytes returns EncodingInt when b can be parsed as a
// 64-bit signed integer (matching Redis OBJ_ENCODING_INT semantics),
// otherwise Raw. Operates directly on []byte to avoid a string conversion.
func detectStringEncodingBytes(b []byte) Encoding {
	if len(b) == 0 || len(b) > 20 { // max int64 is 19 digits + optional '-'
		return EncodingRaw
	}
	if _, err := strconv.ParseInt(string(b), 10, 64); err == nil {
		return EncodingInt
	}
	return EncodingRaw
}

func ReleaseObject(obj *Object) {
	if obj.Type == TypeString {
		obj.Value = nil
		stringObjectPool.Put(obj)
	}
}
