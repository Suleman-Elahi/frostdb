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
// If value is a string that can be parsed as a 64-bit signed integer, the
// encoding is set to EncodingInt to match Redis integer-encoding behaviour.
// This allows OBJECT ENCODING to return "int" and assert_encoding to pass.
func AcquireStringObject(value any) *Object {
	obj := stringObjectPool.Get().(*Object)
	obj.Type = TypeString
	obj.ExpireAt = 0
	obj.Version = 0

	switch v := value.(type) {
	case string:
		obj.Value = v
		obj.Encoding = detectStringEncoding(v)
	default:
		obj.Value = value
		obj.Encoding = EncodingRaw
	}
	return obj
}

// AcquireRawStringObject is like AcquireStringObject but always forces
// EncodingRawForce — used by commands like APPEND, SETBIT, SETRANGE where Redis
// always returns a raw string (not embstr) even if the value could be integer-encoded.
func AcquireRawStringObject(value string) *Object {
	obj := stringObjectPool.Get().(*Object)
	obj.Type = TypeString
	obj.Encoding = EncodingRawForce
	obj.Value = value
	obj.ExpireAt = 0
	obj.Version = 0
	return obj
}

// detectStringEncoding returns EncodingInt when s can be parsed as a 64-bit
// signed integer (matching Redis OBJ_ENCODING_INT semantics), otherwise Raw.
func detectStringEncoding(s string) Encoding {
	if len(s) == 0 || len(s) > 20 { // max int64 is 19 digits + optional '-'
		return EncodingRaw
	}
	if _, err := strconv.ParseInt(s, 10, 64); err == nil {
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
