// Package encoder writes RESP protocol responses.
// Design goals:
//   - Zero allocation for the common case (GET, SET, PING)
//   - Pool-backed buffers for variable-length responses
//   - Single Write() call per response (minimises syscalls)
package encoder

import (
	"io"
	"math"
	"sync"

	"frostdb/internal/resp/types"
)

// bufPool holds reusable byte slices for encoding responses.
// 512 bytes covers the vast majority of responses without reallocation.
var bufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 512)
		return &b
	},
}

// acquire gets a buffer from the pool, reset to zero length.
func acquire() []byte {
	bp := bufPool.Get().(*[]byte)
	return (*bp)[:0]
}

// release returns a buffer to the pool.
func release(b []byte) {
	if cap(b) > 64*1024 {
		// Don't pool very large buffers — they'd hold memory indefinitely.
		return
	}
	bp := &b
	bufPool.Put(bp)
}

// Encode writes val to w using a pooled buffer, minimising allocations.
// For arrays it builds the entire response in one buffer before writing,
// so the kernel sees a single write syscall per response.
func Encode(w io.Writer, val types.Value) error {
	buf := acquire()
	buf = appendValue(buf, val)
	_, err := w.Write(buf)
	release(buf)
	return err
}

// appendValue serialises val into buf and returns the extended slice.
func appendValue(buf []byte, val types.Value) []byte {
	switch val.Type {
	case types.TypeSimpleString:
		buf = append(buf, '+')
		buf = append(buf, val.Str...)
		buf = append(buf, '\r', '\n')

	case types.TypeError:
		buf = append(buf, '-')
		buf = append(buf, val.Str...)
		buf = append(buf, '\r', '\n')

	case types.TypeInteger:
		buf = append(buf, ':')
		buf = appendInt(buf, val.Num)
		buf = append(buf, '\r', '\n')

	case types.TypeBulkString:
		if val.IsNull {
			buf = append(buf, '$', '-', '1', '\r', '\n')
		} else {
			buf = append(buf, '$')
			buf = appendInt(buf, int64(len(val.Str)))
			buf = append(buf, '\r', '\n')
			buf = append(buf, val.Str...)
			buf = append(buf, '\r', '\n')
		}

	case types.TypeArray:
		if val.IsNull {
			buf = append(buf, '*', '-', '1', '\r', '\n')
		} else {
			buf = append(buf, '*')
			buf = appendInt(buf, int64(len(val.Array)))
			buf = append(buf, '\r', '\n')
			for i := range val.Array {
				buf = appendValue(buf, val.Array[i])
			}
		}

	case types.TypeMap:
		if val.IsNull {
			buf = append(buf, '%', '-', '1', '\r', '\n')
		} else {
			buf = append(buf, '%')
			buf = appendInt(buf, int64(len(val.Array)/2))
			buf = append(buf, '\r', '\n')
			for i := range val.Array {
				buf = appendValue(buf, val.Array[i])
			}
		}

	case types.TypeSet:
		if val.IsNull {
			buf = append(buf, '~', '-', '1', '\r', '\n')
		} else {
			buf = append(buf, '~')
			buf = appendInt(buf, int64(len(val.Array)))
			buf = append(buf, '\r', '\n')
			for i := range val.Array {
				buf = appendValue(buf, val.Array[i])
			}
		}

	case types.TypeBoolean:
		if val.Num == 1 {
			buf = append(buf, '#', 't', '\r', '\n')
		} else {
			buf = append(buf, '#', 'f', '\r', '\n')
		}

	case types.TypeDouble:
		buf = append(buf, ',')
		buf = append(buf, val.Str...)
		buf = append(buf, '\r', '\n')

	case types.TypeNull:
		buf = append(buf, '_', '\r', '\n')

	case types.TypeBigNumber:
		buf = append(buf, '(')
		buf = append(buf, val.Str...)
		buf = append(buf, '\r', '\n')

	case types.TypeVerbatim:
		buf = append(buf, '=')
		// The format + string length needs to be calculated
		buf = appendInt(buf, int64(len(val.Str)))
		buf = append(buf, '\r', '\n')
		buf = append(buf, val.Str...)
		buf = append(buf, '\r', '\n')
	}

	return buf
}

// appendInt appends the decimal representation of n to buf without allocating.
func appendInt(buf []byte, n int64) []byte {
	if n == 0 {
		return append(buf, '0')
	}
	if n > 0 {
		var tmp [20]byte
		pos := len(tmp)
		for n > 0 {
			pos--
			tmp[pos] = byte('0' + n%10)
			n /= 10
		}
		return append(buf, tmp[pos:]...)
	}
	if n == math.MinInt64 {
		return append(buf, '-', '9', '2', '2', '3', '3', '7', '2', '0', '3', '6', '8', '5', '4', '7', '7', '5', '8', '0', '8')
	}
	buf = append(buf, '-')
	n = -n
	var tmp [20]byte
	pos := len(tmp)
	for n > 0 {
		pos--
		tmp[pos] = byte('0' + n%10)
		n /= 10
	}
	return append(buf, tmp[pos:]...)
}
