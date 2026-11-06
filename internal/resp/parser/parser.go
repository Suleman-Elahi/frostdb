// Package parser reads RESP protocol from a connection.
// Design goals:
//   - Large read buffer (32 KB) to amortise syscalls across pipelined commands
//   - Pool-backed bulk-string buffers to avoid per-request allocation
//   - Fast inline integer parsing (no strconv, no string conversion)
//   - Zero-copy line reading via bufio.Reader.ReadSlice
package parser

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"sync"

	"frostdb/internal/resp/types"
)

var (
	ErrInvalidSyntax = errors.New("invalid RESP syntax")
	ErrInvalidLength = errors.New("RESP length out of range")
)

const (
	// readBufSize is the per-connection read buffer. 32 KB amortises syscalls
	// for pipelined workloads.
	readBufSize = 32 * 1024

	// MaxBulkStringLength and MaxArrayLength bound a single client frame before
	// any payload or array allocation. They prevent malformed length prefixes
	// from turning into process-wide memory exhaustion.
	MaxBulkStringLength int64 = 64 * 1024 * 1024
	MaxArrayLength      int64 = 1_000_000
)

// bulkPool holds reusable byte slices for bulk string payloads.
// Most Redis keys/values fit in 256 bytes; the pool avoids GC churn.
var bulkPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 256)
		return &b
	},
}

// Parser reads RESP values from a connection.
type Parser struct {
	r *bufio.Reader
}

// New creates a Parser with a 32 KB read buffer.
func New(r io.Reader) *Parser {
	return &Parser{r: bufio.NewReaderSize(r, readBufSize)}
}

// Parse reads the next RESP value from the connection.
func (p *Parser) Parse() (types.Value, error) {
	b, err := p.r.ReadByte()
	if err != nil {
		return types.Value{}, err
	}

	switch types.ValueType(b) {
	case types.TypeSimpleString:
		return p.parseSimpleString()
	case types.TypeError:
		return p.parseError()
	case types.TypeInteger:
		return p.parseInteger()
	case types.TypeBulkString:
		return p.parseBulkString()
	case types.TypeArray:
		return p.parseArray()
	default:
		return p.parseInlineCommand(b)
	}
}

// readLine reads up to \n and returns the slice without the trailing \r\n.
// It uses ReadSlice which returns a reference into the internal buffer —
// no allocation for short lines (the common case for length prefixes).
// The returned slice is only valid until the next read call.
func (p *Parser) readLine() ([]byte, error) {
	line, err := p.r.ReadSlice('\n')
	if err != nil {
		if err == bufio.ErrBufferFull {
			// Line too long for ReadSlice; fall back to ReadBytes (allocates).
			rest, err2 := p.r.ReadBytes('\n')
			if err2 != nil {
				return nil, err2
			}
			line = append(line, rest...)
		} else {
			return nil, err
		}
	}
	// Strip \r\n or just \n
	n := len(line)
	if n >= 2 && line[n-2] == '\r' {
		return line[:n-2], nil
	}
	if n >= 1 {
		return line[:n-1], nil
	}
	return nil, ErrInvalidSyntax
}

func (p *Parser) parseSimpleString() (types.Value, error) {
	line, err := p.readLine()
	if err != nil {
		return types.Value{}, err
	}
	// Copy: the slice points into bufio's internal buffer which will be overwritten.
	out := make([]byte, len(line))
	copy(out, line)
	return types.Value{Type: types.TypeSimpleString, Str: out}, nil
}

func (p *Parser) parseError() (types.Value, error) {
	line, err := p.readLine()
	if err != nil {
		return types.Value{}, err
	}
	out := make([]byte, len(line))
	copy(out, line)
	return types.Value{Type: types.TypeError, Str: out}, nil
}

func (p *Parser) parseInteger() (types.Value, error) {
	line, err := p.readLine()
	if err != nil {
		return types.Value{}, err
	}
	n, err := parseDecimal(line)
	if err != nil {
		return types.Value{}, err
	}
	return types.Value{Type: types.TypeInteger, Num: n}, nil
}

func (p *Parser) parseBulkString() (types.Value, error) {
	line, err := p.readLine()
	if err != nil {
		return types.Value{}, err
	}
	length, err := parseDecimal(line)
	if err != nil {
		return types.Value{}, err
	}
	if length == -1 {
		return types.Value{Type: types.TypeBulkString, IsNull: true}, nil
	}
	if length < 0 || length > MaxBulkStringLength {
		return types.Value{}, ErrInvalidLength
	}

	// Acquire a pooled buffer and resize it to hold payload + \r\n.
	bp := bulkPool.Get().(*[]byte)
	total := int(length) + 2
	if cap(*bp) < total {
		*bp = make([]byte, total)
	}
	buf := (*bp)[:total]

	if _, err = io.ReadFull(p.r, buf); err != nil {
		bulkPool.Put(bp)
		return types.Value{}, err
	}

	// Return a copy of just the payload (without \r\n) so the pool buffer
	// can be reused immediately. For small values this is one allocation;
	// for large values it's unavoidable anyway.
	payload := make([]byte, length)
	copy(payload, buf[:length])

	// Return the pool buffer.
	bulkPool.Put(bp)

	return types.Value{Type: types.TypeBulkString, Str: payload}, nil
}

func (p *Parser) parseArray() (types.Value, error) {
	line, err := p.readLine()
	if err != nil {
		return types.Value{}, err
	}
	length, err := parseDecimal(line)
	if err != nil {
		return types.Value{}, err
	}
	if length == -1 {
		return types.Value{Type: types.TypeArray, IsNull: true}, nil
	}
	if length < 0 || length > MaxArrayLength {
		return types.Value{}, ErrInvalidLength
	}

	array := make([]types.Value, length)
	for i := range array {
		array[i], err = p.Parse()
		if err != nil {
			return types.Value{}, err
		}
	}
	return types.Value{Type: types.TypeArray, Array: array}, nil
}

func (p *Parser) parseInlineCommand(firstByte byte) (types.Value, error) {
	line, err := p.readLine()
	if err != nil {
		return types.Value{}, err
	}
	fullLine := append([]byte{firstByte}, line...)
	parts := bytes.Fields(fullLine)
	array := make([]types.Value, len(parts))
	for i, part := range parts {
		array[i] = types.Value{Type: types.TypeBulkString, Str: part}
	}
	return types.Value{Type: types.TypeArray, Array: array}, nil
}

// parseDecimal parses a signed decimal integer from a byte slice
// without any allocation or string conversion.
func parseDecimal(b []byte) (int64, error) {
	if len(b) == 0 {
		return 0, ErrInvalidSyntax
	}
	neg := false
	i := 0
	if b[0] == '-' {
		neg = true
		i = 1
	}
	if i >= len(b) {
		return 0, ErrInvalidSyntax
	}
	var n uint64
	limit := uint64(1<<63 - 1)
	if neg {
		// The negative range contains one additional value: MinInt64.
		limit++
	}
	for ; i < len(b); i++ {
		c := b[i]
		if c < '0' || c > '9' {
			return 0, ErrInvalidSyntax
		}
		digit := uint64(c - '0')
		if n > (limit-digit)/10 {
			return 0, ErrInvalidSyntax
		}
		n = n*10 + digit
	}
	if neg {
		if n == 1<<63 {
			return -1 << 63, nil
		}
		return -int64(n), nil
	}
	return int64(n), nil
}

// Buffered returns the number of bytes currently buffered in the reader.
func (p *Parser) Buffered() int {
	return p.r.Buffered()
}
