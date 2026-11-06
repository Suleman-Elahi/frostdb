package str

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/zeebo/xxh3"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// digestStringBytes returns a deterministic 64-bit hash of the given bytes
// using XXH3_64bits. The output is always represented as a 16-character
// lowercase hex string with leading zeros preserved (e.g. "00006c38adf31777").
func digestStringBytes(b []byte) string {
	h := xxh3.Hash(b)
	return fmt.Sprintf("%016x", h)
}

// DigestCommand implements DIGEST key
// Returns a 16-hex-character FNV-1a hash of the string value.
// – Nil for a non-existent key.
// – WRONGTYPE for non-string keys.
type DigestCommand struct{}

func (c DigestCommand) Name() string { return "DIGEST" }

func (c DigestCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewError("ERR wrong number of arguments for 'digest' command")
	}

	obj, ok := ctx.Engine.GetBytes(args[0].Str)
	if !ok {
		return types.RespNil
	}
	if obj.Type != object.TypeString {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	strVal, _ := object.StringBytes(obj)
	return types.NewString(digestStringBytes(strVal))
}

// parseDigestArg validates that a digest argument is exactly 16 lowercase/uppercase
// hexadecimal characters. Returns the canonical lowercase form on success.
func parseDigestArg(b []byte) (string, error) {
	if len(b) != 16 {
		return "", fmt.Errorf("ERR digest argument must be exactly 16 hexadecimal characters")
	}
	s := string(b)
	lower := strings.ToLower(s)
	for _, c := range lower {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return "", fmt.Errorf("ERR digest argument must be exactly 16 hexadecimal characters")
		}
	}
	return lower, nil
}

// uint64FromHex parses a 16-char lowercase hex string into uint64.
// caller must ensure the string has already been validated by parseDigestArg.
func uint64FromHex(s string) uint64 {
	b := []byte(s)
	var buf [8]byte
	for i := 0; i < 8; i++ {
		hi := hexNibble(b[i*2])
		lo := hexNibble(b[i*2+1])
		buf[i] = (hi << 4) | lo
	}
	return binary.BigEndian.Uint64(buf[:])
}

func hexNibble(c byte) byte {
	if c >= '0' && c <= '9' {
		return c - '0'
	}
	return c - 'a' + 10
}
