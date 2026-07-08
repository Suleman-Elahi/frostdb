package str

import (
	"strconv"
	"strings"
)

type overflowMode uint8

const (
	overflowWrap overflowMode = iota
	overflowSat
	overflowFail
)

type bitfieldEncoding struct {
	signed bool
	width  int
}

func parseBitfieldEncoding(s string) (bitfieldEncoding, bool) {
	if len(s) < 2 {
		return bitfieldEncoding{}, false
	}
	prefix := s[0]
	if prefix != 'i' && prefix != 'I' && prefix != 'u' && prefix != 'U' {
		return bitfieldEncoding{}, false
	}

	width, err := strconv.Atoi(s[1:])
	if err != nil {
		return bitfieldEncoding{}, false
	}

	signed := prefix == 'i' || prefix == 'I'
	if signed {
		if width < 1 || width > 64 {
			return bitfieldEncoding{}, false
		}
	} else {
		if width < 1 || width > 63 {
			return bitfieldEncoding{}, false
		}
	}

	return bitfieldEncoding{signed: signed, width: width}, true
}

func parseBitfieldOffset(s string, width int) (int64, bool) {
	mul := int64(1)
	if strings.HasPrefix(s, "#") {
		s = s[1:]
		mul = int64(width)
	}

	base, err := strconv.ParseInt(s, 10, 64)
	if err != nil || base < 0 {
		return 0, false
	}

	if mul != 1 && base > (1<<63-1)/mul {
		return 0, false
	}
	offset := base * mul
	return offset, true
}

func parseOverflowMode(s string) (overflowMode, bool) {
	switch strings.ToUpper(s) {
	case "WRAP":
		return overflowWrap, true
	case "SAT":
		return overflowSat, true
	case "FAIL":
		return overflowFail, true
	default:
		return overflowWrap, false
	}
}
