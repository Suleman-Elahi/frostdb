package str

const (
	maxStringBytes = 512 * 1024 * 1024
	maxBitOffset   = int64(maxStringBytes*8 - 1)
)

func ensureByteLen(b []byte, n int) ([]byte, bool) {
	if n < 0 || n > maxStringBytes {
		return nil, false
	}
	if len(b) >= n {
		return b, true
	}
	out := make([]byte, n)
	copy(out, b)
	return out, true
}

func readUnsignedBits(data []byte, offset int64, width int) uint64 {
	var v uint64
	for i := 0; i < width; i++ {
		bitPos := offset + int64(i)
		byteIdx := int(bitPos >> 3)
		bitInByte := 7 - int(bitPos&7)

		var bit uint64
		if byteIdx >= 0 && byteIdx < len(data) {
			bit = uint64((data[byteIdx] >> bitInByte) & 1)
		}
		v = (v << 1) | bit
	}
	return v
}

func writeUnsignedBits(data []byte, offset int64, width int, value uint64) {
	for i := width - 1; i >= 0; i-- {
		bitPos := offset + int64(i)
		byteIdx := int(bitPos >> 3)
		bitInByte := 7 - int(bitPos&7)
		bit := value & 1
		value >>= 1
		if bit == 1 {
			data[byteIdx] |= 1 << bitInByte
		} else {
			data[byteIdx] &^= 1 << bitInByte
		}
	}
}

func decodeSigned(u uint64, width int) int64 {
	if width == 64 {
		return int64(u)
	}
	signBit := uint64(1) << (width - 1)
	if u&signBit == 0 {
		return int64(u)
	}
	mask := ^((uint64(1) << width) - 1)
	return int64(u | mask)
}

func encodeUnsignedFromSigned(v int64, width int) uint64 {
	if width == 64 {
		return uint64(v)
	}
	mask := (uint64(1) << width) - 1
	return uint64(v) & mask
}
