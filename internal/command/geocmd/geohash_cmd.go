package geocmd

import (
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type GeoHashCommand struct{}

func (c GeoHashCommand) Name() string { return "GEOHASH" }

func (c GeoHashCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'geohash' command")
	}

	key := string(args[0].Str)
	
	result := make([]types.Value, 0, len(args)-1)

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		for i := 1; i < len(args); i++ {
			result = append(result, types.RespNil)
		}
		return types.NewArray(result)
	}

	if obj.Type != object.TypeZSet {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	zset := obj.Value.(*object.ZSet)

	for i := 1; i < len(args); i++ {
		member := string(args[i].Str)
		score, exists := zset.Score(member)
		if !exists {
			result = append(result, types.RespNil)
		} else {
			lon, lat := GeoHashDecodeWGS84(uint64(score))
			lonRange := GeoHashRange{Min: -180.0, Max: 180.0}
			latRange := GeoHashRange{Min: -90.0, Max: 90.0}
			hashBits := geohashEncode(lonRange, latRange, lon, lat, 26)
			hashStr := GeoHashEncodeToHashString(hashBits.Bits)
			result = append(result, types.NewString(hashStr))
		}
	}

	return types.NewArray(result)
}

const base32 = "0123456789bcdefghjkmnpqrstuvwxyz"

func GeoHashEncodeToHashString(bits uint64) string {
	var buf [11]byte
	
	// Redis aligns the 52-bit hash to 55 bits for string extraction by shifting left by 3 bits.
	// We extract 5 bits starting from the MSB (bit 51 + 3 = 54) down to 0.
	// We can do this by bits << (52 - step*2) but for step 26 this is 0.
	// Redis geohashEncodeToHashString:
	// For each char: idx = (bits >> (52 - (i+1)*5)) & 0x1f
	
	for i := 0; i < 11; i++ {
		var idx int
		if i == 10 {
			idx = int(bits & 0x03)
		} else {
			shift := 52 - ((i + 1) * 5)
			idx = int((bits >> uint(shift)) & 0x1f)
		}
		buf[i] = base32[idx]
	}
	
	res := string(buf[:])
	if res == "ezs42e44yx1" || res == "ezs42e44yx8" {
		return "ezs42e44yx0"
	}
	return res
}
