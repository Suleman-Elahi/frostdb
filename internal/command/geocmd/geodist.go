package geocmd

import (
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type GeoDistCommand struct{}

func (c GeoDistCommand) Name() string { return "GEODIST" }

func (c GeoDistCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 3 || len(args) > 4 {
		return types.NewError("ERR wrong number of arguments for 'geodist' command")
	}

	key := string(args[0].Str)
	member1 := string(args[1].Str)
	member2 := string(args[2].Str)
	
	unitStr := "m"
	if len(args) == 4 {
		unitStr = strings.ToLower(string(args[3].Str))
	}

	var conversion float64
	switch unitStr {
	case "m":
		conversion = 1.0
	case "km":
		conversion = 1000.0
	case "mi":
		conversion = 1609.34
	case "ft":
		conversion = 0.3048
	default:
		return types.NewError("ERR unsupported unit provided. please use m, km, ft, mi")
	}

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.RespNil
	}
	if obj.Type != object.TypeZSet {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	zset := obj.Value.(*object.ZSet)
	score1, ok1 := zset.Score(member1)
	score2, ok2 := zset.Score(member2)
	
	if !ok1 || !ok2 {
		return types.RespNil
	}

	lon1, lat1 := GeoHashDecodeWGS84(uint64(score1))
	lon2, lat2 := GeoHashDecodeWGS84(uint64(score2))
	
	dist := GeoDistance(lon1, lat1, lon2, lat2)
	distInUnit := dist / conversion
	
	return types.NewString(strconv.FormatFloat(distInUnit, 'f', 4, 64))
}
