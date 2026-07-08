package geocmd

import (
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type GeoRadiusCommand struct{}

func (c GeoRadiusCommand) Name() string { return "GEORADIUS" }

func (c GeoRadiusCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 5 {
		return types.NewError("ERR wrong number of arguments for 'georadius' command")
	}

	key := string(args[0].Str)
	lon, err1 := strconv.ParseFloat(string(args[1].Str), 64)
	lat, err2 := strconv.ParseFloat(string(args[2].Str), 64)
	radius, err3 := strconv.ParseFloat(string(args[3].Str), 64)
	if err1 != nil || err2 != nil || err3 != nil {
		return types.NewError("ERR value is not a valid float")
	}
	if lon < GeoLongMin || lon > GeoLongMax || lat < GeoLatMin || lat > GeoLatMax {
		return types.NewError("ERR invalid longitude,latitude pair")
	}
	if radius < 0 {
		return types.NewError("ERR radius cannot be negative")
	}

	conv, errVal, ok := parseUnit(string(args[4].Str))
	if !ok {
		return errVal
	}

	var q geoQuery
	q.conversion = conv
	q.count = -1
	q.shape.type_ = shapeCircular
	q.shape.centerLon = lon
	q.shape.centerLat = lat
	q.shape.radius = radius * conv

	idx := 5
	for idx < len(args) {
		arg := strings.ToUpper(string(args[idx].Str))
		switch arg {
		case "WITHCOORD":
			q.withCoord = true
			idx++
		case "WITHDIST":
			q.withDist = true
			idx++
		case "WITHHASH":
			q.withHash = true
			idx++
		case "ASC":
			q.sort = sortAsc
			idx++
		case "DESC":
			q.sort = sortDesc
			idx++
		case "COUNT":
			if idx+1 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			countArg, err := strconv.Atoi(string(args[idx+1].Str))
			if err != nil || countArg <= 0 {
				return types.NewError("ERR COUNT must be > 0")
			}
			q.count = countArg
			idx += 2
			if idx < len(args) && strings.ToUpper(string(args[idx].Str)) == "ANY" {
				q.any = true
				idx++
			}
		case "STORE":
			if idx+1 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			q.storeKey = string(args[idx+1].Str)
			q.storeDist = false
			idx += 2
		case "STOREDIST":
			if idx+1 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			q.storeKey = string(args[idx+1].Str)
			q.storeDist = true
			idx += 2
		case "ANY":
			return types.NewError("ERR ANY option requires COUNT option")
		default:
			return types.NewError("ERR syntax error")
		}
	}

	if q.storeKey != "" && (q.withCoord || q.withDist || q.withHash) {
		return types.NewError("ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options")
	}

	obj, ok := ctx.Engine.Get(key)
	var zset *object.ZSet
	if ok {
		if obj.Type != object.TypeZSet {
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		zset = obj.Value.(*object.ZSet)
	}

	return runGeoSearch(ctx, zset, ok, &q)
}
