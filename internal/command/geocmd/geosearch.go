package geocmd

import (
	"sort"
	"strconv"
	"strings"

	"frostdb/internal/engine/keyevent"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// sortMode controls result ordering for geo queries.
type sortMode int

const (
	sortNone sortMode = iota
	sortAsc
	sortDesc
)

// geoQuery holds the parsed options shared by GEOSEARCH/GEORADIUS.
type geoQuery struct {
	shape      geoShape
	conversion float64 // meters per unit (for distance replies)
	withCoord  bool
	withDist   bool
	withHash   bool
	sort       sortMode
	count      int  // <= 0 means unlimited
	any        bool // COUNT ANY: stop early, no forced sort
	storeKey   string
	storeDist  bool
}

type GeoSearchCommand struct{}

func (c GeoSearchCommand) Name() string { return "GEOSEARCH" }

func (c GeoSearchCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	q, zset, exists, errVal := parseGeoSearch(ctx, args)
	if errVal.Type == types.TypeError {
		return errVal
	}
	return runGeoSearch(ctx, zset, exists, &q)
}

// parseGeoSearch parses a GEOSEARCH argument vector and resolves the source
// key + FROMMEMBER. It is used by both GeoSearchCommand and
// GeoSearchStoreCommand.
//
// Returns the parsed query, the resolved zset (nil if missing), whether the
// key exists, and an error value (zero Value on success).
func parseGeoSearch(ctx *context.Context, args []types.Value) (geoQuery, *object.ZSet, bool, types.Value) {
	if len(args) < 4 {
		return geoQuery{}, nil, false, types.NewError("ERR wrong number of arguments for 'geosearch' command")
	}

	key := string(args[0].Str)

	obj, ok := ctx.Engine.Get(key)
	var zset *object.ZSet
	if ok {
		if obj.Type != object.TypeZSet {
			return geoQuery{}, nil, false, types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		zset = obj.Value.(*object.ZSet)
	}

	var q geoQuery
	// Default unit is meters.
	q.conversion = 1.0
	q.count = -1

	var hasFromMember, hasFromLonLat bool
	var fromMemberName string
	var byRadius, byBox bool

	idx := 1
	for idx < len(args) {
		arg := strings.ToUpper(string(args[idx].Str))
		switch arg {
		case "FROMMEMBER":
			if idx+1 >= len(args) || hasFromMember || hasFromLonLat {
				return geoQuery{}, nil, false, types.NewError("ERR syntax error")
			}
			fromMemberName = string(args[idx+1].Str)
			hasFromMember = true
			idx += 2
		case "FROMLONLAT":
			if idx+2 >= len(args) || hasFromMember || hasFromLonLat {
				return geoQuery{}, nil, false, types.NewError("ERR syntax error")
			}
			lon, err1 := strconv.ParseFloat(string(args[idx+1].Str), 64)
			lat, err2 := strconv.ParseFloat(string(args[idx+2].Str), 64)
			if err1 != nil || err2 != nil {
				return geoQuery{}, nil, false, types.NewError("ERR value is not a valid float")
			}
			if lon < GeoLongMin || lon > GeoLongMax || lat < GeoLatMin || lat > GeoLatMax {
				return geoQuery{}, nil, false, types.NewError("ERR invalid longitude,latitude pair")
			}
			q.shape.centerLon = lon
			q.shape.centerLat = lat
			hasFromLonLat = true
			idx += 3
		case "BYRADIUS":
			if idx+2 >= len(args) || byRadius || byBox {
				return geoQuery{}, nil, false, types.NewError("ERR syntax error")
			}
			radius, err := strconv.ParseFloat(string(args[idx+1].Str), 64)
			if err != nil {
				return geoQuery{}, nil, false, types.NewError("ERR value is not a valid float")
			}
			if radius < 0 {
				return geoQuery{}, nil, false, types.NewError("ERR radius cannot be negative")
			}
			conv, errVal, ok := parseUnit(string(args[idx+2].Str))
			if !ok {
				return geoQuery{}, nil, false, errVal
			}
			q.conversion = conv
			q.shape.type_ = shapeCircular
			q.shape.radius = radius * conv
			byRadius = true
			idx += 3
		case "BYBOX":
			if idx+3 >= len(args) || byRadius || byBox {
				return geoQuery{}, nil, false, types.NewError("ERR syntax error")
			}
			width, err1 := strconv.ParseFloat(string(args[idx+1].Str), 64)
			height, err2 := strconv.ParseFloat(string(args[idx+2].Str), 64)
			if err1 != nil || err2 != nil {
				return geoQuery{}, nil, false, types.NewError("ERR value is not a valid float")
			}
			if width < 0 || height < 0 {
				return geoQuery{}, nil, false, types.NewError("ERR width or height cannot be negative")
			}
			conv, errVal, ok := parseUnit(string(args[idx+3].Str))
			if !ok {
				return geoQuery{}, nil, false, errVal
			}
			q.conversion = conv
			q.shape.type_ = shapeRectangle
			q.shape.width = width * conv
			q.shape.height = height * conv
			byBox = true
			idx += 4
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
				return geoQuery{}, nil, false, types.NewError("ERR syntax error")
			}
			countArg, err := strconv.Atoi(string(args[idx+1].Str))
			if err != nil || countArg <= 0 {
				return geoQuery{}, nil, false, types.NewError("ERR COUNT must be > 0")
			}
			q.count = countArg
			idx += 2
			if idx < len(args) && strings.ToUpper(string(args[idx].Str)) == "ANY" {
				q.any = true
				idx++
			}
		case "ANY":
			return geoQuery{}, nil, false, types.NewError("ERR ANY option requires COUNT option")
		case "STORE", "STOREDIST":
			// In raw GEOSEARCH, STORE/STOREDIST are syntax errors (they belong
			// to GEOSEARCHSTORE / GEORADIUS). GEOSEARCHSTORE rewrites them out
			// before calling us, so seeing them here is always a user error.
			return geoQuery{}, nil, false, types.NewError("ERR syntax error")
		default:
			return geoQuery{}, nil, false, types.NewError("ERR syntax error")
		}
	}

	// Resolve FROMMEMBER now that we know we have a zset (matches Redis:
	// "could not decode requested zset member" if the member is missing).
	if hasFromMember {
		if ok {
			score, exists := zset.Score(fromMemberName)
			if !exists {
				return geoQuery{}, nil, false, types.NewError("ERR could not decode requested zset member")
			}
			q.shape.centerLon, q.shape.centerLat = GeoHashDecodeWGS84(uint64(score))
		}
	}

	if q.storeKey != "" && (q.withCoord || q.withDist || q.withHash) {
		return geoQuery{}, nil, false, types.NewError("ERR STORE option in GEOSEARCH is not compatible with WITHDIST, WITHHASH and WITHCOORD options")
	}
	if !hasFromMember && !hasFromLonLat {
		return geoQuery{}, nil, false, types.NewError("ERR exactly one of FROMMEMBER or FROMLONLAT can be specified")
	}
	if (byRadius && byBox) || (!byRadius && !byBox) {
		return geoQuery{}, nil, false, types.NewError("ERR exactly one of BYRADIUS and BYBOX can be specified")
	}

	return q, zset, ok, types.Value{}
}

// runGeoSearch performs the actual scan + sort + reply for both GEOSEARCH and
// GEORADIUS. It is shared so both commands use identical matching/sorting.
func runGeoSearch(ctx *context.Context, zset *object.ZSet, exists bool, q *geoQuery) types.Value {
	// Source key missing: empty array, or 0 + delete store key if STORE given.
	if !exists {
		if q.storeKey != "" {
			deleteAndSignal(ctx, q.storeKey)
			return types.NewInt(0)
		}
		return types.RespEmptyArr
	}

	type geoPoint struct {
		member string
		dist   float64
		hash   uint64
		lon    float64
		lat    float64
	}

	points := make([]geoPoint, 0, 64)
	zset.ScanEntries(func(member string, score float64) bool {
		hash := uint64(score)
		ptLon, ptLat := GeoHashDecodeWGS84(hash)
		inside, dist := withinShape(&q.shape, ptLon, ptLat)
		if !inside {
			return true
		}
		points = append(points, geoPoint{
			member: member,
			dist:   dist,
			hash:   hash,
			lon:    ptLon,
			lat:    ptLat,
		})
		if q.any && q.count > 0 && len(points) >= q.count {
			return false
		}
		return true
	})

	// Redis: COUNT without explicit sort forces ASC (unless ANY).
	effectiveSort := q.sort
	if q.count > 0 && effectiveSort == sortNone && !q.any {
		effectiveSort = sortAsc
	}

	// When no sort is specified, ensure the center point (distance 0) appears
	// first, matching Redis's area-scanning behavior where the center area is
	// processed first. This is important for GEORADIUSBYMEMBER without sort.
	if effectiveSort == sortNone && len(points) > 1 {
		for i := 1; i < len(points); i++ {
			if points[i].dist == 0.0 {
				points[0], points[i] = points[i], points[0]
				break
			}
		}
	}

	switch effectiveSort {
	case sortAsc:
		sort.Slice(points, func(i, j int) bool { return points[i].dist < points[j].dist })
	case sortDesc:
		sort.Slice(points, func(i, j int) bool { return points[i].dist > points[j].dist })
	}

	// Apply COUNT (after sort). ANY already truncated above.
	if q.count > 0 && !q.any && len(points) > q.count {
		points = points[:q.count]
	}

	// STORE / STOREDIST path: write a zset and return the count.
	if q.storeKey != "" {
		if len(points) == 0 {
			deleteAndSignal(ctx, q.storeKey)
			return types.NewInt(0)
		}
		newZSet := object.NewZSet()
		for _, p := range points {
			score := float64(p.hash)
			if q.storeDist {
				score = p.dist / q.conversion
			}
			newZSet.Add(score, p.member)
		}
		oldObj, existed := ctx.Engine.Get(q.storeKey)
		ctx.Engine.Set(q.storeKey, &object.Object{
			Type:     object.TypeZSet,
			Encoding: object.EncodingRaw,
			Value:    newZSet,
		})
		if existed && oldObj != nil {
			ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagOverwrite), "overwritten", q.storeKey)
			if oldObj.Type != object.TypeZSet {
				ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagTypeChanged), "type_changed", q.storeKey)
			}
		}
		signalKey(ctx, q.storeKey)
		return types.NewInt(int64(len(points)))
	}

	// Reply path.
	result := make([]types.Value, 0, len(points))
	for _, p := range points {
		if !q.withCoord && !q.withDist && !q.withHash {
			result = append(result, types.NewString(p.member))
			continue
		}
		props := make([]types.Value, 0, 4)
		props = append(props, types.NewString(p.member))
		if q.withDist {
			props = append(props, types.NewString(strconv.FormatFloat(p.dist/q.conversion, 'f', 4, 64)))
		}
		if q.withHash {
			props = append(props, types.NewInt(int64(p.hash)))
		}
		if q.withCoord {
			props = append(props, types.NewArray([]types.Value{
				types.NewString(strconv.FormatFloat(p.lon, 'f', -1, 64)),
				types.NewString(strconv.FormatFloat(p.lat, 'f', -1, 64)),
			}))
		}
		result = append(result, types.NewArray(props))
	}
	return types.NewArray(result)
}

// parseUnit converts a GEOSEARCH unit name to its meters-per-unit factor.
// On success conv is valid and errVal is the zero Value (ok == true).
// On failure it returns the error reply to send to the client.
func parseUnit(unit string) (conv float64, errVal types.Value, ok bool) {
	switch strings.ToLower(unit) {
	case "m":
		return 1.0, types.Value{}, true
	case "km":
		return 1000.0, types.Value{}, true
	case "mi":
		return 1609.34, types.Value{}, true
	case "ft":
		return 0.3048, types.Value{}, true
	default:
		return 0, types.NewError("ERR unsupported unit provided. please use m, km, ft, mi"), false
	}
}

// signalKey notifies watchers of a key change, respecting MULTI/EXEC batching.
func signalKey(ctx *context.Context, key string) {
	ctx.SignalKey(key)
}

// deleteAndSignal removes a key and signals the change.
func deleteAndSignal(ctx *context.Context, key string) {
	ctx.Engine.Del(key)
	signalKey(ctx, key)
}
