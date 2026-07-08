package geocmd

import (
	"strings"

	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type GeoSearchStoreCommand struct{}

func (c GeoSearchStoreCommand) Name() string { return "GEOSEARCHSTORE" }

// GEOSEARCHSTORE destination source [FROMMEMBER member | FROMLONLAT lon lat]
//   [BYRADIUS radius unit | BYBOX width height unit] [ASC|DESC] [COUNT count [ANY]]
//   [STOREDIST]
//
// We parse the GEOSEARCH options here and set the store target on the query,
// then call runGeoSearch directly (NOT GEOSEARCH), because GEOSEARCH itself
// rejects STORE/STOREDIST.
func (c GeoSearchStoreCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 5 {
		return types.NewError("ERR wrong number of arguments for 'geosearchstore' command")
	}

	destKey := string(args[0].Str)
	srcKey := string(args[1].Str)

	// Forward arguments as if it were a GEOSEARCH on srcKey, collecting
	// STOREDIST and rejecting WITH*/STORE tokens.
	searchArgs := make([]types.Value, 0, len(args))
	searchArgs = append(searchArgs, types.NewString(srcKey))

	storeDist := false
	for i := 2; i < len(args); i++ {
		arg := strings.ToUpper(string(args[i].Str))
		switch arg {
		case "STOREDIST":
			storeDist = true
		case "STORE":
			return types.NewError("ERR syntax error")
		case "WITHCOORD", "WITHDIST", "WITHHASH":
			return types.NewError("ERR STORE option in GEOSEARCH is not compatible with WITHDIST, WITHHASH and WITHCOORD options")
		default:
			searchArgs = append(searchArgs, args[i])
		}
	}

	// Parse the GEOSEARCH portion to get a validated query + resolved zset,
	// then swap in the destination store key.
	gq, zset, exists, errVal := parseGeoSearch(ctx, searchArgs)
	if errVal.Type == types.TypeError {
		return errVal
	}
	q := gq
	q.storeKey = destKey
	q.storeDist = storeDist
	return runGeoSearch(ctx, zset, exists, &q)
}
