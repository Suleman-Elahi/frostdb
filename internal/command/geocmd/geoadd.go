package geocmd

import (
	"fmt"
	"strconv"
	"strings"

	"frostdb/internal/command/zsetcmd"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type GeoAddCommand struct{}

func (c GeoAddCommand) Name() string { return "GEOADD" }

func (c GeoAddCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 4 {
		return types.NewError("ERR wrong number of arguments for 'geoadd' command")
	}

	key := string(args[0].Str)
	zaddArgs := []types.Value{types.NewString(key)}

	idx := 1
	var hasXX, hasNX bool
	for idx < len(args) {
		arg := strings.ToUpper(string(args[idx].Str))
		if arg == "NX" {
			hasNX = true
			zaddArgs = append(zaddArgs, args[idx])
			idx++
		} else if arg == "XX" {
			hasXX = true
			zaddArgs = append(zaddArgs, args[idx])
			idx++
		} else if arg == "CH" {
			zaddArgs = append(zaddArgs, args[idx])
			idx++
		} else {
			break
		}
	}

	if hasXX && hasNX {
		return types.NewError("ERR syntax error")
	}

	remaining := args[idx:]
	if len(remaining) == 0 || len(remaining)%3 != 0 {
		return types.NewError("ERR syntax error")
	}

	for i := 0; i < len(remaining); i += 3 {
		lonStr := string(remaining[i].Str)
		latStr := string(remaining[i+1].Str)
		member := remaining[i+2]

		lon, err1 := strconv.ParseFloat(lonStr, 64)
		lat, err2 := strconv.ParseFloat(latStr, 64)

		if err1 != nil || err2 != nil {
			return types.NewError("ERR value is not a valid float")
		}

		if lon < GeoLongMin || lon > GeoLongMax || lat < GeoLatMin || lat > GeoLatMax {
			return types.NewError("ERR invalid longitude,latitude pair " + lonStr + "," + latStr)
		}

		hash := GeoHashEncodeWGS84(lon, lat)
		scoreStr := fmt.Sprintf("%d", hash)

		zaddArgs = append(zaddArgs, types.NewString(scoreStr), member)
	}

	zaddCmd := zsetcmd.ZAddCommand{}
	return zaddCmd.Execute(ctx, zaddArgs)
}
