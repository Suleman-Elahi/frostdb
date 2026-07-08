package geocmd

import (
	"strings"

	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type GeoRadiusROCommand struct{}

func (c GeoRadiusROCommand) Name() string { return "GEORADIUS_RO" }

func (c GeoRadiusROCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	for i := 5; i < len(args); i++ {
		arg := strings.ToUpper(string(args[i].Str))
		if arg == "STORE" || arg == "STOREDIST" {
			return types.NewError("ERR syntax error")
		}
	}
	return GeoRadiusCommand{}.Execute(ctx, args)
}
