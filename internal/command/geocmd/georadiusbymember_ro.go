package geocmd

import (
	"strings"

	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type GeoRadiusByMemberROCommand struct{}

func (c GeoRadiusByMemberROCommand) Name() string { return "GEORADIUSBYMEMBER_RO" }

func (c GeoRadiusByMemberROCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	for i := 4; i < len(args); i++ {
		arg := strings.ToUpper(string(args[i].Str))
		if arg == "STORE" || arg == "STOREDIST" {
			return types.NewError("ERR syntax error")
		}
	}
	return GeoRadiusByMemberCommand{}.Execute(ctx, args)
}
