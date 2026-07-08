package generic

import (
	"strings"

	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// ClusterCommand implements CLUSTER INFO|MYID|SLOTS stubs for client compatibility.
type ClusterCommand struct{}

func (c ClusterCommand) Name() string { return "CLUSTER" }

func (c ClusterCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'cluster' command")
	}

	sub := strings.ToUpper(string(args[0].Str))
	switch sub {
	case "INFO":
		return types.NewString(
			"cluster_enabled:0\r\n" +
				"cluster_state:ok\r\n" +
				"cluster_slots_assigned:0\r\n" +
				"cluster_slots_ok:0\r\n" +
				"cluster_slots_pfail:0\r\n" +
				"cluster_slots_fail:0\r\n" +
				"cluster_known_nodes:1\r\n" +
				"cluster_size:0\r\n",
		)
	case "MYID":
		return types.NewString("0000000000000000000000000000000000000000")
	case "SLOTS":
		return types.NewArray(nil)
	case "RESET":
		if ctx.InScript {
			return types.NewError("ERR command is not allowed from script")
		}
		return types.RespOK
	default:
		return types.NewError("ERR unknown subcommand '" + sub + "'")
	}
}
