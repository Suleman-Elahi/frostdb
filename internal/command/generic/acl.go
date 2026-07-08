package generic

import (
	"strings"

	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
	"frostdb/internal/acl"
)

// AclCommand implements ACL WHOAMI|CAT|LIST|HELP — stub for compatibility.
type AclCommand struct{}

func (c AclCommand) Name() string { return "ACL" }

func (c AclCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) == 0 {
		return types.NewError("ERR wrong number of arguments for 'acl' command")
	}

	sub := strings.ToUpper(string(args[0].Str))
	switch sub {
	case "WHOAMI":
		return types.NewString("default")
	case "CAT":
		return types.RespEmptyArr
	case "LIST":
		return types.NewArray([]types.Value{
			types.NewString("user default on ~* &* +@all"),
		})
	case "USERS":
		return types.NewArray([]types.Value{
			types.NewString("default"),
		})
	case "HELP":
		return types.NewArray([]types.Value{
			types.NewString("ACL WHOAMI - Return the current connection username"),
			types.NewString("ACL LIST - List all ACL rules"),
			types.NewString("ACL USERS - List all usernames"),
		})
	case "SETUSER":
		if len(args) < 2 {
			return types.NewError("ERR wrong number of arguments for 'acl setuser' command")
		}
		username := string(args[1].Str)
		user := acl.GetUser(username)
		if user == nil {
			user = acl.NewUser(username)
		}
		for i := 2; i < len(args); i++ {
			rule := string(args[i].Str)
			user.AddRule(rule)
		}
		acl.SetUser(user)
		return types.RespOK
	default:
		return types.NewError("ERR unknown subcommand '" + sub + "'")
	}
}
