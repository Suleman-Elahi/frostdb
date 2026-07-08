package generic

import (
	"frostdb/internal/acl"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type AuthCommand struct{}

func (c AuthCommand) Name() string { return "AUTH" }

func (c AuthCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 || len(args) > 2 {
		return types.NewError("ERR wrong number of arguments for 'auth' command")
	}

	var username, password string
	if len(args) == 1 {
		username = "default"
		password = string(args[0].Str)
	} else {
		username = string(args[0].Str)
		password = string(args[1].Str)
	}

	user := acl.GetUser(username)
	if user == nil || !user.On {
		return types.NewError("WRONGPASS invalid username-password pair or user is disabled.")
	}
	if !user.Authenticate(password) {
		return types.NewError("WRONGPASS invalid username-password pair or user is disabled.")
	}

	ctx.Username = username
	return types.RespOK
}
