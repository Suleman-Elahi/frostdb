package generic

import (
	"strconv"
	"strings"

	"frostdb/internal/engine/api"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// SelectCommand implements SELECT db — switches the active database (0-15).
type SelectCommand struct{}

func (c SelectCommand) Name() string { return "SELECT" }

func (c SelectCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewError("ERR wrong number of arguments for 'select' command")
	}
	db, err := strconv.Atoi(string(args[0].Str))
	if err != nil || db < 0 {
		return types.NewError("ERR value is not an integer or out of range")
	}
	if ctx.DBSelector == nil {
		if db != 0 {
			return types.NewError("ERR DB index is out of range")
		}
		return types.RespOK
	}
	if errMsg := ctx.DBSelector(ctx, db); errMsg != "" {
		return types.NewError(errMsg)
	}
	return types.RespOK
}

// MoveCommand implements MOVE key db — moves a key to another database.
type MoveCommand struct{}

func (c MoveCommand) Name() string { return "MOVE" }

func (c MoveCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 2 {
		return types.NewError("ERR wrong number of arguments for 'move' command")
	}
	key := string(args[0].Str)
	db, err := strconv.Atoi(string(args[1].Str))
	if err != nil || db < 0 {
		return types.NewError("ERR value is not an integer or out of range")
	}

	mdb, ok := ctx.Engine.(api.MultiDB)
	if !ok {
		return types.RespZero
	}
	if db >= mdb.NumDBs() {
		return types.NewError("ERR invalid DB index")
	}
	if mdb.MoveKey(key, ctx.DB, db) {
		return types.RespOne
	}
	return types.RespZero
}

// SwapDBCommand implements SWAPDB db1 db2 — swaps two databases.
type SwapDBCommand struct{}

func (c SwapDBCommand) Name() string { return "SWAPDB" }

func (c SwapDBCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 2 {
		return types.NewError("ERR wrong number of arguments for 'swapdb' command")
	}
	db1, err1 := strconv.Atoi(string(args[0].Str))
	db2, err2 := strconv.Atoi(string(args[1].Str))
	if err1 != nil || err2 != nil || db1 < 0 || db2 < 0 {
		return types.NewError("ERR value is not an integer or out of range")
	}

	mdb, ok := ctx.Engine.(api.MultiDB)
	if !ok {
		return types.NewError("ERR SWAPDB not supported")
	}
	if db1 >= mdb.NumDBs() || db2 >= mdb.NumDBs() {
		return types.NewError("ERR invalid DB index")
	}
	mdb.SwapDB(db1, db2) //nolint:errcheck

	// Propagate SWAPDB to replicas for replication testing
	ctx.Engine.LogCommand([][]byte{
		[]byte("SWAPDB"),
		args[0].Str,
		args[1].Str,
	})

	return types.RespOK
}

// UnlinkCommand implements UNLINK key [key ...] — async DEL (same as DEL for us).
type UnlinkCommand struct{}

func (c UnlinkCommand) Name() string { return "UNLINK" }

func (c UnlinkCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'unlink' command")
	}
	count := int64(0)
	for _, arg := range args {
		if ctx.Engine.Del(string(arg.Str)) {
			count++
		}
	}
	return types.NewInt(count)
}

// RandomKeyCommand implements RANDOMKEY
type RandomKeyCommand struct{}

func (c RandomKeyCommand) Name() string { return "RANDOMKEY" }

func (c RandomKeyCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	keys := ctx.Engine.Keys("*")
	if len(keys) == 0 {
		return types.RespNil
	}
	return types.NewString(keys[0])
}

// KeysCommand implements KEYS pattern
type KeysCommand struct{}

func (c KeysCommand) Name() string { return "KEYS" }

func (c KeysCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewError("ERR wrong number of arguments for 'keys' command")
	}
	pattern := string(args[0].Str)
	var keys []string
	if AllowAccessExpired.Load() {
		keys = ctx.Engine.KeysAll(pattern)
	} else {
		keys = ctx.Engine.Keys(pattern)
	}
	result := make([]types.Value, len(keys))
	for i, k := range keys {
		result[i] = types.NewString(k)
	}
	return types.NewArray(result)
}

// matchPattern does simple glob matching for CONFIG GET patterns.
// Defined here to avoid duplication; used by config.go via the same package.
func matchPattern(pattern, key string) bool {
	if pattern == "*" {
		return true
	}
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(key, pattern[:len(pattern)-1])
	}
	return pattern == key
}
