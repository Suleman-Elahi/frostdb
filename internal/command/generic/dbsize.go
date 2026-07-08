package generic

import (
	"frostdb/internal/engine/api"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// DbSizeCommand implements DBSIZE — returns number of keys in the current DB.
type DbSizeCommand struct{}

func (c DbSizeCommand) Name() string { return "DBSIZE" }

func (c DbSizeCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if mdb, ok := ctx.Engine.(api.MultiDB); ok {
		return types.NewInt(int64(mdb.DBSize(ctx.DB)))
	}
	// Fallback: count via Keys
	return types.NewInt(int64(len(ctx.Engine.Keys("*"))))
}

// FlushDbCommand implements FLUSHDB / FLUSHALL — removes all keys.
type FlushDbCommand struct {
	All bool
}

func (c FlushDbCommand) Name() string {
	if c.All {
		return "FLUSHALL"
	}
	return "FLUSHDB"
}

func (c FlushDbCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	mdb, hasMultiDB := ctx.Engine.(api.MultiDB)

	if c.All && hasMultiDB {
		// FLUSHALL: flush every database
		for i := 0; i < mdb.NumDBs(); i++ {
			mdb.FlushDB(i)
		}
	} else if hasMultiDB {
		// FLUSHDB: flush only the current database
		mdb.FlushDB(ctx.DB)
	} else {
		// Fallback: delete all keys in current engine view
		for _, k := range ctx.Engine.Keys("*") {
			ctx.Engine.Del(k)
		}
	}
	return types.RespOK
}
