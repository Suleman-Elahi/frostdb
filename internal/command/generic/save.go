package generic

import (
	"os"
	"path/filepath"

	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// SaveCommand implements SAVE — synchronous RDB dump of live state.
type SaveCommand struct{}

func (c SaveCommand) Name() string { return "SAVE" }

func (c SaveCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	path := defaultRDBPath()
	if len(args) >= 1 {
		path = string(args[0].Str)
	}

	n, err := ctx.Engine.SaveRDB(path)
	if err != nil {
		return types.NewError("ERR " + err.Error())
	}
	_ = n
	return types.NewSimpleString("OK")
}

// BgSaveCommand implements BGSAVE — async RDB dump (runs in goroutine).
type BgSaveCommand struct{}

func (c BgSaveCommand) Name() string { return "BGSAVE" }

func (c BgSaveCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	path := defaultRDBPath()
	if len(args) >= 1 {
		path = string(args[0].Str)
	}

	go ctx.Engine.SaveRDB(path)
	return types.NewSimpleString("Background saving started")
}

func defaultRDBPath() string {
	dataDir := filepath.Join(".", "data")
	if envDir := os.Getenv("FROST_DATA_DIR"); envDir != "" {
		dataDir = envDir
	}
	return filepath.Join(dataDir, "dump.rdb")
}
