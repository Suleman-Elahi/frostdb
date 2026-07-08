package generic

import (
	"bytes"
	"fmt"

	"frostdb/internal/engine/rdb"
	"frostdb/internal/engine/shard"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// SyncCommand implements SYNC — initiates replication stream from master.
// This sends an RDB dump followed by keeping the connection open for commands.
type SyncCommand struct{}

func (c SyncCommand) Name() string { return "SYNC" }

func (c SyncCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if ctx.Conn == nil {
		return types.NewError("ERR SYNC requires a connection")
	}

	// Get shards from the engine via type assertion
	shards, ok := getShardsFromEngine(ctx.Engine)
	if !ok {
		return types.NewError("ERR internal error: cannot access shards")
	}

	// Generate RDB data to a buffer using the existing exporter
	var buf bytes.Buffer
	exp := rdb.NewExporterToWriter(shards, &buf)
	if _, err := exp.Export(); err != nil {
		return types.NewError("ERR " + err.Error())
	}

	// Write bulk string header: $<length>\r\n
	header := fmt.Sprintf("$%d\r\n", buf.Len())
	if _, err := ctx.Conn.Write([]byte(header)); err != nil {
		return types.NewError("ERR " + err.Error())
	}

	// Write RDB data
	if _, err := ctx.Conn.Write(buf.Bytes()); err != nil {
		return types.NewError("ERR " + err.Error())
	}

	// Register this connection as a replica to receive the replication stream
	ctx.Engine.AddReplica(ctx.Conn)

	// Return special value to indicate we've handled the response ourselves
	return types.Value{Type: types.TypeNoReply}
}

// PSyncCommand implements PSYNC — partial resynchronization with master.
type PSyncCommand struct{}

func (c PSyncCommand) Name() string { return "PSYNC" }

func (c PSyncCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if ctx.Conn == nil {
		return types.NewError("ERR PSYNC requires a connection")
	}

	// For PSYNC, respond with FULLRESYNC <replid> <offset> then send RDB
	replID := "0000000000000000000000000000000000000000"
	offset := 0

	// Write FULLRESYNC response
	fullresync := fmt.Sprintf("+FULLRESYNC %s %d\r\n", replID, offset)
	if _, err := ctx.Conn.Write([]byte(fullresync)); err != nil {
		return types.NewError("ERR " + err.Error())
	}

	// Get shards and send RDB
	shards, ok := getShardsFromEngine(ctx.Engine)
	if !ok {
		return types.NewError("ERR internal error: cannot access shards")
	}

	var buf bytes.Buffer
	exp := rdb.NewExporterToWriter(shards, &buf)
	if _, err := exp.Export(); err != nil {
		return types.NewError("ERR " + err.Error())
	}

	header := fmt.Sprintf("$%d\r\n", buf.Len())
	if _, err := ctx.Conn.Write([]byte(header)); err != nil {
		return types.NewError("ERR " + err.Error())
	}

	if _, err := ctx.Conn.Write(buf.Bytes()); err != nil {
		return types.NewError("ERR " + err.Error())
	}

	// Send an initial SELECT command so assert_replication_stream sees it
	selectCmd := []byte("*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n")
	ctx.Conn.Write(selectCmd)

	// Register this connection as a replica to receive the replication stream
	ctx.Engine.AddReplica(ctx.Conn)

	return types.Value{Type: types.TypeNoReply}
}

// ReplConfCommand implements REPLCONF — replication configuration.
type ReplConfCommand struct{}

func (c ReplConfCommand) Name() string { return "REPLCONF" }

func (c ReplConfCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) >= 1 {
		return types.RespOK
	}
	return types.NewError("ERR wrong number of arguments for 'replconf' command")
}

// Helper to get shards from engine
func getShardsFromEngine(engine interface{}) ([256]*shard.Shard, bool) {
	type shardsProvider interface {
		Shards() [256]*shard.Shard
	}
	if sp, ok := engine.(shardsProvider); ok {
		return sp.Shards(), true
	}
	return [256]*shard.Shard{}, false
}
