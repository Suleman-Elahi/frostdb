package main

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"frostdb/internal/command/arraycmd"
	"frostdb/internal/command/generic"
	"frostdb/internal/command/geocmd"
	"frostdb/internal/command/hashcmd"
	"frostdb/internal/command/listcmd"
	"frostdb/internal/command/pubsub"
	"frostdb/internal/command/registry"
	"frostdb/internal/command/setcmd"
	streamcmd "frostdb/internal/command/streamcmd"
	strcmd "frostdb/internal/command/string"
	"frostdb/internal/command/tx"
	"frostdb/internal/command/zsetcmd"
	"frostdb/internal/engine/core"
	"frostdb/internal/engine/eviction"
	"frostdb/internal/engine/rdb"
	"frostdb/internal/lifecycle"
	"frostdb/internal/logger"
	"frostdb/internal/network/server"
	"frostdb/internal/scripting"
)

func main() {
	// CLI flags
	logLevel := "info"
	logJSON := false
	portStr := "6379"

	for i, arg := range os.Args {
		if (arg == "--port" || arg == "-port") && i+1 < len(os.Args) {
			portStr = os.Args[i+1]
		}
		if (arg == "--loglevel" || arg == "-loglevel") && i+1 < len(os.Args) {
			logLevel = os.Args[i+1]
		}
		if arg == "--log-json" || arg == "-log-json" {
			logJSON = true
		}
	}

	// Data directory
	dataDir := filepath.Join(".", "data")
	if envDir := os.Getenv("FROST_DATA_DIR"); envDir != "" {
		dataDir = envDir
	}

	// Parse config file if passed
	for _, arg := range os.Args[1:] {
		if !strings.HasPrefix(arg, "-") {
			if _, err := os.Stat(arg); err == nil {
				data, err := os.ReadFile(arg)
				if err == nil {
					lines := strings.Split(string(data), "\n")
					for _, line := range lines {
						line = strings.TrimSpace(line)
						if line == "" || strings.HasPrefix(line, "#") {
							continue
						}
						parts := strings.Fields(line)
						if len(parts) >= 2 {
							key := strings.ToLower(parts[0])
							val := parts[1]
							if key == "port" {
								portStr = val
							} else if key == "dir" {
								dataDir = val
							}
						}
					}
				}
				break
			}
		}
	}

	// Initialize structured logger
	level := parseLogLevel(logLevel)
	logger.Set(logger.New(logger.Options{
		Level:  level,
		JSON:   logJSON,
		Output: os.Stderr,
	}))
	logger.Debug("debug logging enabled", "level", logLevel)

	// Register all commands
	registerCommands()

	// Wire up Lua scripting dispatcher (avoids circular import)
	scripting.SetDispatcher(registry.Dispatch)

	// Initialize engine with persistence
	engine, err := core.NewWithPersistence(core.PersistOptions{
		DataDir:            dataDir,
		WALSyncOnWrite:     false,
		CheckpointInterval: 5 * time.Minute,
		Eviction: eviction.Config{
			MaxMemory:    0,
			Policy:       eviction.PolicyNoEviction,
			MaxSamples:   5,
			LFULogFactor: 10,
			LFUDecayTime: 1,
		},
	})
	if err != nil {
		logger.Error("failed to initialize engine", "error", err)
		os.Exit(1)
	}

	// Cast to concrete type for shard access
	db0View := engine.(*core.DBView)
	coreEngine := db0View.ParentEngine()

	// Handle RDB import
	rdbPath := ""
	if rdbPath == "" {
		defaultRDB := filepath.Join(dataDir, "dump.rdb")
		if _, err := os.Stat(defaultRDB); err == nil {
			rdbPath = defaultRDB
		}
	}
	if rdbPath != "" {
		imp := rdb.NewImporter(db0View.Shards())
		n, err := imp.ImportFile(rdbPath)
		if err != nil {
			logger.Error("RDB import failed", "path", rdbPath, "error", err)
			os.Exit(1)
		}
		logger.Info("RDB import complete", "keys", n, "path", rdbPath)
	}

	// Graceful shutdown manager
	shutdown := lifecycle.NewManager()

	// Register engine cleanup (WAL flush + close)
	shutdown.Register("engine", func() error {
		return coreEngine.Close()
	})

	// Start server
	addr := ":" + portStr
	srv := server.New(addr, engine)

	// Register server stop (drain connections)
	shutdown.Register("server", func() error {
		return srv.Stop()
	})

	logger.Info("starting FrostDB", "addr", addr, "dataDir", dataDir)

	// Run server in background, wait for shutdown signal
	go func() {
		if err := srv.Start(); err != nil {
			logger.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	fmt.Printf(" PID: %d Server initialized\n", os.Getpid())

	shutdown.Wait()
}

func parseLogLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func registerCommands() {
	registry.Register(hashcmd.HSetExCommand{})
	registry.Register(hashcmd.HGetExCommand{})
	registry.Register(hashcmd.NewHExpireCommand())
	registry.Register(hashcmd.NewHExpireAtCommand())
	registry.Register(hashcmd.HGetDelCommand{})
	registry.Register(hashcmd.NewHPExpireCommand())
	registry.Register(hashcmd.NewHPExpireAtCommand())
	registry.Register(hashcmd.NewHPersistCommand())
	registry.Register(hashcmd.NewHTtlCommand())
	registry.Register(hashcmd.NewHPTtlCommand())
	registry.Register(hashcmd.NewHExpireTimeCommand())
	registry.Register(hashcmd.NewHPExpireTimeCommand())
	registry.Register(generic.XClaimDummy{})
	registry.Register(generic.PingCommand{})
	registry.Register(generic.EchoCommand{})
	registry.Register(generic.ExistsCommand{})
	registry.Register(generic.DelCommand{})
	registry.Register(geocmd.GeoAddCommand{})
	registry.Register(geocmd.GeoPosCommand{})
	registry.Register(geocmd.GeoRadiusCommand{})
	registry.Register(geocmd.GeoRadiusROCommand{})
	registry.Register(geocmd.GeoRadiusByMemberCommand{})
	registry.Register(geocmd.GeoRadiusByMemberROCommand{})
	registry.Register(geocmd.GeoSearchCommand{})
	registry.Register(geocmd.GeoSearchStoreCommand{})
	registry.Register(geocmd.GeoHashCommand{})
	registry.Register(geocmd.GeoDistCommand{})
	registry.Register(generic.ExpireCommand{})
	registry.Register(generic.PersistCommand{})
	registry.Register(generic.TtlCommand{})
	registry.Register(generic.InfoCommand{})
	registry.Register(generic.ScanCommand{})
	registry.Register(generic.ClientCommand{})
	registry.Register(generic.HelloCommand{})
	registry.Register(generic.ConfigCommand{})
	registry.Register(generic.SaveCommand{})
	registry.Register(generic.BgSaveCommand{})
	registry.Register(generic.BgRewriteAofCommand{})
	registry.Register(generic.ObjectCommand{})
	registry.Register(generic.MemoryCommand{})
	registry.Register(generic.ClusterCommand{})
	registry.Register(generic.TypeCommand{})
	registry.Register(generic.RenameCommand{})
	registry.Register(generic.RenameNXCommand{})
	registry.Register(generic.DbSizeCommand{})
	registry.Register(generic.FlushDbCommand{All: false})
	registry.Register(generic.FlushDbCommand{All: true})
	registry.Register(generic.PExpireCommand{})
	registry.Register(generic.ExpireAtCommand{})
	registry.Register(generic.PExpireAtCommand{})
	registry.Register(generic.PTtlCommand{})
	registry.Register(generic.ExpireTimeCommand{})
	registry.Register(generic.PExpireTimeCommand{})
	registry.Register(generic.SelectCommand{})
	registry.Register(generic.MoveCommand{})
	registry.Register(generic.SwapDBCommand{})
	registry.Register(generic.UnlinkCommand{})
	registry.Register(generic.RandomKeyCommand{})
	registry.Register(generic.KeysCommand{})
	registry.Register(generic.CopyCommand{})
	registry.Register(generic.TouchCommand{})
	registry.Register(generic.TimeCommand{})
	registry.Register(generic.WaitCommand{})
	registry.Register(generic.WaitAofCommand{})
	registry.Register(generic.SortCommand{})
	registry.Register(generic.SortROCommand{})
	registry.Register(generic.DumpCommand{})
	registry.Register(generic.RestoreCommand{})
	registry.Register(generic.SlowlogCommand{})
	registry.Register(generic.DebugCommand{})
	registry.Register(generic.CommandCommand{})
	registry.Register(generic.LatencyCommand{})
	registry.Register(generic.AclCommand{})
	registry.Register(generic.ResetCommand{})
	registry.Register(generic.EvalShaCommand{})
	registry.Register(generic.EvalCommand{})
	registry.Register(generic.EvalRoCommand{})
	registry.Register(generic.EvalShaRoCommand{})
	registry.Register(generic.ScriptCommand{})
	registry.Register(generic.FunctionCommand{})
	registry.Register(generic.FcallCommand{})
	registry.Register(generic.FcallRoCommand{})
	registry.Register(generic.AuthCommand{})

	// Replication commands
	registry.Register(generic.SyncCommand{})
	registry.Register(generic.PSyncCommand{})
	registry.Register(generic.ReplConfCommand{})

	// String commands
	registry.Register(strcmd.GetCommand{})
	registry.Register(strcmd.SetCommand{})
	registry.Register(strcmd.IncrCommand{})
	registry.Register(strcmd.DecrCommand{})
	registry.Register(strcmd.IncrByCommand{})
	registry.Register(strcmd.DecrByCommand{})
	registry.Register(strcmd.IncrByFloatCommand{})
	registry.Register(strcmd.IncrExCommand{})
	registry.Register(strcmd.AppendCommand{})
	registry.Register(strcmd.StrlenCommand{})
	registry.Register(strcmd.SetBitCommand{})
	registry.Register(strcmd.GetBitCommand{})
	registry.Register(strcmd.BitCountCommand{})
	registry.Register(strcmd.BitPosCommand{})
	registry.Register(strcmd.MGetCommand{})
	registry.Register(strcmd.MSetCommand{})
	registry.Register(strcmd.MSetNXCommand{})
	registry.Register(strcmd.MSetEXCommand{})
	registry.Register(strcmd.GetSetCommand{})
	registry.Register(strcmd.GetExCommand{})
	registry.Register(strcmd.GetDelCommand{})
	registry.Register(strcmd.SetNXCommand{})
	registry.Register(strcmd.SetEXCommand{})
	registry.Register(strcmd.PSetEXCommand{})
	registry.Register(strcmd.GetRangeCommand{})
	registry.Register(strcmd.SubstrCommand{})
	registry.Register(strcmd.SetRangeCommand{})
	registry.Register(strcmd.BitFieldCommand{})
	registry.Register(strcmd.BitFieldROCommand{})
	registry.Register(strcmd.BitOpCommand{})
	registry.Register(strcmd.LCSCommand{})
	registry.Register(strcmd.DigestCommand{})
	registry.Register(strcmd.DelexCommand{})

	// Array commands
	registry.Register(arraycmd.ArSetCommand{})
	registry.Register(arraycmd.ArGetCommand{})
	registry.Register(arraycmd.ArLenCommand{})
	registry.Register(arraycmd.ArCountCommand{})
	registry.Register(arraycmd.ArDelCommand{})
	registry.Register(arraycmd.ArDelRangeCommand{})
	registry.Register(arraycmd.ArMSetCommand{})
	registry.Register(arraycmd.ArMGetCommand{})
	registry.Register(arraycmd.ArGetRangeCommand{})
	registry.Register(arraycmd.ArScanCommand{})
	registry.Register(arraycmd.ArGrepCommand{})
	registry.Register(arraycmd.ArInsertCommand{})
	registry.Register(arraycmd.ArRingCommand{})
	registry.Register(arraycmd.ArNextCommand{})
	registry.Register(arraycmd.ArSeekCommand{})
	registry.Register(arraycmd.ArLastItemsCommand{})
	registry.Register(arraycmd.ArOpCommand{})
	registry.Register(arraycmd.ArInfoCommand{})

	// Hash commands
	registry.Register(hashcmd.HSetCommand{})
	registry.Register(hashcmd.HExistsCommand{})
	registry.Register(hashcmd.HGetCommand{})
	registry.Register(hashcmd.HIncrByCommand{})
	registry.Register(hashcmd.HIncrByFloatCommand{})
	registry.Register(hashcmd.HDelCommand{})
	registry.Register(hashcmd.HGetAllCommand{})
	registry.Register(hashcmd.HLenCommand{})
	registry.Register(hashcmd.HSetNXCommand{})
	registry.Register(hashcmd.HKeysCommand{})
	registry.Register(hashcmd.HValsCommand{})
	registry.Register(hashcmd.HMGetCommand{})
	registry.Register(hashcmd.HMSetCommand{})
	registry.Register(hashcmd.HStrLenCommand{})
	registry.Register(hashcmd.HScanCommand{})
	registry.Register(hashcmd.HRandFieldCommand{})

	// List commands
	registry.Register(listcmd.PushCommand{IsLeft: true})
	registry.Register(listcmd.PushCommand{IsLeft: false})
	registry.Register(listcmd.PopCommand{IsLeft: true})
	registry.Register(listcmd.PopCommand{IsLeft: false})
	registry.Register(listcmd.BPopCommand{IsLeft: true})
	registry.Register(listcmd.BPopCommand{IsLeft: false})
	registry.Register(listcmd.BLMPOPCommand{})
	registry.Register(listcmd.BLMoveCommand{})
	registry.Register(listcmd.LMPOPCommand{})
	registry.Register(listcmd.RPopLPushCommand{})
	registry.Register(listcmd.BRPopLPushCommand{})
	registry.Register(listcmd.LRangeCommand{})
	registry.Register(listcmd.LMoveCommand{})
	registry.Register(listcmd.LRemCommand{})
	registry.Register(listcmd.LLenCommand{})
	registry.Register(listcmd.LIndexCommand{})
	registry.Register(listcmd.LSetCommand{})
	registry.Register(listcmd.LInsertCommand{})
	registry.Register(listcmd.LTrimCommand{})
	registry.Register(listcmd.LPosCommand{})
	registry.Register(listcmd.PushXCommand{IsLeft: true})
	registry.Register(listcmd.PushXCommand{IsLeft: false})

	// Sorted set commands
	registry.Register(zsetcmd.ZAddCommand{})
	registry.Register(zsetcmd.ZIncrByCommand{})
	registry.Register(zsetcmd.ZRangeCommand{})
	registry.Register(zsetcmd.ZRevRangeCommand{})
	registry.Register(zsetcmd.ZRangeByScoreCommand{})
	registry.Register(zsetcmd.ZRevRangeByScoreCommand{})
	registry.Register(zsetcmd.ZRangeByLexCommand{})
	registry.Register(zsetcmd.ZRevRangeByLexCommand{})
	registry.Register(zsetcmd.ZLexCountCommand{})
	registry.Register(zsetcmd.ZRemRangeByScoreCommand{})
	registry.Register(zsetcmd.ZRemRangeByRankCommand{})
	registry.Register(zsetcmd.ZRemRangeByLexCommand{})
	registry.Register(zsetcmd.ZRemCommand{})
	registry.Register(zsetcmd.ZPopMinCommand{})
	registry.Register(zsetcmd.ZPopMaxCommand{})
	registry.Register(zsetcmd.ZCardCommand{})
	registry.Register(zsetcmd.ZScoreCommand{})
	registry.Register(zsetcmd.ZCountCommand{})
	registry.Register(zsetcmd.BZPopMinCommand{})
	registry.Register(zsetcmd.ZRankCommand{})
	registry.Register(zsetcmd.ZRevRankCommand{})
	registry.Register(zsetcmd.ZScanCommand{})
	registry.Register(zsetcmd.ZUnionStoreCommand{})
	registry.Register(zsetcmd.ZInterStoreCommand{})
	registry.Register(zsetcmd.ZDiffStoreCommand{})
	registry.Register(zsetcmd.ZUnionCommand{})
	registry.Register(zsetcmd.ZInterCommand{})
	registry.Register(zsetcmd.ZDiffCommand{})
	registry.Register(zsetcmd.ZInterCardCommand{})
	registry.Register(zsetcmd.ZMPopCommand{})
	registry.Register(zsetcmd.BZPopMaxCommand{})
	registry.Register(zsetcmd.BZMPopCommand{})
	registry.Register(zsetcmd.ZMScoreCommand{})
	registry.Register(zsetcmd.ZRandMemberCommand{})
	registry.Register(zsetcmd.ZRangeStoreCommand{})

	// Set commands
	registry.Register(setcmd.SAddCommand{})
	registry.Register(setcmd.SRemCommand{})
	registry.Register(setcmd.SMembersCommand{})
	registry.Register(setcmd.SIsMemberCommand{})
	registry.Register(setcmd.SCardCommand{})
	registry.Register(setcmd.SPopCommand{})
	registry.Register(setcmd.SRandMemberCommand{})
	registry.Register(setcmd.SInterCommand{})
	registry.Register(setcmd.SInterStoreCommand{})
	registry.Register(setcmd.SUnionCommand{})
	registry.Register(setcmd.SUnionStoreCommand{})
	registry.Register(setcmd.SDiffCommand{})
	registry.Register(setcmd.SDiffStoreCommand{})
	registry.Register(setcmd.SMisMemberCommand{})
	registry.Register(setcmd.SInterCardCommand{})
	registry.Register(setcmd.SUnionCardCommand{})
	registry.Register(setcmd.SMoveCommand{})
	registry.Register(setcmd.SScanCommand{})

	// Stream commands
	registry.Register(streamcmd.XAddCommand{})
	registry.Register(streamcmd.XGroupCommand{})
	registry.Register(streamcmd.XReadGroupCommand{})
	registry.Register(streamcmd.XAckCommand{})
	registry.Register(streamcmd.XPendingCommand{})
	registry.Register(streamcmd.XAutoClaimCommand{})
	registry.Register(streamcmd.XRevRangeCommand{})
	registry.Register(streamcmd.XLenCommand{})
	registry.Register(streamcmd.XRangeCommand{})
	registry.Register(streamcmd.XTrimCommand{})
	registry.Register(streamcmd.XInfoCommand{})
	registry.Register(streamcmd.XReadCommand{})
	registry.Register(streamcmd.XDelCommand{})
	registry.Register(streamcmd.XAckDelCommand{})
	registry.Register(streamcmd.XIDMPRecordCommand{})
	registry.Register(streamcmd.XCfgSetCommand{})
	registry.Register(streamcmd.XSetIDCommand{})
	registry.Register(streamcmd.XDelexCommand{})

	// Transaction commands
	registry.Register(tx.MultiCommand{})
	registry.Register(tx.ExecCommand{})
	registry.Register(tx.DiscardCommand{})
	registry.Register(tx.WatchCommand{})
	registry.Register(tx.UnwatchCommand{})

	// PubSub commands
	registry.Register(pubsub.SubscribeCommand{})
	registry.Register(pubsub.UnsubscribeCommand{})
	registry.Register(pubsub.PublishCommand{})
	registry.Register(pubsub.PSubscribeCommand{})
	registry.Register(pubsub.PUnsubscribeCommand{})
	registry.Register(pubsub.PubSubMetaCommand{})
}
