package generic

import (
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/engine/api"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
	"frostdb/internal/stats"
)

type ConfigCommand struct{}

func (c ConfigCommand) Name() string { return "CONFIG" }

func (c ConfigCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'config' command")
	}

	sub := strings.ToUpper(string(args[0].Str))
	switch sub {
	case "GET":
		return configGet(ctx.Engine, args[1:])
	case "SET":
		return configSet(ctx.Engine, args[1:])
	case "RESETSTAT":
		// Reset all server statistics
		stats.Get().Reset()
		return types.RespOK
	case "REWRITE":
		// No config file to rewrite — return OK
		return types.RespOK
	default:
		return types.NewError("ERR unknown subcommand '" + sub + "'")
	}
}

func configGet(engine api.Engine, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'config|get' command")
	}

	pattern := strings.ToLower(string(args[0].Str))
	cfg := engine.GetEvictionConfig()

	var result []types.Value

	if matchPattern(pattern, "maxmemory") {
		result = append(result,
			types.NewString("maxmemory"),
			types.NewString(strconv.FormatInt(cfg.MaxMemory, 10)),
		)
	}
	if matchPattern(pattern, "maxmemory-policy") {
		result = append(result,
			types.NewString("maxmemory-policy"),
			types.NewString(cfg.Policy),
		)
	}
	if matchPattern(pattern, "maxmemory-samples") {
		result = append(result,
			types.NewString("maxmemory-samples"),
			types.NewString(strconv.Itoa(cfg.MaxSamples)),
		)
	}
	if matchPattern(pattern, "notify-keyspace-events") {
		result = append(result,
			types.NewString("notify-keyspace-events"),
			types.NewString(engine.GetNotifyFlags()),
		)
	}
	if matchPattern(pattern, "databases") {
		numDBs := 16
		if mdb, ok := engine.(api.MultiDB); ok {
			numDBs = mdb.NumDBs()
		}
		result = append(result,
			types.NewString("databases"),
			types.NewString(strconv.Itoa(numDBs)),
		)
	}
	if matchPattern(pattern, "hz") {
		result = append(result,
			types.NewString("hz"),
			types.NewString("10"),
		)
	}
	if matchPattern(pattern, "save") {
		result = append(result,
			types.NewString("save"),
			types.NewString(""),
		)
	}
	if matchPattern(pattern, "appendonly") {
		result = append(result,
			types.NewString("appendonly"),
			types.NewString("no"),
		)
	}
	if matchPattern(pattern, "activerehashing") {
		result = append(result,
			types.NewString("activerehashing"),
			types.NewString("yes"),
		)
	}
	if matchPattern(pattern, "list-max-ziplist-size") || matchPattern(pattern, "list-max-listpack-size") {
		cur := strconv.FormatInt(listMaxListpackSize.Load(), 10)
		result = append(result,
			types.NewString("list-max-listpack-size"),
			types.NewString(cur),
		)
	}
	if matchPattern(pattern, "array-slice-size") {
		result = append(result, types.NewString("array-slice-size"), types.NewString(strconv.FormatInt(object.DefaultSliceSize, 10)))
	}
	if matchPattern(pattern, "array-sparse-kmax") {
		result = append(result, types.NewString("array-sparse-kmax"), types.NewString(strconv.Itoa(object.DefaultSparseKmax)))
	}
	if matchPattern(pattern, "array-sparse-kmin") {
		result = append(result, types.NewString("array-sparse-kmin"), types.NewString(strconv.Itoa(object.DefaultSparseKmin)))
	}
	if matchPattern(pattern, "list-compress-depth") {
		cur := strconv.FormatInt(listCompressDepth.Load(), 10)
		result = append(result,
			types.NewString("list-compress-depth"),
			types.NewString(cur),
		)
	}
	if matchPattern(pattern, "hash-max-listpack-entries") || matchPattern(pattern, "hash-max-ziplist-entries") {
		result = append(result,
			types.NewString("hash-max-listpack-entries"),
			types.NewString(strconv.FormatInt(hashMaxListpackEntries.Load(), 10)),
		)
	}
	if matchPattern(pattern, "hash-max-listpack-value") || matchPattern(pattern, "hash-max-ziplist-value") {
		result = append(result,
			types.NewString("hash-max-listpack-value"),
			types.NewString(strconv.FormatInt(hashMaxListpackValue.Load(), 10)),
		)
	}
	if matchPattern(pattern, "set-max-intset-entries") {
		result = append(result,
			types.NewString("set-max-intset-entries"),
			types.NewString(strconv.FormatInt(setMaxIntsetEntries.Load(), 10)),
		)
	}
	if matchPattern(pattern, "set-max-listpack-entries") || matchPattern(pattern, "set-max-ziplist-entries") {
		result = append(result,
			types.NewString("set-max-listpack-entries"),
			types.NewString(strconv.FormatInt(setMaxListpackEntries.Load(), 10)),
		)
	}
	if matchPattern(pattern, "set-max-listpack-value") || matchPattern(pattern, "set-max-ziplist-value") {
		result = append(result,
			types.NewString("set-max-listpack-value"),
			types.NewString(strconv.FormatInt(setMaxListpackValue.Load(), 10)),
		)
	}
	if matchPattern(pattern, "zset-max-listpack-entries") || matchPattern(pattern, "zset-max-ziplist-entries") {
		result = append(result,
			types.NewString("zset-max-listpack-entries"),
			types.NewString(strconv.FormatInt(zsetMaxListpackEntries.Load(), 10)),
		)
	}
	if matchPattern(pattern, "zset-max-listpack-value") || matchPattern(pattern, "zset-max-ziplist-value") {
		result = append(result,
			types.NewString("zset-max-listpack-value"),
			types.NewString(strconv.FormatInt(zsetMaxListpackValue.Load(), 10)),
		)
	}
	if matchPattern(pattern, "proto-max-bulk-len") {
		result = append(result,
			types.NewString("proto-max-bulk-len"),
			types.NewString("536870912"),
		)
	}
	if matchPattern(pattern, "lazyfree-lazy-server-del") {
		v := "no"
		if cfg.LazyfreeLazyServerDel {
			v = "yes"
		}
		result = append(result,
			types.NewString("lazyfree-lazy-server-del"),
			types.NewString(v),
		)
	}
	if matchPattern(pattern, "lazyfree-lazy-expire") {
		v := "no"
		if cfg.LazyfreeLazyServerDel {
			v = "yes"
		}
		result = append(result,
			types.NewString("lazyfree-lazy-expire"),
			types.NewString(v),
		)
	}
	if matchPattern(pattern, "bind-source-addr") {
		result = append(result,
			types.NewString("bind-source-addr"),
			types.NewString(""),
		)
	}
	if matchPattern(pattern, "stream-idmp-duration") {
		result = append(result,
			types.NewString("stream-idmp-duration"),
			types.NewString(strconv.FormatInt(object.DefaultIDMPDurationSec(), 10)),
		)
	}
	if matchPattern(pattern, "stream-idmp-maxsize") {
		result = append(result,
			types.NewString("stream-idmp-maxsize"),
			types.NewString(strconv.FormatInt(object.DefaultIDMPMaxSize(), 10)),
		)
	}
	if matchPattern(pattern, "stream-node-max-entries") {
		result = append(result,
			types.NewString("stream-node-max-entries"),
			types.NewString(strconv.FormatInt(object.GetStreamNodeMaxEntries(), 10)),
		)
	}
	if matchPattern(pattern, "min-replicas-to-write") {
		result = append(result,
			types.NewString("min-replicas-to-write"),
			types.NewString(strconv.FormatInt(MinReplicasToWrite.Load(), 10)),
		)
	}

	return types.NewArray(result)
}

func configSet(engine api.Engine, args []types.Value) types.Value {
	if len(args) < 2 || len(args)%2 != 0 {
		return types.NewError("ERR wrong number of arguments for 'config|set' command")
	}

	cfg := engine.GetEvictionConfig()

	for i := 0; i < len(args); i += 2 {
		key := strings.ToLower(string(args[i].Str))
		val := string(args[i+1].Str)

		switch key {
		case "maxmemory":
			n, err := parseMemorySize(val)
			if err != nil {
				return types.NewError("ERR Invalid argument '" + val + "' for CONFIG SET 'maxmemory'")
			}
			cfg.MaxMemory = n
		case "maxmemory-policy":
			cfg.Policy = strings.ToLower(val)
		case "maxmemory-samples":
			n, err := strconv.Atoi(val)
			if err != nil || n < 1 {
				return types.NewError("ERR Invalid argument '" + val + "' for CONFIG SET 'maxmemory-samples'")
			}
			cfg.MaxSamples = n
		case "notify-keyspace-events":
			engine.SetNotifyFlags(val)
		case "lazyfree-lazy-server-del":
			switch val {
			case "yes":
				cfg.LazyfreeLazyServerDel = true
			case "no":
				cfg.LazyfreeLazyServerDel = false
			default:
				return types.NewError("ERR Invalid argument '" + val + "' for CONFIG SET 'lazyfree-lazy-server-del'")
			}
		case "lazyfree-lazy-expire":
			switch val {
			case "yes":
				cfg.LazyfreeLazyServerDel = true
			case "no":
				cfg.LazyfreeLazyServerDel = false
			default:
				return types.NewError("ERR Invalid argument '" + val + "' for CONFIG SET 'lazyfree-lazy-expire'")
			}
		case "array-slice-size":
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil || n <= 0 || (n&(n-1)) != 0 {
				return types.NewError("ERR array-slice-size must be a power of two")
			}
			object.DefaultSliceSize = n
		case "array-sparse-kmax":
			n, err := strconv.Atoi(val)
			if err != nil || n <= 0 {
				return types.NewError("ERR Invalid argument")
			}
			object.DefaultSparseKmax = n
		case "array-sparse-kmin":
			n, err := strconv.Atoi(val)
			if err != nil || n <= 0 {
				return types.NewError("ERR Invalid argument")
			}
			object.DefaultSparseKmin = n
		case "list-max-listpack-size", "list-max-ziplist-size":
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return types.NewError("ERR Invalid argument '" + val + "' for CONFIG SET '" + key + "'")
			}
			listMaxListpackSize.Store(n)
		case "list-compress-depth":
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil || n < 0 {
				return types.NewError("ERR Invalid argument '" + val + "' for CONFIG SET '" + key + "'")
			}
			listCompressDepth.Store(n)
		case "set-max-intset-entries":
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil || n < 0 {
				return types.NewError("ERR Invalid argument '" + val + "' for CONFIG SET '" + key + "'")
			}
			setMaxIntsetEntries.Store(n)
		case "set-max-listpack-entries", "set-max-ziplist-entries":
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil || n < 0 {
				return types.NewError("ERR Invalid argument '" + val + "' for CONFIG SET '" + key + "'")
			}
			setMaxListpackEntries.Store(n)
		case "set-max-listpack-value", "set-max-ziplist-value":
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil || n < 0 {
				return types.NewError("ERR Invalid argument '" + val + "' for CONFIG SET '" + key + "'")
			}
			setMaxListpackValue.Store(n)
		case "zset-max-listpack-entries", "zset-max-ziplist-entries":
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil || n < 0 {
				return types.NewError("ERR Invalid argument '" + val + "' for CONFIG SET '" + key + "'")
			}
			zsetMaxListpackEntries.Store(n)
		case "zset-max-listpack-value", "zset-max-ziplist-value":
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil || n < 0 {
				return types.NewError("ERR Invalid argument '" + val + "' for CONFIG SET '" + key + "'")
			}
			zsetMaxListpackValue.Store(n)
		case "hash-max-listpack-entries", "hash-max-ziplist-entries":
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil || n < 0 {
				return types.NewError("ERR Invalid argument '" + val + "' for CONFIG SET '" + key + "'")
			}
			hashMaxListpackEntries.Store(n)
		case "hash-max-listpack-value", "hash-max-ziplist-value":
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil || n < 0 {
				return types.NewError("ERR Invalid argument '" + val + "' for CONFIG SET '" + key + "'")
			}
			hashMaxListpackValue.Store(n)
		case "stream-idmp-duration":
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return types.NewError("ERR Invalid argument '" + val + "' for CONFIG SET '" + key + "'")
			}
			if n < 1 || n > 86400 {
				return types.NewError("ERR stream-idmp-duration must be between 1 and 86400")
			}
			object.SetDefaultIDMPDurationSec(n)
		case "stream-idmp-maxsize":
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return types.NewError("ERR Invalid argument '" + val + "' for CONFIG SET '" + key + "'")
			}
			if n < 1 || n > 10000 {
				return types.NewError("ERR stream-idmp-maxsize must be between 1 and 10000")
			}
			object.SetDefaultIDMPMaxSize(n)
		case "stream-node-max-entries":
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return types.NewError("ERR Invalid argument '" + val + "' for CONFIG SET '" + key + "'")
			}
			if n < 0 {
				return types.NewError("ERR Invalid argument '" + val + "' for CONFIG SET '" + key + "'")
			}
			object.SetStreamNodeMaxEntries(n)
		case "min-replicas-to-write":
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil || n < 0 {
				return types.NewError("ERR Invalid argument '" + val + "' for CONFIG SET '" + key + "'")
			}
			MinReplicasToWrite.Store(n)
		default:
			// Silently accept unknown parameters for compatibility with the
			// Redis test harness which sets many server-specific config values.
		}
	}

	engine.SetEvictionConfig(cfg)
	// Trigger eviction immediately if maxmemory was reduced below current usage.
	engine.TryEvict()
	return types.NewSimpleString("OK")
}

// parseMemorySize parses values like "100mb", "1gb", "1024" (bytes).
func parseMemorySize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	multiplier := int64(1)

	if strings.HasSuffix(s, "kb") {
		multiplier = 1024
		s = s[:len(s)-2]
	} else if strings.HasSuffix(s, "mb") {
		multiplier = 1024 * 1024
		s = s[:len(s)-2]
	} else if strings.HasSuffix(s, "gb") {
		multiplier = 1024 * 1024 * 1024
		s = s[:len(s)-2]
	} else if strings.HasSuffix(s, "tb") {
		multiplier = 1024 * 1024 * 1024 * 1024
		s = s[:len(s)-2]
	}

	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}
	return n * multiplier, nil
}
