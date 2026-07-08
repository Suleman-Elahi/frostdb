package generic

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"

	"frostdb/internal/blocking"
	"frostdb/internal/engine/api"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
	"frostdb/internal/scripting"
	"frostdb/internal/stats"
)

// Version info - these can be set at build time via ldflags
var (
	Version   = "7.2.0"
	GitSHA1   = "00000000"
	BuildID   = "frostdb"
	Mode      = "standalone"
	OS        = runtime.GOOS
	ArchBits  = fmt.Sprintf("%d", 32<<(^uint(0)>>63))
	GoVersion = runtime.Version()
)

// ServerStartTime is set at server startup
var ServerStartTime = time.Now()

type InfoCommand struct{}

func (c InfoCommand) Name() string {
	return "INFO"
}

func (c InfoCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	// Parse sections - Redis 7.0+ supports multiple sections
	sections := make(map[string]bool)
	
	if len(args) == 0 {
		sections["default"] = true
	} else {
		for _, arg := range args {
			section := strings.ToLower(string(arg.Str))
			sections[section] = true
		}
	}

	// Handle special section aliases
	if sections["default"] {
		sections["server"] = true
		sections["clients"] = true
		sections["memory"] = true
		sections["persistence"] = true
		sections["stats"] = true
		sections["replication"] = true
		sections["cpu"] = true
		sections["cluster"] = true
		sections["keyspace"] = true
		delete(sections, "default")
	}

	if sections["all"] {
		sections["server"] = true
		sections["clients"] = true
		sections["memory"] = true
		sections["persistence"] = true
		sections["stats"] = true
		sections["replication"] = true
		sections["cpu"] = true
		sections["commandstats"] = true
		sections["errorstats"] = true
		sections["keyspace"] = true
		sections["cluster"] = true
		delete(sections, "all")
	}

	if sections["everything"] {
		sections["server"] = true
		sections["clients"] = true
		sections["memory"] = true
		sections["persistence"] = true
		sections["stats"] = true
		sections["replication"] = true
		sections["cpu"] = true
		sections["commandstats"] = true
		sections["errorstats"] = true
		sections["keyspace"] = true
		sections["cluster"] = true
		sections["modules"] = true
		delete(sections, "everything")
	}

	var sb strings.Builder

	// Build sections in canonical order
	sectionOrder := []string{
		"server", "clients", "memory", "persistence", "stats",
		"replication", "cpu", "commandstats", "latencystats",
		"errorstats", "cluster", "modules", "keyspace",
	}

	for _, section := range sectionOrder {
		if !sections[section] {
			continue
		}
		
		content := buildSection(ctx, section)
		if content != "" {
			sb.WriteString(content)
		}
	}

	return types.NewString(sb.String())
}

func buildSection(ctx *context.Context, section string) string {
	switch section {
	case "server":
		return buildServerSection()
	case "clients":
		return buildClientsSection()
	case "memory":
		return buildMemorySection(ctx)
	case "persistence":
		return buildPersistenceSection(ctx)
	case "stats":
		return buildStatsSection()
	case "replication":
		return buildReplicationSection()
	case "cpu":
		return buildCPUSection()
	case "commandstats":
		return buildCommandStatsSection()
	case "latencystats":
		return buildLatencyStatsSection()
	case "errorstats":
		return buildErrorStatsSection()
	case "cluster":
		return buildClusterSection()
	case "modules":
		return buildModulesSection()
	case "keyspace":
		return buildKeyspaceSection(ctx)
	default:
		return ""
	}
}

func buildServerSection() string {
	st := stats.Get()
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "localhost"
	}

	var sb strings.Builder
	sb.WriteString("# Server\r\n")
	sb.WriteString(fmt.Sprintf("redis_version:%s\r\n", Version))
	sb.WriteString(fmt.Sprintf("redis_git_sha1:%s\r\n", GitSHA1))
	sb.WriteString("redis_git_dirty:0\r\n")
	sb.WriteString(fmt.Sprintf("redis_build_id:%s\r\n", BuildID))
	sb.WriteString(fmt.Sprintf("redis_mode:%s\r\n", Mode))
	sb.WriteString(fmt.Sprintf("os:%s %s\r\n", OS, runtime.GOARCH))
	sb.WriteString(fmt.Sprintf("arch_bits:%s\r\n", ArchBits))
	sb.WriteString("monotonic_clock:POSIX clock_gettime\r\n")
	sb.WriteString(fmt.Sprintf("multiplexing_api:go-%s\r\n", GoVersion))
	sb.WriteString(fmt.Sprintf("go_version:%s\r\n", GoVersion))
	sb.WriteString(fmt.Sprintf("process_id:%d\r\n", os.Getpid()))
	sb.WriteString("process_supervised:no\r\n")
	sb.WriteString(fmt.Sprintf("run_id:%s\r\n", getRunID()))
	sb.WriteString("tcp_port:6379\r\n")
	sb.WriteString(fmt.Sprintf("server_time_usec:%d\r\n", time.Now().UnixMicro()))
	sb.WriteString(fmt.Sprintf("uptime_in_seconds:%d\r\n", st.UptimeSeconds()))
	sb.WriteString(fmt.Sprintf("uptime_in_days:%d\r\n", st.UptimeDays()))
	sb.WriteString("hz:10\r\n")
	sb.WriteString("configured_hz:10\r\n")
	sb.WriteString(fmt.Sprintf("lru_clock:%d\r\n", uint32(time.Now().Unix())&((1<<24)-1)))
	
	exe, _ := os.Executable()
	if exe == "" {
		exe = "./frostdb"
	}
	sb.WriteString(fmt.Sprintf("executable:%s\r\n", exe))
	sb.WriteString("config_file:\r\n")
	sb.WriteString("io_threads_active:0\r\n")
	sb.WriteString("listener0:name=tcp,bind=*,port=6379\r\n")
	sb.WriteString("\r\n")
	
	return sb.String()
}

func buildClientsSection() string {
	st := stats.Get()
	
	var sb strings.Builder
	sb.WriteString("# Clients\r\n")
	sb.WriteString(fmt.Sprintf("connected_clients:%d\r\n", st.ConnectedClients.Load()))
	sb.WriteString("cluster_connections:0\r\n")
	sb.WriteString("maxclients:10000\r\n")
	sb.WriteString("client_recent_max_input_buffer:0\r\n")
	sb.WriteString("client_recent_max_output_buffer:0\r\n")
	sb.WriteString(fmt.Sprintf("blocked_clients:%d\r\n", blocking.Count()))
	sb.WriteString("tracking_clients:0\r\n")
	sb.WriteString("pubsub_clients:0\r\n")
	sb.WriteString("watching_clients:0\r\n")
	sb.WriteString("clients_in_timeout_table:0\r\n")
	sb.WriteString("total_watched_keys:0\r\n")
	sb.WriteString("total_blocking_keys:0\r\n")
	sb.WriteString("total_blocking_keys_on_nokey:0\r\n")
	sb.WriteString("\r\n")
	
	return sb.String()
}

func buildMemorySection(ctx *context.Context) string {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	// Get eviction config for maxmemory (handle nil engine)
	var evCfg api.EvictionConfig
	if ctx != nil && ctx.Engine != nil {
		evCfg = ctx.Engine.GetEvictionConfig()
	}

	var sb strings.Builder
	sb.WriteString("# Memory\r\n")
	sb.WriteString(fmt.Sprintf("used_memory:%d\r\n", ms.Alloc))
	sb.WriteString(fmt.Sprintf("used_memory_human:%s\r\n", formatBytes(ms.Alloc)))
	sb.WriteString(fmt.Sprintf("used_memory_rss:%d\r\n", ms.Sys))
	sb.WriteString(fmt.Sprintf("used_memory_rss_human:%s\r\n", formatBytes(ms.Sys)))
	sb.WriteString(fmt.Sprintf("used_memory_peak:%d\r\n", ms.TotalAlloc))
	sb.WriteString(fmt.Sprintf("used_memory_peak_human:%s\r\n", formatBytes(ms.TotalAlloc)))
	sb.WriteString(fmt.Sprintf("used_memory_peak_perc:%.2f%%\r\n", float64(ms.Alloc)/float64(ms.TotalAlloc)*100))
	sb.WriteString(fmt.Sprintf("used_memory_overhead:%d\r\n", ms.Sys-ms.HeapAlloc))
	sb.WriteString(fmt.Sprintf("used_memory_startup:%d\r\n", ms.Sys-ms.HeapAlloc))
	sb.WriteString(fmt.Sprintf("used_memory_dataset:%d\r\n", ms.HeapAlloc))
	sb.WriteString("used_memory_dataset_perc:100.00%\r\n")
	sb.WriteString(fmt.Sprintf("allocator_allocated:%d\r\n", ms.HeapAlloc))
	sb.WriteString(fmt.Sprintf("allocator_active:%d\r\n", ms.HeapInuse))
	sb.WriteString(fmt.Sprintf("allocator_resident:%d\r\n", ms.HeapSys))
	sb.WriteString(fmt.Sprintf("allocator_muzzy:%d\r\n", ms.HeapIdle))
	sb.WriteString(fmt.Sprintf("total_system_memory:%d\r\n", getSystemMemory()))
	sb.WriteString(fmt.Sprintf("total_system_memory_human:%s\r\n", formatBytes(getSystemMemory())))
	sb.WriteString("used_memory_lua:0\r\n")
	sb.WriteString("used_memory_vm_eval:0\r\n")
	sb.WriteString("used_memory_lua_human:0B\r\n")
	sb.WriteString("used_memory_scripts_eval:0\r\n")
	sb.WriteString(fmt.Sprintf("number_of_cached_scripts:%d\r\n", scripting.Global().Count()))
	sb.WriteString("number_of_functions:0\r\n")
	sb.WriteString("number_of_libraries:0\r\n")
	sb.WriteString("used_memory_vm_functions:0\r\n")
	sb.WriteString("used_memory_vm_total:0\r\n")
	sb.WriteString("used_memory_vm_total_human:0B\r\n")
	sb.WriteString("used_memory_functions:0\r\n")
	sb.WriteString("used_memory_scripts:0\r\n")
	sb.WriteString("used_memory_scripts_human:0B\r\n")
	sb.WriteString(fmt.Sprintf("maxmemory:%d\r\n", evCfg.MaxMemory))
	if evCfg.MaxMemory > 0 {
		sb.WriteString(fmt.Sprintf("maxmemory_human:%s\r\n", formatBytes(uint64(evCfg.MaxMemory))))
	} else {
		sb.WriteString("maxmemory_human:0B\r\n")
	}
	sb.WriteString(fmt.Sprintf("maxmemory_policy:%s\r\n", evCfg.Policy))
	sb.WriteString("allocator_frag_ratio:1.00\r\n")
	sb.WriteString("allocator_frag_bytes:0\r\n")
	sb.WriteString("allocator_rss_ratio:1.00\r\n")
	sb.WriteString("allocator_rss_bytes:0\r\n")
	sb.WriteString("rss_overhead_ratio:1.00\r\n")
	sb.WriteString("rss_overhead_bytes:0\r\n")
	
	fragRatio := float64(1.0)
	if ms.Alloc > 0 {
		fragRatio = float64(ms.Sys) / float64(ms.Alloc)
	}
	sb.WriteString(fmt.Sprintf("mem_fragmentation_ratio:%.2f\r\n", fragRatio))
	sb.WriteString(fmt.Sprintf("mem_fragmentation_bytes:%d\r\n", ms.Sys-ms.Alloc))
	sb.WriteString("mem_not_counted_for_evict:0\r\n")
	sb.WriteString("mem_replication_backlog:0\r\n")
	sb.WriteString("mem_total_replication_buffers:0\r\n")
	sb.WriteString("mem_clients_slaves:0\r\n")
	sb.WriteString("mem_clients_normal:0\r\n")
	sb.WriteString("mem_cluster_links:0\r\n")
	sb.WriteString("mem_aof_buffer:0\r\n")
	sb.WriteString("mem_allocator:go-runtime\r\n")
	sb.WriteString("mem_overhead_db_hashtable_rehashing:0\r\n")
	sb.WriteString("active_defrag_running:0\r\n")
	sb.WriteString("lazyfree_pending_objects:0\r\n")
	sb.WriteString("lazyfreed_objects:0\r\n")
	sb.WriteString("\r\n")
	
	return sb.String()
}

func buildPersistenceSection(ctx *context.Context) string {
	var sb strings.Builder
	sb.WriteString("# Persistence\r\n")
	sb.WriteString("loading:0\r\n")
	sb.WriteString("async_loading:0\r\n")
	sb.WriteString("current_cow_peak:0\r\n")
	sb.WriteString("current_cow_size:0\r\n")
	sb.WriteString("current_cow_size_age:0\r\n")
	sb.WriteString("current_fork_perc:0.00\r\n")
	sb.WriteString("current_save_keys_processed:0\r\n")
	sb.WriteString("current_save_keys_total:0\r\n")
	sb.WriteString(fmt.Sprintf("rdb_changes_since_last_save:%d\r\n", ctx.Engine.GetDirty()))
	sb.WriteString("rdb_bgsave_in_progress:0\r\n")
	sb.WriteString(fmt.Sprintf("rdb_last_save_time:%d\r\n", ServerStartTime.Unix()))
	sb.WriteString("rdb_last_bgsave_status:ok\r\n")
	sb.WriteString("rdb_last_bgsave_time_sec:-1\r\n")
	sb.WriteString("rdb_current_bgsave_time_sec:-1\r\n")
	sb.WriteString("rdb_saves:0\r\n")
	sb.WriteString("rdb_last_cow_size:0\r\n")
	sb.WriteString("rdb_last_load_keys_expired:0\r\n")
	sb.WriteString("rdb_last_load_keys_loaded:0\r\n")
	sb.WriteString("aof_enabled:1\r\n")  // FrostDB uses WAL
	sb.WriteString(fmt.Sprintf("aof_rewrite_in_progress:%d\r\n", ctx.Engine.AOFRewriteInProgress()))
	sb.WriteString("aof_rewrite_scheduled:0\r\n")
	sb.WriteString("aof_last_rewrite_time_sec:-1\r\n")
	sb.WriteString("aof_current_rewrite_time_sec:-1\r\n")
	sb.WriteString("aof_last_bgrewrite_status:ok\r\n")
	sb.WriteString("aof_last_write_status:ok\r\n")
	sb.WriteString("aof_last_cow_size:0\r\n")
	sb.WriteString("module_fork_in_progress:0\r\n")
	sb.WriteString("module_fork_last_cow_size:0\r\n")
	sb.WriteString("aof_rewrites:0\r\n")
	sb.WriteString("aof_current_size:0\r\n")
	sb.WriteString("aof_base_size:0\r\n")
	sb.WriteString("aof_pending_rewrite:0\r\n")
	sb.WriteString("aof_buffer_length:0\r\n")
	sb.WriteString("aof_pending_bio_fsync:0\r\n")
	sb.WriteString("aof_delayed_fsync:0\r\n")
	sb.WriteString("\r\n")
	
	return sb.String()
}

func buildStatsSection() string {
	st := stats.Get()
	
	var sb strings.Builder
	sb.WriteString("# Stats\r\n")
	sb.WriteString(fmt.Sprintf("total_connections_received:%d\r\n", st.TotalConnectionsReceived.Load()))
	sb.WriteString(fmt.Sprintf("total_commands_processed:%d\r\n", st.TotalCommandsProcessed.Load()))
	sb.WriteString("instantaneous_ops_per_sec:0\r\n")
	sb.WriteString(fmt.Sprintf("total_net_input_bytes:%d\r\n", st.TotalNetInputBytes.Load()))
	sb.WriteString(fmt.Sprintf("total_net_output_bytes:%d\r\n", st.TotalNetOutputBytes.Load()))
	sb.WriteString("total_net_repl_input_bytes:0\r\n")
	sb.WriteString("total_net_repl_output_bytes:0\r\n")
	sb.WriteString("instantaneous_input_kbps:0.00\r\n")
	sb.WriteString("instantaneous_output_kbps:0.00\r\n")
	sb.WriteString("instantaneous_input_repl_kbps:0.00\r\n")
	sb.WriteString("instantaneous_output_repl_kbps:0.00\r\n")
	sb.WriteString(fmt.Sprintf("rejected_connections:%d\r\n", st.RejectedConnections.Load()))
	sb.WriteString("sync_full:0\r\n")
	sb.WriteString("sync_partial_ok:0\r\n")
	sb.WriteString("sync_partial_err:0\r\n")
	sb.WriteString(fmt.Sprintf("expired_keys:%d\r\n", st.ExpiredKeys.Load()))
	sb.WriteString("expired_stale_perc:0.00\r\n")
	sb.WriteString("expired_time_cap_reached_count:0\r\n")
	sb.WriteString("expire_cycle_cpu_milliseconds:0\r\n")
	sb.WriteString(fmt.Sprintf("evicted_keys:%d\r\n", st.EvictedKeys.Load()))
	sb.WriteString("evicted_clients:0\r\n")
	sb.WriteString("evicted_scripts:0\r\n")
	sb.WriteString("total_eviction_exceeded_time:0\r\n")
	sb.WriteString("current_eviction_exceeded_time:0\r\n")
	sb.WriteString(fmt.Sprintf("keyspace_hits:%d\r\n", st.KeyspaceHits.Load()))
	sb.WriteString(fmt.Sprintf("keyspace_misses:%d\r\n", st.KeyspaceMisses.Load()))
	sb.WriteString("pubsub_channels:0\r\n")
	sb.WriteString("pubsub_patterns:0\r\n")
	sb.WriteString("pubsubshard_channels:0\r\n")
	sb.WriteString("latest_fork_usec:0\r\n")
	sb.WriteString("total_forks:0\r\n")
	sb.WriteString("migrate_cached_sockets:0\r\n")
	sb.WriteString("slave_expires_tracked_keys:0\r\n")
	sb.WriteString("active_defrag_hits:0\r\n")
	sb.WriteString("active_defrag_misses:0\r\n")
	sb.WriteString("active_defrag_key_hits:0\r\n")
	sb.WriteString("active_defrag_key_misses:0\r\n")
	sb.WriteString("total_active_defrag_time:0\r\n")
	sb.WriteString("current_active_defrag_time:0\r\n")
	sb.WriteString("tracking_total_keys:0\r\n")
	sb.WriteString("tracking_total_items:0\r\n")
	sb.WriteString("tracking_total_prefixes:0\r\n")
	sb.WriteString("unexpected_error_replies:0\r\n")
	sb.WriteString(fmt.Sprintf("total_error_replies:%d\r\n", st.TotalErrorReplies.Load()))
	sb.WriteString("dump_payload_sanitizations:0\r\n")
	sb.WriteString("total_reads_processed:0\r\n")
	sb.WriteString("total_writes_processed:0\r\n")
	sb.WriteString("io_threaded_reads_processed:0\r\n")
	sb.WriteString("io_threaded_writes_processed:0\r\n")
	sb.WriteString("client_query_buffer_limit_disconnections:0\r\n")
	sb.WriteString("client_output_buffer_limit_disconnections:0\r\n")
	sb.WriteString("reply_buffer_shrinks:0\r\n")
	sb.WriteString("reply_buffer_expands:0\r\n")
	sb.WriteString("eventloop_cycles:0\r\n")
	sb.WriteString("eventloop_duration_sum:0\r\n")
	sb.WriteString("eventloop_duration_cmd_sum:0\r\n")
	sb.WriteString("instantaneous_eventloop_cycles_per_sec:0\r\n")
	sb.WriteString("instantaneous_eventloop_duration_usec:0\r\n")
	sb.WriteString("acl_access_denied_auth:0\r\n")
	sb.WriteString("acl_access_denied_cmd:0\r\n")
	sb.WriteString("acl_access_denied_key:0\r\n")
	sb.WriteString("acl_access_denied_channel:0\r\n")
	sb.WriteString("\r\n")
	
	return sb.String()
}

func buildReplicationSection() string {
	var sb strings.Builder
	sb.WriteString("# Replication\r\n")
	sb.WriteString("role:master\r\n")
	sb.WriteString("connected_slaves:0\r\n")
	sb.WriteString("master_failover_state:no-failover\r\n")
	sb.WriteString(fmt.Sprintf("master_replid:%s\r\n", getRunID()))
	sb.WriteString("master_replid2:0000000000000000000000000000000000000000\r\n")
	sb.WriteString("master_repl_offset:0\r\n")
	sb.WriteString("second_repl_offset:-1\r\n")
	sb.WriteString("repl_backlog_active:0\r\n")
	sb.WriteString("repl_backlog_size:1048576\r\n")
	sb.WriteString("repl_backlog_first_byte_offset:0\r\n")
	sb.WriteString("repl_backlog_histlen:0\r\n")
	sb.WriteString("\r\n")
	
	return sb.String()
}

func buildCPUSection() string {
	var sb strings.Builder
	sb.WriteString("# CPU\r\n")
	// Go doesn't expose per-process CPU time easily, use zeros
	sb.WriteString("used_cpu_sys:0.000000\r\n")
	sb.WriteString("used_cpu_user:0.000000\r\n")
	sb.WriteString("used_cpu_sys_children:0.000000\r\n")
	sb.WriteString("used_cpu_user_children:0.000000\r\n")
	sb.WriteString("used_cpu_sys_main_thread:0.000000\r\n")
	sb.WriteString("used_cpu_user_main_thread:0.000000\r\n")
	sb.WriteString("\r\n")
	
	return sb.String()
}

func buildCommandStatsSection() string {
	st := stats.Get()
	cmdStats := st.GetCommandStats()
	
	var sb strings.Builder
	sb.WriteString("# Commandstats\r\n")
	
	// Sort command names for consistent output
	names := make([]string, 0, len(cmdStats))
	for name := range cmdStats {
		names = append(names, name)
	}
	sort.Strings(names)
	
	for _, name := range names {
		stat := cmdStats[name]
		sb.WriteString(fmt.Sprintf("cmdstat_%s:calls=%d,usec=%d,usec_per_call=%.2f,rejected_calls=%d,failed_calls=%d\r\n",
			strings.ToLower(name),
			stat.Calls,
			stat.Usec,
			stat.UsecPerCall,
			stat.RejectedCalls,
			stat.FailedCalls,
		))
	}
	sb.WriteString("\r\n")
	
	return sb.String()
}

func buildLatencyStatsSection() string {
	var sb strings.Builder
	sb.WriteString("# Latencystats\r\n")
	// Latency percentile tracking not implemented yet
	sb.WriteString("\r\n")
	return sb.String()
}

func buildErrorStatsSection() string {
	var sb strings.Builder
	sb.WriteString("# Errorstats\r\n")
	st := stats.Get()
	errStats := st.GetErrorStats()
	for prefix, count := range errStats {
		sb.WriteString(fmt.Sprintf("errorstat_%s:count=%d\r\n", prefix, count))
	}
	sb.WriteString("\r\n")
	return sb.String()
}

func buildClusterSection() string {
	var sb strings.Builder
	sb.WriteString("# Cluster\r\n")
	sb.WriteString("cluster_enabled:0\r\n")
	sb.WriteString("\r\n")
	return sb.String()
}

func buildModulesSection() string {
	var sb strings.Builder
	sb.WriteString("# Modules\r\n")
	sb.WriteString("\r\n")
	return sb.String()
}

func buildKeyspaceSection(ctx *context.Context) string {
	var sb strings.Builder
	sb.WriteString("# Keyspace\r\n")
	
	if ctx == nil || ctx.Engine == nil {
		sb.WriteString("\r\n")
		return sb.String()
	}
	
	// Try to get MultiDB interface for keyspace stats
	if mdb, ok := ctx.Engine.(api.MultiDB); ok {
		for db := 0; db < mdb.NumDBs(); db++ {
			size := mdb.DBSize(db)
			if size > 0 {
				// Count keys with expiry - this is expensive, approximate as 0 for now
				sb.WriteString(fmt.Sprintf("db%d:keys=%d,expires=0,avg_ttl=0\r\n", db, size))
			}
		}
	} else {
		// Single DB mode
		keys := ctx.Engine.Keys("*")
		if len(keys) > 0 {
			sb.WriteString(fmt.Sprintf("db0:keys=%d,expires=0,avg_ttl=0\r\n", len(keys)))
		}
	}
	
	sb.WriteString("\r\n")
	return sb.String()
}

// Helper functions

var runID string

func getRunID() string {
	if runID == "" {
		// Generate a pseudo-random run ID based on start time and PID
		runID = fmt.Sprintf("%040x", uint64(ServerStartTime.UnixNano())^uint64(os.Getpid()))
		if len(runID) > 40 {
			runID = runID[:40]
		}
	}
	return runID
}

func formatBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%dB", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f%c", float64(b)/float64(div), "KMGTPE"[exp])
}

func getSystemMemory() uint64 {
	// Try to get system memory - platform specific
	// For simplicity, return a reasonable default
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.Sys * 10 // Approximate
}
