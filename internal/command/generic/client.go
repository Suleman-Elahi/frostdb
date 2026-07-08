package generic

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// ClientInfo holds per-client information for CLIENT LIST.
type ClientInfo struct {
	Addr      string
	LastCmd   string
	Blocked   bool
	ID        int
	UnblockCh chan UnblockType // closed/non-nil when client wants unblock notifications
}

// UnblockType indicates the type of unblock.
type UnblockType int

const (
	UnblockNormal UnblockType = iota
	UnblockTimeout
	UnblockError
)

// clientStates tracks connected clients for CLIENT LIST.
var clientStates sync.Map

// clientUnblockChs maps client ID to unblock channel.
var clientUnblockChs sync.Map

// RegisterClient adds a client to the tracking map.
func RegisterClient(ctx *context.Context) {
	if ctx.Conn == nil {
		return
	}
	addr := ctx.Conn.RemoteAddr().String()
	clientStates.Store(addr, &ClientInfo{
		Addr:    addr,
		LastCmd: "",
		Blocked: false,
		ID:      ctx.ClientID,
	})
}

// UnregisterClient removes a client from the tracking map.
func UnregisterClient(ctx *context.Context) {
	if ctx.Conn == nil {
		return
	}
	addr := ctx.Conn.RemoteAddr().String()
	clientStates.Delete(addr)
}

// UpdateClientBlocked updates the blocked flag for a client.
func UpdateClientBlocked(ctx *context.Context, blocked bool) {
	ctx.Blocked = blocked
	if ctx.Conn == nil {
		return
	}
	addr := ctx.Conn.RemoteAddr().String()
	if v, ok := clientStates.Load(addr); ok {
		info := v.(*ClientInfo)
		info.Blocked = blocked
	}
}

// UpdateClientLastCmd updates the last command for a client.
func UpdateClientLastCmd(ctx *context.Context, cmd string) {
	ctx.LastCmd = cmd
	if ctx.Conn == nil {
		return
	}
	addr := ctx.Conn.RemoteAddr().String()
	if v, ok := clientStates.Load(addr); ok {
		info := v.(*ClientInfo)
		info.LastCmd = cmd
	}
}

// RegisterClientUnblockCh registers a client's unblock channel for CLIENT UNBLOCK.
func RegisterClientUnblockCh(clientID int, ch chan UnblockType) {
	clientUnblockChs.Store(clientID, ch)
}

// UnregisterClientUnblockCh removes a client's unblock channel.
func UnregisterClientUnblockCh(clientID int) {
	clientUnblockChs.Delete(clientID)
}

// UnblockClientByID unblocks a client by its ID. Returns 1 if the client was
// unblocked, 0 if the client was not found or not blocked.
func UnblockClientByID(clientID int, errorMode bool) int {
	chVal, ok := clientUnblockChs.Load(clientID)
	if !ok {
		return 0
	}
	ch := chVal.(chan UnblockType)
	unblockType := UnblockNormal
	if errorMode {
		unblockType = UnblockError
	}
	select {
	case ch <- unblockType:
		return 1
	default:
		return 0
	}
}

type ClientCommand struct{}

func (c ClientCommand) Name() string {
	return "CLIENT"
}

func (c ClientCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) == 0 {
		return types.NewError("ERR wrong number of arguments for 'client' command")
	}

	sub := strings.ToUpper(string(args[0].Str))
	switch sub {
	case "SETINFO":
		// Compatibility no-op: modern clients may send this on connect.
		if len(args) != 3 {
			return types.NewError("ERR wrong number of arguments for 'client|setinfo' command")
		}
		attr := strings.ToUpper(string(args[1].Str))
		if attr != "LIB-NAME" && attr != "LIB-VER" {
			return types.NewError("ERR syntax error")
		}
		return types.NewSimpleString("OK")
	case "SETNAME":
		if len(args) != 2 {
			return types.NewError("ERR wrong number of arguments for 'client|setname' command")
		}
		ctx.ClientName = string(args[1].Str)
		return types.NewSimpleString("OK")
	case "GETNAME":
		if len(args) != 1 {
			return types.NewError("ERR wrong number of arguments for 'client|getname' command")
		}
		if ctx.ClientName == "" {
			return types.RespNil
		}
		return types.NewString(ctx.ClientName)
	case "LIST":
		// Return info about all connected clients
		// Supports optional filter: CLIENT LIST [TYPE type] [ID id1 id2 ...]
		var sb strings.Builder
		filterIDs := make(map[int]bool)
		hasFilter := false
		// Parse optional filters
		for i := 1; i < len(args); i++ {
			filterType := strings.ToUpper(string(args[i].Str))
			switch filterType {
			case "ID":
				i++
				for i < len(args) {
					id, err := strconv.Atoi(string(args[i].Str))
					if err != nil {
						break
					}
					filterIDs[id] = true
					hasFilter = true
					i++
				}
			}
		}
		clientStates.Range(func(key, value interface{}) bool {
			info := value.(*ClientInfo)
			if hasFilter && !filterIDs[info.ID] {
				return true
			}
			flags := "N"
			if info.Blocked {
				flags = "b"
			}
			sb.WriteString(fmt.Sprintf("id=%d addr=%s name= cmd=%s flags=%s\n",
				info.ID, info.Addr, info.LastCmd, flags))
			return true
		})
		return types.NewString(sb.String())
	case "INFO":
		// Return info about the current connection
		addr := ""
		if ctx.Conn != nil {
			addr = ctx.Conn.RemoteAddr().String()
		}
		info := fmt.Sprintf("id=%d addr=%s name=%s cmd=client", ctx.ClientID, addr, ctx.ClientName)
		return types.NewString(info)
	case "ID":
		if len(args) != 1 {
			return types.NewError("ERR wrong number of arguments for 'client|id' command")
		}
		return types.NewInt(int64(ctx.ClientID))
	case "REPLY":
		if len(args) != 2 {
			return types.NewError("ERR wrong number of arguments for 'client|reply' command")
		}
		mode := strings.ToUpper(string(args[1].Str))
		if mode == "OFF" || mode == "SKIP" {
			// In Redis, CLIENT REPLY OFF and SKIP do not return a reply!
			return types.Value{Type: types.TypeNoReply}
		}
		return types.NewSimpleString("OK")
	case "NO-TOUCH":
		if len(args) != 2 {
			return types.NewError("ERR wrong number of arguments for 'client|no-touch' command")
		}
		mode := strings.ToUpper(string(args[1].Str))
		if mode != "ON" && mode != "OFF" {
			return types.NewError("ERR syntax error")
		}
		return types.NewSimpleString("OK")
	case "UNBLOCK":
		if len(args) < 2 || len(args) > 3 {
			return types.NewError("ERR wrong number of arguments for 'client|unblock' command")
		}
		targetID, err := strconv.Atoi(string(args[1].Str))
		if err != nil {
			return types.NewError("ERR value is not an integer or out of range")
		}
		errorMode := false
		if len(args) == 3 && strings.ToUpper(string(args[2].Str)) == "ERROR" {
			errorMode = true
		}
		unblocked := UnblockClientByID(targetID, errorMode)
		return types.NewInt(int64(unblocked))
	default:
		return types.NewError("ERR unknown subcommand '" + strings.ToLower(sub) + "'")
	}
}

