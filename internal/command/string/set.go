package str

import (
	"errors"
	"strconv"
	"strings"
	"unsafe"

	"frostdb/internal/engine/keyevent"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
	"frostdb/internal/stats"
)

type SetCommand struct{}

func (c SetCommand) Name() string { return "SET" }

func (c SetCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'set' command")
	}

	// Use unsafe.String for the key: the string is used only as a map key
	// inside Engine.Set and does not escape to the heap in the common path.
	key := unsafe.String(unsafe.SliceData(args[0].Str), len(args[0].Str))
	val := string(args[1].Str) // value must be a real string (stored in Object)

	var expireAt int64 // 0 = no expiry
	nx := false        // SET only if Not eXists
	xx := false        // SET only if eXists
	keepTTL := false
	var getOld bool // GET option: return old value

	// Conditional overwrite options (Frost extensions)
	cond := DelexNone
	var condArg []byte // condition argument value (for IFEQ/IFNE/IFDEQ/IFDNE)

	// Parse options: EX, PX, EXAT, PXAT, NX, XX, KEEPTTL, GET, IFEQ, IFNE, IFDEQ, IFDNE
	for i := 2; i < len(args); i++ {
		opt := strings.ToUpper(string(args[i].Str))
		switch opt {
		case "EX":
			if i+1 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			i++
			secs, err := parseInt(args[i].Str)
			if err != nil || secs <= 0 {
				return types.NewError("ERR invalid expire time in 'set' command")
			}
			expireAt = ctx.NowMs() + secs*1000
		case "PX":
			if i+1 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			i++
			ms, err := parseInt(args[i].Str)
			if err != nil || ms <= 0 {
				return types.NewError("ERR invalid expire time in 'set' command")
			}
			expireAt = ctx.NowMs() + ms
		case "EXAT":
			if i+1 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			i++
			ts, err := parseInt(args[i].Str)
			if err != nil || ts <= 0 {
				return types.NewError("ERR invalid expire time in 'set' command")
			}
			expireAt = ts * 1000
		case "PXAT":
			if i+1 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			i++
			ts, err := parseInt(args[i].Str)
			if err != nil || ts <= 0 {
				return types.NewError("ERR invalid expire time in 'set' command")
			}
			expireAt = ts
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "KEEPTTL":
			keepTTL = true
		case "GET":
			getOld = true
		case "IFEQ", "IFNE", "IFDEQ", "IFDNE":
			if i+1 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			newCond, _ := parseDelexCondition(args[i].Str)
			cond = newCond
			i++
			condArg = args[i].Str
		default:
			return types.NewError("ERR syntax error")
		}
	}

	// NX and XX are mutually exclusive
	if nx && xx {
		return types.NewError("ERR syntax error")
	}
	// Conditional options are mutually exclusive with NX/XX
	if cond != DelexNone && (nx || xx) {
		return types.NewError("ERR syntax error")
	}

	// Always fetch existing object for NX/XX/GET/KEEPTTL/conditional checks
	// and for key event notifications (new/overwritten/type_changed).
	var oldObj *object.Object
	oldObj, _ = ctx.Engine.Get(key)

	// ── GET option: validate type up-front (WRONGTYPE before any SET) ─────
	var oldStrVal string
	var oldStrIsValid bool
	if getOld && oldObj != nil {
		if oldObj.Type != object.TypeString {
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		oldStrVal, oldStrIsValid = oldObj.Value.(string)
	}

	// ── NX: only set if key does not exist ────────────────────────────────
	if nx && oldObj != nil {
		if getOld {
			if oldStrIsValid {
				return types.Value{Type: types.TypeBulkString, Str: []byte(oldStrVal)}
			}
			return types.RespNil
		}
		return types.RespNil
	}
	// ── XX: only set if key exists ────────────────────────────────────────
	if xx && oldObj == nil {
		if getOld {
			return types.RespNil
		}
		return types.RespNil
	}

	// ── Conditional (IFEQ/IFNE/IFDEQ/IFDNE) ──────────────────────────────
	if cond != DelexNone {
		if oldObj != nil && oldObj.Type != object.TypeString {
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		// Validate digest format only when the key exists and we'll actually
		// compare. When the key is missing, the condition can never be met
		// for IFEQ/IFDEQ, so we don't need a valid digest.
		var condDigest string
		if (cond == DelexIFDEQ || cond == DelexIFDNE) && oldObj != nil {
			var err error
			condDigest, err = parseDigestArg(condArg)
			if err != nil {
				return types.NewError(err.Error())
			}
		}

		var condMet bool
		if oldObj == nil {
			// Key doesn't exist:
			//   IFEQ / IFDEQ → condition can never be met (no value to compare)
			//   IFNE / IFDNE → condition is met (key absent = value differs from any arg)
			switch cond {
			case DelexIFEQ, DelexIFDEQ:
				condMet = false
			case DelexIFNE, DelexIFDNE:
				condMet = true
			}
		} else {
			sv, _ := oldObj.Value.(string)
			switch cond {
			case DelexIFEQ:
				condMet = sv == string(condArg)
			case DelexIFNE:
				condMet = sv != string(condArg)
			case DelexIFDEQ:
				condMet = digestString(sv) == condDigest
			case DelexIFDNE:
				condMet = digestString(sv) != condDigest
			}
		}

		if !condMet {
			// Condition not met – don't set. Return old value if GET is set.
			if getOld {
				if oldObj == nil {
					return types.RespNil
				}
				if oldStrIsValid {
					return types.Value{Type: types.TypeBulkString, Str: []byte(oldStrVal)}
				}
				return types.RespNil
			}
			return types.RespNil
		}
	}

	// ── Preserve existing TTL if KEEPTTL ──────────────────────────────────
	if keepTTL && oldObj != nil {
		expireAt = oldObj.ExpireAt
	}

	// ── Handle past expiration: key should not be stored ──────────────────
	// Redis behaviour: when expireAt is in the past, the key is not persisted
	// and expired_keys is incremented (regardless of whether key previously existed).
	if expireAt > 0 && expireAt <= ctx.NowMs() {
		// We must delete the key if it exists, regardless of needExisting
		ctx.Engine.Del(key)
		// Always increment expired_keys — matches `[s expired_keys]` in tests.
		stats.Get().ExpiredKeys.Add(1)

		delCmd := "DEL"
		if ctx.Engine.GetEvictionConfig().LazyfreeLazyServerDel {
			delCmd = "UNLINK"
		}
		ctx.ReplicateAs = [][]byte{[]byte(delCmd), []byte(key)}

		if getOld {
			if oldStrIsValid {
				return types.Value{Type: types.TypeBulkString, Str: []byte(oldStrVal)}
			}
			return types.RespNil
		}
		return types.RespOK
	}

	obj := object.AcquireStringObject(val)
	obj.ExpireAt = expireAt
	ctx.Engine.Set(key, obj)
	if oldObj == nil {
		ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagNew), "new", key)
	} else {
		ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagOverwrite), "overwritten", key)
		if oldObj.Type != object.TypeString {
			ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagTypeChanged), "type_changed", key)
		}
	}
	ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagString), "set", key)
	if expireAt > 0 {
		ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagGeneric), "expire", key)
	}

	if expireAt > 0 {
		ctx.ReplicateAs = [][]byte{[]byte("SET"), args[0].Str, []byte(val), []byte("PXAT"), []byte(strconv.FormatInt(expireAt, 10))}
	} else if keepTTL {
		ctx.ReplicateAs = [][]byte{[]byte("SET"), args[0].Str, []byte(val), []byte("KEEPTTL")}
	}

	if getOld {
		if oldObj == nil {
			return types.RespNil
		}
		if oldStrIsValid {
			return types.Value{Type: types.TypeBulkString, Str: []byte(oldStrVal)}
		}
		return types.RespNil
	}

	return types.RespOK
}

// parseInt parses a decimal integer from a byte slice without allocation.
func parseInt(b []byte) (int64, error) {
	if len(b) == 0 {
		return 0, errInvalidInt
	}
	neg := false
	i := 0
	if b[0] == '-' {
		neg = true
		i = 1
	}
	var n int64
	for ; i < len(b); i++ {
		c := b[i]
		if c < '0' || c > '9' {
			return 0, errInvalidInt
		}
		n = n*10 + int64(c-'0')
	}
	if neg {
		return -n, nil
	}
	return n, nil
}

var errInvalidInt = errors.New("not an integer")
