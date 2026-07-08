package str

import (
	"math"
	"strconv"
	"time"

	"frostdb/internal/engine/keyevent"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
	"frostdb/internal/stats"
)

const (
	incrExFlagByFloat = 1 << iota
	incrExFlagByInt
	incrExFlagLbound
	incrExFlagUbound
	incrExFlagSaturate
	incrExFlagENX
	incrExFlagPersist
	incrExFlagEX
	incrExFlagPX
	incrExFlagEXAT
	incrExFlagPXAT
)

type incrExArgs struct {
	flags      uint32
	byFloat    bool
	hasIncr    bool
	incrI64    int64
	incrF64    float64
	lbF64      float64
	ubF64      float64
	lbI64      int64
	ubI64      int64
	unit       int // EX or PX
	expireMs   int64
}

type IncrExCommand struct{}

func (c IncrExCommand) Name() string { return "INCREX" }

func (c IncrExCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'increx' command")
	}

	key := string(args[0].Str)
	ia := incrExArgs{}

	if err := parseIncrExArguments(args[1:], &ia); err != "" {
		return types.NewError(err)
	}

	if !ia.hasIncr {
		ia.flags |= incrExFlagByInt
		ia.incrI64 = 1
	}

	var isFloat bool
	var currentI64 int64
	var currentF64 float64
	var existed bool
	if ia.byFloat {
		isFloat = true
	}

	obj, ok := ctx.Engine.Get(key)
	if ok {
		existed = true
		if obj.Type != object.TypeString {
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		strVal := obj.Value.(string)
		if isFloat {
			var err error
			currentF64, err = strconv.ParseFloat(strVal, 64)
			if err != nil {
				return types.NewError("ERR value is not a valid float")
			}
			if isInfOrNaN(currentF64) {
				return types.NewError("ERR value cannot be Infinity or NaN for BYFLOAT")
			}
		} else {
			var err error
			currentI64, err = strconv.ParseInt(strVal, 10, 64)
			if err != nil {
				if ia.flags&incrExFlagByInt != 0 {
					return types.NewError("ERR value is not an integer or out of range")
				}
			}
		}
	}

	var ttlSet bool
	var expireMs int64
	var enxSkipped bool

	if ia.flags&incrExFlagEXAT != 0 {
		expireMs = ia.expireMs
	} else if ia.flags&incrExFlagPXAT != 0 {
		expireMs = ia.expireMs
	} else if ia.flags&incrExFlagEX != 0 {
		expireMs = ctx.NowMs() + ia.expireMs
	} else if ia.flags&incrExFlagPX != 0 {
		expireMs = ctx.NowMs() + ia.expireMs
	}

	hasExpireArgs := ia.flags&(incrExFlagEX|incrExFlagPX|incrExFlagEXAT|incrExFlagPXAT) != 0
	hasPersist := ia.flags&incrExFlagPersist != 0

	if hasExpireArgs || hasPersist {
		ttlSet = true
	}

	if ia.flags&incrExFlagENX != 0 {
		if !existed || (obj.ExpireAt > 0 && obj.ExpireAt <= ctx.NowMs()) || obj.ExpireAt == 0 {
		} else {
			enxSkipped = true
			ttlSet = false
		}
	}

	if ttlSet && expireMs > 0 && expireMs <= ctx.NowMs() && !enxSkipped {
		if existed {
			ctx.Engine.Del(key)
		}
		stats.Get().ExpiredKeys.Add(1)
		ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagGeneric), "del", key)
		lazyFree := ctx.Engine.GetEvictionConfig().LazyfreeLazyServerDel
		if lazyFree {
			ctx.ReplicateAs = [][]byte{[]byte("UNLINK"), args[0].Str}
		} else {
			ctx.ReplicateAs = [][]byte{[]byte("DEL"), args[0].Str}
		}
		ri := []types.Value{types.RespZero, types.RespZero}
		return types.NewArray(ri)
	}

	if isFloat {
		return executeIncrExFloat(ctx, key, &ia, currentF64, currentF64, existed, expireMs, ttlSet, obj, args)
	}
	return executeIncrExInt(ctx, key, &ia, currentI64, currentI64, existed, expireMs, ttlSet, obj, args)
}

func executeIncrExInt(ctx *context.Context, key string, ia *incrExArgs, currentVal int64, originalVal int64, existed bool, expireMs int64, ttlSet bool, oldObj *object.Object, args []types.Value) types.Value {
	result := currentVal
	incr := ia.incrI64

	if incr > 0 && currentVal > math.MaxInt64-incr {
		if ia.flags&incrExFlagSaturate != 0 {
			result = math.MaxInt64
		} else {
			return incrExIntReply(originalVal, 0)
		}
	} else if incr < 0 && currentVal < math.MinInt64-incr {
		if ia.flags&incrExFlagSaturate != 0 {
			result = math.MinInt64
		} else {
			return incrExIntReply(originalVal, 0)
		}
	} else {
		result = currentVal + incr
	}

	if ia.flags&incrExFlagLbound != 0 {
		if result < ia.lbI64 {
			if ia.flags&incrExFlagSaturate != 0 {
				result = ia.lbI64
			} else if existed {
				return incrExIntReply(originalVal, 0)
			} else {
				return incrExIntReply(0, 0)
			}
		}
	}
	if ia.flags&incrExFlagUbound != 0 {
		if result > ia.ubI64 {
			if ia.flags&incrExFlagSaturate != 0 {
				result = ia.ubI64
			} else if existed {
				return incrExIntReply(originalVal, 0)
			} else {
				return incrExIntReply(0, 0)
			}
		}
	}

	if !deltaFitsInInt64(originalVal, result) {
		return types.NewError("ERR applied increment would overflow")
	}
	actualDelta := result - originalVal

	resultStr := strconv.FormatInt(result, 10)
	newObj := object.AcquireStringObject(resultStr)
	setExpire(ia, newObj, oldObj, existed, expireMs, ttlSet, resultStr)
	ctx.Engine.Set(key, newObj)

	ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagString), "incrby", key)
	if ttlSet && ia.flags&incrExFlagPersist != 0 {
		if oldObj != nil && oldObj.ExpireAt > 0 {
			ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagGeneric), "persist", key)
		}
	} else if ttlSet && expireMs > 0 {
		ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagGeneric), "expire", key)
	}

	setIncrExReplicate(ctx, key, resultStr, ia, oldObj, expireMs)

	return incrExIntReply(result, actualDelta)
}

func executeIncrExFloat(ctx *context.Context, key string, ia *incrExArgs, currentVal float64, originalVal float64, existed bool, expireMs int64, ttlSet bool, oldObj *object.Object, args []types.Value) types.Value {
	result := currentVal
	incr := ia.incrF64

	if isInfOrNaN(incr) {
		return types.NewError("ERR BYFLOAT increment cannot be Infinity or NaN")
	}
	result = currentVal + incr

	if isInfOrNaN(result) {
		return types.NewError("ERR increment would produce NaN or Infinity")
	}

	if ia.flags&incrExFlagLbound != 0 {
		if result < ia.lbF64 {
			if ia.flags&incrExFlagSaturate != 0 {
				result = ia.lbF64
			} else if existed {
				return incrExFloatReply(originalVal, 0)
			} else {
				return incrExFloatReply(0, 0)
			}
		}
	}
	if ia.flags&incrExFlagUbound != 0 {
		if result > ia.ubF64 {
			if ia.flags&incrExFlagSaturate != 0 {
				result = ia.ubF64
			} else if existed {
				return incrExFloatReply(originalVal, 0)
			} else {
				return incrExFloatReply(0, 0)
			}
		}
	}

	actualDelta := result - originalVal

	var resultStr string
	if result == 0 {
		resultStr = "0"
	} else {
		resultStr = strconv.FormatFloat(result, 'f', -1, 64)
	}

	newObj := object.AcquireStringObject(resultStr)
	setExpire(ia, newObj, oldObj, existed, expireMs, ttlSet, resultStr)
	ctx.Engine.Set(key, newObj)

	ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagString), "incrbyfloat", key)
	if ttlSet && ia.flags&incrExFlagPersist != 0 {
		if oldObj != nil && oldObj.ExpireAt > 0 {
			ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagGeneric), "persist", key)
		}
	} else if ttlSet && expireMs > 0 {
		ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagGeneric), "expire", key)
	}

	setIncrExReplicate(ctx, key, resultStr, ia, oldObj, expireMs)

	return incrExFloatReply(result, actualDelta)
}

func setExpire(ia *incrExArgs, newObj *object.Object, oldObj *object.Object, existed bool, expireMs int64, ttlSet bool, resultStr string) {
	if ttlSet {
		if ia.flags&incrExFlagPersist != 0 {
			newObj.ExpireAt = 0
		} else if expireMs > 0 {
			newObj.ExpireAt = expireMs
		}
	} else if existed && oldObj != nil {
		newObj.ExpireAt = oldObj.ExpireAt
	}
}

func setIncrExReplicate(ctx *context.Context, key string, resultStr string, ia *incrExArgs, oldObj *object.Object, expireMs int64) {
	hasPersist := ia.flags&incrExFlagPersist != 0
	hasExpireArgs := ia.flags&(incrExFlagEX|incrExFlagPX|incrExFlagEXAT|incrExFlagPXAT) != 0
	enx := ia.flags&incrExFlagENX != 0

	if enx && oldObj != nil && oldObj.ExpireAt > 0 {
		ctx.ReplicateAs = [][]byte{[]byte("SET"), []byte(key), []byte(resultStr), []byte("KEEPTTL")}
		return
	}

	if hasPersist {
		if oldObj != nil && oldObj.ExpireAt > 0 {
			ctx.ReplicateAs = [][]byte{[]byte("SET"), []byte(key), []byte(resultStr)}
			return
		}
	} else if hasExpireArgs && expireMs > 0 {
		expireMsStr := strconv.FormatInt(expireMs, 10)
		ctx.ReplicateAs = [][]byte{[]byte("SET"), []byte(key), []byte(resultStr), []byte("PXAT"), []byte(expireMsStr)}
		return
	}

	ctx.ReplicateAs = [][]byte{[]byte("SET"), []byte(key), []byte(resultStr), []byte("KEEPTTL")}
}

func incrExIntReply(val int64, delta int64) types.Value {
	return types.NewArray([]types.Value{
		types.NewInt(val),
		types.NewInt(delta),
	})
}

func incrExFloatReply(val float64, delta float64) types.Value {
	var valStr, deltaStr string
	if val == 0 {
		valStr = "0"
	} else {
		valStr = strconv.FormatFloat(val, 'f', -1, 64)
	}
	if delta == 0 {
		deltaStr = "0"
	} else {
		deltaStr = strconv.FormatFloat(delta, 'f', -1, 64)
	}
	return types.NewArray([]types.Value{
		types.NewString(valStr),
		types.NewString(deltaStr),
	})
}

func parseIncrExArguments(args []types.Value, ia *incrExArgs) string {
	hasByFloat := false
	hasByInt := false
	hasSaturate := false
	hasENX := false
	hasPersist := false
	hasExpireFlag := false
	hasLbound := false
	hasUbound := false

	i := 0
	for i < len(args) {
		tok := string(args[i].Str)
		switch tok {
		case "BYFLOAT":
			if hasByInt {
				return "ERR syntax error"
			}
			if hasByFloat {
				return "ERR syntax error"
			}
			hasByFloat = true
			ia.byFloat = true
			ia.hasIncr = true
			ia.flags |= incrExFlagByFloat
			i++
			if i >= len(args) {
				return "ERR syntax error"
			}
			f, err := strconv.ParseFloat(string(args[i].Str), 64)
			if err != nil {
				return "ERR Increment is not a valid float"
			}
			ia.incrF64 = f
			i++

		case "BYINT":
			if hasByFloat {
				return "ERR syntax error"
			}
			if hasByInt {
				return "ERR syntax error"
			}
			hasByInt = true
			ia.flags |= incrExFlagByInt
			ia.hasIncr = true
			i++
			if i >= len(args) {
				return "ERR syntax error"
			}
			v, err := strconv.ParseInt(string(args[i].Str), 10, 64)
			if err != nil {
				return "ERR Increment is not an integer or out of range"
			}
			ia.incrI64 = v
			i++

		case "SATURATE":
			if hasSaturate {
				return "ERR syntax error"
			}
			hasSaturate = true
			ia.flags |= incrExFlagSaturate
			i++

		case "ENX":
			if hasENX {
				return "ERR syntax error"
			}
			if hasPersist {
				return "ERR syntax error"
			}
			hasENX = true
			ia.flags |= incrExFlagENX
			i++

		case "PERSIST":
			if hasPersist {
				return "ERR syntax error"
			}
			if hasENX {
				return "ERR syntax error"
			}
			if hasExpireFlag {
				return "ERR syntax error"
			}
			hasPersist = true
			ia.flags |= incrExFlagPersist
			i++

		case "EX":
			if hasPersist {
				return "ERR syntax error"
			}
			if hasExpireFlag {
				return "ERR syntax error"
			}
			hasExpireFlag = true
			ia.flags |= incrExFlagEX
			i++
			if i >= len(args) {
				return "ERR syntax error"
			}
			v, err := strconv.ParseInt(string(args[i].Str), 10, 64)
			if err != nil || v <= 0 {
				return "ERR invalid expire time in 'increx' command"
			}
			ia.unit = int(time.Second)
			ia.expireMs = v * 1000
			i++

		case "PX":
			if hasPersist {
				return "ERR syntax error"
			}
			if hasExpireFlag {
				return "ERR syntax error"
			}
			hasExpireFlag = true
			ia.flags |= incrExFlagPX
			i++
			if i >= len(args) {
				return "ERR syntax error"
			}
			v, err := strconv.ParseInt(string(args[i].Str), 10, 64)
			if err != nil || v <= 0 {
				return "ERR invalid expire time in 'increx' command"
			}
			ia.unit = int(time.Millisecond)
			ia.expireMs = v
			i++

		case "EXAT":
			if hasPersist {
				return "ERR syntax error"
			}
			if hasExpireFlag {
				return "ERR syntax error"
			}
			hasExpireFlag = true
			ia.flags |= incrExFlagEXAT
			i++
			if i >= len(args) {
				return "ERR syntax error"
			}
			v, err := strconv.ParseInt(string(args[i].Str), 10, 64)
			if err != nil || v <= 0 {
				return "ERR invalid expire time in 'increx' command"
			}
			ia.expireMs = v * 1000
			i++

		case "PXAT":
			if hasPersist {
				return "ERR syntax error"
			}
			if hasExpireFlag {
				return "ERR syntax error"
			}
			hasExpireFlag = true
			ia.flags |= incrExFlagPXAT
			i++
			if i >= len(args) {
				return "ERR syntax error"
			}
			v, err := strconv.ParseInt(string(args[i].Str), 10, 64)
			if err != nil || v <= 0 {
				return "ERR invalid expire time in 'increx' command"
			}
			ia.expireMs = v
			i++

		case "LBOUND":
			if hasLbound {
				return "ERR syntax error"
			}
			hasLbound = true
			ia.flags |= incrExFlagLbound
			i++
			if i >= len(args) {
				return "ERR syntax error"
			}
			lbStr := string(args[i].Str)
			lbF64, errF := strconv.ParseFloat(lbStr, 64)
			lbI64, errI := strconv.ParseInt(lbStr, 10, 64)
			_ = lbF64
			if errF != nil && errI != nil {
				return "ERR syntax error"
			}
			ia.lbF64 = lbF64
			ia.lbI64 = lbI64
			if ia.flags&incrExFlagUbound != 0 {
				if ia.lbF64 > ia.ubF64 || ia.lbI64 > ia.ubI64 {
					return "ERR LBOUND can't be greater than UBOUND"
				}
			}
			i++

		case "UBOUND":
			if hasUbound {
				return "ERR syntax error"
			}
			hasUbound = true
			ia.flags |= incrExFlagUbound
			i++
			if i >= len(args) {
				return "ERR syntax error"
			}
			ubStr := string(args[i].Str)
			ubF64, errF := strconv.ParseFloat(ubStr, 64)
			ubI64, errI := strconv.ParseInt(ubStr, 10, 64)
			_ = ubF64
			if errF != nil && errI != nil {
				return "ERR syntax error"
			}
			ia.ubF64 = ubF64
			ia.ubI64 = ubI64
			if ia.flags&incrExFlagLbound != 0 {
				if ia.ubF64 < ia.lbF64 || ia.ubI64 < ia.lbI64 {
					return "ERR LBOUND can't be greater than UBOUND"
				}
			}
			i++

		default:
			return "ERR syntax error"
		}
	}

	if ia.flags&incrExFlagENX != 0 && (ia.flags&(incrExFlagEX|incrExFlagPX|incrExFlagEXAT|incrExFlagPXAT) == 0) {
		return "ERR ENX flag requires an expiration"
	}

	if ia.flags&incrExFlagENX != 0 && ia.flags&incrExFlagPersist != 0 {
		return "ERR syntax error"
	}

	return ""
}

func deltaFitsInInt64(original, result int64) bool {
	if original >= 0 && result >= 0 {
		return true
	}
	if original <= 0 && result <= 0 {
		return true
	}
	if result >= 0 {
		return result <= math.MaxInt64+original
	}
	return result >= math.MinInt64+original
}
