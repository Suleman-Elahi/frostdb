package arraycmd

import (
	"math"
	"regexp"
	"strconv"
	"strings"

	"frostdb/internal/engine/keyevent"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

func parseIndex(s string) (int64, bool) {
	if s == "-" {
		return 0, true
	}
	if s == "+" {
		return 1<<63 - 1, true
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		// Try uint64 for huge values
		un, uerr := strconv.ParseUint(s, 10, 64)
		if uerr == nil {
			if un >= math.MaxInt64 {
				return -1, true // terminal state
			}
			return int64(un), true
		}
		return 0, false
	}
	if n < 0 {
		return 0, false
	}
	return n, true
}

func getOrCreateArray(ctx *context.Context, key string, create bool) (*object.Array, *object.Object, types.Value) {
	sliceSize := int64(object.DefaultSliceSize)
	kmax := object.DefaultSparseKmax
	kmin := object.DefaultSparseKmin

	obj, exists := ctx.Engine.Get(key)
	if exists {
		if obj.Type != object.TypeArray {
			return nil, nil, types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		arr, ok := obj.Value.(*object.Array)
		if !ok {
			return nil, nil, types.NewError("ERR internal array encoding")
		}
		return arr, obj, types.Value{}
	}

	if !create {
		return nil, nil, types.Value{}
	}

	arr := object.NewArray(sliceSize, kmax, kmin)
	newObj := &object.Object{Type: object.TypeArray, Value: arr}
	ctx.Engine.Set(key, newObj)
	return arr, nil, types.Value{}
}

// --- ARSET ---

type ArSetCommand struct{}

func (c ArSetCommand) Name() string { return "ARSET" }

func (c ArSetCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 3 {
		return types.NewError("ERR wrong number of arguments for 'arset' command")
	}
	key := string(args[0].Str)
	idx, ok := parseIndex(string(args[1].Str))
	if !ok {
		return types.NewError("ERR invalid array index")
	}
	arr, _, errVal := getOrCreateArray(ctx, key, true)
	if errVal.Type != 0 {
		return errVal
	}
	arr.Mu.Lock()
	defer arr.Mu.Unlock()

	if len(args) == 3 {
		v := string(args[2].Str)
		n := arr.Set(idx, v)
		return types.NewInt(n)
	}
	// Contiguous write
	values := make([]string, len(args)-2)
	for i := 2; i < len(args); i++ {
		values[i-2] = string(args[i].Str)
	}
	n := arr.SetContiguous(idx, values)
	return types.NewInt(n)
}

// --- ARGET ---

type ArGetCommand struct{}

func (c ArGetCommand) Name() string { return "ARGET" }

func (c ArGetCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 2 {
		return types.NewError("ERR wrong number of arguments for 'arget' command")
	}
	key := string(args[0].Str)
	idx, ok := parseIndex(string(args[1].Str))
	if !ok {
		return types.NewError("ERR invalid array index")
	}
	arr, _, errVal := getOrCreateArray(ctx, key, false)
	if errVal.Type != 0 {
		return errVal
	}
	if arr == nil {
		return types.RespNil
	}
	arr.Mu.RLock()
	defer arr.Mu.RUnlock()
	v, ok := arr.Get(idx)
	if !ok {
		return types.RespNil
	}
	return types.NewString(v)
}

// --- ARLEN ---

type ArLenCommand struct{}

func (c ArLenCommand) Name() string { return "ARLEN" }

func (c ArLenCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewError("ERR wrong number of arguments for 'arlen' command")
	}
	arr, _, errVal := getOrCreateArray(ctx, string(args[0].Str), false)
	if errVal.Type != 0 {
		return errVal
	}
	if arr == nil {
		return types.RespZero
	}
	return types.NewInt(arr.Len())
}

// --- ARCOUNT ---

type ArCountCommand struct{}

func (c ArCountCommand) Name() string { return "ARCOUNT" }

func (c ArCountCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewError("ERR wrong number of arguments for 'arcount' command")
	}
	arr, _, errVal := getOrCreateArray(ctx, string(args[0].Str), false)
	if errVal.Type != 0 {
		return errVal
	}
	if arr == nil {
		return types.RespZero
	}
	return types.NewInt(arr.Count())
}

// --- ARDEL ---

type ArDelCommand struct{}

func (c ArDelCommand) Name() string { return "ARDEL" }

func (c ArDelCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'ardel' command")
	}
	key := string(args[0].Str)
	arr, _, errVal := getOrCreateArray(ctx, key, false)
	if errVal.Type != 0 {
		return errVal
	}
	if arr == nil {
		return types.RespZero
	}
	arr.Mu.Lock()
	defer arr.Mu.Unlock()

	var total int64
	for i := 1; i < len(args); i++ {
		idx, ok := parseIndex(string(args[i].Str))
		if !ok {
			continue
		}
		if arr.Del(idx) {
			total++
		}
	}
	if total > 0 {
		ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagGeneric), "ardel", key)
		if arr.Count() == 0 {
			if ctx.Engine.Del(key) {
				ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagGeneric), "del", key)
			}
		}
	}
	return types.NewInt(total)
}

// --- ARDELRANGE ---

type ArDelRangeCommand struct{}

func (c ArDelRangeCommand) Name() string { return "ARDELRANGE" }

func (c ArDelRangeCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 3 || len(args)%2 != 1 {
		return types.NewError("ERR wrong number of arguments for 'ardelrange' command")
	}
	key := string(args[0].Str)
	arr, _, errVal := getOrCreateArray(ctx, key, false)
	if errVal.Type != 0 {
		return errVal
	}
	if arr == nil {
		return types.RespZero
	}
	arr.Mu.Lock()
	defer arr.Mu.Unlock()

	var total int64
	for i := 1; i < len(args); i += 2 {
		lo, ok1 := parseIndex(string(args[i].Str))
		hi, ok2 := parseIndex(string(args[i+1].Str))
		if !ok1 || !ok2 {
			continue
		}
		if lo > hi {
			lo, hi = hi, lo
		}
		total += arr.DelRange(lo, hi)
	}
	if total > 0 {
		ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagGeneric), "ardelrange", key)
		if arr.Count() == 0 {
			if ctx.Engine.Del(key) {
				ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagGeneric), "del", key)
			}
		}
	}
	return types.NewInt(total)
}

// --- ARMSET ---

type ArMSetCommand struct{}

func (c ArMSetCommand) Name() string { return "ARMSET" }

func (c ArMSetCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 3 || len(args)%2 != 1 {
		return types.NewError("ERR wrong number of arguments for 'armset' command")
	}
	key := string(args[0].Str)
	arr, _, errVal := getOrCreateArray(ctx, key, true)
	if errVal.Type != 0 {
		return errVal
	}
	arr.Mu.Lock()
	defer arr.Mu.Unlock()

	var filled int64
	for i := 1; i < len(args); i += 2 {
		idx, ok := parseIndex(string(args[i].Str))
		if !ok {
			continue
		}
		val := string(args[i+1].Str)
		filled += arr.Set(idx, val)
	}
	return types.NewInt(filled)
}

// --- ARMGET ---

type ArMGetCommand struct{}

func (c ArMGetCommand) Name() string { return "ARMGET" }

func (c ArMGetCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'armget' command")
	}
	key := string(args[0].Str)
	arr, _, errVal := getOrCreateArray(ctx, key, false)
	if errVal.Type != 0 {
		return errVal
	}
	result := make([]types.Value, len(args)-1)
	for i := 1; i < len(args); i++ {
		idx, ok := parseIndex(string(args[i].Str))
		if !ok || arr == nil {
			result[i-1] = types.RespNil
			continue
		}
		v, found := arr.Get(idx)
		if found {
			result[i-1] = types.NewString(v)
		} else {
			result[i-1] = types.RespNil
		}
	}
	return types.NewArray(result)
}

// --- ARGETRANGE ---

type ArGetRangeCommand struct{}

func (c ArGetRangeCommand) Name() string { return "ARGETRANGE" }

func (c ArGetRangeCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 3 {
		return types.NewError("ERR wrong number of arguments for 'argetrange' command")
	}
	key := string(args[0].Str)
	start, ok1 := parseIndex(string(args[1].Str))
	end, ok2 := parseIndex(string(args[2].Str))
	if !ok1 || !ok2 {
		return types.NewError("ERR invalid array index")
	}
	diff := start - end
	if diff < 0 {
		diff = -diff
	}
	if diff >= 1000000 {
		return types.NewError("ERR range exceeds maximum of 1000000 items")
	}
	arr, _, errVal := getOrCreateArray(ctx, key, false)
	if errVal.Type != 0 {
		return errVal
	}
	if arr == nil {
		return types.RespEmptyArr
	}
	arr.Mu.RLock()
	defer arr.Mu.RUnlock()
	items := arr.Scan(start, end, 0)
	result := make([]types.Value, len(items))
	for i, item := range items {
		result[i] = types.NewString(item[1])
	}
	return types.NewArray(result)
}

// --- ARSCAN ---

type ArScanCommand struct{}

func (c ArScanCommand) Name() string { return "ARSCAN" }

func (c ArScanCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 3 {
		return types.NewError("ERR wrong number of arguments for 'arscan' command")
	}
	key := string(args[0].Str)
	start, ok1 := parseIndex(string(args[1].Str))
	end, ok2 := parseIndex(string(args[2].Str))
	if !ok1 || !ok2 {
		return types.NewError("ERR invalid array index")
	}
	var limit int64 = 0
	i := 3
	for i < len(args) {
		tok := strings.ToUpper(string(args[i].Str))
		if tok == "LIMIT" && i+1 < len(args) {
			n, err := strconv.ParseInt(string(args[i+1].Str), 10, 64)
			if err == nil && n > 0 {
				limit = n
			}
			i += 2
		} else {
			i++
		}
	}
	arr, _, errVal := getOrCreateArray(ctx, key, false)
	if errVal.Type != 0 {
		return errVal
	}
	if arr == nil {
		return types.RespEmptyArr
	}
	arr.Mu.RLock()
	defer arr.Mu.RUnlock()
	items := arr.Scan(start, end, limit)
	result := make([]types.Value, len(items))
	for i, item := range items {
		idxVal := types.NewString(item[0])
		valVal := types.NewString(item[1])
		result[i] = types.NewArray([]types.Value{idxVal, valVal})
	}
	return types.NewArray(result)
}

// --- ARGREP ---

type ArGrepCommand struct{}

func (c ArGrepCommand) Name() string { return "ARGREP" }

func (c ArGrepCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 4 {
		return types.NewError("ERR wrong number of arguments for 'argrep' command")
	}
	key := string(args[0].Str)
	start, ok1 := parseIndex(string(args[1].Str))
	end, ok2 := parseIndex(string(args[2].Str))
	if !ok1 || !ok2 {
		return types.NewError("ERR invalid array index")
	}

	withValues := false
	nocase := false
	limit := int64(0)
	combine := "AND"

	type grepPredicate struct {
		typ     string
		pattern string
		re      *regexp.Regexp
	}
	var predicates []grepPredicate

	i := 3
	for i < len(args) {
		tok := strings.ToUpper(string(args[i].Str))
		switch tok {
		case "EXACT", "MATCH", "RE", "GLOB":
			if i+1 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			pat := string(args[i+1].Str)
			
			var re *regexp.Regexp
			if tok == "RE" {
				if len(pat) == 0 {
					return types.NewError("ERR regular expression is empty")
				}
				if len(pat) > 2048 {
					return types.NewError("ERR maximum is 2048 bytes")
				}
				if strings.Contains(pat, "\\1") || strings.Contains(pat, "\\2") {
					return types.NewError("ERR backreferences are not supported")
				}
				var err error
				re, err = regexp.Compile(pat)
				if err != nil {
					return types.NewError("ERR invalid regular expression")
				}
			}

			predicates = append(predicates, grepPredicate{
				typ:     tok,
				pattern: pat,
				re:      re,
			})
			i += 2
		case "WITHVALUES":
			withValues = true
			i++
		case "NOCASE":
			nocase = true
			i++
		case "LIMIT":
			if i+1 < len(args) {
				n, err := strconv.ParseInt(string(args[i+1].Str), 10, 64)
				if err == nil && n > 0 {
					limit = n
				}
				i += 2
			} else {
				i++
			}
		case "AND":
			combine = "AND"
			i++
		case "OR":
			combine = "OR"
			i++
		default:
			i++
		}
	}

	if len(predicates) == 0 {
		return types.NewError("ERR syntax error")
	}
	if len(predicates) > 250 {
		return types.NewError("ERR maximum is 250 predicates")
	}

	arr, _, errVal := getOrCreateArray(ctx, key, false)
	if errVal.Type != 0 {
		return errVal
	}
	if arr == nil {
		return types.RespEmptyArr
	}
	arr.Mu.RLock()
	defer arr.Mu.RUnlock()
	
	if limit <= 0 {
		limit = math.MaxInt64
	}

	items := arr.Scan(start, end, 0)
	var result [][2]string
	var emitted int64

	for _, item := range items {
		if emitted >= limit {
			break
		}
		val := item[1]
		
		matched := false
		if combine == "AND" {
			matched = true
		} else {
			matched = false
		}

		for _, p := range predicates {
			pMatch := false
			switch p.typ {
			case "RE":
				v := val
				if nocase {
					v = strings.ToLower(val)
				}
				pMatch = p.re.MatchString(v)
			case "GLOB":
				v := val
				pat := p.pattern
				if nocase {
					v = strings.ToLower(v)
					pat = strings.ToLower(pat)
				}
				pat = strings.Trim(pat, "*")
				pMatch = strings.Contains(v, pat)
			case "MATCH":
				if nocase {
					pMatch = strings.Contains(strings.ToLower(val), strings.ToLower(p.pattern))
				} else {
					pMatch = strings.Contains(val, p.pattern)
				}
			case "EXACT":
				if nocase {
					pMatch = strings.EqualFold(val, p.pattern)
				} else {
					pMatch = val == p.pattern
				}
			}

			if combine == "AND" {
				if !pMatch {
					matched = false
					break
				}
			} else { // OR
				if pMatch {
					matched = true
					break
				}
			}
		}

		if matched {
			if withValues {
				result = append(result, item)
			} else {
				result = append(result, [2]string{item[0], ""})
			}
			emitted++
		}
	}

	if withValues {
		res := make([]types.Value, len(result))
		for i, item := range result {
			res[i] = types.NewArray([]types.Value{types.NewString(item[0]), types.NewString(item[1])})
		}
		return types.NewArray(res)
	}
	res := make([]types.Value, len(result))
	for i, item := range result {
		res[i] = types.NewString(item[0])
	}
	return types.NewArray(res)
}

// --- ARINSERT ---

type ArInsertCommand struct{}

func (c ArInsertCommand) Name() string { return "ARINSERT" }

func (c ArInsertCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 2 {
		return types.NewError("ERR wrong number of arguments for 'arinsert' command")
	}
	key := string(args[0].Str)
	val := string(args[1].Str)
	arr, _, errVal := getOrCreateArray(ctx, key, true)
	if errVal.Type != 0 {
		return errVal
	}
	arr.Mu.Lock()
	defer arr.Mu.Unlock()
	if arr.InsertIdx() < 0 {
		return types.NewError("ERR insert index overflow")
	}
	pos := arr.Insert(val)
	return types.NewInt(pos)
}

// --- ARRING ---

type ArRingCommand struct{}

func (c ArRingCommand) Name() string { return "ARRING" }

func (c ArRingCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 3 {
		return types.NewError("ERR wrong number of arguments for 'arring' command")
	}
	key := string(args[0].Str)
	maxlen, err := strconv.ParseInt(string(args[1].Str), 10, 64)
	if err != nil || maxlen <= 0 {
		return types.NewError("ERR value is not an integer or out of range")
	}
	val := string(args[2].Str)
	arr, _, errVal := getOrCreateArray(ctx, key, true)
	if errVal.Type != 0 {
		return errVal
	}
	arr.Mu.Lock()
	defer arr.Mu.Unlock()
	arr.SetRingSize(maxlen)
	pos := arr.RingInsert(maxlen, val)
	return types.NewInt(pos)
}

// --- ARNEXT ---

type ArNextCommand struct{}

func (c ArNextCommand) Name() string { return "ARNEXT" }

func (c ArNextCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewError("ERR wrong number of arguments for 'arnext' command")
	}
	arr, _, errVal := getOrCreateArray(ctx, string(args[0].Str), false)
	if errVal.Type != 0 {
		return errVal
	}
	if arr == nil {
		return types.RespZero
	}
	n := arr.InsertIdx()
	if n < 0 {
		return types.RespNil
	}
	return types.NewInt(n)
}

// --- ARSEEK ---

type ArSeekCommand struct{}

func (c ArSeekCommand) Name() string { return "ARSEEK" }

func (c ArSeekCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 2 {
		return types.NewError("ERR wrong number of arguments for 'arseek' command")
	}
	key := string(args[0].Str)
	idx, ok := parseIndex(string(args[1].Str))
	if !ok {
		return types.NewError("ERR invalid array index")
	}
	arr, _, errVal := getOrCreateArray(ctx, key, false)
	if errVal.Type != 0 {
		return errVal
	}
	if arr == nil {
		return types.RespZero
	}
	arr.SetInsertIdx(idx)
	return types.RespOne
}

// --- ARLASTITEMS ---

type ArLastItemsCommand struct{}

func (c ArLastItemsCommand) Name() string { return "ARLASTITEMS" }

func (c ArLastItemsCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 || len(args) > 3 {
		return types.NewError("ERR wrong number of arguments for 'arlastitems' command")
	}
	key := string(args[0].Str)
	n, err := strconv.ParseInt(string(args[1].Str), 10, 64)
	if err != nil || n < 0 {
		return types.NewError("ERR value is not an integer or out of range")
	}
	rev := false
	if len(args) == 3 {
		rev = strings.ToUpper(string(args[2].Str)) == "REV"
	}
	arr, _, errVal := getOrCreateArray(ctx, key, false)
	if errVal.Type != 0 {
		return errVal
	}
	if arr == nil {
		return types.RespEmptyArr
	}
	arr.Mu.RLock()
	defer arr.Mu.RUnlock()
	items := arr.LastItems(n, rev)
	res := make([]types.Value, len(items))
	for i, v := range items {
		res[i] = types.NewString(v)
	}
	return types.NewArray(res)
}

// --- AROP ---

type ArOpCommand struct{}

func (c ArOpCommand) Name() string { return "AROP" }

func (c ArOpCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 4 {
		return types.NewError("ERR wrong number of arguments for 'arop' command")
	}
	key := string(args[0].Str)
	start, ok1 := parseIndex(string(args[1].Str))
	end, ok2 := parseIndex(string(args[2].Str))
	if !ok1 || !ok2 {
		return types.NewError("ERR invalid array index")
	}
	op := strings.ToUpper(string(args[3].Str))

	arr, _, errVal := getOrCreateArray(ctx, key, false)
	if errVal.Type != 0 {
		return errVal
	}
	if arr == nil {
		switch op {
		case "USED", "MATCH", "AND", "OR", "XOR":
			return types.RespZero
		case "SUM":
			return types.RespNil
		default:
			return types.RespZero
		}
	}

	arr.Mu.RLock()
	defer arr.Mu.RUnlock()

	switch op {
	case "USED":
		return types.NewInt(arr.Arop(start, end, "USED", ""))
	case "MATCH":
		if len(args) < 5 {
			return types.NewError("ERR wrong number of arguments for 'arop' command")
		}
		pat := string(args[4].Str)
		return types.NewInt(arr.Arop(start, end, "MATCH", pat))
	case "SUM":
		items := arr.Scan(start, end, 0)
		var sumv float64
		hasNum := false
		for _, item := range items {
			v, err := strconv.ParseFloat(item[1], 64)
			if err == nil {
				sumv += v
				hasNum = true
			}
		}
		if !hasNum {
			return types.RespNil
		}
		if float64(int64(sumv)) == sumv {
			return types.NewInt(int64(sumv))
		}
		return types.NewString(strconv.FormatFloat(sumv, 'g', -1, 64))
	case "MIN":
		items := arr.Scan(start, end, 0)
		var minv float64
		first := true
		for _, item := range items {
			v, err := strconv.ParseFloat(item[1], 64)
			if err == nil {
				if first || v < minv {
					minv = v
					first = false
				}
			}
		}
		if first {
			return types.RespNil
		}
		if float64(int64(minv)) == minv {
			return types.NewInt(int64(minv))
		}
		return types.NewString(strconv.FormatFloat(minv, 'g', -1, 64))
	case "MAX":
		items := arr.Scan(start, end, 0)
		var maxv float64
		first := true
		for _, item := range items {
			v, err := strconv.ParseFloat(item[1], 64)
			if err == nil {
				if first || v > maxv {
					maxv = v
					first = false
				}
			}
		}
		if first {
			return types.RespNil
		}
		if float64(int64(maxv)) == maxv {
			return types.NewInt(int64(maxv))
		}
		return types.NewString(strconv.FormatFloat(maxv, 'g', -1, 64))
	case "AND":
		items := arr.Scan(start, end, 0)
		var result int64 = -1
		for _, item := range items {
			v, err := strconv.ParseFloat(item[1], 64)
			if err == nil {
				if result == -1 {
					result = int64(v)
				} else {
					result &= int64(v)
				}
			}
		}
		if result == -1 {
			result = 0
		}
		return types.NewInt(result)
	case "OR":
		items := arr.Scan(start, end, 0)
		var result int64
		for _, item := range items {
			v, err := strconv.ParseFloat(item[1], 64)
			if err == nil {
				result |= int64(v)
			}
		}
		return types.NewInt(result)
	case "XOR":
		items := arr.Scan(start, end, 0)
		var result int64
		for _, item := range items {
			v, err := strconv.ParseFloat(item[1], 64)
			if err == nil {
				result ^= int64(v)
			}
		}
		return types.NewInt(result)
	default:
		return types.NewError("ERR unknown operation")
	}
}

// --- ARINFO ---

type ArInfoCommand struct{}

func (c ArInfoCommand) Name() string { return "ARINFO" }

func (c ArInfoCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 || len(args) > 2 {
		return types.NewError("ERR wrong number of arguments for 'arinfo' command")
	}
	arr, _, errVal := getOrCreateArray(ctx, string(args[0].Str), false)
	if errVal.Type != 0 {
		return errVal
	}
	if arr == nil {
		return types.NewError("ERR no such key")
	}
	arr.Mu.RLock()
	defer arr.Mu.RUnlock()
	info := arr.Info()

	entries := []types.Value{
		types.NewString("count"), types.NewInt(info["count"]),
		types.NewString("len"), types.NewInt(info["len"]),
		types.NewString("directory-size"), types.NewInt(info["directory-size"]),
	}

	full := len(args) == 2 && strings.ToUpper(string(args[1].Str)) == "FULL"
	if full {
		entries = append(entries,
			types.NewString("dense-slices"), types.NewInt(info["dense-slices"]),
			types.NewString("sparse-slices"), types.NewInt(info["sparse-slices"]),
			types.NewString("avg-dense-size"), types.NewInt(info["avg-dense-size"]),
		)
	}

	return types.NewArray(entries)
}
