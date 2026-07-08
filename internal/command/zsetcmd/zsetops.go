package zsetcmd

import (
	"math"
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// NewDouble creates a RESP3 double value.
func newDouble(f float64) types.Value {
	return types.Value{Type: types.TypeDouble, Str: []byte(strconv.FormatFloat(f, 'f', -1, 64))}
}

type ZUnionStoreCommand struct{}

func (c ZUnionStoreCommand) Name() string { return "ZUNIONSTORE" }

func (c ZUnionStoreCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	return zSetOp(ctx, args, false, true)
}

type ZInterStoreCommand struct{}

func (c ZInterStoreCommand) Name() string { return "ZINTERSTORE" }

func (c ZInterStoreCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	return zSetOp(ctx, args, true, true)
}

type ZUnionCommand struct{}

func (c ZUnionCommand) Name() string { return "ZUNION" }

func (c ZUnionCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	return zSetOp(ctx, args, false, false)
}

type ZInterCommand struct{}

func (c ZInterCommand) Name() string { return "ZINTER" }

func (c ZInterCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	return zSetOp(ctx, args, true, false)
}

type ZDiffStoreCommand struct{}

func (c ZDiffStoreCommand) Name() string { return "ZDIFFSTORE" }

func (c ZDiffStoreCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	return zDiffStore(ctx, args, true)
}

type ZDiffCommand struct{}

func (c ZDiffCommand) Name() string { return "ZDIFF" }

func (c ZDiffCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	return zDiffStore(ctx, args, false)
}

type ZInterCardCommand struct{}

func (c ZInterCardCommand) Name() string { return "ZINTERCARD" }

func (c ZInterCardCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'zintercard' command")
	}
	numKeys, err := strconv.Atoi(string(args[0].Str))
	if err != nil {
		return types.NewError("ERR value is not an integer or out of range")
	}
	if numKeys < 1 {
		return types.NewError("ERR at least 1 input key is needed for 'zintercard' command")
	}
	if len(args) < 1+numKeys {
		return types.NewError("ERR Number of keys can't be greater than number of args")
	}

	limitVal := -1
	if len(args) > 1+numKeys {
		extra := strings.ToUpper(string(args[1+numKeys].Str))
		if extra != "LIMIT" {
			return types.NewError("ERR syntax error")
		}
		if len(args) < 1+numKeys+2 {
			return types.NewError("ERR syntax error")
		}
		limitVal, err = strconv.Atoi(string(args[1+numKeys+1].Str))
		if err != nil || limitVal < 0 {
			return types.NewError("ERR LIMIT can't be negative")
		}
	}

	keys := args[1 : 1+numKeys]
	var zsets []*object.ZSet
	for _, kv := range keys {
		obj, ok := ctx.Engine.Get(string(kv.Str))
		if ok && obj.Type == object.TypeZSet {
			zsets = append(zsets, obj.Value.(*object.ZSet))
		} else if ok && obj.Type == object.TypeSet {
			zsets = append(zsets, setAsZSet(obj.Value.(*object.Set)))
		} else {
			return types.NewInt(0)
		}
	}

	count := len(zInter(zsets, nil, "SUM"))
	if limitVal > 0 && limitVal < count {
		count = limitVal
	}
	return types.NewInt(int64(count))
}

func zDiffStore(ctx *context.Context, args []types.Value, store bool) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments")
	}
	var dstKey string
	idx := 0
	if store {
		dstKey = string(args[0].Str)
		idx = 1
	}
	numKeys, err := strconv.Atoi(string(args[idx].Str))
	if err != nil {
		return types.NewError("ERR value is not an integer or out of range")
	}
	if numKeys < 1 {
		cmdName := "zdiff"
		if store {
			cmdName += "store"
		}
		return types.NewError("ERR at least 1 input key is needed for '" + cmdName + "' command")
	}
	idx++
	if len(args) < idx+numKeys {
		return types.NewError("ERR Number of keys can't be greater than number of args")
	}

	keys := args[idx : idx+numKeys]
	idx += numKeys

	withScores := false
	for idx < len(args) {
		if strings.ToUpper(string(args[idx].Str)) == "WITHSCORES" {
			if store {
				return types.NewError("ERR syntax error")
			}
			withScores = true
		}
		idx++
	}

	var zsets []*object.ZSet
	for _, kv := range keys {
		obj, ok := ctx.Engine.Get(string(kv.Str))
		if ok && obj.Type == object.TypeZSet {
			zsets = append(zsets, obj.Value.(*object.ZSet))
		} else if ok && obj.Type == object.TypeSet {
			zsets = append(zsets, setAsZSet(obj.Value.(*object.Set)))
		} else {
			zsets = append(zsets, object.NewZSet())
		}
	}

	result := zDiff(zsets)
	resZSet := object.NewZSet()
	for _, e := range result {
		resZSet.Add(e.Score, e.Member)
	}
	if store {
		ctx.Engine.Set(dstKey, &object.Object{Type: object.TypeZSet, Encoding: object.EncodingRaw, Value: resZSet})
		if len(result) == 0 {
			ctx.Engine.Del(dstKey)
			return types.NewInt(0)
		}
		return types.NewInt(int64(len(result)))
	}

	if len(result) == 0 {
		return types.NewArray([]types.Value{})
	}
	var resp []types.Value
	for _, e := range result {
		resp = append(resp, types.NewString(e.Member))
		if withScores {
			resp = append(resp, types.NewString(formatScore(e.Score)))
		}
	}
	return types.NewArray(resp)
}

func zSetOp(ctx *context.Context, args []types.Value, intersect bool, store bool) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments")
	}
	var dstKey string
	idx := 0
	if store {
		dstKey = string(args[0].Str)
		idx = 1
	}
	numKeys, err := strconv.Atoi(string(args[idx].Str))
	if err != nil {
		return types.NewError("ERR value is not an integer or out of range")
	}
	if numKeys < 1 {
		cmdName := "zunion"
		if intersect {
			cmdName = "zinter"
		}
		if store {
			cmdName += "store"
		}
		return types.NewError("ERR at least 1 input key is needed for '" + cmdName + "' command")
	}
	idx++
	if len(args) < idx+numKeys {
		return types.NewError("ERR Number of keys can't be greater than number of args")
	}

	keys := args[idx : idx+numKeys]
	idx += numKeys

	weights := make([]float64, numKeys)
	for i := range weights {
		weights[i] = 1.0
	}
	aggregate := "SUM"
	withScores := false

	for idx < len(args) {
		opt := strings.ToUpper(string(args[idx].Str))
		switch opt {
		case "WEIGHTS":
			idx++
			if idx+numKeys > len(args) {
				return types.NewError("ERR syntax error")
			}
			for i := range numKeys {
				w, err := strconv.ParseFloat(string(args[idx].Str), 64)
				if err != nil || math.IsNaN(w) {
					return types.NewError("ERR weight value is not a float")
				}
				weights[i] = w
				idx++
			}
		case "AGGREGATE":
			if idx+1 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			aggregate = strings.ToUpper(string(args[idx+1].Str))
			idx += 2
		case "WITHSCORES":
			if store {
				return types.NewError("ERR syntax error")
			}
			withScores = true
			idx++
		case "LIMIT":
			idx += 2
		default:
			idx++
		}
	}

	var zsets []*object.ZSet
	for _, kv := range keys {
		obj, ok := ctx.Engine.Get(string(kv.Str))
		if ok && obj.Type == object.TypeZSet {
			zsets = append(zsets, obj.Value.(*object.ZSet))
		} else if ok && obj.Type == object.TypeSet {
			zsets = append(zsets, setAsZSet(obj.Value.(*object.Set)))
		} else {
			if intersect {
				return storeResult(ctx, dstKey, nil, store, withScores)
			}
			zsets = append(zsets, object.NewZSet())
		}
	}

	var result []object.ZSetEntry
	if intersect {
		result = zInter(zsets, weights, aggregate)
	} else {
		result = zUnion(zsets, weights, aggregate)
	}

	return storeResult(ctx, dstKey, result, store, withScores)
}

func storeResult(ctx *context.Context, dstKey string, result []object.ZSetEntry, store bool, withScores bool) types.Value {
	if store {
		if len(result) == 0 {
			ctx.Engine.Del(dstKey)
			return types.NewInt(0)
		}
		resZSet := object.NewZSet()
		for _, e := range result {
			resZSet.Add(e.Score, e.Member)
		}
		ctx.Engine.Set(dstKey, &object.Object{Type: object.TypeZSet, Encoding: object.EncodingRaw, Value: resZSet})
		return types.NewInt(int64(len(result)))
	}

	if len(result) == 0 {
		return types.NewArray([]types.Value{})
	}

	// RESP3: return array of [member, score_double] pairs when withScores
	if withScores && ctx.Protocol >= 3 {
		var resp []types.Value
		for _, e := range result {
			pair := types.NewArray([]types.Value{
				types.NewString(e.Member),
				newDouble(e.Score),
			})
			resp = append(resp, pair)
		}
		return types.NewArray(resp)
	}

	var resp []types.Value
	for _, e := range result {
		resp = append(resp, types.NewString(e.Member))
		if withScores {
			resp = append(resp, types.NewString(formatScore(e.Score)))
		}
	}
	return types.NewArray(resp)
}

func zUnion(zsets []*object.ZSet, weights []float64, aggregate string) []object.ZSetEntry {
	combined := make(map[string]float64)
	counts := make(map[string]int)

	for i, z := range zsets {
		w := 1.0
		if i < len(weights) {
			w = weights[i]
		}
		for _, m := range z.Members() {
			score, _ := z.Score(m)
			c := counts[m]
			if aggregate == "COUNT" {
				combined[m] = aggregateScore(combined[m], c, w, aggregate)
			} else {
				combined[m] = aggregateScore(combined[m], c, score*w, aggregate)
			}
			counts[m] = c + 1
		}
	}

	return mapToEntries(combined)
}

func zInter(zsets []*object.ZSet, weights []float64, aggregate string) []object.ZSetEntry {
	if len(zsets) == 0 {
		return nil
	}
	combined := make(map[string]float64)
	counts := make(map[string]int)

	if len(zsets) == 1 {
		z := zsets[0]
		w := 1.0
		if len(weights) > 0 {
			w = weights[0]
		}
		for _, m := range z.Members() {
			score, _ := z.Score(m)
			counts[m] = 1
			combined[m] = score * w
		}
		return mapToEntries(combined)
	}

	first := zsets[0]
	set := make(map[string]struct{})
	for _, m := range first.Members() {
		set[m] = struct{}{}
	}

	for i := 1; i < len(zsets); i++ {
		newSet := make(map[string]struct{})
		for _, m := range zsets[i].Members() {
			if _, ok := set[m]; ok {
				newSet[m] = struct{}{}
			}
		}
		set = newSet
	}

	for i, z := range zsets {
		w := 1.0
		if i < len(weights) {
			w = weights[i]
		}
		for m := range set {
			score, _ := z.Score(m)
			c := counts[m]
			if aggregate == "COUNT" {
				combined[m] = aggregateScore(combined[m], c, w, aggregate)
			} else {
				combined[m] = aggregateScore(combined[m], c, score*w, aggregate)
			}
			counts[m] = c + 1
		}
	}

	return mapToEntries(combined)
}

func zDiff(zsets []*object.ZSet) []object.ZSetEntry {
	if len(zsets) == 0 {
		return nil
	}
	first := zsets[0]
	exclude := make(map[string]struct{})
	for i := 1; i < len(zsets); i++ {
		for _, m := range zsets[i].Members() {
			exclude[m] = struct{}{}
		}
	}
	var result []object.ZSetEntry
	for _, m := range first.Members() {
		if _, ok := exclude[m]; !ok {
			score, _ := first.Score(m)
			result = append(result, object.ZSetEntry{Member: m, Score: score})
		}
	}
	sortEntries(result)
	return result
}

func aggregateScore(current float64, count int, newScore float64, aggregate string) float64 {
	switch aggregate {
	case "SUM":
		result := current + newScore
		if math.IsNaN(result) {
			return 0
		}
		return result
	case "MIN":
		if count == 0 || newScore < current {
			return newScore
		}
		return current
	case "MAX":
		if count == 0 || newScore > current {
			return newScore
		}
		return current
	case "COUNT":
		return current + newScore
	default:
		result := current + newScore
		if math.IsNaN(result) {
			return 0
		}
		return result
	}
}

func mapToEntries(m map[string]float64) []object.ZSetEntry {
	result := make([]object.ZSetEntry, 0, len(m))
	for member, score := range m {
		result = append(result, object.ZSetEntry{Member: member, Score: score})
	}
	sortEntries(result)
	return result
}

func sortEntries(entries []object.ZSetEntry) {
	for i := 0; i < len(entries); i++ {
		for j := i + 1; j < len(entries); j++ {
			if entries[i].Score > entries[j].Score ||
				(entries[i].Score == entries[j].Score && entries[i].Member > entries[j].Member) {
				entries[i], entries[j] = entries[j], entries[i]
			}
		}
	}
}

func setAsZSet(s *object.Set) *object.ZSet {
	z := object.NewZSet()
	for _, m := range s.Members() {
		z.Add(1.0, m)
	}
	return z
}
