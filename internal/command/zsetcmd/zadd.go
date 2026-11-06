package zsetcmd

import (
	"math"
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type ZAddCommand struct{}

func (c ZAddCommand) Name() string { return "ZADD" }

func (c ZAddCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 3 {
		return types.NewError("ERR wrong number of arguments for 'zadd' command")
	}

	key := string(args[0].Str)

	idx := 1
	flagNX, flagXX, flagGT, flagLT, flagCH, flagINCR := false, false, false, false, false, false

	for idx < len(args) {
		flag := strings.ToUpper(string(args[idx].Str))
		switch flag {
		case "NX":
			flagNX = true
			idx++
		case "XX":
			flagXX = true
			idx++
		case "GT":
			flagGT = true
			idx++
		case "LT":
			flagLT = true
			idx++
		case "CH":
			flagCH = true
			idx++
		case "INCR":
			flagINCR = true
			idx++
		default:
			goto parseScores
		}
	}

parseScores:
	if flagNX && flagXX {
		return types.NewError("ERR XX and NX options at the same time are not compatible")
	}
	if flagGT && flagNX {
		return types.NewError("ERR GT and NX options at the same time are not compatible")
	}
	if flagLT && flagNX {
		return types.NewError("ERR LT and NX options at the same time are not compatible")
	}
	if flagLT && flagGT {
		return types.NewError("ERR LT and GT options at the same time are not compatible")
	}

	if flagCH && flagINCR {
		return types.NewError("ERR INCR option is not allowed with CH option")
	}

	remaining := args[idx:]
	if len(remaining) == 0 || (len(remaining)%2 != 0) {
		return types.NewError("ERR syntax error")
	}
	if flagINCR && len(remaining) != 2 {
		return types.NewError("ERR INCR option supports a single increment-element pair")
	}

	type pair struct {
		score  float64
		member string
	}

	if flagINCR {
		increment, err := parseScore(string(remaining[0].Str))
		if err != nil {
			return types.NewError("ERR value is not a valid float")
		}
		member := string(remaining[1].Str)
		return c.zaddIncr(ctx, key, increment, member, flagNX, flagXX, flagGT, flagLT)
	}

	pairs := make([]pair, 0, len(remaining)/2)
	for i := 0; i < len(remaining); i += 2 {
		score, err := parseScore(string(remaining[i].Str))
		if err != nil {
			return types.NewError("ERR value is not a valid float")
		}
		pairs = append(pairs, pair{score, string(remaining[i+1].Str)})
	}

	obj, ok := ctx.Engine.Get(key)
	var zset *object.ZSet
	isNew := false

	if ok {
		if obj.Type != object.TypeZSet {
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		zset = obj.Value.(*object.ZSet)
	} else {
		if flagXX {
			return types.NewInt(0)
		}
		zset = object.NewZSet()
		isNew = true
	}

	added := int64(0)
	changed := int64(0)

	for _, p := range pairs {
		oldScore, exists := zset.Score(p.member)

		if exists {
			if flagNX {
				continue
			}
			if flagGT && p.score <= oldScore {
				continue
			}
			if flagLT && p.score >= oldScore {
				continue
			}
			if p.score != oldScore {
				zset.Add(p.score, p.member)
				changed++
			}
		} else {
			if flagXX {
				continue
			}
			zset.Add(p.score, p.member)
			added++
			changed++
		}
	}

	if isNew && added > 0 {
		ctx.Engine.Set(key, &object.Object{
			Type:     object.TypeZSet,
			Encoding: object.EncodingRaw,
			Value:    zset,
		})
	}

	if changed > 0 || added > 0 {
		ctx.SignalKey(key)
	}

	if flagCH {
		return types.NewInt(changed)
	}
	return types.NewInt(added)
}

func (c ZAddCommand) zaddIncr(ctx *context.Context, key string, increment float64, member string, flagNX, flagXX, flagGT, flagLT bool) types.Value {
	obj, ok := ctx.Engine.Get(key)
	var zset *object.ZSet
	isNew := false

	if ok {
		if obj.Type != object.TypeZSet {
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		zset = obj.Value.(*object.ZSet)
	} else {
		if flagXX {
			return types.RespNil
		}
		zset = object.NewZSet()
		isNew = true
	}

	oldScore, exists := zset.Score(member)

	if exists {
		if flagNX {
			return types.RespNil
		}
		newScore := oldScore + increment
		if math.IsNaN(newScore) {
			return types.NewError("ERR resulting score is not a number (NaN)")
		}
		if flagGT && newScore <= oldScore {
			return types.RespNil
		}
		if flagLT && newScore >= oldScore {
			return types.RespNil
		}
		zset.Add(newScore, member)
		if isNew {
			ctx.Engine.Set(key, &object.Object{
				Type:     object.TypeZSet,
				Encoding: object.EncodingRaw,
				Value:    zset,
			})
		}
		ctx.SignalKey(key)
		return types.NewString(formatScore(newScore))
	}

	if flagXX {
		return types.RespNil
	}

	newScore := increment
	if math.IsNaN(newScore) {
		return types.NewError("ERR resulting score is not a number (NaN)")
	}
	zset.Add(newScore, member)
	ctx.Engine.Set(key, &object.Object{
		Type:     object.TypeZSet,
		Encoding: object.EncodingRaw,
		Value:    zset,
	})
	ctx.SignalKey(key)
	return types.NewString(formatScore(newScore))
}

func parseScore(s string) (float64, error) {
	switch strings.ToLower(s) {
	case "+inf", "inf", "-inf":
	default:
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, err
		}
		if math.IsNaN(f) {
			return 0, &floatParseError{}
		}
		return f, nil
	}
	switch strings.ToLower(s) {
	case "+inf", "inf":
		return math.Inf(1), nil
	case "-inf":
		return math.Inf(-1), nil
	}
	return strconv.ParseFloat(s, 64)
}

type floatParseError struct{}

func (e *floatParseError) Error() string {
	return "not a valid float"
}
