package zsetcmd

import (
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type ZRangeByLexCommand struct{}

func (c ZRangeByLexCommand) Name() string { return "ZRANGEBYLEX" }

func (c ZRangeByLexCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 3 {
		return types.NewError("ERR wrong number of arguments for 'zrangebylex' command")
	}

	key := string(args[0].Str)
	minStr := string(args[1].Str)
	maxStr := string(args[2].Str)

	if !validLexBound(minStr) || !validLexBound(maxStr) {
		return types.NewError("ERR min or max not valid string range item")
	}

	limitSet := false
	offset := 0
	count := -1

	for i := 3; i < len(args); i++ {
		opt := strings.ToUpper(string(args[i].Str))
		if opt == "LIMIT" {
			if i+2 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			var err error
			offset, err = strconv.Atoi(string(args[i+1].Str))
			if err != nil {
				return types.NewError("ERR value is not an integer or out of range")
			}
			count, err = strconv.Atoi(string(args[i+2].Str))
			if err != nil {
				return types.NewError("ERR value is not an integer or out of range")
			}
			limitSet = true
			i += 2
		} else {
			return types.NewError("ERR syntax error")
		}
	}

	if limitSet && (offset < 0 || count < 0) {
		return types.NewArray([]types.Value{})
	}

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewArray([]types.Value{})
	}

	if obj.Type != object.TypeZSet {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	zset := obj.Value.(*object.ZSet)
	min, minExcl, _ := object.ParseLexBound(minStr)
	max, maxExcl, _ := object.ParseLexBound(maxStr)

	entries := zset.RangeByLex(min, max, minExcl, maxExcl)

	if offset > 0 {
		if offset >= len(entries) {
			entries = nil
		} else {
			entries = entries[offset:]
		}
	}
	if count >= 0 && count < len(entries) {
		entries = entries[:count]
	}

	result := make([]types.Value, 0, len(entries))
	for _, e := range entries {
		result = append(result, types.NewString(e.Member))
	}
	return types.NewArray(result)
}

func validLexBound(s string) bool {
	if len(s) == 0 {
		return false
	}
	switch s[0] {
	case '(', '[':
		return true
	case '-', '+':
		return len(s) == 1
	}
	return false
}

type ZRevRangeByLexCommand struct{}

func (c ZRevRangeByLexCommand) Name() string { return "ZREVRANGEBYLEX" }

func (c ZRevRangeByLexCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 3 {
		return types.NewError("ERR wrong number of arguments for 'zrevrangebylex' command")
	}

	key := string(args[0].Str)
	maxStr := string(args[1].Str)
	minStr := string(args[2].Str)

	if !validLexBound(maxStr) || !validLexBound(minStr) {
		return types.NewError("ERR min or max not valid string range item")
	}

	limitSet := false
	offset := 0
	count := -1

	for i := 3; i < len(args); i++ {
		opt := strings.ToUpper(string(args[i].Str))
		if opt == "LIMIT" {
			if i+2 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			var err error
			offset, err = strconv.Atoi(string(args[i+1].Str))
			if err != nil {
				return types.NewError("ERR value is not an integer or out of range")
			}
			count, err = strconv.Atoi(string(args[i+2].Str))
			if err != nil {
				return types.NewError("ERR value is not an integer or out of range")
			}
			limitSet = true
			i += 2
		} else {
			return types.NewError("ERR syntax error")
		}
	}

	if limitSet && (offset < 0 || count < 0) {
		return types.NewArray([]types.Value{})
	}

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewArray([]types.Value{})
	}

	if obj.Type != object.TypeZSet {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	zset := obj.Value.(*object.ZSet)
	min, minExcl, _ := object.ParseLexBound(minStr)
	max, maxExcl, _ := object.ParseLexBound(maxStr)

	entries := zset.RangeByLex(min, max, minExcl, maxExcl)

	for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
		entries[i], entries[j] = entries[j], entries[i]
	}

	if offset > 0 {
		if offset >= len(entries) {
			entries = nil
		} else {
			entries = entries[offset:]
		}
	}
	if count >= 0 && count < len(entries) {
		entries = entries[:count]
	}

	result := make([]types.Value, 0, len(entries))
	for _, e := range entries {
		result = append(result, types.NewString(e.Member))
	}
	return types.NewArray(result)
}

type ZLexCountCommand struct{}

func (c ZLexCountCommand) Name() string { return "ZLEXCOUNT" }

func (c ZLexCountCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 3 {
		return types.NewError("ERR wrong number of arguments for 'zlexcount' command")
	}

	key := string(args[0].Str)
	minStr := string(args[1].Str)
	maxStr := string(args[2].Str)

	if !validLexBound(minStr) || !validLexBound(maxStr) {
		return types.NewError("ERR min or max not valid string range item")
	}

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewInt(0)
	}

	if obj.Type != object.TypeZSet {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	zset := obj.Value.(*object.ZSet)
	min, minExcl, _ := object.ParseLexBound(minStr)
	max, maxExcl, _ := object.ParseLexBound(maxStr)

	count := len(zset.RangeByLex(min, max, minExcl, maxExcl))
	return types.NewInt(int64(count))
}
