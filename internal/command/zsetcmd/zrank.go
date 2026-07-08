package zsetcmd

import (
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// ZRankCommand implements ZRANK key member [WITHSCORE].
type ZRankCommand struct{}

func (c ZRankCommand) Name() string { return "ZRANK" }

func (c ZRankCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'zrank' command")
	}

	key := string(args[0].Str)
	member := string(args[1].Str)
	withScore := len(args) >= 3 && strings.ToUpper(string(args[2].Str)) == "WITHSCORE"

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		if withScore {
			return types.RespNilArr
		}
		return types.RespNil
	}

	if obj.Type != object.TypeZSet {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	z := obj.Value.(*object.ZSet)
	rank := zsetRank(z, member)
	if rank < 0 {
		if withScore {
			return types.RespNilArr
		}
		return types.RespNil
	}

	if withScore {
		score, _ := z.Score(member)
		return types.NewArray([]types.Value{
			types.NewInt(int64(rank)),
			types.NewString(formatScore(score)),
		})
	}
	return types.NewInt(int64(rank))
}

// ZRevRankCommand implements ZREVRANK key member [WITHSCORE].
type ZRevRankCommand struct{}

func (c ZRevRankCommand) Name() string { return "ZREVRANK" }

func (c ZRevRankCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'zrevrank' command")
	}

	key := string(args[0].Str)
	member := string(args[1].Str)
	withScore := len(args) >= 3 && strings.ToUpper(string(args[2].Str)) == "WITHSCORE"

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		if withScore {
			return types.RespNilArr
		}
		return types.RespNil
	}

	if obj.Type != object.TypeZSet {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	z := obj.Value.(*object.ZSet)
	rank := zsetRank(z, member)
	if rank < 0 {
		if withScore {
			return types.RespNilArr
		}
		return types.RespNil
	}
	revRank := z.Len() - 1 - rank

	if withScore {
		score, _ := z.Score(member)
		return types.NewArray([]types.Value{
			types.NewInt(int64(revRank)),
			types.NewString(formatScore(score)),
		})
	}
	return types.NewInt(int64(revRank))
}

func zsetRank(z *object.ZSet, member string) int {
	score, exists := z.Score(member)
	if !exists {
		return -1
	}

	rank := 0
	node := z.Skiplist.Head.Level[0].Forward
	for node != nil {
		if node.Score == score && node.Member == member {
			return rank
		}
		rank++
		node = node.Level[0].Forward
	}
	return -1
}
