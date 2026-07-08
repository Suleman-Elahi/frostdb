package zsetcmd

import (
	"math/rand"
	"strconv"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// ZRandMemberCommand implements ZRANDMEMBER key [count [WITHSCORES]]
type ZRandMemberCommand struct{}

func (c ZRandMemberCommand) Name() string { return "ZRANDMEMBER" }

func (c ZRandMemberCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'zrandmember' command")
	}

	key := string(args[0].Str)

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		if len(args) >= 2 {
			return types.NewArray([]types.Value{})
		}
		return types.RespNil
	}

	if obj.Type != object.TypeZSet {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	zset := obj.Value.(*object.ZSet)
	members := zset.Members()

	if len(members) == 0 {
		if len(args) >= 2 {
			return types.NewArray([]types.Value{})
		}
		return types.RespNil
	}

	// No count argument: return a single random member
	if len(args) < 2 {
		idx := rand.Intn(len(members))
		return types.NewString(members[idx])
	}

	count, err := strconv.ParseInt(string(args[1].Str), 10, 64)
	if err != nil {
		return types.NewError("ERR value is not an integer or out of range")
	}

	// Check for overflow on the negative side
	if count < -1<<53 || count > 1<<53 {
		return types.NewError("ERR value is out of range")
	}

	withScores := false
	if len(args) >= 3 {
		if string(args[2].Str) == "WITHSCORES" || string(args[2].Str) == "withscores" {
			withScores = true
		}
	}

	if count == 0 {
		return types.NewArray([]types.Value{})
	}

	allowDuplicates := count < 0
	if count < 0 {
		count = -count
	}

	var result []types.Value

	if allowDuplicates {
		// Negative count: allow duplicates
		for i := int64(0); i < count; i++ {
			idx := rand.Intn(len(members))
			m := members[idx]
			if withScores {
				score, _ := zset.Score(m)
				if ctx.Protocol >= 3 {
					result = append(result, types.NewArray([]types.Value{
						types.NewString(m),
						newDouble(score),
					}))
				} else {
					result = append(result, types.NewString(m))
					result = append(result, types.NewString(formatScore(score)))
				}
			} else {
				result = append(result, types.NewString(m))
			}
		}
	} else {
		// Positive count: unique elements only
		n := int(count)
		if n > len(members) {
			n = len(members)
		}

		// Fisher-Yates shuffle on a copy to get n unique random elements
		shuffled := make([]string, len(members))
		copy(shuffled, members)
		for i := len(shuffled) - 1; i > 0; i-- {
			j := rand.Intn(i + 1)
			shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
		}

		for i := 0; i < n; i++ {
			m := shuffled[i]
			if withScores {
				score, _ := zset.Score(m)
				if ctx.Protocol >= 3 {
					result = append(result, types.NewArray([]types.Value{
						types.NewString(m),
						newDouble(score),
					}))
				} else {
					result = append(result, types.NewString(m))
					result = append(result, types.NewString(formatScore(score)))
				}
			} else {
				result = append(result, types.NewString(m))
			}
		}
	}

	return types.NewArray(result)
}
