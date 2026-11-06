package setcmd

import (
	"math"
	"math/rand"
	"strconv"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// SPopCommand implements SPOP key [count]
type SPopCommand struct{}

func (c SPopCommand) Name() string { return "SPOP" }

func (c SPopCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'spop' command")
	}

	withCount := false
	count := 1
	if len(args) > 1 {
		withCount = true
		c, err := strconv.ParseInt(string(args[1].Str), 10, 64)
		if err != nil {
			return types.NewError("ERR value is not an integer or out of range")
		}
		if c < 0 {
			return types.NewError("ERR value is out of range, must be positive")
		}
		count = int(c)
	}

	obj, ok := ctx.Engine.GetBytes(args[0].Str)
	if !ok {
		ctx.SkipReplication = true
		if withCount {
			return types.RespEmptyArr
		}
		return types.RespNil
	}

	s, ok := obj.Value.(*object.Set)
	if !ok {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	members := s.Members()
	if len(members) == 0 {
		ctx.SkipReplication = true
		if withCount {
			return types.RespEmptyArr
		}
		return types.RespNil
	}

	if count > len(members) {
		count = len(members)
	}

	if count == 0 {
		ctx.SkipReplication = true
		return types.RespEmptyArr
	}

	// Shuffle the members slice for random selection
	rand.Shuffle(len(members), func(i, j int) {
		members[i], members[j] = members[j], members[i]
	})

	selected := members[:count]
	s.Remove(selected...)

	if s.Len() == 0 {
		ctx.Engine.Del(string(args[0].Str))
		cfg := ctx.Engine.GetEvictionConfig()
		cmd := []byte("DEL")
		if cfg.LazyfreeLazyServerDel {
			cmd = []byte("UNLINK")
		}
		ctx.ReplicateAs = [][]byte{cmd, args[0].Str}
	} else {
		replArgs := make([][]byte, 0, len(selected)+2)
		replArgs = append(replArgs, []byte("SREM"), args[0].Str)
		for _, m := range selected {
			replArgs = append(replArgs, []byte(m))
		}
		ctx.ReplicateAs = replArgs
	}

	if withCount {
		res := make([]types.Value, count)
		for i, m := range selected {
			res[i] = types.NewString(m)
		}
		return types.NewArray(res)
	}
	return types.NewString(selected[0])
}

// SRandMemberCommand implements SRANDMEMBER key [count]
type SRandMemberCommand struct{}

func (c SRandMemberCommand) Name() string { return "SRANDMEMBER" }

func (c SRandMemberCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'srandmember' command")
	}

	withCount := false
	count := 1
	allowDupes := false
	if len(args) > 1 {
		withCount = true
		c, err := strconv.ParseInt(string(args[1].Str), 10, 64)
		if err != nil {
			return types.NewError("ERR value is not an integer or out of range")
		}
		if c == math.MinInt64 {
			return types.NewError("ERR value is out of range")
		}
		if c < 0 {
			allowDupes = true
			count = int(-c)
		} else {
			count = int(c)
		}
	}

	obj, ok := ctx.Engine.GetBytes(args[0].Str)
	if !ok {
		if withCount {
			return types.RespEmptyArr
		}
		return types.RespNil
	}

	s, ok := obj.Value.(*object.Set)
	if !ok {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	members := s.Members()
	if len(members) == 0 {
		if withCount {
			return types.RespEmptyArr
		}
		return types.RespNil
	}

	if count == 0 {
		return types.RespEmptyArr
	}

	if !allowDupes && count > len(members) {
		count = len(members)
	}

	if !withCount {
		return types.NewString(members[0])
	}

	if !allowDupes {
		// Shuffle for unique random selection
		rand.Shuffle(len(members), func(i, j int) {
			members[i], members[j] = members[j], members[i]
		})
	}

	res := make([]types.Value, count)
	if allowDupes {
		for i := 0; i < count; i++ {
			// Randomly select with replacement
			res[i] = types.NewString(members[rand.Intn(len(members))])
		}
	} else {
		for i := 0; i < count; i++ {
			res[i] = types.NewString(members[i])
		}
	}
	return types.NewArray(res)
}
