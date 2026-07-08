package setcmd

import (
	"strconv"
	"strings"

	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// SUnionCardCommand implements SUNIONCARD numkeys key [key ...] [APPROX] [LIMIT limit].
type SUnionCardCommand struct{}

func (c SUnionCardCommand) Name() string { return "SUNIONCARD" }

func (c SUnionCardCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'sunioncard' command")
	}

	numkeys, err := strconv.Atoi(string(args[0].Str))
	if err != nil || numkeys < 1 {
		return types.NewError("ERR numkeys must be a positive integer")
	}

	if len(args) < 1+numkeys {
		return types.NewError("ERR Number of keys can't be greater than number of args")
	}

	keys := args[1 : 1+numkeys]
	limit := 0

	pos := 1 + numkeys
	for pos < len(args) {
		word := strings.ToUpper(string(args[pos].Str))
		switch word {
		case "APPROX":
			pos++
		case "LIMIT":
			if pos+1 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			l, err := strconv.Atoi(string(args[pos+1].Str))
			if err != nil || l < 0 {
				return types.NewError("ERR LIMIT value is not a positive integer")
			}
			limit = l
			pos += 2
		default:
			return types.NewError("ERR syntax error")
		}
	}

	result := make(map[string]struct{})
	for _, key := range keys {
		s, errVal := getSet(ctx, key.Str)
		if errVal.Type != 0 {
			return errVal
		}
		if s == nil {
			continue
		}
		for _, m := range s.Members() {
			result[m] = struct{}{}
			if limit > 0 && len(result) >= limit {
				return types.NewInt(int64(len(result)))
			}
		}
	}

	return types.NewInt(int64(len(result)))
}
