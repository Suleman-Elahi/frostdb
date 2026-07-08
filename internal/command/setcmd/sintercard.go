package setcmd

import (
	"strconv"
	"strings"

	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// SInterCardCommand implements SINTERCARD numkeys key [key ...] [LIMIT limit].
type SInterCardCommand struct{}

func (c SInterCardCommand) Name() string { return "SINTERCARD" }

func (c SInterCardCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'sintercard' command")
	}

	numkeys, err := strconv.Atoi(string(args[0].Str))
	if err != nil || numkeys < 1 {
		return types.NewError("ERR numkeys can't be non-positive value")
	}

	if len(args) < 1+numkeys {
		return types.NewError("ERR Number of keys can't be greater than number of args")
	}

	keys := args[1 : 1+numkeys]
	limit := 0

	// Parse LIMIT option
	limitPos := 1 + numkeys
	if limitPos < len(args) {
		if strings.ToUpper(string(args[limitPos].Str)) == "LIMIT" && limitPos+1 < len(args) {
			l, err := strconv.Atoi(string(args[limitPos+1].Str))
			if err != nil || l < 0 {
				return types.NewError("ERR LIMIT value is not a positive integer")
			}
			limit = l
			limitPos += 2
		} else {
			return types.NewError("ERR syntax error")
		}
	}
	
	if limitPos < len(args) {
		return types.NewError("ERR syntax error")
	}

	// Compute intersection
	first, errVal := getSet(ctx, keys[0].Str)
	if errVal.Type != 0 {
		return errVal
	}
	if first == nil {
		return types.NewInt(0)
	}

	count := int64(0)
	for _, m := range first.Members() {
		inAll := true
		for i := 1; i < len(keys); i++ {
			other, errVal := getSet(ctx, keys[i].Str)
			if errVal.Type != 0 {
				return errVal
			}
			if other == nil {
				return types.NewInt(0)
			}
			if !other.Contains(m) {
				inAll = false
				break
			}
		}
		if inAll {
			count++
			if limit > 0 && count >= int64(limit) {
				break
			}
		}
	}

	return types.NewInt(count)
}
