package streamcmd

import (
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/resp/types"
)

type xaddTrimConfig struct {
	strategy  string
	approx    bool
	threshold string
	limit     int
	refPolicy object.StreamTrimRefPolicy
}

type xaddOptions struct {
	noMkStream bool
	trim       xaddTrimConfig
	idmp       *xaddIDMPConfig
}

type xaddIDMPConfig struct {
	auto     bool
	producer string
	iid      string
}

func parseXAddOptions(args []types.Value, start int) (xaddOptions, int, types.Value, bool) {
	opts := xaddOptions{
		trim: xaddTrimConfig{
			refPolicy: object.StreamTrimKeepRef,
			limit:     -1,
		},
	}
	idx := start

	for idx < len(args) {
		switch strings.ToUpper(string(args[idx].Str)) {
		case "NOMKSTREAM":
			opts.noMkStream = true
			idx++
		case "MAXLEN", "MINID":
			opts.trim.strategy = strings.ToUpper(string(args[idx].Str))
			idx++
			if idx < len(args) {
				mod := string(args[idx].Str)
				if mod == "~" || mod == "=" {
					opts.trim.approx = mod == "~"
					idx++
				}
			}
			if idx >= len(args) {
				return opts, idx, types.NewError("ERR syntax error"), false
			}
			opts.trim.threshold = string(args[idx].Str)
			idx++
		case "LIMIT":
			if idx+1 >= len(args) {
				return opts, idx, types.NewError("ERR syntax error"), false
			}
			v, err := strconv.Atoi(string(args[idx+1].Str))
			if err != nil || v < 0 {
				return opts, idx, types.NewError("ERR value is not an integer or out of range"), false
			}
			opts.trim.limit = v
			idx += 2
		case "IDMP":
			if opts.idmp != nil {
				return opts, idx, types.NewError("ERR IDMP/IDMPAUTO specified multiple times"), false
			}
			if idx+2 >= len(args) {
				return opts, idx, types.NewError("ERR syntax error"), false
			}
			producer := string(args[idx+1].Str)
			iid := string(args[idx+2].Str)
			if producer == "" {
				return opts, idx, types.NewError("ERR IDMP requires a non-empty producer ID"), false
			}
			if iid == "" {
				return opts, idx, types.NewError("ERR IDMP requires a non-empty idempotent ID"), false
			}
			opts.idmp = &xaddIDMPConfig{
				auto:     false,
				producer: producer,
				iid:      iid,
			}
			idx += 3
		case "IDMPAUTO":
			if opts.idmp != nil {
				return opts, idx, types.NewError("ERR IDMP/IDMPAUTO specified multiple times"), false
			}
			if idx+1 >= len(args) {
				return opts, idx, types.NewError("ERR syntax error"), false
			}
			producer := string(args[idx+1].Str)
			if producer == "" {
				return opts, idx, types.NewError("ERR IDMPAUTO requires a non-empty producer ID"), false
			}
			opts.idmp = &xaddIDMPConfig{
				auto:     true,
				producer: producer,
			}
			idx += 2
		case "KEEPREF":
			opts.trim.refPolicy = object.StreamTrimKeepRef
			idx++
		case "DELREF":
			opts.trim.refPolicy = object.StreamTrimDelRef
			idx++
		case "ACKED":
			opts.trim.refPolicy = object.StreamTrimAcked
			idx++
		default:
			return opts, idx, types.Value{}, true
		}
	}

	return opts, idx, types.Value{}, true
}
