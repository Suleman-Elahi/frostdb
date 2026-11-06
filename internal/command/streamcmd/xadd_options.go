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
	hasPolicy := false

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
			if opts.trim.strategy == "MAXLEN" {
				maxlen, err := strconv.Atoi(opts.trim.threshold)
				if err != nil {
					return opts, idx, types.NewError("ERR value is not an integer or out of range"), false
				}
				if maxlen < 0 {
					return opts, idx, types.NewError("ERR The MAXLEN argument must be >= 0."), false
				}
			} else {
				if _, err := object.ParseStreamID(opts.trim.threshold); err != nil {
					return opts, idx, types.NewError("ERR Invalid stream ID specified as stream command argument"), false
				}
			}
			idx++
		case "LIMIT":
			if !opts.trim.approx {
				return opts, idx, types.NewError("ERR syntax error, LIMIT cannot be used without the special ~ option"), false
			}
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
			if hasPolicy {
				return opts, idx, types.NewError("ERR syntax error, only one of KEEPREF, DELREF, or ACKED can be specified"), false
			}
			opts.trim.refPolicy = object.StreamTrimKeepRef
			hasPolicy = true
			idx++
		case "DELREF":
			if hasPolicy {
				return opts, idx, types.NewError("ERR syntax error, only one of KEEPREF, DELREF, or ACKED can be specified"), false
			}
			opts.trim.refPolicy = object.StreamTrimDelRef
			hasPolicy = true
			idx++
		case "ACKED":
			if hasPolicy {
				return opts, idx, types.NewError("ERR syntax error, only one of KEEPREF, DELREF, or ACKED can be specified"), false
			}
			opts.trim.refPolicy = object.StreamTrimAcked
			hasPolicy = true
			idx++
		default:
			return opts, idx, types.Value{}, true
		}
	}

	return opts, idx, types.Value{}, true
}
