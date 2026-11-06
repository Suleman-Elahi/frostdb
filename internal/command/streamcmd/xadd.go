package streamcmd

import (
	"hash/fnv"
	"sort"
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type XAddCommand struct{}

func (c XAddCommand) Name() string { return "XADD" }

func idmpAutoIID(fields []object.StreamField) string {
	type kv struct {
		k string
		v string
	}
	kvs := make([]kv, len(fields))
	for i, f := range fields {
		kvs[i] = kv{k: f.Name, v: f.Value}
	}
	sort.Slice(kvs, func(i, j int) bool {
		if kvs[i].k == kvs[j].k {
			return kvs[i].v < kvs[j].v
		}
		return kvs[i].k < kvs[j].k
	})

	h := fnv.New64a()
	for _, f := range kvs {
		_, _ = h.Write([]byte(f.k))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(f.v))
		_, _ = h.Write([]byte{0xff})
	}
	return strconv.FormatUint(h.Sum64(), 16)
}

func validateXAddIDSyntax(id string) error {
	if id == "*" {
		return nil
	}
	if strings.HasSuffix(id, "-*") {
		ms := strings.TrimSuffix(id, "-*")
		if ms == "" {
			return strconv.ErrSyntax
		}
		_, err := strconv.ParseUint(ms, 10, 64)
		return err
	}
	_, err := object.ParseStreamID(id)
	return err
}

func (c XAddCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	key := string(args[0].Str)

	opts, idx, errVal, ok := parseXAddOptions(args, 1)
	if !ok {
		return errVal
	}

	if idx >= len(args) {
		return types.NewError("ERR wrong number of arguments for 'xadd' command")
	}

	id := string(args[idx].Str)
	idx++

	if err := validateXAddIDSyntax(id); err != nil {
		return types.NewError("ERR Invalid stream ID specified")
	}

	if idx >= len(args) || (len(args)-idx)%2 != 0 {
		return types.NewError("ERR wrong number of arguments for 'xadd' command")
	}

	fields := streamFieldPairs(args[idx:])

	obj, ok := ctx.Engine.Get(key)
	var stream *object.Stream
	if ok {
		if obj.Type != object.TypeStream {
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		stream = obj.Value.(*object.Stream)
	} else {
		if opts.noMkStream {
			return types.RespNil
		}
		stream = object.NewStream()
	}

	if opts.idmp != nil && id != "*" {
		return types.NewError("ERR IDMP/IDMPAUTO can be used only with auto-generated IDs")
	}

	actualID := ""
	resolvedIID := ""
	if opts.idmp != nil {
		resolvedIID = opts.idmp.iid
		if opts.idmp.auto {
			resolvedIID = idmpAutoIID(fields)
		}
		idv, _, err := stream.AddIDMP(id, fields, opts.idmp.producer, resolvedIID)
		if err != nil {
			if err.Error() == "ERR Invalid stream ID" {
				return types.NewError("ERR Invalid stream ID specified")
			}
			return types.NewError(err.Error())
		}
		actualID = idv
	} else {
		idv, err := stream.Add(id, fields)
		if err != nil {
			if err.Error() == "ERR Invalid stream ID" {
				return types.NewError("ERR Invalid stream ID specified")
			}
			return types.NewError(err.Error())
		}
		actualID = idv
	}

	if !ok {
		obj = &object.Object{
			Type:     object.TypeStream,
			Encoding: object.EncodingRaw,
			Value:    stream,
		}
		ctx.Engine.Set(key, obj)
	}

	// Apply inline trimming semantics after insertion.
	switch opts.trim.strategy {
	case "MAXLEN":
		maxlen, err := strconv.Atoi(opts.trim.threshold)
		if err != nil || maxlen < 0 {
			return types.NewError("ERR value is not an integer or out of range")
		}
		stream.TrimMaxLenFull(maxlen, opts.trim.approx, opts.trim.limit, opts.trim.refPolicy)
	case "MINID":
		stream.TrimMinIDFull(opts.trim.threshold, opts.trim.approx, opts.trim.limit, opts.trim.refPolicy)
	}

	// Normalize WAL replication so generated IDs and IDMPAUTO are deterministic on replay.
	rep := [][]byte{[]byte("XADD"), []byte(key)}
	if opts.noMkStream {
		rep = append(rep, []byte("NOMKSTREAM"))
	}
	switch opts.trim.refPolicy {
	case object.StreamTrimDelRef:
		rep = append(rep, []byte("DELREF"))
	case object.StreamTrimAcked:
		rep = append(rep, []byte("ACKED"))
	}
	if opts.idmp != nil {
		rep = append(rep, []byte("IDMP"), []byte(opts.idmp.producer), []byte(resolvedIID))
	}
	if opts.trim.strategy != "" {
		if opts.trim.approx && opts.trim.refPolicy == object.StreamTrimKeepRef {
			rep = append(rep, []byte("MAXLEN"), []byte("="), []byte(strconv.Itoa(stream.Len())))
		} else {
			rep = append(rep, []byte(opts.trim.strategy))
			if opts.trim.approx {
				rep = append(rep, []byte("~"))
			} else {
				rep = append(rep, []byte("="))
			}
			rep = append(rep, []byte(opts.trim.threshold))
			if opts.trim.approx && opts.trim.limit >= 0 {
				rep = append(rep, []byte("LIMIT"), []byte(strconv.Itoa(opts.trim.limit)))
			}
		}
	}
	rep = append(rep, []byte(actualID))
	for _, f := range fields {
		rep = append(rep, []byte(f.Name), []byte(f.Value))
	}
	ctx.ReplicateAs = rep

	ctx.SignalKey(key)
	return types.NewString(actualID)
}
