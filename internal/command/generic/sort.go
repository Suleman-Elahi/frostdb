package generic

import (
	"sort"
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type sortEntry struct {
	src    string
	weight float64
	alpha  string
}

type sortParams struct {
	byPattern string
	byField   string
	getPatterns []getPattern
	limit      [2]int
	hasLimit   bool
	desc       bool
	alpha      bool
	store      string
	nosort     bool
	isReadOnly bool
}

type getPattern struct {
	raw    string
	keyPat string
	field  string
}

// SortCommand implements SORT.
type SortCommand struct{}

func (c SortCommand) Name() string { return "SORT" }

func (c SortCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	return execSort(ctx, args, false)
}

// SortROCommand implements SORT_RO — read-only SORT (no STORE).
type SortROCommand struct{}

func (c SortROCommand) Name() string { return "SORT_RO" }

func (c SortROCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	return execSort(ctx, args, true)
}

func execSort(ctx *context.Context, args []types.Value, readOnly bool) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'sort' command")
	}

	key := string(args[0].Str)
	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return handleEmptySort(ctx, args, readOnly)
	}

	var elements []string
	switch obj.Type {
	case object.TypeList:
		vals := obj.Value.(*object.List).Snapshot()
		elements = make([]string, len(vals))
		for i, v := range vals {
			elements[i] = string(v)
		}
	case object.TypeSet:
		elements = obj.Value.(*object.Set).Members()
	case object.TypeZSet:
		elements = obj.Value.(*object.ZSet).Members()
	default:
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	origOrder := make(map[string]int, len(elements))
	for i, e := range elements {
		origOrder[e] = i
	}

	p := parseSortOptions(args, readOnly)
	if p == nil {
		return types.NewError("ERR syntax error")
	}

	entries := make([]sortEntry, 0, len(elements))
	for _, e := range elements {
		var se sortEntry
		se.src = e

		if p.nosort {
			// nosort preserves original order — no weight/alpha needed
		} else if p.byPattern == "" {
			if p.alpha {
				se.alpha = e
			} else {
				w, err := strconv.ParseFloat(e, 64)
				if err != nil {
					return types.NewError("ERR One or more scores can't be converted into double")
				}
				se.weight = w
			}
		} else {
			k := strings.ReplaceAll(p.byPattern, "*", e)
			if p.byField != "" {
				hashObj, hashOk := ctx.Engine.Get(k)
				if hashOk && hashObj.Type == object.TypeHash {
					h := hashObj.Value.(*object.Hash)
					if val, found := h.Get(p.byField); found {
						w, err := strconv.ParseFloat(val, 64)
						if err != nil {
							return types.NewError("ERR One or more scores can't be converted into double")
						}
						se.weight = w
						entries = append(entries, se)
						continue
					}
				}
				se.weight = 0
			} else {
				wObj, wOk := ctx.Engine.Get(k)
				if wOk && wObj.Type == object.TypeString {
					val := wObj.Value.(string)
					w, err := strconv.ParseFloat(val, 64)
					if err != nil {
						return types.NewError("ERR One or more scores can't be converted into double")
					}
					se.weight = w
				} else {
					se.weight = 0
				}
			}
		}

		entries = append(entries, se)
	}

	if p.nosort {
		sort.SliceStable(entries, func(i, j int) bool {
			if p.desc {
				return origOrder[entries[i].src] > origOrder[entries[j].src]
			}
			return origOrder[entries[i].src] < origOrder[entries[j].src]
		})
	} else {
		sort.SliceStable(entries, func(i, j int) bool {
			ai, aj := entries[i], entries[j]
			if ai.weight != aj.weight {
				if p.desc {
					return ai.weight > aj.weight
				}
				return ai.weight < aj.weight
			}
			if p.desc {
				return ai.src > aj.src
			}
			return ai.src < aj.src
		})
	}

	if p.hasLimit {
		off, cnt := p.limit[0], p.limit[1]
		if off < 0 {
			off += len(entries)
			if off < 0 {
				off = 0
			}
		}
		if off > len(entries) {
			entries = nil
		} else {
			end := off + cnt
			if end < off || end > len(entries) {
				end = len(entries)
			}
			entries = entries[off:end]
		}
	}

	return buildSortResult(ctx, entries, p)
}

func handleEmptySort(ctx *context.Context, args []types.Value, readOnly bool) types.Value {
	// Check for STORE (and reject for SORT_RO)
	hasStore := false
	storeDest := ""
	for i := 1; i < len(args); i++ {
		if strings.ToUpper(string(args[i].Str)) == "STORE" && i+1 < len(args) {
			if readOnly {
				return types.NewError("ERR syntax error")
			}
			hasStore = true
			storeDest = string(args[i+1].Str)
			break
		}
	}
	if hasStore {
		ctx.Engine.Del(storeDest)
		return types.NewInt(0)
	}
	if readOnly {
		return types.RespEmptyArr
	}
	return types.RespEmptyArr
}

func buildSortResult(ctx *context.Context, entries []sortEntry, p *sortParams) types.Value {
	var result []types.Value

	if len(p.getPatterns) > 0 {
		for _, e := range entries {
			for _, gp := range p.getPatterns {
				if gp.raw == "#" {
					result = append(result, types.NewString(e.src))
				} else {
					k := strings.ReplaceAll(gp.keyPat, "*", e.src)
					if gp.field != "" {
						hashObj, hashOk := ctx.Engine.Get(k)
						if hashOk && hashObj.Type == object.TypeHash {
							h := hashObj.Value.(*object.Hash)
							if val, found := h.Get(gp.field); found {
								result = append(result, types.NewString(val))
							} else {
								result = append(result, types.RespNil)
							}
						} else {
							result = append(result, types.RespNil)
						}
					} else {
						vObj, vOk := ctx.Engine.Get(k)
						if vOk && vObj.Type == object.TypeString {
							result = append(result, types.NewString(vObj.Value.(string)))
						} else {
							result = append(result, types.RespNil)
						}
					}
				}
			}
		}
	} else {
		for _, e := range entries {
			result = append(result, types.NewString(e.src))
		}
	}

	if p.store != "" {
		if len(result) == 0 {
			// If the sort result is empty, delete the destination key
			ctx.Engine.Del(p.store)
		} else {
			list := object.NewList()
			for _, r := range result {
				var val string
				if r.Type == types.TypeBulkString || r.Type == types.TypeSimpleString {
					val = string(r.Str)
				} else {
					val = ""
				}
				list.PushBack([]byte(val))
			}
			ctx.Engine.Set(p.store, &object.Object{
				Type:     object.TypeList,
				Encoding: object.EncodingRaw,
				Value:    list,
			})
		}
		ctx.Engine.SignalKey(p.store)
		return types.NewInt(int64(len(result)))
	}

	return types.NewArray(result)
}

func parseSortOptions(args []types.Value, readOnly bool) *sortParams {
	p := &sortParams{isReadOnly: readOnly}

	i := 1
	for i < len(args) {
		opt := strings.ToUpper(string(args[i].Str))
		switch opt {
		case "ASC":
			p.desc = false
			i++
		case "DESC":
			p.desc = true
			i++
		case "ALPHA":
			p.alpha = true
			i++
		case "STORE":
			if readOnly {
				return nil
			}
			if i+1 >= len(args) {
				return nil
			}
			p.store = string(args[i+1].Str)
			i += 2
		case "LIMIT":
			if i+2 >= len(args) {
				return nil
			}
			o, err1 := strconv.Atoi(string(args[i+1].Str))
			c, err2 := strconv.Atoi(string(args[i+2].Str))
			if err1 != nil || err2 != nil {
				return nil
			}
			p.limit = [2]int{o, c}
			p.hasLimit = true
			i += 3
		case "BY":
			if i+1 >= len(args) {
				return nil
			}
			by := string(args[i+1].Str)
			i += 2

			idx := strings.Index(by, "->")
			if idx >= 0 && idx < len(by)-2 {
				// -> not at end: hash field lookup
				p.byPattern = by[:idx]
				p.byField = by[idx+2:]
			} else if strings.EqualFold(by, "nosort") {
				p.nosort = true
			} else if !strings.Contains(by, "*") {
				// BY <const> without *: treat each element's lookup as the same key.
				// The key likely doesn't exist, so all get weight 0 — sorted by elements.
				p.byPattern = by
			} else {
				p.byPattern = by
			}
		case "GET":
			if i+1 >= len(args) {
				return nil
			}
			gpStr := string(args[i+1].Str)
			i += 2

			var g getPattern
			g.raw = gpStr
			if gpStr == "#" {
				g.keyPat = "#"
			} else if idx := strings.Index(gpStr, "->"); idx >= 0 && idx < len(gpStr)-2 {
				g.keyPat = gpStr[:idx]
				g.field = gpStr[idx+2:]
			} else {
				g.keyPat = gpStr
			}
			p.getPatterns = append(p.getPatterns, g)
		default:
			return nil
		}
	}

	return p
}
