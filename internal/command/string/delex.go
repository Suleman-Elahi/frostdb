package str

import (
	"bytes"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// DelexCondition is the comparison mode for DELEX / conditional SET.
type DelexCondition int

const (
	DelexNone  DelexCondition = iota // no condition
	DelexIFEQ                        // delete/set if value equals arg
	DelexIFNE                        // delete/set if value does NOT equal arg
	DelexIFDEQ                       // delete/set if digest(value) equals arg
	DelexIFDNE                       // delete/set if digest(value) does NOT equal arg
)

// parseDelexCondition converts the condition string (case-insensitive) to a
// DelexCondition constant. Returns (DelexNone, false) for unknown tokens.
func parseDelexCondition(token []byte) (DelexCondition, bool) {
	switch strings.ToUpper(string(token)) {
	case "IFEQ":
		return DelexIFEQ, true
	case "IFNE":
		return DelexIFNE, true
	case "IFDEQ":
		return DelexIFDEQ, true
	case "IFDNE":
		return DelexIFDNE, true
	}
	return DelexNone, false
}

// DelexCommand implements DELEX key [IFEQ|IFNE|IFDEQ|IFDNE value]
//
// Conditional delete extension unique to Frost:
//
//	DELEX key             – unconditional delete (returns 1 if key existed)
//	DELEX key IFEQ val    – delete if string value equals val
//	DELEX key IFNE val    – delete if string value does NOT equal val
//	DELEX key IFDEQ hex   – delete if FNV-1a digest of value equals hex (16 chars)
//	DELEX key IFDNE hex   – delete if FNV-1a digest of value does NOT equal hex
//
// Returns:
//
//	1  – key was deleted
//	0  – condition not met (or key didn't exist for conditional variants)
//	ERR – wrong type / invalid args
type DelexCommand struct{}

func (c DelexCommand) Name() string { return "DELEX" }

func (c DelexCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'delex' command")
	}

	keyBytes := args[0].Str

	// ── Unconditional delete ──────────────────────────────────────────────
	if len(args) == 1 {
		if ctx.Engine.DelBytes(keyBytes) {
			ctx.ReplicateAs = [][]byte{[]byte("DEL"), keyBytes}
			return types.RespOne
		}
		ctx.SkipReplication = true
		return types.RespZero
	}

	// ── Conditional: must have exactly cond + value (3 total args) ────────
	// Check for missing value arg first (DELEX key COND — no value).
	cond, isKnown := parseDelexCondition(args[1].Str)
	if !isKnown {
		return types.NewError("ERR Invalid condition")
	}
	if len(args) == 2 {
		return types.NewError("ERR wrong number of arguments for 'delex' command")
	}
	if len(args) > 3 {
		return types.NewError("ERR wrong number of arguments for 'delex' command")
	}

	condArg := args[2].Str

	// ── Fetch existing object ─────────────────────────────────────────────
	obj, ok := ctx.Engine.GetBytes(keyBytes)
	if !ok {
		// Key doesn't exist — condition can never be met.
		// Do NOT validate digest format for missing keys (test passes arbitrary
		// non-hex strings like "1234567890" and expects 0, not an error).
		ctx.SkipReplication = true
		return types.RespZero
	}

	// Only string type is supported for conditional comparisons.
	if obj.Type != object.TypeString {
		ctx.SkipReplication = true
		return types.NewError("ERR DELEX requires a string value for conditional operations")
	}

	// Validate digest format only now that we know the key exists.
	var condDigest string
	if cond == DelexIFDEQ || cond == DelexIFDNE {
		var err error
		condDigest, err = parseDigestArg(condArg)
		if err != nil {
			return types.NewError(err.Error())
		}
	}

	strVal, _ := obj.Value.([]byte)

	// ── Evaluate condition ─────────────────────────────────────────────────
	var shouldDelete bool
	switch cond {
	case DelexIFEQ:
		shouldDelete = bytes.Equal(strVal, condArg)
	case DelexIFNE:
		shouldDelete = !bytes.Equal(strVal, condArg)
	case DelexIFDEQ:
		shouldDelete = digestStringBytes(strVal) == condDigest
	case DelexIFDNE:
		shouldDelete = digestStringBytes(strVal) != condDigest
	}

	if shouldDelete {
		ctx.Engine.DelBytes(keyBytes)
		ctx.ReplicateAs = [][]byte{[]byte("DEL"), keyBytes}
		return types.RespOne
	}
	ctx.SkipReplication = true
	return types.RespZero
}
