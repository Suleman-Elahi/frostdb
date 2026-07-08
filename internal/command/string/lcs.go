package str

import (
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// LCSCommand implements LCS key1 key2 [LEN] [IDX [WITHMATCHLEN] [MINMATCHLEN min]]
//
// Returns the Longest Common Subsequence of two string values.
// Options:
//
//	LEN          – return only the length of the LCS
//	IDX          – return a structured response with match positions
//	WITHMATCHLEN – (with IDX) include match lengths in the output
//	MINMATCHLEN  – (with IDX) filter out matches shorter than min
type LCSCommand struct{}

func (c LCSCommand) Name() string { return "LCS" }

func (c LCSCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'lcs' command")
	}

	// Fetch both strings
	s1, ok := getStringVal(ctx, args[0].Str)
	if !ok {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	s2, ok := getStringVal(ctx, args[1].Str)
	if !ok {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	// Parse options
	wantLen := false
	wantIdx := false
	withMatchLen := false
	minMatchLen := 0

	for i := 2; i < len(args); i++ {
		opt := strings.ToUpper(string(args[i].Str))
		switch opt {
		case "LEN":
			wantLen = true
		case "IDX":
			wantIdx = true
		case "WITHMATCHLEN":
			withMatchLen = true
		case "MINMATCHLEN":
			if i+1 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			i++
			n, err := parseInt(args[i].Str)
			if err != nil || n < 0 {
				return types.NewError("ERR value is not an integer or out of range")
			}
			minMatchLen = int(n)
		default:
			return types.NewError("ERR syntax error")
		}
	}

	// Build the LCS DP table
	lcsStr, matches := computeLCS(s1, s2)

	// ── LEN only ───────────────────────────────────────────────────────────
	if wantLen && !wantIdx {
		return types.NewInt(int64(len(lcsStr)))
	}

	// ── IDX ────────────────────────────────────────────────────────────────
	if wantIdx {
		return buildIDXResponse(matches, lcsStr, withMatchLen, minMatchLen)
	}

	// ── Default: return LCS string ─────────────────────────────────────────
	return types.NewString(lcsStr)
}

// lcsMatch represents a matching block between two strings.
// a: [aStart, aEnd] inclusive in string1
// b: [bStart, bEnd] inclusive in string2
type lcsMatch struct {
	aStart, aEnd int
	bStart, bEnd int
}

// computeLCS runs the standard DP algorithm and returns the LCS string plus
// the list of matching blocks (in reverse order, as Redis returns them).
func computeLCS(a, b string) (string, []lcsMatch) {
	la, lb := len(a), len(b)
	if la == 0 || lb == 0 {
		return "", nil
	}

	// Allocate DP table: dp[i][j] = length of LCS of a[:i] and b[:j]
	dp := make([][]int, la+1)
	for i := range dp {
		dp[i] = make([]int, lb+1)
	}
	for i := 1; i <= la; i++ {
		for j := 1; j <= lb; j++ {
			if a[i-1] == b[j-1] {
				dp[i][j] = dp[i-1][j-1] + 1
			} else if dp[i-1][j] >= dp[i][j-1] {
				dp[i][j] = dp[i-1][j]
			} else {
				dp[i][j] = dp[i][j-1]
			}
		}
	}

	// Back-trace to collect matching blocks
	var matches []lcsMatch
	i, j := la, lb
	var lcs []byte

	for i > 0 && j > 0 {
		if a[i-1] == b[j-1] {
			// Extend or start a new match block
			aEnd, bEnd := i-1, j-1
			for i > 0 && j > 0 && a[i-1] == b[j-1] {
				lcs = append(lcs, a[i-1])
				i--
				j--
			}
			aStart, bStart := i, j
			matches = append(matches, lcsMatch{aStart, aEnd, bStart, bEnd})
		} else if dp[i-1][j] >= dp[i][j-1] {
			i--
		} else {
			j--
		}
	}

	// Reverse the collected LCS bytes (we built it backwards)
	for l, r := 0, len(lcs)-1; l < r; l, r = l+1, r-1 {
		lcs[l], lcs[r] = lcs[r], lcs[l]
	}

	return string(lcs), matches
}

// buildIDXResponse constructs the Redis IDX response map.
// Redis format (as a RESP map / array of key-value pairs):
//
//	"matches" -> [ [[a1_start a1_end] [b1_start b1_end] (len?)] ... ]
//	"len"     -> <total lcs length>
func buildIDXResponse(matches []lcsMatch, lcsStr string, withMatchLen bool, minMatchLen int) types.Value {
	// Filter by minMatchLen and build match entries
	var matchEntries []types.Value
	for _, m := range matches {
		matchLen := m.aEnd - m.aStart + 1
		if matchLen < minMatchLen {
			continue
		}

		aRange := types.NewArray([]types.Value{
			types.NewInt(int64(m.aStart)),
			types.NewInt(int64(m.aEnd)),
		})
		bRange := types.NewArray([]types.Value{
			types.NewInt(int64(m.bStart)),
			types.NewInt(int64(m.bEnd)),
		})

		var entry []types.Value
		if withMatchLen {
			entry = []types.Value{aRange, bRange, types.NewInt(int64(matchLen))}
		} else {
			entry = []types.Value{aRange, bRange}
		}
		matchEntries = append(matchEntries, types.NewArray(entry))
	}

	if matchEntries == nil {
		matchEntries = []types.Value{}
	}

	// Build the result as an array of key-value pairs (Redis map-style response)
	result := types.NewArray([]types.Value{
		types.NewString("matches"),
		types.NewArray(matchEntries),
		types.NewString("len"),
		types.NewInt(int64(len(lcsStr))),
	})
	return result
}

// getStringVal retrieves the string value for a key, returning ("", false) when
// the key does not exist and ("", false) (with wrong type) for non-string types.
// The bool return is true only if the key holds a valid string (including missing key).
func getStringVal(ctx *context.Context, keyBytes []byte) (string, bool) {
	obj, ok := ctx.Engine.GetBytes(keyBytes)
	if !ok {
		return "", true // missing key is treated as empty string for LCS
	}
	if obj.Type != object.TypeString {
		return "", false
	}
	s, _ := obj.Value.(string)
	return s, true
}
