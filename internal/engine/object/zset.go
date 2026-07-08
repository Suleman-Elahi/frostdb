package object

import (
	"errors"
	"math"
	"math/rand"
	"sync"
)

const skiplistMaxLevel = 32
const skiplistP = 0.25

// ZSet is a sorted set backed by a skiplist + dict for O(1) member lookup
// and O(log N) score-ordered operations.
type ZSet struct {
	Dict     map[string]float64
	Skiplist *Skiplist

	// scanSnap caches a sorted (hash, member) snapshot for ZSCAN cursor
	// iteration. It is built lazily and invalidated (via scanSnapVer) whenever
	// the set's membership changes.
	scanSnap    []scanEntry
	scanSnapVer uint64
	snapVer     uint64
}

func NewZSet() *ZSet {
	return &ZSet{
		Dict:     make(map[string]float64),
		Skiplist: NewSkiplist(),
	}
}

func (z *ZSet) Add(score float64, member string) bool {
	oldScore, exists := z.Dict[member]
	if exists {
		if oldScore == score {
			return false // no change
		}
		// Remove old entry from skiplist before re-inserting
		z.Skiplist.Delete(oldScore, member)
	}
	z.Dict[member] = score
	z.Skiplist.Insert(score, member)
	if !exists {
		z.scanSnapVer++ // membership changed; invalidate scan snapshot
	}
	return !exists // true if new element
}

func (z *ZSet) Remove(member string) bool {
	score, exists := z.Dict[member]
	if !exists {
		return false
	}
	delete(z.Dict, member)
	z.Skiplist.Delete(score, member)
	z.scanSnapVer++ // membership changed; invalidate scan snapshot
	return true
}

func (z *ZSet) Score(member string) (float64, bool) {
	score, ok := z.Dict[member]
	return score, ok
}

func (z *ZSet) Len() int {
	return len(z.Dict)
}

// Members returns all members in score order.
func (z *ZSet) Members() []string {
	result := make([]string, 0, len(z.Dict))
	node := z.Skiplist.Head.Level[0].Forward
	for node != nil {
		result = append(result, node.Member)
		node = node.Level[0].Forward
	}
	return result
}

// Scan iterates members using a value-based (hash) cursor over a cached sorted
// snapshot, returning up to count members whose hash exceeds the cursor and the
// next cursor (0 when complete). This preserves the SCAN guarantee under
// concurrent modification while keeping a full scan at O(n log n) total.
func (z *ZSet) Scan(cursor uint64, count int) ([]string, uint64) {
	if z.scanSnap == nil || z.snapVer != z.scanSnapVer {
		members := make([]string, 0, len(z.Dict))
		for m := range z.Dict {
			members = append(members, m)
		}
		z.scanSnap = buildScanSnapshot(members)
		z.snapVer = z.scanSnapVer
	}
	return scanFromSnapshot(z.scanSnap, cursor, count)
}

// PopMin removes and returns up to count elements with the lowest scores.
func (z *ZSet) PopMin(count int) []ZSetEntry {
	result := make([]ZSetEntry, 0, count)
	for i := 0; i < count; i++ {
		node := z.Skiplist.Head.Level[0].Forward
		if node == nil {
			break
		}
		mem := node.Member
		sc := node.Score
		result = append(result, ZSetEntry{Member: mem, Score: sc})
		z.Skiplist.Delete(sc, mem)
		delete(z.Dict, mem)
	}
	if len(result) > 0 {
		z.scanSnapVer++
	}
	return result
}

func (z *ZSet) PopMax(count int) []ZSetEntry {
	result := make([]ZSetEntry, 0, count)
	for i := 0; i < count; i++ {
		node := z.Skiplist.Head
		for node.Level[0].Forward != nil {
			node = node.Level[0].Forward
		}
		if node == z.Skiplist.Head {
			break
		}
		mem := node.Member
		sc := node.Score
		result = append(result, ZSetEntry{Member: mem, Score: sc})
		z.Skiplist.Delete(sc, mem)
		delete(z.Dict, mem)
	}
	if len(result) > 0 {
		z.scanSnapVer++
	}
	return result
}

// ScanEntries calls fn for each entry in score order. If fn returns false,
// iteration stops early. Returns the number of entries visited.
func (z *ZSet) ScanEntries(fn func(member string, score float64) bool) int {
	count := 0
	node := z.Skiplist.Head.Level[0].Forward
	for node != nil {
		count++
		if !fn(node.Member, node.Score) {
			return count
		}
		node = node.Level[0].Forward
	}
	return count
}

// RangeByIndex returns elements by rank [start, stop] (0-based, inclusive).
// Supports negative indexes.
func (z *ZSet) RangeByIndex(start, stop int) []ZSetEntry {
	length := z.Len()
	if length == 0 {
		return nil
	}

	// Normalize negative indexes
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop {
		return nil
	}

	result := make([]ZSetEntry, 0, stop-start+1)
	node := z.Skiplist.Head
	// Skip to the start-th element (1-indexed in skiplist traversal)
	for i := 0; i <= start; i++ {
		node = node.Level[0].Forward
		if node == nil {
			return result
		}
	}

	for i := start; i <= stop; i++ {
		if node == nil {
			break
		}
		result = append(result, ZSetEntry{Member: node.Member, Score: node.Score})
		node = node.Level[0].Forward
	}

	return result
}

// RangeByScore returns elements with scores in [min, max].
// minExcl/maxExcl control whether boundaries are exclusive.
func (z *ZSet) RangeByScore(min, max float64, minExcl, maxExcl bool) []ZSetEntry {
	var result []ZSetEntry
	node := z.Skiplist.Head.Level[0].Forward

	for node != nil {
		if maxExcl && node.Score >= max {
			break
		}
		if !maxExcl && node.Score > max {
			break
		}

		include := true
		if minExcl && node.Score <= min {
			include = false
		}
		if !minExcl && node.Score < min {
			include = false
		}

		if include {
			result = append(result, ZSetEntry{Member: node.Member, Score: node.Score})
		}
		node = node.Level[0].Forward
	}

	return result
}

// RemRangeByScore removes elements with scores in [min, max] and returns the count.
func (z *ZSet) RemRangeByScore(min, max float64, minExcl, maxExcl bool) int {
	removed := 0
	
	// We could optimize this by finding the first node, but a linear scan of valid nodes is fine for now
	node := z.Skiplist.Head.Level[0].Forward
	
	var toRemove []ZSetEntry

	for node != nil {
		if maxExcl && node.Score >= max {
			break
		}
		if !maxExcl && node.Score > max {
			break
		}

		include := true
		if minExcl && node.Score <= min {
			include = false
		}
		if !minExcl && node.Score < min {
			include = false
		}

		if include {
			toRemove = append(toRemove, ZSetEntry{Member: node.Member, Score: node.Score})
		}
		node = node.Level[0].Forward
	}

	for _, entry := range toRemove {
		z.Remove(entry.Member)
		removed++
	}

	return removed
}

func (z *ZSet) RemRangeByRank(start, stop int) int {
	entries := z.RangeByIndex(start, stop)
	for _, e := range entries {
		z.Remove(e.Member)
	}
	return len(entries)
}

func ParseLexBound(s string) (member string, exclusive bool, err error) {
	if len(s) == 0 {
		return "", false, errors.New("not valid")
	}
	if s == "+" || s == "-" {
		return s, false, nil
	}
	if s[0] == '(' {
		return s[1:], true, nil
	}
	if s[0] == '[' {
		return s[1:], false, nil
	}
	return "", false, errors.New("not valid")
}

func (z *ZSet) RangeByLex(min, max string, minExcl, maxExcl bool) []ZSetEntry {
	var result []ZSetEntry
	node := z.Skiplist.Head.Level[0].Forward

	for node != nil {
		member := node.Member

		if max == "+" {
			// no upper bound — keep going
		} else if max == "-" {
			break // no element can be <= -inf
		} else {
			if maxExcl && member >= max {
				break
			}
			if !maxExcl && member > max {
				break
			}
		}

		include := true
		if min == "+" {
			include = false // no element can be >= +inf
		} else if min == "-" {
			include = true // all elements are >= -inf
		} else {
			if minExcl && member <= min {
				include = false
			}
			if !minExcl && member < min {
				include = false
			}
		}

		if include {
			result = append(result, ZSetEntry{Member: member, Score: node.Score})
		}
		node = node.Level[0].Forward
	}

	return result
}

func (z *ZSet) RemRangeByLex(min, max string, minExcl, maxExcl bool) int {
	var toRemove []string
	node := z.Skiplist.Head.Level[0].Forward

	for node != nil {
		member := node.Member

		if max == "+" {
			// no upper bound — keep going
		} else if max == "-" {
			break // no element can be <= -inf
		} else {
			if maxExcl && member >= max {
				break
			}
			if !maxExcl && member > max {
				break
			}
		}

		include := true
		if min == "+" {
			include = false
		} else if min == "-" {
			include = true
		} else {
			if minExcl && member <= min {
				include = false
			}
			if !minExcl && member < min {
				include = false
			}
		}

		if include {
			toRemove = append(toRemove, member)
		}
		node = node.Level[0].Forward
	}

	for _, m := range toRemove {
		z.Remove(m)
	}
	return len(toRemove)
}

type ZSetEntry struct {
	Member string
	Score  float64
}

// --- Skiplist Implementation ---

type SkiplistLevel struct {
	Forward *SkiplistNode
	Span    int
}

type SkiplistNode struct {
	Member string
	Score  float64
	Level  []SkiplistLevel
}

type Skiplist struct {
	Head   *SkiplistNode
	Length int
	Level  int
	update [skiplistMaxLevel]*SkiplistNode
}

var skiplistNodePool = sync.Pool{
	New: func() any {
		return &SkiplistNode{
			Level: make([]SkiplistLevel, skiplistMaxLevel),
		}
	},
}

func NewSkiplist() *Skiplist {
	head := skiplistNodePool.Get().(*SkiplistNode)
	return &Skiplist{
		Head:  head,
		Level: 1,
	}
}

func randomLevel() int {
	level := 1
	for level < skiplistMaxLevel && rand.Float64() < skiplistP {
		level++
	}
	return level
}

func (sl *Skiplist) Insert(score float64, member string) {
	update := &sl.update
	node := sl.Head

	for i := sl.Level - 1; i >= 0; i-- {
		for node.Level[i].Forward != nil &&
			(node.Level[i].Forward.Score < score ||
				(node.Level[i].Forward.Score == score && node.Level[i].Forward.Member < member)) {
			node = node.Level[i].Forward
		}
		update[i] = node
	}

	level := randomLevel()
	if level > sl.Level {
		for i := sl.Level; i < level; i++ {
			update[i] = sl.Head
		}
		sl.Level = level
	}

	newNode := skiplistNodePool.Get().(*SkiplistNode)
	newNode.Member = member
	newNode.Score = score

	for i := 0; i < level; i++ {
		newNode.Level[i].Forward = update[i].Level[i].Forward
		update[i].Level[i].Forward = newNode
	}

	sl.Length++
}

func (sl *Skiplist) Delete(score float64, member string) bool {
	update := &sl.update
	node := sl.Head

	for i := sl.Level - 1; i >= 0; i-- {
		for node.Level[i].Forward != nil &&
			(node.Level[i].Forward.Score < score ||
				(node.Level[i].Forward.Score == score && node.Level[i].Forward.Member < member)) {
			node = node.Level[i].Forward
		}
		update[i] = node
	}

	target := node.Level[0].Forward
	if target == nil || target.Score != score || target.Member != member {
		return false
	}

	for i := 0; i < sl.Level; i++ {
		if update[i].Level[i].Forward == target {
			update[i].Level[i].Forward = target.Level[i].Forward
		}
	}

	for sl.Level > 1 && sl.Head.Level[sl.Level-1].Forward == nil {
		sl.Level--
	}

	sl.Length--
	
	target.Member = ""
	for i := range target.Level {
		target.Level[i].Forward = nil
	}
	skiplistNodePool.Put(target)
	
	return true
}

// Ensure math is used
var _ = math.Inf
