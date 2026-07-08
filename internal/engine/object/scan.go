package object

import "sort"

// This file implements the shared cursor-scanning machinery used by SSCAN,
// HSCAN and ZSCAN.
//
// Design goals:
//  1. A full scan of N elements must be O(N log N) total, not O(N^2 log N).
//     We achieve this by caching a sorted snapshot and only rebuilding it when
//     the collection is mutated (tracked via a version counter).
//  2. The SCAN guarantee (Redis issue #4906): any element that is present for
//     the entire duration of a scan must be returned at least once, even if
//     the collection is resized/rehashed mid-scan. We achieve this with a
//     value-based cursor (the element's hash) rather than a positional index.
//     Because the snapshot is always sorted by (hash, member), an element that
//     survives a rebuild keeps its position relative to the cursor and can
//     never be skipped.

// scanEntry is a member paired with its hash, sorted by (hash, member).
type scanEntry struct {
	hash   uint64
	member string
}

// scanFNV computes a non-zero 63-bit FNV-1a hash. Zero is reserved as the
// "start / done" cursor sentinel, so we bump a computed 0 to 1.
func scanFNV(s string) uint64 {
	var h uint64 = 14695981039346656037 // FNV offset basis
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211 // FNV prime
	}
	h &= 0x7FFFFFFFFFFFFFFF // positive 63-bit
	if h == 0 {
		h = 1
	}
	return h
}

// buildScanSnapshot creates a snapshot sorted by (hash, member). Sorting by
// member as a tiebreaker makes the ordering fully deterministic even when two
// members hash to the same value.
func buildScanSnapshot(members []string) []scanEntry {
	snap := make([]scanEntry, len(members))
	for i, m := range members {
		snap[i] = scanEntry{hash: scanFNV(m), member: m}
	}
	sort.Slice(snap, func(i, j int) bool {
		if snap[i].hash != snap[j].hash {
			return snap[i].hash < snap[j].hash
		}
		return snap[i].member < snap[j].member
	})
	return snap
}

// scanFromSnapshot returns up to count members starting after the given cursor,
// along with the next cursor (0 when iteration is complete).
//
// A cursor is the hash of the last element returned by the previous call. To
// avoid splitting a group of same-hash elements across calls (which could skip
// members), a batch is always extended to include every element sharing the
// hash of the batch's last element. The next cursor is therefore always a
// clean group boundary, and the following call resumes at hash > cursor.
func scanFromSnapshot(snap []scanEntry, cursor uint64, count int) ([]string, uint64) {
	total := len(snap)
	if total == 0 {
		return nil, 0
	}
	if count < 1 {
		count = 1
	}

	// Find the first entry with hash strictly greater than the cursor.
	// For cursor 0 (start) this is index 0 since all hashes are >= 1.
	start := sort.Search(total, func(i int) bool {
		return snap[i].hash > cursor
	})
	if start >= total {
		return nil, 0
	}

	end := start + count
	if end > total {
		end = total
	}
	// Extend to the end of the current hash group so we never split same-hash
	// members across two calls.
	for end < total && snap[end].hash == snap[end-1].hash {
		end++
	}

	members := make([]string, 0, end-start)
	for i := start; i < end; i++ {
		members = append(members, snap[i].member)
	}

	var next uint64
	if end < total {
		next = snap[end-1].hash
	} else {
		next = 0 // reached the end
	}
	return members, next
}
