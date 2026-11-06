package object

import "sync"

type Set struct {
	mu       sync.RWMutex
	Elements map[string]struct{}

	// scanSnap is a cached (hash, member) snapshot sorted for SSCAN cursor
	// iteration. It is lazily built and invalidated (via scanSnapVer) when the
	// set is mutated.
	scanSnap    []scanEntry
	scanSnapVer uint64 // version counter; incremented on mutation
	snapVer      uint64 // version when scanSnap was built
	RehashSteps  int    // mock rehashing steps
	RehashedSize int    // size at which rehashing mock was last triggered
}

func NewSet() *Set {
	return &Set{
		Elements: make(map[string]struct{}),
	}
}

func (s *Set) Add(members ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	added := 0
	for _, member := range members {
		if _, exists := s.Elements[member]; !exists {
			s.Elements[member] = struct{}{}
			added++
		}
	}
	if added > 0 {
		s.scanSnapVer++ // invalidate scan snapshot
	}
	return added
}

func (s *Set) Remove(members ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	removed := 0
	for _, member := range members {
		if _, exists := s.Elements[member]; exists {
			delete(s.Elements, member)
			removed++
		}
	}
	if removed > 0 {
		s.scanSnapVer++ // invalidate scan snapshot
	}
	return removed
}

func (s *Set) Members() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	members := make([]string, 0, len(s.Elements))
	for member := range s.Elements {
		members = append(members, member)
	}
	return members
}

func (s *Set) Contains(member string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, exists := s.Elements[member]
	return exists
}

func (s *Set) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.Elements)
}

// Scan iterates the set in chunks, returning up to count members whose hash is
// greater than the given cursor. Returns the members and the next cursor (0
// when iteration is complete).
//
// The cursor is the hash of the last element returned by the previous call
// (0 to start). Using a value-based cursor over a sorted snapshot preserves
// the Redis SCAN guarantee: elements present for the whole scan are always
// returned even if the set is resized mid-scan (see issue #4906). The sorted
// snapshot is cached and only rebuilt when the set is mutated.
func (s *Set) Scan(cursor uint64, count int) ([]string, uint64) {
	s.mu.Lock()
	if s.scanSnap == nil || s.snapVer != s.scanSnapVer {
		members := make([]string, 0, len(s.Elements))
		for m := range s.Elements {
			members = append(members, m)
		}
		s.scanSnap = buildScanSnapshot(members)
		s.snapVer = s.scanSnapVer
	}
	snap := s.scanSnap
	s.mu.Unlock()

	return scanFromSnapshot(snap, cursor, count)
}
