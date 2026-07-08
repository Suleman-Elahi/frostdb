package object

import "sync"

// ListNode represents a single node in our doubly-linked list.
type ListNode struct {
	Value []byte
	Prev  *ListNode
	Next  *ListNode
}

// List represents a doubly-linked list optimized for push/pop from both ends.
// All exported methods are safe for concurrent use.
// Read-only traversals (Range, Index, FindPositions, Snapshot) use an RLock
// so they can proceed concurrently with each other.
type List struct {
	mu   sync.RWMutex
	Head *ListNode
	Tail *ListNode
	Len  int
}

var listNodePool = sync.Pool{
	New: func() any { return &ListNode{} },
}

// NewList creates a new empty list.
func NewList() *List {
	return &List{}
}

func (l *List) PushFront(val []byte) {
	node := listNodePool.Get().(*ListNode)
	node.Value = val
	node.Prev = nil
	node.Next = nil

	l.mu.Lock()
	if l.Len == 0 {
		l.Head = node
		l.Tail = node
	} else {
		node.Next = l.Head
		l.Head.Prev = node
		l.Head = node
	}
	l.Len++
	l.mu.Unlock()
}

func (l *List) PushBack(val []byte) {
	node := listNodePool.Get().(*ListNode)
	node.Value = val
	node.Prev = nil
	node.Next = nil

	l.mu.Lock()
	if l.Len == 0 {
		l.Head = node
		l.Tail = node
	} else {
		node.Prev = l.Tail
		l.Tail.Next = node
		l.Tail = node
	}
	l.Len++
	l.mu.Unlock()
}

func (l *List) PopFront() ([]byte, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.Len == 0 {
		return nil, false
	}
	node := l.Head
	if l.Len == 1 {
		l.Head = nil
		l.Tail = nil
	} else {
		l.Head = node.Next
		l.Head.Prev = nil
	}
	l.Len--

	val := node.Value
	node.Value = nil
	node.Prev = nil
	node.Next = nil
	listNodePool.Put(node)

	return val, true
}

func (l *List) PopBack() ([]byte, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.Len == 0 {
		return nil, false
	}
	node := l.Tail
	if l.Len == 1 {
		l.Head = nil
		l.Tail = nil
	} else {
		l.Tail = node.Prev
		l.Tail.Next = nil
	}
	l.Len--

	val := node.Value
	node.Value = nil
	node.Prev = nil
	node.Next = nil
	listNodePool.Put(node)

	return val, true
}

// Remove removes up to count occurrences of val from the list.
// If count > 0, removes elements moving from head to tail.
// If count < 0, removes elements moving from tail to head.
// If count == 0, removes all occurrences.
func (l *List) Remove(val []byte, count int) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.Len == 0 {
		return 0
	}

	removed := 0

	if count >= 0 {
		node := l.Head
		for node != nil {
			next := node.Next
			if bytesEqual(node.Value, val) {
				l.removeNodeLocked(node)
				removed++
				if count > 0 && removed >= count {
					break
				}
			}
			node = next
		}
	} else {
		count = -count
		node := l.Tail
		for node != nil {
			prev := node.Prev
			if bytesEqual(node.Value, val) {
				l.removeNodeLocked(node)
				removed++
				if removed >= count {
					break
				}
			}
			node = prev
		}
	}

	return removed
}

func (l *List) removeNodeLocked(node *ListNode) {
	if node.Prev != nil {
		node.Prev.Next = node.Next
	} else {
		l.Head = node.Next
	}

	if node.Next != nil {
		node.Next.Prev = node.Prev
	} else {
		l.Tail = node.Prev
	}

	l.Len--
	node.Value = nil
	node.Prev = nil
	node.Next = nil
	listNodePool.Put(node)
}

// LLen returns the number of elements in the list.
func (l *List) LLen() int {
	l.mu.RLock()
	n := l.Len
	l.mu.RUnlock()
	return n
}

// Index returns the element at the given index (0-based, negative counts from tail).
// Returns nil, false if out of range.
func (l *List) Index(idx int) ([]byte, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if idx < 0 {
		idx = l.Len + idx
	}
	if idx < 0 || idx >= l.Len {
		return nil, false
	}
	node := l.Head
	for i := 0; i < idx; i++ {
		node = node.Next
	}
	val := make([]byte, len(node.Value))
	copy(val, node.Value)
	return val, true
}

// Set replaces the element at the given index. Returns false if out of range.
func (l *List) Set(idx int, val []byte) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if idx < 0 {
		idx = l.Len + idx
	}
	if idx < 0 || idx >= l.Len {
		return false
	}
	node := l.Head
	for i := 0; i < idx; i++ {
		node = node.Next
	}
	node.Value = val
	return true
}

// Insert inserts element before or after the first occurrence of pivot.
// before=true means BEFORE, before=false means AFTER.
// Returns the new length, or -1 if pivot not found, or -2 if list is empty.
func (l *List) Insert(pivot []byte, element []byte, before bool) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.Len == 0 {
		return -2
	}

	node := l.Head
	for node != nil {
		if bytesEqual(node.Value, pivot) {
			newNode := listNodePool.Get().(*ListNode)
			newNode.Value = element
			newNode.Prev = nil
			newNode.Next = nil

			if before {
				newNode.Next = node
				newNode.Prev = node.Prev
				if node.Prev != nil {
					node.Prev.Next = newNode
				} else {
					l.Head = newNode
				}
				node.Prev = newNode
			} else {
				newNode.Prev = node
				newNode.Next = node.Next
				if node.Next != nil {
					node.Next.Prev = newNode
				} else {
					l.Tail = newNode
				}
				node.Next = newNode
			}
			l.Len++
			return l.Len
		}
		node = node.Next
	}
	return -1 // pivot not found
}

// Range returns elements from start to end (inclusive, 0-based, negative indices supported).
func (l *List) Range(start, end int) [][]byte {
	l.mu.RLock()
	defer l.mu.RUnlock()

	length := l.Len
	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}
	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}
	if start > end || start >= length {
		return nil
	}

	result := make([][]byte, 0, end-start+1)
	node := l.Head
	idx := 0
	for node != nil && idx < start {
		node = node.Next
		idx++
	}
	for node != nil && idx <= end {
		val := make([]byte, len(node.Value))
		copy(val, node.Value)
		result = append(result, val)
		node = node.Next
		idx++
	}
	return result
}

// Trim keeps only elements in the inclusive [start, end] range (Redis LTRIM semantics).
// Negative indexes are resolved from tail. Returns the resulting list length.
func (l *List) Trim(start, end int) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	length := l.Len
	if length == 0 {
		return 0
	}

	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}
	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}

	// Out-of-range selection trims to empty.
	if start > end || start >= length {
		node := l.Head
		for node != nil {
			next := node.Next
			node.Value = nil
			node.Prev = nil
			node.Next = nil
			listNodePool.Put(node)
			node = next
		}
		l.Head = nil
		l.Tail = nil
		l.Len = 0
		return 0
	}

	// Drop [0, start).
	idx := 0
	node := l.Head
	for node != nil && idx < start {
		next := node.Next
		node.Value = nil
		node.Prev = nil
		node.Next = nil
		listNodePool.Put(node)
		l.Len--
		node = next
		idx++
	}
	l.Head = node
	if l.Head != nil {
		l.Head.Prev = nil
	}

	// Remaining kept range length.
	keepLen := end - start + 1

	// Move to last kept node.
	keptTail := l.Head
	for i := 1; i < keepLen && keptTail != nil; i++ {
		keptTail = keptTail.Next
	}

	// Drop tail after keptTail.
	if keptTail != nil {
		node = keptTail.Next
		keptTail.Next = nil
		l.Tail = keptTail
		for node != nil {
			next := node.Next
			node.Value = nil
			node.Prev = nil
			node.Next = nil
			listNodePool.Put(node)
			l.Len--
			node = next
		}
	} else {
		// Defensive: should not happen for valid keepLen, but keep structure consistent.
		l.Head = nil
		l.Tail = nil
		l.Len = 0
	}

	return l.Len
}

// FindPositions finds positions of element in the list.
// rank: 1-based rank of first match to return (negative = search from tail).
// count: max number of matches to return (0 = all).
// maxlen: max elements to scan (0 = no limit).
func (l *List) FindPositions(element string, rank, count, maxlen int) []int {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var matches []int
	wantCount := count
	if wantCount < 0 {
		wantCount = 1
	}
	if wantCount == 0 {
		wantCount = l.Len
	}

	reverse := rank < 0
	if reverse {
		rank = -rank
	}

	elemBytes := []byte(element)
	scanned := 0
	matchNum := 0

	if !reverse {
		idx := 0
		node := l.Head
		for node != nil {
			if maxlen > 0 && scanned >= maxlen {
				break
			}
			if bytesEqual(node.Value, elemBytes) {
				matchNum++
				if matchNum >= rank {
					matches = append(matches, idx)
					if len(matches) >= wantCount {
						break
					}
				}
			}
			scanned++
			idx++
			node = node.Next
		}
	} else {
		idx := l.Len - 1
		node := l.Tail
		for node != nil {
			if maxlen > 0 && scanned >= maxlen {
				break
			}
			if bytesEqual(node.Value, elemBytes) {
				matchNum++
				if matchNum >= rank {
					matches = append(matches, idx)
					if len(matches) >= wantCount {
						break
					}
				}
			}
			scanned++
			idx--
			node = node.Prev
		}
	}

	return matches
}

// Snapshot returns all values as a slice (used for serialization/export).
func (l *List) Snapshot() [][]byte {
	l.mu.RLock()
	defer l.mu.RUnlock()

	result := make([][]byte, 0, l.Len)
	node := l.Head
	for node != nil {
		val := make([]byte, len(node.Value))
		copy(val, node.Value)
		result = append(result, val)
		node = node.Next
	}
	return result
}

// bytesEqual compares two byte slices without allocating a string.
// Inlined by the compiler for small slices.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
