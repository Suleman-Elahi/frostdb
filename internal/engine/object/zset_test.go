package object

import (
	"math"
	"testing"
)

func TestZSet_AddAndScore(t *testing.T) {
	z := NewZSet()

	added := z.Add(1.0, "one")
	if !added {
		t.Error("Expected new element to return true")
	}

	added = z.Add(2.0, "two")
	if !added {
		t.Error("Expected new element to return true")
	}

	// Update existing
	added = z.Add(1.5, "one")
	if added {
		t.Error("Expected update of existing element to return false")
	}

	score, ok := z.Score("one")
	if !ok || score != 1.5 {
		t.Errorf("Expected score 1.5, got %v", score)
	}

	if z.Len() != 2 {
		t.Errorf("Expected length 2, got %d", z.Len())
	}
}

func TestZSet_Remove(t *testing.T) {
	z := NewZSet()
	z.Add(1.0, "one")
	z.Add(2.0, "two")

	removed := z.Remove("one")
	if !removed {
		t.Error("Expected remove to return true")
	}

	removed = z.Remove("nonexistent")
	if removed {
		t.Error("Expected remove of nonexistent to return false")
	}

	if z.Len() != 1 {
		t.Errorf("Expected length 1, got %d", z.Len())
	}
}

func TestZSet_RangeByIndex(t *testing.T) {
	z := NewZSet()
	z.Add(1.0, "one")
	z.Add(2.0, "two")
	z.Add(3.0, "three")

	// Full range
	entries := z.RangeByIndex(0, -1)
	if len(entries) != 3 {
		t.Fatalf("Expected 3 entries, got %d", len(entries))
	}
	if entries[0].Member != "one" || entries[1].Member != "two" || entries[2].Member != "three" {
		t.Errorf("Unexpected order: %v", entries)
	}

	// Partial range
	entries = z.RangeByIndex(1, 2)
	if len(entries) != 2 {
		t.Fatalf("Expected 2 entries, got %d", len(entries))
	}
	if entries[0].Member != "two" || entries[1].Member != "three" {
		t.Errorf("Unexpected entries: %v", entries)
	}

	// Negative index
	entries = z.RangeByIndex(-2, -1)
	if len(entries) != 2 {
		t.Fatalf("Expected 2 entries, got %d", len(entries))
	}
	if entries[0].Member != "two" || entries[1].Member != "three" {
		t.Errorf("Unexpected entries: %v", entries)
	}
}

func TestZSet_RangeByScore(t *testing.T) {
	z := NewZSet()
	z.Add(1.0, "one")
	z.Add(2.0, "two")
	z.Add(3.0, "three")

	// Inclusive range
	entries := z.RangeByScore(1.0, 2.0, false, false)
	if len(entries) != 2 {
		t.Fatalf("Expected 2 entries, got %d", len(entries))
	}

	// Exclusive min
	entries = z.RangeByScore(1.0, 3.0, true, false)
	if len(entries) != 2 {
		t.Fatalf("Expected 2 entries (excl min=1), got %d", len(entries))
	}
	if entries[0].Member != "two" {
		t.Errorf("Expected first entry to be 'two', got %s", entries[0].Member)
	}

	// -inf to +inf
	entries = z.RangeByScore(math.Inf(-1), math.Inf(1), false, false)
	if len(entries) != 3 {
		t.Fatalf("Expected 3 entries for -inf to +inf, got %d", len(entries))
	}
}

func TestZSet_PopMin(t *testing.T) {
	z := NewZSet()
	z.Add(3.0, "three")
	z.Add(1.0, "one")
	z.Add(2.0, "two")

	entries := z.PopMin(2)
	if len(entries) != 2 {
		t.Fatalf("Expected 2 entries, got %d", len(entries))
	}
	if entries[0].Member != "one" || entries[0].Score != 1.0 {
		t.Errorf("Expected first pop to be 'one' with score 1.0, got %v", entries[0])
	}
	if entries[1].Member != "two" || entries[1].Score != 2.0 {
		t.Errorf("Expected second pop to be 'two' with score 2.0, got %v", entries[1])
	}

	if z.Len() != 1 {
		t.Errorf("Expected length 1 after popping 2, got %d", z.Len())
	}
}

func TestZSet_SameScoreLexOrder(t *testing.T) {
	z := NewZSet()
	z.Add(1.0, "b")
	z.Add(1.0, "a")
	z.Add(1.0, "c")

	entries := z.RangeByIndex(0, -1)
	if len(entries) != 3 {
		t.Fatalf("Expected 3 entries, got %d", len(entries))
	}
	if entries[0].Member != "a" || entries[1].Member != "b" || entries[2].Member != "c" {
		t.Errorf("Expected lexicographic order [a, b, c], got %v", entries)
	}
}
