package object

import (
	"bytes"
	"testing"
)

func TestList_PushPop(t *testing.T) {
	l := NewList()

	// PushFront 1, 2
	l.PushFront([]byte("1"))
	l.PushFront([]byte("2")) // [2, 1]

	// PushBack 3
	l.PushBack([]byte("3")) // [2, 1, 3]

	if l.Len != 3 {
		t.Errorf("Expected length 3, got %d", l.Len)
	}

	// PopFront
	val, ok := l.PopFront()
	if !ok || !bytes.Equal(val, []byte("2")) {
		t.Errorf("Expected 2, got %s", val)
	}

	// PopBack
	val, ok = l.PopBack()
	if !ok || !bytes.Equal(val, []byte("3")) {
		t.Errorf("Expected 3, got %s", val)
	}

	// PopBack
	val, ok = l.PopBack()
	if !ok || !bytes.Equal(val, []byte("1")) {
		t.Errorf("Expected 1, got %s", val)
	}

	// Pop empty
	_, ok = l.PopFront()
	if ok {
		t.Errorf("Expected false popping empty list")
	}
}
