package parser

import (
	"bytes"
	"testing"

	"frostdb/internal/resp/types"
)

func TestParse_SimpleString(t *testing.T) {
	data := []byte("+OK\r\n")
	p := New(bytes.NewReader(data))
	
	val, err := p.Parse()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if val.Type != types.TypeSimpleString {
		t.Errorf("Expected SimpleString, got %v", val.Type)
	}

	if string(val.Str) != "OK" {
		t.Errorf("Expected 'OK', got '%s'", string(val.Str))
	}
}

func TestParse_Array(t *testing.T) {
	// *2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n
	data := []byte("*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n")
	p := New(bytes.NewReader(data))

	val, err := p.Parse()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if val.Type != types.TypeArray {
		t.Fatalf("Expected Array, got %v", val.Type)
	}

	if len(val.Array) != 2 {
		t.Fatalf("Expected array length 2, got %d", len(val.Array))
	}

	if string(val.Array[0].Str) != "LLEN" {
		t.Errorf("Expected 'LLEN', got '%s'", string(val.Array[0].Str))
	}

	if string(val.Array[1].Str) != "mylist" {
		t.Errorf("Expected 'mylist', got '%s'", string(val.Array[1].Str))
	}
}

func TestParse_InlineCommand(t *testing.T) {
	data := []byte("PING\r\n")
	p := New(bytes.NewReader(data))
	
	val, err := p.Parse()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if val.Type != types.TypeArray {
		t.Errorf("Expected inline to parse to Array, got %v", val.Type)
	}
	
	if len(val.Array) != 1 {
		t.Errorf("Expected 1 element, got %d", len(val.Array))
	}

	if string(val.Array[0].Str) != "PING" {
		t.Errorf("Expected 'PING', got '%s'", string(val.Array[0].Str))
	}
}
