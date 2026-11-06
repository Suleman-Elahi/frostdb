package rdb

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"frostdb/internal/engine/object"
	"frostdb/internal/engine/shard"
)

func newShards() [256]*shard.Shard {
	var shards [256]*shard.Shard
	for i := range shards {
		shards[i] = shard.New()
	}
	return shards
}

func findKey(shards [256]*shard.Shard, key string) *object.Object {
	for i := 0; i < 256; i++ {
		obj, ok := shards[i].Get(key)
		if ok {
			return obj
		}
	}
	return nil
}

func TestExportImportRoundTrip(t *testing.T) {
	shards := newShards()

	shards[0].Set("str-key", &object.Object{Type: object.TypeString, Value: []byte("hello world")})
	shards[0].Set("str-ttl", &object.Object{Type: object.TypeString, Value: []byte("expires"), ExpireAt: 9999999999999})

	list := object.NewList()
	list.PushBack([]byte("a"))
	list.PushBack([]byte("b"))
	list.PushBack([]byte("c"))
	shards[1].Set("my-list", &object.Object{Type: object.TypeList, Value: list})

	set := object.NewSet()
	set.Add("x", "y", "z")
	shards[2].Set("my-set", &object.Object{Type: object.TypeSet, Value: set})

	hash := object.NewHash()
	hash.Set("field1", "val1")
	hash.Set("field2", "val2")
	shards[3].Set("my-hash", &object.Object{Type: object.TypeHash, Value: hash})

	zset := object.NewZSet()
	zset.Add(1.0, "alice")
	zset.Add(2.5, "bob")
	zset.Add(3.7, "charlie")
	shards[4].Set("my-zset", &object.Object{Type: object.TypeZSet, Value: zset})

	dir := t.TempDir()
	rdbPath := filepath.Join(dir, "dump.rdb")

	exp := NewExporter(shards)
	n, err := exp.ExportFile(rdbPath)
	if err != nil {
		t.Fatalf("export failed: %v", err)
	}
	if n != 6 {
		t.Fatalf("expected 6 keys exported, got %d", n)
	}

	info, err := os.Stat(rdbPath)
	if err != nil {
		t.Fatalf("rdb file not found: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("rdb file is empty")
	}

	importShards := newShards()
	imp := NewImporter(importShards)
	imported, err := imp.ImportFile(rdbPath)
	if err != nil {
		t.Fatalf("import failed: %v", err)
	}
	if imported != 6 {
		t.Fatalf("expected 6 keys imported, got %d", imported)
	}

	strObj := findKey(importShards, "str-key")
	if strObj == nil {
		t.Fatal("str-key not found after import")
	}
	if string(strObj.Value.([]byte)) != "hello world" {
		t.Fatalf("str-key value mismatch: %v", strObj.Value)
	}

	ttlObj := findKey(importShards, "str-ttl")
	if ttlObj == nil {
		t.Fatal("str-ttl not found after import")
	}
	if ttlObj.ExpireAt == 0 {
		t.Fatal("str-ttl TTL not preserved")
	}

	listObj := findKey(importShards, "my-list")
	if listObj == nil {
		t.Fatal("my-list not found after import")
	}
	importedList := listObj.Value.(*object.List)
	if importedList.Len != 3 {
		t.Fatalf("list length mismatch: got %d", importedList.Len)
	}

	setObj := findKey(importShards, "my-set")
	if setObj == nil {
		t.Fatal("my-set not found after import")
	}
	importedSet := setObj.Value.(*object.Set)
	if importedSet.Len() != 3 {
		t.Fatalf("set length mismatch: got %d", importedSet.Len())
	}

	hashObj := findKey(importShards, "my-hash")
	if hashObj == nil {
		t.Fatal("my-hash not found after import")
	}
	importedHash := hashObj.Value.(*object.Hash)
	if importedHash.Len() != 2 {
		t.Fatalf("hash length mismatch: got %d", importedHash.Len())
	}
	v, ok := importedHash.Get("field1")
	if !ok || v != "val1" {
		t.Fatalf("hash field1 mismatch: %v", v)
	}

	zsetObj := findKey(importShards, "my-zset")
	if zsetObj == nil {
		t.Fatal("my-zset not found after import")
	}
	importedZSet := zsetObj.Value.(*object.ZSet)
	if importedZSet.Len() != 3 {
		t.Fatalf("zset length mismatch: got %d", importedZSet.Len())
	}
	score, ok := importedZSet.Score("bob")
	if !ok || score != 2.5 {
		t.Fatalf("zset bob score mismatch: %v", score)
	}
}

func TestArrayRDBRoundTrip(t *testing.T) {
	shards := newShards()

	arr := object.NewArray(4096, 10, 5)
	arr.Set(0, "hello")
	arr.Set(1, "world")
	arr.Set(100, "sparse")
	arr.Set(4096, "next-slice")
	arr.Mu.Lock()
	arr.SetInsertIdx(101)
	arr.Mu.Unlock()
	shards[0].Set("myarray", &object.Object{Type: object.TypeArray, Value: arr})

	ringArr := object.NewArray(4096, 10, 5)
	ringArr.Set(0, "ring0")
	ringArr.Set(1, "ring1")
	ringArr.Mu.Lock()
	ringArr.SetRawInsertIdx(2)
	ringArr.SetRawRingSize(10)
	ringArr.Mu.Unlock()
	shards[1].Set("ringarray", &object.Object{Type: object.TypeArray, Value: ringArr})

	dir := t.TempDir()
	rdbPath := filepath.Join(dir, "array-test.rdb")

	exp := NewExporter(shards)
	n, err := exp.ExportFile(rdbPath)
	if err != nil {
		t.Fatalf("export failed: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 keys exported, got %d", n)
	}

	importShards := newShards()
	imp := NewImporter(importShards)
	imported, err := imp.ImportFile(rdbPath)
	if err != nil {
		t.Fatalf("import failed: %v", err)
	}
	if imported != 2 {
		t.Fatalf("expected 2 keys imported, got %d", imported)
	}

	arrObj := findKey(importShards, "myarray")
	if arrObj == nil {
		t.Fatal("myarray not found after import")
	}
	if arrObj.Type != object.TypeArray {
		t.Fatalf("myarray type mismatch: got %v, want TypeArray", arrObj.Type)
	}
	importedArr := arrObj.Value.(*object.Array)

	v, ok := importedArr.Get(0)
	if !ok || v != "hello" {
		t.Fatalf("myarray[0] mismatch: got %q, want 'hello'", v)
	}
	v, ok = importedArr.Get(1)
	if !ok || v != "world" {
		t.Fatalf("myarray[1] mismatch: got %q, want 'world'", v)
	}
	v, ok = importedArr.Get(100)
	if !ok || v != "sparse" {
		t.Fatalf("myarray[100] mismatch: got %q, want 'sparse'", v)
	}
	v, ok = importedArr.Get(4096)
	if !ok || v != "next-slice" {
		t.Fatalf("myarray[4096] mismatch: got %q, want 'next-slice'", v)
	}

	ringObj := findKey(importShards, "ringarray")
	if ringObj == nil {
		t.Fatal("ringarray not found after import")
	}
	importedRing := ringObj.Value.(*object.Array)
	v, ok = importedRing.Get(0)
	if !ok || v != "ring0" {
		t.Fatalf("ringarray[0] mismatch: got %q, want 'ring0'", v)
	}
	v, ok = importedRing.Get(1)
	if !ok || v != "ring1" {
		t.Fatalf("ringarray[1] mismatch: got %q, want 'ring1'", v)
	}

	if importedRing.RingSize() != 10 {
		t.Fatalf("ringarray ring size mismatch: got %d, want 10", importedRing.RingSize())
	}
}

func TestArrayRDBRoundTripDebugReloadStyle(t *testing.T) {
	// Tests that exactly mirror the Tcl test failure scenarios.
	// Each test creates an array, exports to RDB, reloads into fresh shards,
	// and verifies all values are preserved.

	t.Run("basic-strings", func(t *testing.T) {
		shards := newShards()
		arr := object.NewArray(4096, 10, 5)
		arr.Set(0, "hello")
		arr.Set(1, "world")
		arr.Set(100, "test")
		// Simulate arseek myarray 101 + arinsert myarray value:
		// arinsert advances insertIdx then calls Set at that position.
		arr.Mu.Lock()
		arr.SetInsertIdx(101)
		arr.Mu.Unlock()
		arr.Insert("value") // calls Set(101, value) and advances insertIdx to 102
		shards[0].Set("myarray", &object.Object{Type: object.TypeArray, Value: arr})

		dir := t.TempDir()
		rdbPath := filepath.Join(dir, "dump.rdb")
		exp := NewExporter(shards)
		if _, err := exp.ExportFile(rdbPath); err != nil {
			t.Fatalf("export: %v", err)
		}

		importShards := newShards()
		imp := NewImporter(importShards)
		if _, err := imp.ImportFile(rdbPath); err != nil {
			t.Fatalf("import: %v", err)
		}

		obj := findKey(importShards, "myarray")
		if obj == nil {
			t.Fatal("myarray not found")
		}
		importedArr := obj.Value.(*object.Array)

		v, ok := importedArr.Get(0)
		if !ok || v != "hello" {
			t.Fatalf("[0] got %q want 'hello'", v)
		}
		v, ok = importedArr.Get(1)
		if !ok || v != "world" {
			t.Fatalf("[1] got %q want 'world'", v)
		}
		v, ok = importedArr.Get(100)
		if !ok || v != "test" {
			t.Fatalf("[100] got %q want 'test'", v)
		}
		v, ok = importedArr.Get(101)
		if !ok || v != "value" {
			t.Fatalf("[101] got %q want 'value'", v)
		}
		if importedArr.InsertIdx() != 102 {
			t.Fatalf("insertIdx got %d want 102", importedArr.InsertIdx())
		}
	})

	t.Run("negative-integers", func(t *testing.T) {
		shards := newShards()
		arr := object.NewArray(4096, 10, 5)
		arr.Set(0, "-1")
		arr.Set(1, "-100")
		arr.Set(2, "0")
		arr.Set(3, "1")
		shards[0].Set("myarray", &object.Object{Type: object.TypeArray, Value: arr})

		dir := t.TempDir()
		rdbPath := filepath.Join(dir, "dump.rdb")
		exp := NewExporter(shards)
		if _, err := exp.ExportFile(rdbPath); err != nil {
			t.Fatalf("export: %v", err)
		}

		importShards := newShards()
		imp := NewImporter(importShards)
		if _, err := imp.ImportFile(rdbPath); err != nil {
			t.Fatalf("import: %v", err)
		}

		obj := findKey(importShards, "myarray")
		if obj == nil {
			t.Fatal("myarray not found")
		}
		importedArr := obj.Value.(*object.Array)

		v, ok := importedArr.Get(0)
		if !ok || v != "-1" {
			t.Fatalf("[0] got %q want '-1'", v)
		}
		v, ok = importedArr.Get(1)
		if !ok || v != "-100" {
			t.Fatalf("[1] got %q want '-100'", v)
		}
		v, ok = importedArr.Get(2)
		if !ok || v != "0" {
			t.Fatalf("[2] got %q want '0'", v)
		}
		v, ok = importedArr.Get(3)
		if !ok || v != "1" {
			t.Fatalf("[3] got %q want '1'", v)
		}
	})

	t.Run("numeric-strings", func(t *testing.T) {
		shards := newShards()
		arr := object.NewArray(4096, 10, 5)
		values := map[int64]string{
			0: "3.141592653589793",
			1: "-2.718281828459045",
			2: "1.0e-10",
			3: "1.0e+100",
		}
		for idx, val := range values {
			arr.Set(idx, val)
		}
		shards[0].Set("myarray", &object.Object{Type: object.TypeArray, Value: arr})

		dir := t.TempDir()
		rdbPath := filepath.Join(dir, "dump.rdb")
		exp := NewExporter(shards)
		if _, err := exp.ExportFile(rdbPath); err != nil {
			t.Fatalf("export: %v", err)
		}

		importShards := newShards()
		imp := NewImporter(importShards)
		if _, err := imp.ImportFile(rdbPath); err != nil {
			t.Fatalf("import: %v", err)
		}

		obj := findKey(importShards, "myarray")
		if obj == nil {
			t.Fatal("myarray not found")
		}
		importedArr := obj.Value.(*object.Array)

		for idx, expected := range values {
			v, ok := importedArr.Get(idx)
			if !ok || v != expected {
				t.Fatalf("[%d] got %q want %q", idx, v, expected)
			}
		}
	})

	t.Run("whole-number-floats", func(t *testing.T) {
		shards := newShards()
		arr := object.NewArray(4096, 10, 5)
		arr.Set(0, "1.0")
		arr.Set(1, "-1.0")
		arr.Set(2, "0.0")
		arr.Set(3, "42.0")
		arr.Set(4, "-42.0")
		arr.Set(5, "1000000.0")
		arr.Set(6, "-9999999.0")
		shards[0].Set("myarray", &object.Object{Type: object.TypeArray, Value: arr})

		dir := t.TempDir()
		rdbPath := filepath.Join(dir, "dump.rdb")
		exp := NewExporter(shards)
		if _, err := exp.ExportFile(rdbPath); err != nil {
			t.Fatalf("export: %v", err)
		}

		importShards := newShards()
		imp := NewImporter(importShards)
		if _, err := imp.ImportFile(rdbPath); err != nil {
			t.Fatalf("import: %v", err)
		}

		obj := findKey(importShards, "myarray")
		if obj == nil {
			t.Fatal("myarray not found")
		}
		importedArr := obj.Value.(*object.Array)

		expected := map[int64]string{
			0: "1.0", 1: "-1.0", 2: "0.0", 3: "42.0",
			4: "-42.0", 5: "1000000.0", 6: "-9999999.0",
		}
		for idx, exp := range expected {
			v, ok := importedArr.Get(idx)
			if !ok || v != exp {
				t.Fatalf("[%d] got %q want %q", idx, v, exp)
			}
		}
	})

	t.Run("integers", func(t *testing.T) {
		shards := newShards()
		arr := object.NewArray(4096, 10, 5)
		arr.Set(0, "1")
		arr.Set(1, "-1")
		arr.Set(2, "0")
		arr.Set(3, "42")
		arr.Set(4, "9999999")
		shards[0].Set("myarray", &object.Object{Type: object.TypeArray, Value: arr})

		dir := t.TempDir()
		rdbPath := filepath.Join(dir, "dump.rdb")
		exp := NewExporter(shards)
		if _, err := exp.ExportFile(rdbPath); err != nil {
			t.Fatalf("export: %v", err)
		}

		importShards := newShards()
		imp := NewImporter(importShards)
		if _, err := imp.ImportFile(rdbPath); err != nil {
			t.Fatalf("import: %v", err)
		}

		obj := findKey(importShards, "myarray")
		if obj == nil {
			t.Fatal("myarray not found")
		}
		importedArr := obj.Value.(*object.Array)

		expected := map[int64]string{0: "1", 1: "-1", 2: "0", 3: "42", 4: "9999999"}
		for idx, exp := range expected {
			v, ok := importedArr.Get(idx)
			if !ok || v != exp {
				t.Fatalf("[%d] got %q want %q", idx, v, exp)
			}
		}
	})

	t.Run("ring-buffer", func(t *testing.T) {
		shards := newShards()
		arr := object.NewArray(4096, 10, 5)
		arr.SetRingSize(5)
		// Simulate ring buffer: ARRING myarray 5 for 15 values
		for i := 0; i < 15; i++ {
			arr.RingInsert(5, fmt.Sprintf("val%d", i))
		}
		shards[0].Set("myarray", &object.Object{Type: object.TypeArray, Value: arr})

		dir := t.TempDir()
		rdbPath := filepath.Join(dir, "dump.rdb")
		exp := NewExporter(shards)
		if _, err := exp.ExportFile(rdbPath); err != nil {
			t.Fatalf("export: %v", err)
		}

		importShards := newShards()
		imp := NewImporter(importShards)
		if _, err := imp.ImportFile(rdbPath); err != nil {
			t.Fatalf("import: %v", err)
		}

		obj := findKey(importShards, "myarray")
		if obj == nil {
			t.Fatal("myarray not found")
		}
		importedArr := obj.Value.(*object.Array)

		if importedArr.RingSize() != 5 {
			t.Fatalf("ring size: got %d want 5", importedArr.RingSize())
		}
		// After 15 RingInsert operations with MOD 5:
		// Positions: 0:val10, 1:val11, 2:val12, 3:val13, 4:val14
		v, ok := importedArr.Get(0)
		if !ok || v != "val10" {
			t.Fatalf("[0] got %q want 'val10'", v)
		}
		v, ok = importedArr.Get(1)
		if !ok || v != "val11" {
			t.Fatalf("[1] got %q want 'val11'", v)
		}
	})

	t.Run("count-preserved", func(t *testing.T) {
		shards := newShards()
		arr := object.NewArray(4096, 10, 5)
		// Create many elements to ensure they count properly
		for i := int64(0); i < 100; i++ {
			arr.Set(i*10, fmt.Sprintf("val%d", i))
		}
		origCount := arr.Count()
		shards[0].Set("myarray", &object.Object{Type: object.TypeArray, Value: arr})

		dir := t.TempDir()
		rdbPath := filepath.Join(dir, "dump.rdb")
		exp := NewExporter(shards)
		if _, err := exp.ExportFile(rdbPath); err != nil {
			t.Fatalf("export: %v", err)
		}

		importShards := newShards()
		imp := NewImporter(importShards)
		if _, err := imp.ImportFile(rdbPath); err != nil {
			t.Fatalf("import: %v", err)
		}

		obj := findKey(importShards, "myarray")
		if obj == nil {
			t.Fatal("myarray not found")
		}
		importedArr := obj.Value.(*object.Array)

		if importedArr.Count() != origCount {
			t.Fatalf("count: got %d want %d", importedArr.Count(), origCount)
		}
	})
}
