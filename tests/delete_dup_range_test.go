package tests

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/JkLondon/gdbx"
)

// helper: create env + open DupSort table + insert keys with values
func setupDupRangeDB(t *testing.T, numKeys int, valsPerKey int) (*gdbx.Env, gdbx.DBI) {
	t.Helper()
	path := t.TempDir() + "/dup_range.db"

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	env.SetMaxDBs(10)
	env.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync|gdbx.WriteMap, 0644); err != nil {
		t.Fatal(err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	dbi, err := txn.OpenDBISimple("duptest", gdbx.Create|gdbx.DupSort)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	key := make([]byte, 8)
	val := make([]byte, 8)
	for k := 0; k < numKeys; k++ {
		binary.BigEndian.PutUint64(key, uint64(k))
		for v := 0; v < valsPerKey; v++ {
			binary.BigEndian.PutUint64(val, uint64(v))
			if err := txn.Put(dbi, key, val, 0); err != nil {
				txn.Abort()
				t.Fatal(err)
			}
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	return env, dbi
}

// helper: count all values for a key via cursor
func countValuesForKey(t *testing.T, env *gdbx.Env, dbi gdbx.DBI, keyVal uint64) int {
	t.Helper()
	txn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Abort()

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()

	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, keyVal)

	count := 0
	k, _, err := cursor.Get(key, nil, gdbx.Set)
	if err != nil || k == nil {
		return 0
	}
	// Count: first + all NextDup
	count = 1
	for {
		_, _, err := cursor.Get(nil, nil, gdbx.NextDup)
		if err != nil {
			break
		}
		count++
	}
	return count
}

// helper: collect all values for a key
func collectValuesForKey(t *testing.T, env *gdbx.Env, dbi gdbx.DBI, keyVal uint64) []uint64 {
	t.Helper()
	txn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Abort()

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()

	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, keyVal)

	var result []uint64
	k, v, err := cursor.Get(key, nil, gdbx.Set)
	if err != nil || k == nil {
		return nil
	}
	result = append(result, binary.BigEndian.Uint64(v))
	for {
		_, v, err := cursor.Get(nil, nil, gdbx.NextDup)
		if err != nil {
			break
		}
		result = append(result, binary.BigEndian.Uint64(v))
	}
	return result
}

// helper: count total entries in the database
func countTotalEntries(t *testing.T, env *gdbx.Env, dbi gdbx.DBI) int {
	t.Helper()
	txn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Abort()

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()

	count := 0
	for k, _, err := cursor.Get(nil, nil, gdbx.First); k != nil && err == nil; k, _, err = cursor.Get(nil, nil, gdbx.Next) {
		count++
	}
	return count
}

// ==================== Inline sub-page tests (few values per key) ====================

func TestDeleteDupRangeBasic(t *testing.T) {
	env, dbi := setupDupRangeDB(t, 5, 10)
	defer env.Close()

	// Delete values [3, 7) for key 2
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	key := make([]byte, 8)
	from := make([]byte, 8)
	to := make([]byte, 8)
	binary.BigEndian.PutUint64(key, 2)
	binary.BigEndian.PutUint64(from, 3)
	binary.BigEndian.PutUint64(to, 7)

	deleted, err := txn.DeleteDupRange(dbi, key, from, to)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	if deleted != 4 {
		txn.Abort()
		t.Fatalf("expected 4 deleted, got %d", deleted)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Verify: key 2 should have values 0,1,2,7,8,9
	vals := collectValuesForKey(t, env, dbi, 2)
	expected := []uint64{0, 1, 2, 7, 8, 9}
	if len(vals) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, vals)
	}
	for i, v := range vals {
		if v != expected[i] {
			t.Fatalf("value %d: expected %d, got %d", i, expected[i], v)
		}
	}

	// Other keys should be untouched
	if c := countValuesForKey(t, env, dbi, 0); c != 10 {
		t.Fatalf("key 0 should have 10 values, got %d", c)
	}
	if c := countValuesForKey(t, env, dbi, 4); c != 10 {
		t.Fatalf("key 4 should have 10 values, got %d", c)
	}
}

func TestDeleteDupRangeAllValues(t *testing.T) {
	env, dbi := setupDupRangeDB(t, 3, 5)
	defer env.Close()

	// Delete ALL values for key 1 (from=nil, to=nil)
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, 1)

	deleted, err := txn.DeleteDupRange(dbi, key, nil, nil)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	if deleted != 5 {
		txn.Abort()
		t.Fatalf("expected 5 deleted, got %d", deleted)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Key 1 should not exist
	if c := countValuesForKey(t, env, dbi, 1); c != 0 {
		t.Fatalf("key 1 should have 0 values, got %d", c)
	}

	// Other keys should be untouched
	if c := countValuesForKey(t, env, dbi, 0); c != 5 {
		t.Fatalf("key 0 should have 5 values, got %d", c)
	}
	if c := countValuesForKey(t, env, dbi, 2); c != 5 {
		t.Fatalf("key 2 should have 5 values, got %d", c)
	}
}

func TestDeleteDupRangeFromNil(t *testing.T) {
	env, dbi := setupDupRangeDB(t, 3, 8)
	defer env.Close()

	// Delete values [nil, 4) for key 0 → deletes values 0,1,2,3
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	key := make([]byte, 8)
	to := make([]byte, 8)
	binary.BigEndian.PutUint64(key, 0)
	binary.BigEndian.PutUint64(to, 4)

	deleted, err := txn.DeleteDupRange(dbi, key, nil, to)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	if deleted != 4 {
		txn.Abort()
		t.Fatalf("expected 4 deleted, got %d", deleted)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	vals := collectValuesForKey(t, env, dbi, 0)
	expected := []uint64{4, 5, 6, 7}
	if len(vals) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, vals)
	}
	for i, v := range vals {
		if v != expected[i] {
			t.Fatalf("value %d: expected %d, got %d", i, expected[i], v)
		}
	}
}

func TestDeleteDupRangeToNil(t *testing.T) {
	env, dbi := setupDupRangeDB(t, 3, 8)
	defer env.Close()

	// Delete values [5, nil) for key 2 → deletes values 5,6,7
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	key := make([]byte, 8)
	from := make([]byte, 8)
	binary.BigEndian.PutUint64(key, 2)
	binary.BigEndian.PutUint64(from, 5)

	deleted, err := txn.DeleteDupRange(dbi, key, from, nil)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	if deleted != 3 {
		txn.Abort()
		t.Fatalf("expected 3 deleted, got %d", deleted)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	vals := collectValuesForKey(t, env, dbi, 2)
	expected := []uint64{0, 1, 2, 3, 4}
	if len(vals) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, vals)
	}
}

func TestDeleteDupRangeNoMatch(t *testing.T) {
	env, dbi := setupDupRangeDB(t, 3, 5)
	defer env.Close()

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	key := make([]byte, 8)
	from := make([]byte, 8)
	to := make([]byte, 8)
	binary.BigEndian.PutUint64(key, 1)
	binary.BigEndian.PutUint64(from, 100)
	binary.BigEndian.PutUint64(to, 200)

	deleted, err := txn.DeleteDupRange(dbi, key, from, to)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	if deleted != 0 {
		txn.Abort()
		t.Fatalf("expected 0 deleted, got %d", deleted)
	}

	txn.Abort()
}

func TestDeleteDupRangeKeyNotFound(t *testing.T) {
	env, dbi := setupDupRangeDB(t, 3, 5)
	defer env.Close()

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	key := make([]byte, 8)
	from := make([]byte, 8)
	to := make([]byte, 8)
	binary.BigEndian.PutUint64(key, 999) // non-existent key
	binary.BigEndian.PutUint64(from, 0)
	binary.BigEndian.PutUint64(to, 5)

	deleted, err := txn.DeleteDupRange(dbi, key, from, to)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	if deleted != 0 {
		t.Fatalf("expected 0 deleted for non-existent key, got %d", deleted)
	}

	txn.Abort()
}

func TestDeleteDupRangeLeaveOneValue(t *testing.T) {
	env, dbi := setupDupRangeDB(t, 1, 5)
	defer env.Close()

	// Delete values [0, 4) for key 0 → leaves only value 4
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	key := make([]byte, 8)
	from := make([]byte, 8)
	to := make([]byte, 8)
	binary.BigEndian.PutUint64(key, 0)
	binary.BigEndian.PutUint64(from, 0)
	binary.BigEndian.PutUint64(to, 4)

	deleted, err := txn.DeleteDupRange(dbi, key, from, to)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	if deleted != 4 {
		txn.Abort()
		t.Fatalf("expected 4 deleted, got %d", deleted)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Should have exactly 1 value left
	vals := collectValuesForKey(t, env, dbi, 0)
	if len(vals) != 1 || vals[0] != 4 {
		t.Fatalf("expected [4], got %v", vals)
	}
}

func TestDeleteDupRangeNonDupSortError(t *testing.T) {
	path := t.TempDir() + "/nondups.db"
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	dbi, err := txn.OpenDBISimple("plain", gdbx.Create)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	key := []byte("key")
	_, err = txn.DeleteDupRange(dbi, key, nil, nil)
	if err == nil {
		txn.Abort()
		t.Fatal("expected error for non-DupSort table")
	}
	txn.Abort()
}

func TestDeleteDupRangeViaCursor(t *testing.T) {
	env, dbi := setupDupRangeDB(t, 3, 10)
	defer env.Close()

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	key := make([]byte, 8)
	from := make([]byte, 8)
	to := make([]byte, 8)
	binary.BigEndian.PutUint64(key, 1)
	binary.BigEndian.PutUint64(from, 2)
	binary.BigEndian.PutUint64(to, 8)

	deleted, err := cursor.DeleteDupRange(key, from, to)
	cursor.Close()
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	if deleted != 6 {
		txn.Abort()
		t.Fatalf("expected 6 deleted, got %d", deleted)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	vals := collectValuesForKey(t, env, dbi, 1)
	expected := []uint64{0, 1, 8, 9}
	if len(vals) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, vals)
	}
	for i, v := range vals {
		if v != expected[i] {
			t.Fatalf("value %d: expected %d, got %d", i, expected[i], v)
		}
	}
}

func TestDeleteDupRangeFromGreaterThanTo(t *testing.T) {
	env, dbi := setupDupRangeDB(t, 1, 10)
	defer env.Close()

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	key := make([]byte, 8)
	from := make([]byte, 8)
	to := make([]byte, 8)
	binary.BigEndian.PutUint64(key, 0)
	binary.BigEndian.PutUint64(from, 7)
	binary.BigEndian.PutUint64(to, 3) // from > to

	deleted, err := txn.DeleteDupRange(dbi, key, from, to)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	if deleted != 0 {
		t.Fatalf("expected 0 deleted for from>to, got %d", deleted)
	}

	txn.Abort()
}

// ==================== Sub-tree tests (many values per key) ====================
// With enough values, DupSort promotes inline sub-page to separate sub-tree.

func TestDeleteDupRangeSubTree(t *testing.T) {
	// 500 values per key should trigger sub-tree promotion
	env, dbi := setupDupRangeDB(t, 3, 500)
	defer env.Close()

	// Delete values [100, 400) for key 1
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	key := make([]byte, 8)
	from := make([]byte, 8)
	to := make([]byte, 8)
	binary.BigEndian.PutUint64(key, 1)
	binary.BigEndian.PutUint64(from, 100)
	binary.BigEndian.PutUint64(to, 400)

	deleted, err := txn.DeleteDupRange(dbi, key, from, to)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	if deleted != 300 {
		txn.Abort()
		t.Fatalf("expected 300 deleted, got %d", deleted)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Verify count
	remaining := countValuesForKey(t, env, dbi, 1)
	if remaining != 200 {
		t.Fatalf("expected 200 remaining values, got %d", remaining)
	}

	// Verify boundary values exist
	vals := collectValuesForKey(t, env, dbi, 1)
	// First value should be 0, last before gap should be 99, first after gap should be 400
	if len(vals) < 3 {
		t.Fatalf("too few values: %d", len(vals))
	}
	if vals[0] != 0 {
		t.Fatalf("expected first value 0, got %d", vals[0])
	}
	if vals[99] != 99 {
		t.Fatalf("expected value[99]=99, got %d", vals[99])
	}
	if vals[100] != 400 {
		t.Fatalf("expected value[100]=400, got %d", vals[100])
	}

	// Other keys untouched
	if c := countValuesForKey(t, env, dbi, 0); c != 500 {
		t.Fatalf("key 0 should have 500 values, got %d", c)
	}
}

func TestDeleteDupRangeSubTreeAll(t *testing.T) {
	env, dbi := setupDupRangeDB(t, 3, 500)
	defer env.Close()

	// Delete ALL values for key 1
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, 1)

	deleted, err := txn.DeleteDupRange(dbi, key, nil, nil)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	if deleted != 500 {
		txn.Abort()
		t.Fatalf("expected 500 deleted, got %d", deleted)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	if c := countValuesForKey(t, env, dbi, 1); c != 0 {
		t.Fatalf("key 1 should have 0 values, got %d", c)
	}
}

func TestDeleteDupRangeSubTreeFromNil(t *testing.T) {
	env, dbi := setupDupRangeDB(t, 2, 500)
	defer env.Close()

	// Delete [nil, 200) for key 0
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	key := make([]byte, 8)
	to := make([]byte, 8)
	binary.BigEndian.PutUint64(key, 0)
	binary.BigEndian.PutUint64(to, 200)

	deleted, err := txn.DeleteDupRange(dbi, key, nil, to)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	if deleted != 200 {
		txn.Abort()
		t.Fatalf("expected 200 deleted, got %d", deleted)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	remaining := countValuesForKey(t, env, dbi, 0)
	if remaining != 300 {
		t.Fatalf("expected 300 remaining, got %d", remaining)
	}
}

func TestDeleteDupRangeSubTreeToNil(t *testing.T) {
	env, dbi := setupDupRangeDB(t, 2, 500)
	defer env.Close()

	// Delete [300, nil) for key 1
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	key := make([]byte, 8)
	from := make([]byte, 8)
	binary.BigEndian.PutUint64(key, 1)
	binary.BigEndian.PutUint64(from, 300)

	deleted, err := txn.DeleteDupRange(dbi, key, from, nil)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	if deleted != 200 {
		txn.Abort()
		t.Fatalf("expected 200 deleted, got %d", deleted)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	remaining := countValuesForKey(t, env, dbi, 1)
	if remaining != 300 {
		t.Fatalf("expected 300 remaining, got %d", remaining)
	}
}

func TestDeleteDupRangeSubTreeNoMatch(t *testing.T) {
	env, dbi := setupDupRangeDB(t, 1, 500)
	defer env.Close()

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	key := make([]byte, 8)
	from := make([]byte, 8)
	to := make([]byte, 8)
	binary.BigEndian.PutUint64(key, 0)
	binary.BigEndian.PutUint64(from, 1000)
	binary.BigEndian.PutUint64(to, 2000)

	deleted, err := txn.DeleteDupRange(dbi, key, from, to)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	if deleted != 0 {
		t.Fatalf("expected 0 deleted, got %d", deleted)
	}

	txn.Abort()
}

// ==================== Persistence tests ====================

func TestDeleteDupRangePersistence(t *testing.T) {
	env, dbi := setupDupRangeDB(t, 2, 20)
	defer env.Close()

	// Delete values [5, 15) for key 0 across two commits
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}

		key := make([]byte, 8)
		from := make([]byte, 8)
		to := make([]byte, 8)
		binary.BigEndian.PutUint64(key, 0)
		binary.BigEndian.PutUint64(from, 5)
		binary.BigEndian.PutUint64(to, 15)

		deleted, err := txn.DeleteDupRange(dbi, key, from, to)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		if deleted != 10 {
			txn.Abort()
			t.Fatalf("expected 10 deleted, got %d", deleted)
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Verify after commit
	remaining := countValuesForKey(t, env, dbi, 0)
	if remaining != 10 {
		t.Fatalf("expected 10 remaining, got %d", remaining)
	}

	// Key 1 untouched
	if c := countValuesForKey(t, env, dbi, 1); c != 20 {
		t.Fatalf("key 1 should have 20 values, got %d", c)
	}
}

func TestDeleteDupRangeReadOnlyError(t *testing.T) {
	env, dbi := setupDupRangeDB(t, 1, 5)
	defer env.Close()

	txn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Abort()

	key := make([]byte, 8)
	_, err = txn.DeleteDupRange(dbi, key, nil, nil)
	if err == nil {
		t.Fatal("expected error for read-only transaction")
	}
}

func TestDeleteDupRangeSingleValue(t *testing.T) {
	// Test with exactly 1 value per key
	path := t.TempDir() + "/single.db"
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	dbi, err := txn.OpenDBISimple("single", gdbx.Create|gdbx.DupSort)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}
	key := make([]byte, 8)
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(key, 42)
	binary.BigEndian.PutUint64(val, 100)
	txn.Put(dbi, key, val, 0)
	txn.Commit()

	// Delete value range that includes the single value
	txn, err = env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	from := make([]byte, 8)
	to := make([]byte, 8)
	binary.BigEndian.PutUint64(from, 50)
	binary.BigEndian.PutUint64(to, 200)
	deleted, err := txn.DeleteDupRange(dbi, key, from, to)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}
	if deleted != 1 {
		txn.Abort()
		t.Fatalf("expected 1 deleted, got %d", deleted)
	}
	txn.Commit()

	if c := countValuesForKey(t, env, dbi, 42); c != 0 {
		t.Fatalf("key should be gone, got %d values", c)
	}
}

func TestDeleteDupRangeSingleValueNotInRange(t *testing.T) {
	path := t.TempDir() + "/single2.db"
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	dbi, err := txn.OpenDBISimple("single2", gdbx.Create|gdbx.DupSort)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}
	key := make([]byte, 8)
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(key, 42)
	binary.BigEndian.PutUint64(val, 100)
	txn.Put(dbi, key, val, 0)
	txn.Commit()

	// Delete range that does NOT include the single value
	txn, err = env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	from := make([]byte, 8)
	to := make([]byte, 8)
	binary.BigEndian.PutUint64(from, 200)
	binary.BigEndian.PutUint64(to, 300)
	deleted, err := txn.DeleteDupRange(dbi, key, from, to)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}
	if deleted != 0 {
		txn.Abort()
		t.Fatalf("expected 0 deleted, got %d", deleted)
	}
	txn.Abort()

	// Value should still exist
	if c := countValuesForKey(t, env, dbi, 42); c != 1 {
		t.Fatalf("expected 1 value, got %d", c)
	}
}

// ==================== Total item count verification ====================

func TestDeleteDupRangeTotalItemCount(t *testing.T) {
	env, dbi := setupDupRangeDB(t, 5, 10) // 50 total entries
	defer env.Close()

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Delete 3 values from key 2, 5 from key 4
	key := make([]byte, 8)
	from := make([]byte, 8)
	to := make([]byte, 8)

	binary.BigEndian.PutUint64(key, 2)
	binary.BigEndian.PutUint64(from, 0)
	binary.BigEndian.PutUint64(to, 3)
	d1, err := txn.DeleteDupRange(dbi, key, from, to)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	binary.BigEndian.PutUint64(key, 4)
	binary.BigEndian.PutUint64(from, 0)
	binary.BigEndian.PutUint64(to, 5)
	d2, err := txn.DeleteDupRange(dbi, key, from, to)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	total := countTotalEntries(t, env, dbi)
	expected := 50 - int(d1) - int(d2)
	if total != expected {
		t.Fatalf("expected total %d entries, got %d (deleted %d+%d)", expected, total, d1, d2)
	}
}

// ==================== Prefix-based deletion tests ====================
// These tests verify that DeleteDupRange works correctly with variable-length
// from/to (shorter prefix keys). This is useful when values contain a txnum
// prefix followed by additional data, and you want to delete by txnum range.

// setupDupRangeDBWithRawValues creates a DupSort table with a single key
// and inserts the given raw byte slice values.
func setupDupRangeDBWithRawValues(t *testing.T, key []byte, vals [][]byte) (*gdbx.Env, gdbx.DBI) {
	t.Helper()
	path := t.TempDir() + "/dup_prefix.db"

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	env.SetMaxDBs(10)
	env.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync|gdbx.WriteMap, 0644); err != nil {
		t.Fatal(err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	dbi, err := txn.OpenDBISimple("prefixtest", gdbx.Create|gdbx.DupSort)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	for _, v := range vals {
		if err := txn.Put(dbi, key, v, 0); err != nil {
			txn.Abort()
			t.Fatal(err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	return env, dbi
}

// collectRawValuesForKey collects all raw byte values for a given key.
func collectRawValuesForKey(t *testing.T, env *gdbx.Env, dbi gdbx.DBI, key []byte) [][]byte {
	t.Helper()
	txn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Abort()

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()

	var result [][]byte
	k, v, err := cursor.Get(key, nil, gdbx.Set)
	if err != nil || k == nil {
		return nil
	}
	cp := make([]byte, len(v))
	copy(cp, v)
	result = append(result, cp)
	for {
		_, v, err := cursor.Get(nil, nil, gdbx.NextDup)
		if err != nil {
			break
		}
		cp := make([]byte, len(v))
		copy(cp, v)
		result = append(result, cp)
	}
	return result
}

// TestDeleteDupRangePrefixNarrowRange tests prefix-based deletion where
// from/to are shorter (2 bytes) than the stored values (4 bytes).
// Values:  00011234, 00011235, 00111234, 00211234, 00311234
// from=0010, to=0022 → should delete only 00111234, 00211234 (2 values)
// because 00011234 < 0010... and 00011235 < 0010... in bytes.Compare.
func TestDeleteDupRangePrefixNarrowRange(t *testing.T) {
	key := []byte{0x00, 0x00, 0x00, 0x01} // arbitrary key

	vals := [][]byte{
		{0x00, 0x01, 0x12, 0x34}, // prefix 0001 → less than from=0010
		{0x00, 0x01, 0x12, 0x35}, // prefix 0001 → less than from=0010
		{0x00, 0x11, 0x12, 0x34}, // prefix 0011 → in range [0010, 0022)
		{0x00, 0x21, 0x12, 0x34}, // prefix 0021 → in range [0010, 0022)
		{0x00, 0x31, 0x12, 0x34}, // prefix 0031 → >= to=0022
	}

	env, dbi := setupDupRangeDBWithRawValues(t, key, vals)
	defer env.Close()

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Shorter from/to — only 2 bytes (prefix)
	from := []byte{0x00, 0x10}
	to := []byte{0x00, 0x22}

	deleted, err := txn.DeleteDupRange(dbi, key, from, to)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	if deleted != 2 {
		txn.Abort()
		t.Fatalf("expected 2 deleted, got %d", deleted)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	remaining := collectRawValuesForKey(t, env, dbi, key)
	expectedRemaining := [][]byte{
		{0x00, 0x01, 0x12, 0x34},
		{0x00, 0x01, 0x12, 0x35},
		{0x00, 0x31, 0x12, 0x34},
	}
	if len(remaining) != len(expectedRemaining) {
		t.Fatalf("expected %d remaining values, got %d: %v", len(expectedRemaining), len(remaining), remaining)
	}
	for i, v := range remaining {
		if !bytes.Equal(v, expectedRemaining[i]) {
			t.Fatalf("value %d: expected %x, got %x", i, expectedRemaining[i], v)
		}
	}
}

// TestDeleteDupRangePrefixWideRange tests prefix-based deletion with a wider
// range that captures most values. Demonstrates txnum-prefix deletion.
// Values:  00011234, 00011235, 00111234, 00211234, 00311234
// from=0001, to=0022 → should delete 4 values (all with prefix 0001..0021)
// Only 00311234 (prefix 0031 >= to=0022) should remain.
func TestDeleteDupRangePrefixWideRange(t *testing.T) {
	key := []byte{0x00, 0x00, 0x00, 0x01}

	vals := [][]byte{
		{0x00, 0x01, 0x12, 0x34},
		{0x00, 0x01, 0x12, 0x35},
		{0x00, 0x11, 0x12, 0x34},
		{0x00, 0x21, 0x12, 0x34},
		{0x00, 0x31, 0x12, 0x34},
	}

	env, dbi := setupDupRangeDBWithRawValues(t, key, vals)
	defer env.Close()

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	// from=0001 captures values with prefix 0001 and above
	// to=0022 excludes values with prefix 0022 and above
	from := []byte{0x00, 0x01}
	to := []byte{0x00, 0x22}

	deleted, err := txn.DeleteDupRange(dbi, key, from, to)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	if deleted != 4 {
		txn.Abort()
		t.Fatalf("expected 4 deleted, got %d", deleted)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	remaining := collectRawValuesForKey(t, env, dbi, key)
	if len(remaining) != 1 {
		t.Fatalf("expected 1 remaining value, got %d: %v", len(remaining), remaining)
	}
	if !bytes.Equal(remaining[0], []byte{0x00, 0x31, 0x12, 0x34}) {
		t.Fatalf("expected remaining value 00311234, got %x", remaining[0])
	}
}

// TestDeleteDupRangePrefixDeleteAll tests prefix-based deletion
// where from/to cover all existing values.
func TestDeleteDupRangePrefixDeleteAll(t *testing.T) {
	key := []byte{0x00, 0x00, 0x00, 0x01}

	vals := [][]byte{
		{0x00, 0x01, 0x12, 0x34},
		{0x00, 0x01, 0x12, 0x35},
		{0x00, 0x11, 0x12, 0x34},
		{0x00, 0x21, 0x12, 0x34},
		{0x00, 0x31, 0x12, 0x34},
	}

	env, dbi := setupDupRangeDBWithRawValues(t, key, vals)
	defer env.Close()

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	// from=0001 to=0032 covers all values
	from := []byte{0x00, 0x01}
	to := []byte{0x00, 0x32}

	deleted, err := txn.DeleteDupRange(dbi, key, from, to)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	if deleted != 5 {
		txn.Abort()
		t.Fatalf("expected 5 deleted, got %d", deleted)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	remaining := collectRawValuesForKey(t, env, dbi, key)
	if len(remaining) != 0 {
		t.Fatalf("expected 0 remaining values, got %d: %v", len(remaining), remaining)
	}
}

// TestDeleteDupRangePrefixTxnumScenario simulates a realistic txnum-prefix
// scenario: values are (2-byte txnum) + (6-byte hash). Delete by txnum range.
func TestDeleteDupRangePrefixTxnumScenario(t *testing.T) {
	key := []byte("account_A")

	// txnum(2 bytes BE) + hash(6 bytes) — sorted by txnum naturally
	vals := [][]byte{
		{0x00, 0x05, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0x01}, // txnum=5
		{0x00, 0x0A, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66}, // txnum=10
		{0x00, 0x0F, 0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01}, // txnum=15
		{0x00, 0x14, 0xCA, 0xFE, 0xBA, 0xBE, 0x00, 0x00}, // txnum=20
		{0x00, 0x19, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC}, // txnum=25
		{0x00, 0x1E, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, // txnum=30
	}

	env, dbi := setupDupRangeDBWithRawValues(t, key, vals)
	defer env.Close()

	// Delete txnums [10, 25) — using only 2-byte prefix
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	from := []byte{0x00, 0x0A} // txnum=10
	to := []byte{0x00, 0x19}   // txnum=25

	deleted, err := txn.DeleteDupRange(dbi, key, from, to)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// Should delete txnum=10, 15, 20 (3 values)
	if deleted != 3 {
		txn.Abort()
		t.Fatalf("expected 3 deleted (txnums 10,15,20), got %d", deleted)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	remaining := collectRawValuesForKey(t, env, dbi, key)
	if len(remaining) != 3 {
		t.Fatalf("expected 3 remaining values (txnums 5,25,30), got %d", len(remaining))
	}

	// Verify remaining values have txnums 5, 25, 30
	expectedTxnums := []uint16{0x0005, 0x0019, 0x001E}
	for i, v := range remaining {
		gotTxnum := uint16(v[0])<<8 | uint16(v[1])
		if gotTxnum != expectedTxnums[i] {
			t.Fatalf("remaining[%d]: expected txnum %04x, got %04x", i, expectedTxnums[i], gotTxnum)
		}
	}
}

// ensure unused import
var _ = bytes.Compare
