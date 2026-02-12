package tests

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"

	gdbx "github.com/JkLondon/gdbx"
)

// TestNextNoDupReadOnly tests the Next/NextNoDup iteration pattern on clean data
// (no SeekBothRange+Delete+Put cycle). If this fails, the bug is in NextNoDup itself.
// If it passes, the bug is in the write path (SeekBothRange+Del+Put corrupts something).
func TestNextNoDupReadOnly(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-nextnod-readonly-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	env, err := gdbx.NewEnv("")
	if err != nil {
		t.Fatal(err)
	}
	if err := env.SetMaxDBs(10); err != nil {
		t.Fatal(err)
	}
	if err := env.Open(filepath.Join(dir, "test.db"), gdbx.NoSubdir|gdbx.WriteMap, 0644); err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	numKeys := 200
	stepsPerKey := 50
	keySize := 20
	dataSize := 10
	targetStep := uint64(25)

	rng := rand.New(rand.NewSource(42))

	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = make([]byte, keySize)
		rng.Read(keys[i])
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	// Insert data (no updates)
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	dbi, err := txn.OpenDBISimple("test", gdbx.Create|gdbx.DupSort)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	expectedByKey := make(map[string]map[uint64][]byte)
	for _, key := range keys {
		ks := string(key)
		expectedByKey[ks] = make(map[uint64][]byte)
		for step := uint64(0); step < uint64(stepsPerKey); step++ {
			data := make([]byte, dataSize)
			rng.Read(data)
			val := make([]byte, 8+dataSize)
			binary.BigEndian.PutUint64(val[:8], ^step)
			copy(val[8:], data)
			if err := txn.Put(dbi, key, val, 0); err != nil {
				txn.Abort()
				t.Fatal(err)
			}
			expectedByKey[ks][step] = append([]byte{}, data...)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Read using the same iteration pattern
	txn, err = env.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Abort()

	readCursor, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatal(err)
	}
	defer readCursor.Close()

	targetInvStep := ^targetStep
	foundResults := make(map[string][]byte)

	for k, v, err := readCursor.Get(nil, nil, gdbx.First); k != nil; {
		if err != nil && !gdbx.IsNotFound(err) {
			t.Fatalf("Iteration error: %v", err)
		}
		if k == nil {
			break
		}

		if len(v) >= 8 {
			invStep := binary.BigEndian.Uint64(v[:8])
			if invStep == targetInvStep {
				ks := string(k)
				foundResults[ks] = append([]byte{}, v[8:]...)
				k, v, err = readCursor.Get(nil, nil, gdbx.NextNoDup)
			} else {
				k, v, err = readCursor.Get(nil, nil, gdbx.Next)
			}
		} else {
			k, v, err = readCursor.Get(nil, nil, gdbx.Next)
		}
	}

	// Compare
	expectedCount := 0
	mismatchCount := 0
	for _, key := range keys {
		ks := string(key)
		expectedData, ok := expectedByKey[ks][targetStep]
		if !ok {
			continue
		}
		expectedCount++

		foundData, ok := foundResults[ks]
		if !ok {
			t.Errorf("Key %x: NOT FOUND for step %d (read-only, no updates!)", key[:8], targetStep)
			mismatchCount++
			continue
		}
		if !bytes.Equal(foundData, expectedData) {
			t.Errorf("Key %x step %d: data mismatch: got %x, want %x",
				key[:8], targetStep, foundData, expectedData)
			mismatchCount++
		}
	}

	t.Logf("Found %d/%d keys with target step %d, %d mismatches (READ-ONLY test, no updates)",
		len(foundResults), expectedCount, targetStep, mismatchCount)

	if mismatchCount > 0 {
		t.Logf("BUG IS IN NextNoDup ITSELF (not the write path)")
	} else {
		t.Logf("NextNoDup works on clean data - bug must be in SeekBothRange+Del+Put write path")
	}
}

// TestNextNoDupAfterUpdates tests iteration after SeekBothRange+Del+Put updates,
// comparing Next-only iteration vs Next+NextNoDup iteration.
func TestNextNoDupAfterUpdates(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-nextnod-updates-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	env, err := gdbx.NewEnv("")
	if err != nil {
		t.Fatal(err)
	}
	if err := env.SetMaxDBs(10); err != nil {
		t.Fatal(err)
	}
	if err := env.Open(filepath.Join(dir, "test.db"), gdbx.NoSubdir|gdbx.WriteMap, 0644); err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	numKeys := 200
	stepsPerKey := 50
	keySize := 20
	dataSize := 10
	targetStep := uint64(25)

	rng := rand.New(rand.NewSource(42))

	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = make([]byte, keySize)
		rng.Read(keys[i])
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	// Phase 1: Insert
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	dbi, err := txn.OpenDBISimple("test", gdbx.Create|gdbx.DupSort)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	expectedByKey := make(map[string]map[uint64][]byte)
	for _, key := range keys {
		ks := string(key)
		expectedByKey[ks] = make(map[uint64][]byte)
		for step := uint64(0); step < uint64(stepsPerKey); step++ {
			data := make([]byte, dataSize)
			rng.Read(data)
			val := make([]byte, 8+dataSize)
			binary.BigEndian.PutUint64(val[:8], ^step)
			copy(val[8:], data)
			if err := txn.Put(dbi, key, val, 0); err != nil {
				txn.Abort()
				t.Fatal(err)
			}
			expectedByKey[ks][step] = append([]byte{}, data...)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Phase 2: Update every other step
	txn, err = env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	for _, key := range keys {
		ks := string(key)
		for step := uint64(0); step < uint64(stepsPerKey); step += 2 {
			newData := make([]byte, dataSize)
			rng.Read(newData)
			val := make([]byte, 8+dataSize)
			binary.BigEndian.PutUint64(val[:8], ^step)
			copy(val[8:], newData)

			prefix := val[:8]
			_, foundVal, err := cursor.Get(key, prefix, gdbx.GetBothRange)
			if err != nil {
				if gdbx.IsNotFound(err) {
					cursor.Put(key, val, 0)
				} else {
					cursor.Close()
					txn.Abort()
					t.Fatal(err)
				}
			} else if len(foundVal) >= 8 && bytes.Equal(foundVal[:8], prefix) {
				cursor.Del(0)
				cursor.Put(key, val, 0)
			} else {
				cursor.Put(key, val, 0)
			}
			expectedByKey[ks][step] = append([]byte{}, newData...)
		}
	}
	cursor.Close()

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Phase 3: Compare two iteration methods
	txn, err = env.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Abort()

	targetInvStep := ^targetStep

	// Method A: Plain Next iteration (iterate all entries)
	curA, _ := txn.OpenCursor(dbi)
	foundA := make(map[string][]byte)
	for k, v, err := curA.Get(nil, nil, gdbx.First); k != nil; k, v, err = curA.Get(nil, nil, gdbx.Next) {
		if err != nil {
			break
		}
		if len(v) >= 8 && binary.BigEndian.Uint64(v[:8]) == targetInvStep {
			foundA[string(k)] = append([]byte{}, v[8:]...)
		}
	}
	curA.Close()

	// Method B: Next + NextNoDup iteration (the Erigon pattern)
	curB, _ := txn.OpenCursor(dbi)
	foundB := make(map[string][]byte)
	for k, v, err := curB.Get(nil, nil, gdbx.First); k != nil; {
		if err != nil && !gdbx.IsNotFound(err) {
			t.Fatalf("Method B iteration error: %v", err)
		}
		if k == nil {
			break
		}
		if len(v) >= 8 {
			invStep := binary.BigEndian.Uint64(v[:8])
			if invStep == targetInvStep {
				foundB[string(k)] = append([]byte{}, v[8:]...)
				k, v, err = curB.Get(nil, nil, gdbx.NextNoDup)
			} else {
				k, v, err = curB.Get(nil, nil, gdbx.Next)
			}
		} else {
			k, v, err = curB.Get(nil, nil, gdbx.Next)
		}
	}
	curB.Close()

	t.Logf("Method A (Next only): found %d keys", len(foundA))
	t.Logf("Method B (Next+NextNoDup): found %d keys", len(foundB))

	// Compare methods
	missingInB := 0
	for ks, dataA := range foundA {
		dataB, ok := foundB[ks]
		if !ok {
			if missingInB < 5 {
				t.Logf("MISSING in Method B: key %x (Method A has data %x)", []byte(ks)[:8], dataA[:4])
			}
			missingInB++
			continue
		}
		if !bytes.Equal(dataA, dataB) {
			t.Errorf("Data mismatch for key %x: A=%x, B=%x", []byte(ks)[:8], dataA, dataB)
		}
	}

	if missingInB > 0 {
		t.Errorf("%d keys found by Next but MISSED by Next+NextNoDup pattern (NextNoDup bug!)", missingInB)
	}

	extraInB := 0
	for ks := range foundB {
		if _, ok := foundA[ks]; !ok {
			extraInB++
		}
	}
	if extraInB > 0 {
		t.Errorf("%d keys found by NextNoDup but not by Next (unexpected)", extraInB)
	}

	// Also verify against expected data
	mismatchVsExpected := 0
	for _, key := range keys {
		ks := string(key)
		expectedData, ok := expectedByKey[ks][targetStep]
		if !ok {
			continue
		}
		dataA, okA := foundA[ks]
		if !okA {
			t.Errorf("Key %x step %d: not found even by plain Next!", key[:8], targetStep)
			mismatchVsExpected++
			continue
		}
		if !bytes.Equal(dataA, expectedData) {
			t.Errorf("Key %x step %d: data wrong even by plain Next: got %x, want %x",
				key[:8], targetStep, dataA, expectedData)
			mismatchVsExpected++
		}
	}

	if mismatchVsExpected > 0 {
		t.Logf("DATA CORRUPTION: %d keys have wrong data (write path bug)", mismatchVsExpected)
	}
}
