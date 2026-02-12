package tests

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"

	gdbx "github.com/JkLondon/gdbx"
)

// TestDupSortSeekBothRangeDeletePut reproduces a bug where DupSort cursor returns
// wrong data after many SeekBothRange + DeleteCurrent + Put operations.
//
// This replicates the pattern from Erigon's ETL flush into DupSort tables:
//   - key = 20 bytes (address-like)
//   - value = 8-byte prefix (inverted step, ~0xFFFFFFFFFFFFFFxx) + 0-10 bytes data
//   - For each (key, value): SeekBothRange by prefix, if found delete old, then put new
//   - After all writes, read back with a new cursor and verify all data
//
// The bug manifests as wrong values returned for some keys after the write cycle.
func TestDupSortSeekBothRangeDeletePut(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-seekbothrange-del-put-*")
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

	// Parameters matching the Erigon scenario
	numKeys := 200
	maxStepsPerKey := 50
	keySize := 20
	dataSize := 10

	rng := rand.New(rand.NewSource(42))

	// Generate unique keys (20 bytes each, like Ethereum addresses)
	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = make([]byte, keySize)
		rng.Read(keys[i])
	}
	// Sort keys for determinism (DupSort stores keys in sorted order)
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	// Build the expected state: for each key, a set of (step, data) pairs
	// step values are stored as ^step (inverted) to match Erigon's pattern
	type entry struct {
		step uint64
		data []byte
	}
	expected := make(map[string][]entry) // key -> list of entries

	// Generate initial data: each key gets a random number of steps
	var allOps []struct {
		key  []byte
		step uint64
		data []byte
	}

	for _, key := range keys {
		numSteps := rng.Intn(maxStepsPerKey) + 1
		usedSteps := make(map[uint64]bool)
		for s := 0; s < numSteps; s++ {
			step := uint64(rng.Intn(maxStepsPerKey))
			if usedSteps[step] {
				continue
			}
			usedSteps[step] = true

			data := make([]byte, dataSize)
			rng.Read(data)
			allOps = append(allOps, struct {
				key  []byte
				step uint64
				data []byte
			}{key, step, data})
		}
	}

	// Shuffle operations to simulate non-sequential ETL flush
	rng.Shuffle(len(allOps), func(i, j int) {
		allOps[i], allOps[j] = allOps[j], allOps[i]
	})

	t.Logf("Total operations: %d across %d keys", len(allOps), numKeys)

	// Phase 1: Insert initial data using simple Put
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	dbi, err := txn.OpenDBISimple("test", gdbx.Create|gdbx.DupSort)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	for _, op := range allOps {
		val := make([]byte, 8+dataSize)
		// Store inverted step as prefix (like Erigon: ^step)
		binary.BigEndian.PutUint64(val[:8], ^op.step)
		copy(val[8:], op.data)

		if err := txn.Put(dbi, op.key, val, 0); err != nil {
			txn.Abort()
			t.Fatalf("Put failed: %v", err)
		}

		// Track expected state
		ks := string(op.key)
		expected[ks] = append(expected[ks], entry{step: op.step, data: append([]byte{}, op.data...)})
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Phase 2: Update data using the SeekBothRange + DeleteCurrent + Put pattern
	// This is the pattern that triggers the bug
	txn, err = env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// Generate update operations: for some keys, update existing steps with new data
	updateCount := 0
	for _, key := range keys {
		ks := string(key)
		entries := expected[ks]
		if len(entries) == 0 {
			continue
		}

		// Update ~half of the entries for this key
		for i := range entries {
			if rng.Intn(2) == 0 {
				continue
			}

			step := entries[i].step
			newData := make([]byte, dataSize)
			rng.Read(newData)

			// Build the value with inverted step prefix
			val := make([]byte, 8+dataSize)
			binary.BigEndian.PutUint64(val[:8], ^step)
			copy(val[8:], newData)

			// SeekBothRange by prefix (first 8 bytes of value)
			prefix := val[:8]
			_, foundVal, err := cursor.Get(key, prefix, gdbx.GetBothRange)
			if err != nil {
				if gdbx.IsNotFound(err) {
					// Not found - just put
					if err := cursor.Put(key, val, 0); err != nil {
						cursor.Close()
						txn.Abort()
						t.Fatalf("Put after not-found failed: %v", err)
					}
				} else {
					cursor.Close()
					txn.Abort()
					t.Fatalf("GetBothRange failed: %v", err)
				}
			} else if len(foundVal) >= 8 && bytes.Equal(foundVal[:8], prefix) {
				// Found exact prefix match - delete old, put new
				if err := cursor.Del(0); err != nil {
					cursor.Close()
					txn.Abort()
					t.Fatalf("Del failed: %v", err)
				}
				if err := cursor.Put(key, val, 0); err != nil {
					cursor.Close()
					txn.Abort()
					t.Fatalf("Put after delete failed: %v", err)
				}
			} else {
				// No exact prefix match - just put new
				if err := cursor.Put(key, val, 0); err != nil {
					cursor.Close()
					txn.Abort()
					t.Fatalf("Put new entry failed: %v", err)
				}
			}

			// Update expected state
			entries[i].data = append([]byte{}, newData...)
			updateCount++
		}
		expected[ks] = entries
	}

	cursor.Close()
	t.Logf("Updated %d entries", updateCount)

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit after updates failed: %v", err)
	}

	// Phase 3: Read back with a NEW read-only transaction and verify
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

	// Build sorted expected entries for comparison
	type fullEntry struct {
		key  []byte
		step uint64
		data []byte
	}
	var expectedAll []fullEntry
	for _, key := range keys {
		ks := string(key)
		entries := expected[ks]
		// Sort entries by inverted step (since values are sorted by ^step)
		sort.Slice(entries, func(i, j int) bool {
			return ^entries[i].step < ^entries[j].step
		})
		for _, e := range entries {
			expectedAll = append(expectedAll, fullEntry{
				key:  append([]byte{}, key...),
				step: e.step,
				data: append([]byte{}, e.data...),
			})
		}
	}

	// Iterate and compare
	readCount := 0
	mismatchCount := 0
	k, v, err := readCursor.Get(nil, nil, gdbx.First)
	for err == nil && k != nil {
		if readCount >= len(expectedAll) {
			t.Errorf("More entries than expected: got entry %d+ but expected only %d", readCount+1, len(expectedAll))
			break
		}

		exp := expectedAll[readCount]

		if !bytes.Equal(k, exp.key) {
			t.Errorf("Entry %d: key mismatch: got %x, want %x", readCount, k, exp.key)
			mismatchCount++
		}

		if len(v) < 8 {
			t.Errorf("Entry %d: value too short: %x", readCount, v)
			mismatchCount++
		} else {
			gotInvStep := binary.BigEndian.Uint64(v[:8])
			expInvStep := ^exp.step
			if gotInvStep != expInvStep {
				t.Errorf("Entry %d: step prefix mismatch for key %x: got %x, want %x (step=%d)",
					readCount, k, gotInvStep, expInvStep, exp.step)
				mismatchCount++
			}

			gotData := v[8:]
			if !bytes.Equal(gotData, exp.data) {
				t.Errorf("Entry %d: data mismatch for key %x step %d: got %x, want %x",
					readCount, k, exp.step, gotData, exp.data)
				mismatchCount++
			}
		}

		readCount++
		if mismatchCount > 20 {
			t.Fatalf("Too many mismatches (%d), stopping", mismatchCount)
		}

		k, v, err = readCursor.Get(nil, nil, gdbx.Next)
	}

	if err != nil && !gdbx.IsNotFound(err) {
		t.Fatalf("Iteration error: %v", err)
	}

	if readCount != len(expectedAll) {
		t.Errorf("Entry count mismatch: got %d, want %d", readCount, len(expectedAll))
	}

	t.Logf("Verified %d entries, %d mismatches", readCount, mismatchCount)
}

// TestDupSortSeekBothRangeDeletePutNextNoDup is a more targeted test that
// replicates the exact Erigon reading pattern: iterate with First/Next/NextNoDup
// checking for a target step, which is the pattern that reveals wrong values.
func TestDupSortSeekBothRangeDeletePutNextNoDup(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-seekbothrange-nextnod-*")
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
	targetStep := uint64(25) // The step we'll look for when reading

	rng := rand.New(rand.NewSource(42))

	// Generate sorted unique keys
	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = make([]byte, keySize)
		rng.Read(keys[i])
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	// Phase 1: Insert initial data
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	dbi, err := txn.OpenDBISimple("test", gdbx.Create|gdbx.DupSort)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// Track expected data per key per step
	type stepData struct {
		step uint64
		data []byte
	}
	expectedByKey := make(map[string][]stepData)

	for _, key := range keys {
		for step := uint64(0); step < uint64(stepsPerKey); step++ {
			data := make([]byte, dataSize)
			rng.Read(data)

			val := make([]byte, 8+dataSize)
			binary.BigEndian.PutUint64(val[:8], ^step)
			copy(val[8:], data)

			if err := txn.Put(dbi, key, val, 0); err != nil {
				txn.Abort()
				t.Fatalf("Put failed: %v", err)
			}

			ks := string(key)
			expectedByKey[ks] = append(expectedByKey[ks], stepData{step: step, data: append([]byte{}, data...)})
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Initial commit failed: %v", err)
	}

	t.Logf("Inserted %d keys × %d steps = %d entries", numKeys, stepsPerKey, numKeys*stepsPerKey)

	// Phase 2: Update entries using SeekBothRange + DeleteCurrent + Put
	txn, err = env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	updateOps := 0
	for _, key := range keys {
		ks := string(key)
		entries := expectedByKey[ks]

		// Update every other step
		for i := 0; i < len(entries); i += 2 {
			step := entries[i].step
			newData := make([]byte, dataSize)
			rng.Read(newData)

			val := make([]byte, 8+dataSize)
			binary.BigEndian.PutUint64(val[:8], ^step)
			copy(val[8:], newData)

			prefix := val[:8]
			_, foundVal, err := cursor.Get(key, prefix, gdbx.GetBothRange)
			if err != nil {
				if gdbx.IsNotFound(err) {
					if err := cursor.Put(key, val, 0); err != nil {
						cursor.Close()
						txn.Abort()
						t.Fatalf("Put failed: %v", err)
					}
				} else {
					cursor.Close()
					txn.Abort()
					t.Fatalf("GetBothRange failed: %v", err)
				}
			} else if len(foundVal) >= 8 && bytes.Equal(foundVal[:8], prefix) {
				// Delete old, put new
				if err := cursor.Del(0); err != nil {
					cursor.Close()
					txn.Abort()
					t.Fatalf("Del failed: %v", err)
				}
				if err := cursor.Put(key, val, 0); err != nil {
					cursor.Close()
					txn.Abort()
					t.Fatalf("Put after Del failed: %v", err)
				}
			} else {
				if err := cursor.Put(key, val, 0); err != nil {
					cursor.Close()
					txn.Abort()
					t.Fatalf("Put failed: %v", err)
				}
			}

			entries[i].data = append([]byte{}, newData...)
			updateOps++
		}
		expectedByKey[ks] = entries
	}

	cursor.Close()
	t.Logf("Updated %d entries via SeekBothRange+Delete+Put", updateOps)

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Update commit failed: %v", err)
	}

	// Phase 3: Read back using the Erigon pattern: First/Next/NextNoDup
	// looking for the target step
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
	targetPrefix := make([]byte, 8)
	binary.BigEndian.PutUint64(targetPrefix, targetInvStep)

	// Build expected results for the target step
	expectedResults := make(map[string][]byte) // key -> data for targetStep
	for _, key := range keys {
		ks := string(key)
		for _, e := range expectedByKey[ks] {
			if e.step == targetStep {
				expectedResults[ks] = e.data
				break
			}
		}
	}

	// Iterate like Erigon: for each key, check if the target step exists
	foundResults := make(map[string][]byte)
	mismatchCount := 0

	for k, v, err := readCursor.Get(nil, nil, gdbx.First); k != nil; {
		if err != nil && !gdbx.IsNotFound(err) {
			t.Fatalf("Iteration error: %v", err)
		}
		if k == nil {
			break
		}

		if len(v) < 8 {
			k, v, err = readCursor.Get(nil, nil, gdbx.Next)
			continue
		}

		invStep := binary.BigEndian.Uint64(v[:8])
		if invStep == targetInvStep {
			// Found our target step - record the data
			ks := string(k)
			foundResults[ks] = append([]byte{}, v[8:]...)
			// Move to next key (NextNoDup)
			k, v, err = readCursor.Get(nil, nil, gdbx.NextNoDup)
		} else {
			// Not our target step - try next dup
			k, v, err = readCursor.Get(nil, nil, gdbx.Next)
		}
	}

	// Compare results
	for ks, expectedData := range expectedResults {
		foundData, ok := foundResults[ks]
		if !ok {
			t.Errorf("Key %x: expected data for step %d but not found", []byte(ks), targetStep)
			mismatchCount++
			continue
		}
		if !bytes.Equal(foundData, expectedData) {
			t.Errorf("Key %x step %d: data mismatch: got %x, want %x",
				[]byte(ks), targetStep, foundData, expectedData)
			mismatchCount++
		}
	}

	// Check for unexpected entries
	for ks, foundData := range foundResults {
		if _, ok := expectedResults[ks]; !ok {
			t.Errorf("Key %x: found unexpected data for step %d: %x",
				[]byte(ks), targetStep, foundData)
			mismatchCount++
		}
	}

	t.Logf("Found %d/%d keys with target step %d, %d mismatches",
		len(foundResults), len(expectedResults), targetStep, mismatchCount)
}

// TestDupSortSeekBothRangeDeletePutHighValues specifically tests with high-value
// prefixes (~0xFFFFFFFFFFFFFFxx) which may cause sorting/comparison issues.
func TestDupSortSeekBothRangeDeletePutHighValues(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-highval-*")
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

	rng := rand.New(rand.NewSource(42))

	// Generate sorted unique keys - mix of 20-byte and 52-byte keys
	// (matching the bug description)
	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		sz := keySize
		if i%3 == 0 {
			sz = 52 // Some longer keys like in the Erigon scenario
		}
		keys[i] = make([]byte, sz)
		rng.Read(keys[i])
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	// Phase 1: Write initial data
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	dbi, err := txn.OpenDBISimple("test", gdbx.Create|gdbx.DupSort)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// expected[keyString][step] = data
	expected := make(map[string]map[uint64][]byte)

	totalInserted := 0
	for _, key := range keys {
		ks := string(key)
		expected[ks] = make(map[uint64][]byte)

		for step := uint64(0); step < uint64(stepsPerKey); step++ {
			data := make([]byte, dataSize)
			rng.Read(data)

			// Value: bigEndian(^step) || data
			// ^step for small steps gives values like 0xFFFFFFFFFFFFFFCE
			val := make([]byte, 8+dataSize)
			binary.BigEndian.PutUint64(val[:8], ^step)
			copy(val[8:], data)

			if err := txn.Put(dbi, key, val, 0); err != nil {
				txn.Abort()
				t.Fatalf("Put failed: %v", err)
			}

			expected[ks][step] = append([]byte{}, data...)
			totalInserted++
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Initial commit failed: %v", err)
	}

	t.Logf("Inserted %d entries", totalInserted)

	// Phase 2: Update using SeekBothRange + Del + Put (ETL flush pattern)
	txn, err = env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	updateCount := 0
	for _, key := range keys {
		ks := string(key)

		// Update all steps for each key (simulate full ETL flush)
		for step := uint64(0); step < uint64(stepsPerKey); step++ {
			newData := make([]byte, dataSize)
			rng.Read(newData)

			val := make([]byte, 8+dataSize)
			binary.BigEndian.PutUint64(val[:8], ^step)
			copy(val[8:], newData)

			// SeekBothRange by the 8-byte prefix
			prefix := val[:8]
			_, foundVal, err := cursor.Get(key, prefix, gdbx.GetBothRange)
			if err != nil {
				if gdbx.IsNotFound(err) {
					if err := cursor.Put(key, val, 0); err != nil {
						cursor.Close()
						txn.Abort()
						t.Fatalf("Put failed: %v", err)
					}
				} else {
					cursor.Close()
					txn.Abort()
					t.Fatalf("GetBothRange error: %v", err)
				}
			} else if len(foundVal) >= 8 && bytes.Equal(foundVal[:8], prefix) {
				// Exact prefix match - delete + put
				if err := cursor.Del(0); err != nil {
					cursor.Close()
					txn.Abort()
					t.Fatalf("Del failed: %v", err)
				}
				if err := cursor.Put(key, val, 0); err != nil {
					cursor.Close()
					txn.Abort()
					t.Fatalf("Put after Del failed: %v", err)
				}
			} else {
				// Different prefix found - just put
				if err := cursor.Put(key, val, 0); err != nil {
					cursor.Close()
					txn.Abort()
					t.Fatalf("Put failed: %v", err)
				}
			}

			expected[ks][step] = append([]byte{}, newData...)
			updateCount++
		}
	}

	cursor.Close()
	t.Logf("Updated %d entries", updateCount)

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Update commit failed: %v", err)
	}

	// Phase 3: Read back and verify using NEW read-only cursor
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

	readCount := 0
	mismatchCount := 0

	for k, v, err := readCursor.Get(nil, nil, gdbx.First); k != nil; k, v, err = readCursor.Get(nil, nil, gdbx.Next) {
		if err != nil {
			if gdbx.IsNotFound(err) {
				break
			}
			t.Fatalf("Read error: %v", err)
		}
		if k == nil {
			break
		}

		if len(v) < 8 {
			t.Errorf("Entry %d: value too short: %x", readCount, v)
			mismatchCount++
			readCount++
			continue
		}

		ks := string(k)
		invStep := binary.BigEndian.Uint64(v[:8])
		step := ^invStep
		gotData := v[8:]

		keyExpected, ok := expected[ks]
		if !ok {
			t.Errorf("Entry %d: unexpected key %x", readCount, k)
			mismatchCount++
			readCount++
			continue
		}

		expectedData, ok := keyExpected[step]
		if !ok {
			t.Errorf("Entry %d: key %x has no expected data for step %d", readCount, k, step)
			mismatchCount++
			readCount++
			continue
		}

		if !bytes.Equal(gotData, expectedData) {
			t.Errorf("Entry %d: key %x step %d: data mismatch: got %x, want %x",
				readCount, k[:8], step, gotData, expectedData)
			mismatchCount++
		}

		readCount++
		if mismatchCount > 30 {
			t.Fatalf("Too many mismatches (%d), stopping", mismatchCount)
		}
	}

	if readCount != totalInserted {
		t.Errorf("Entry count: got %d, want %d", readCount, totalInserted)
	}

	t.Logf("Read %d entries, %d mismatches", readCount, mismatchCount)
}

// TestDupSortSeekBothRangeDeletePutVerifyPerKey verifies each key individually
// using GetBothRange after the write cycle, which is the most direct verification.
func TestDupSortSeekBothRangeDeletePutVerifyPerKey(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-perkey-verify-*")
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

	numKeys := 100
	stepsPerKey := 30
	keySize := 20
	dataSize := 10

	rng := rand.New(rand.NewSource(12345))

	// Generate sorted keys
	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = make([]byte, keySize)
		rng.Read(keys[i])
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	// expected[keyStr][step] = data
	expected := make(map[string]map[uint64][]byte)

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

	for _, key := range keys {
		ks := string(key)
		expected[ks] = make(map[uint64][]byte)
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
			expected[ks][step] = append([]byte{}, data...)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Phase 2: Update via SeekBothRange + Del + Put
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
		for step := uint64(0); step < uint64(stepsPerKey); step++ {
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
					t.Fatalf("GetBothRange error: %v", err)
				}
			} else if len(foundVal) >= 8 && bytes.Equal(foundVal[:8], prefix) {
				cursor.Del(0)
				cursor.Put(key, val, 0)
			} else {
				cursor.Put(key, val, 0)
			}

			expected[ks][step] = append([]byte{}, newData...)
		}
	}

	cursor.Close()

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Phase 3: Verify each key individually using GetBothRange
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

	mismatchCount := 0
	for _, key := range keys {
		ks := string(key)
		for step := uint64(0); step < uint64(stepsPerKey); step++ {
			prefix := make([]byte, 8)
			binary.BigEndian.PutUint64(prefix, ^step)

			_, v, err := readCursor.Get(key, prefix, gdbx.GetBothRange)
			if err != nil {
				t.Errorf("Key %x step %d: GetBothRange error: %v", key[:8], step, err)
				mismatchCount++
				continue
			}

			if len(v) < 8 {
				t.Errorf("Key %x step %d: value too short: %x", key[:8], step, v)
				mismatchCount++
				continue
			}

			// Verify prefix match
			if !bytes.Equal(v[:8], prefix) {
				t.Errorf("Key %x step %d: prefix mismatch: got %x, want %x",
					key[:8], step, v[:8], prefix)
				mismatchCount++
				continue
			}

			gotData := v[8:]
			expectedData := expected[ks][step]
			if !bytes.Equal(gotData, expectedData) {
				t.Errorf("Key %x step %d: data mismatch: got %x, want %x",
					key[:8], step, gotData, expectedData)
				mismatchCount++
			}

			if mismatchCount > 30 {
				t.Fatalf("Too many mismatches, stopping")
			}
		}
	}

	t.Logf("Verified %d keys × %d steps, %d mismatches", numKeys, stepsPerKey, mismatchCount)
	if mismatchCount > 0 {
		t.Errorf("Total mismatches: %d", mismatchCount)
	} else {
		fmt.Println("All entries verified correctly!")
	}
}
