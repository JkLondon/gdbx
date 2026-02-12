package tests

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	gdbx "github.com/JkLondon/gdbx"
)

// TestNextNoDupMinimal checks NextNoDup with representative key/dup counts:
// small (single page), medium (page boundary), and large (multi-page tree).
func TestNextNoDupMinimal(t *testing.T) {
	cases := []struct{ keys, dups int }{
		{2, 10},  // small: fits on one page
		{5, 30},  // medium: crosses page boundary
		{10, 50}, // large: multi-level B-tree
		{20, 60}, // stress: many keys, many dups
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("keys%d_dups%d", tc.keys, tc.dups), func(t *testing.T) {
			testNextNoDupMinimal(t, tc.keys, tc.dups)
		})
	}
}

func testNextNoDupMinimal(t *testing.T, numKeys, numDups int) {
	dir, err := os.MkdirTemp("", "gdbx-nnd-min-*")
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

	// Insert
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	dbi, err := txn.OpenDBISimple("test", gdbx.Create|gdbx.DupSort)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	for keyIdx := 0; keyIdx < numKeys; keyIdx++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(keyIdx))
		for dupIdx := 0; dupIdx < numDups; dupIdx++ {
			val := make([]byte, 16)
			binary.BigEndian.PutUint64(val[:8], uint64(dupIdx))
			binary.BigEndian.PutUint64(val[8:], uint64(keyIdx*1000+dupIdx))
			if err := txn.Put(dbi, key, val, 0); err != nil {
				txn.Abort()
				t.Fatal(err)
			}
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Read with Next+NextNoDup pattern
	txn, err = env.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Abort()

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()

	// Target: dup index = numDups/2 (middle of the dup range)
	targetDup := uint64(numDups / 2)

	// Iterate using Next+NextNoDup pattern
	foundKeys := make(map[uint64]bool)
	k, v, err := cursor.Get(nil, nil, gdbx.First)
	for k != nil {
		if err != nil && !gdbx.IsNotFound(err) {
			t.Fatalf("Iteration error: %v", err)
		}
		if k == nil {
			break
		}

		if len(v) >= 8 {
			dupVal := binary.BigEndian.Uint64(v[:8])
			if dupVal == targetDup {
				keyVal := binary.BigEndian.Uint64(k)
				foundKeys[keyVal] = true
				k, v, err = cursor.Get(nil, nil, gdbx.NextNoDup)
			} else {
				k, v, err = cursor.Get(nil, nil, gdbx.Next)
			}
		} else {
			k, v, err = cursor.Get(nil, nil, gdbx.Next)
		}
	}

	if len(foundKeys) != numKeys {
		t.Errorf("Found %d/%d keys with targetDup=%d", len(foundKeys), numKeys, targetDup)
		// Show which ones are missing
		for keyIdx := 0; keyIdx < numKeys; keyIdx++ {
			if !foundKeys[uint64(keyIdx)] {
				t.Logf("  MISSING key=%d", keyIdx)
			}
		}
	}
}
