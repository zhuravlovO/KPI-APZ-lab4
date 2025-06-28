package datastore

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSegmentRotationAndCompaction(t *testing.T) {
	if os.Getenv("SKIP_LONG_TESTS") == "1" {
		t.Skip("skipping long test in Docker")
	}

	dir := t.TempDir()
	db, err := NewDb(dir, 1024)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	for i := 0; i < 200; i++ {
		key := "key-" + strconv.Itoa(i)
		value := strings.Repeat("x", 20)
		if err := db.Put(key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	if err := db.Delete("key-150"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	for i := 0; i < 50; i++ {
		db.Put("pad-"+strconv.Itoa(i), strings.Repeat("y", 30))
	}

	_, err = db.Get("key-150")
	if err == nil {
		t.Error("expected an error for deleted key, but got nil")
	} else if err != ErrNotFound {
		t.Errorf("expected ErrNotFound for deleted key, got %v", err)
	}

	t.Log("waiting for compaction...")
	time.Sleep(1 * time.Second)

	_, err = db.Get("key-150")
	if err == nil {
		t.Error("expected an error for deleted key after compaction, but got nil")
	} else if err != ErrNotFound {
		t.Errorf("expected ErrNotFound after compaction, got %v", err)
	}
}
