package merger

import (
	"fmt"
	"os"
	"testing"

	"github.com/jeremytregunna/bftree/pkg/node"
	"github.com/jeremytregunna/bftree/pkg/storage"
)

func TestMergeSimple(t *testing.T) {
	// Create temporary storage
	tmpFile, err := os.CreateTemp("", "bftree-merger-*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	store, err := storage.NewStorage(tmpFile.Name())
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer store.Close()

	merger := NewMerger(store)

	// Create initial leaf page with one record
	initialRecords := []node.Record{
		{Key: []byte("key1"), Value: []byte("value1")},
	}
	initialPageData := serializeRecords(initialRecords)
	leafOffset, err := store.WritePage(initialPageData)
	if err != nil {
		t.Fatalf("failed to write initial page: %v", err)
	}

	// Create mini-page with update
	miniPage := node.NewMiniPage(1, 64)
	miniPage.Insert([]byte("key1"), []byte("updated_value1"))

	// Merge
	result, err := merger.MergeMiniPageToDisk(miniPage, leafOffset)
	if err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	// Verify result
	if result.RightPage != nil {
		t.Fatal("expected no split, but got right page")
	}

	// Verify the merged page has the updated value
	_, found := result.LeftPage.Search([]byte("key1"))
	if !found {
		t.Fatal("expected to find key1 after merge")
	}

	value, found := result.LeftPage.Search([]byte("key1"))
	if !found || string(value) != "updated_value1" {
		t.Fatalf("expected updated_value1, got %v", string(value))
	}
}

func TestMergeWithDelete(t *testing.T) {
	// Create temporary storage
	tmpFile, err := os.CreateTemp("", "bftree-merger-*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	store, err := storage.NewStorage(tmpFile.Name())
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer store.Close()

	merger := NewMerger(store)

	// Create initial leaf page
	initialRecords := []node.Record{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
	}
	initialPageData := serializeRecords(initialRecords)
	leafOffset, err := store.WritePage(initialPageData)
	if err != nil {
		t.Fatalf("failed to write initial page: %v", err)
	}

	// Create mini-page with delete
	miniPage := node.NewMiniPage(1, 64)
	miniPage.InsertWithType([]byte("key1"), nil, node.Tombstone)

	// Merge
	result, err := merger.MergeMiniPageToDisk(miniPage, leafOffset)
	if err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	// Verify key1 is deleted
	_, found := result.LeftPage.Search([]byte("key1"))
	if found {
		t.Fatal("expected key1 to be deleted")
	}

	// Verify key2 still exists
	_, found = result.LeftPage.Search([]byte("key2"))
	if !found {
		t.Fatal("expected key2 to still exist")
	}
}

func TestMergeSplit(t *testing.T) {
	// Create temporary storage
	tmpFile, err := os.CreateTemp("", "bftree-merger-*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	store, err := storage.NewStorage(tmpFile.Name())
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer store.Close()

	merger := NewMerger(store)

	// Create large leaf page that fills most of the storage page
	var initialRecords []node.Record
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := make([]byte, 40) // Larger values to fill space
		initialRecords = append(initialRecords, node.Record{Key: key, Value: value})
	}

	initialPageData := serializeRecords(initialRecords)
	leafOffset, err := store.WritePage(initialPageData)
	if err != nil {
		t.Fatalf("failed to write initial page: %v", err)
	}

	// Create mini-page with more inserts to cause split (exceeds page size)
	miniPage := node.NewMiniPage(1, 64)
	insertedCount := 0
	for i := 50; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := make([]byte, 40) // Same size values

		// Try to insert, grow if needed (matching tree behavior)
		success := false
		for !success && miniPage.Meta.NodeSize < 4096 {
			if miniPage.Insert(key, value) {
				success = true
				insertedCount++
				break
			}
			newSize := miniPage.Meta.NodeSize * 2
			if newSize > 4096 {
				newSize = 4096
			}
			miniPage.Grow(newSize)
		}

		// Final attempt at max size
		if !success && miniPage.Insert(key, value) {
			insertedCount++
		}
	}
	t.Logf("Successfully inserted %d records into mini-page", insertedCount)

	// Check how many records we actually have
	miniPageRecords := miniPage.GetRecords()
	t.Logf("Mini-page has %d records", len(miniPageRecords))

	// Merge - should trigger split
	result, err := merger.MergeMiniPageToDisk(miniPage, leafOffset)
	if err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	// Verify split occurred
	if result.RightPage == nil {
		t.Fatal("expected split to occur but got no right page")
	}

	if len(result.SplitKey) == 0 {
		t.Fatal("expected split key")
	}

	// Verify both pages have records
	leftRecords := result.LeftPage.GetRecords()
	rightRecords := result.RightPage.GetRecords()

	if len(leftRecords) == 0 || len(rightRecords) == 0 {
		t.Fatalf("expected both pages to have records, got left=%d, right=%d", len(leftRecords), len(rightRecords))
	}

	// Verify split key separates them
	for _, rec := range leftRecords {
		if compare(rec.Key, result.SplitKey) >= 0 {
			t.Fatalf("left page key %s should be < split key %s", string(rec.Key), string(result.SplitKey))
		}
	}

	for _, rec := range rightRecords {
		if compare(rec.Key, result.SplitKey) < 0 {
			t.Fatalf("right page key %s should be >= split key %s", string(rec.Key), string(result.SplitKey))
		}
	}
}
