package merger

import (
	"fmt"

	"github.com/jeremytregunna/bftree/internal/node"
	"github.com/jeremytregunna/bftree/internal/storage"
)

// MergeResult contains the result of merging a mini-page with a leaf page
type MergeResult struct {
	LeftPage  *node.MiniPage  // Left page after merge (always returned)
	RightPage *node.MiniPage  // Right page if split occurred (nil if no split)
	SplitKey  []byte          // Key that separates left and right pages
}

// Merger handles merging mini-pages with disk leaf pages
type Merger struct {
	store *storage.Storage
}

// NewMerger creates a new merger
func NewMerger(store *storage.Storage) *Merger {
	return &Merger{
		store: store,
	}
}

// MergeMiniPageToDisk merges a mini-page with its corresponding disk leaf page
func (m *Merger) MergeMiniPageToDisk(miniPage *node.MiniPage, leafPageOffset uint64) (*MergeResult, error) {
	// Get dirty records from mini-page
	dirtyRecords := miniPage.GetDirtyRecords()
	if len(dirtyRecords) == 0 {
		// Nothing to merge, return mini-page as is
		return &MergeResult{
			LeftPage:  miniPage,
			RightPage: nil,
			SplitKey:  nil,
		}, nil
	}

	// Read the existing leaf page from disk
	leafPageData, err := m.store.ReadPage(leafPageOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to read leaf page: %w", err)
	}

	// Parse leaf page records
	leafRecords, err := parseLeafPage(leafPageData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse leaf page: %w", err)
	}

	// Merge records: apply mini-page changes to leaf records
	mergedRecords := mergeRecords(leafRecords, dirtyRecords)

	// Check if merged page fits in single page
	if canFitInPage(mergedRecords) {
		// Write back to disk
		mergedPageData := serializeRecords(mergedRecords)
		_, err := m.store.WritePage(mergedPageData)
		if err != nil {
			return nil, fmt.Errorf("failed to write merged page: %w", err)
		}

		// Create new mini-page for the merged data
		newMiniPage := node.NewMiniPage(miniPage.LeafPageID, uint32(len(mergedPageData)))
		for _, rec := range mergedRecords {
			newMiniPage.Insert(rec.Key, rec.Value)
		}

		return &MergeResult{
			LeftPage:  newMiniPage,
			RightPage: nil,
			SplitKey:  nil,
		}, nil
	}

	// Page too large, need to split
	splitKey, leftRecords, rightRecords := splitRecords(mergedRecords)

	// Write left page
	leftPageData := serializeRecords(leftRecords)
	_, err = m.store.WritePage(leftPageData)
	if err != nil {
		return nil, fmt.Errorf("failed to write left page: %w", err)
	}

	// Write right page
	rightPageData := serializeRecords(rightRecords)
	_, err = m.store.WritePage(rightPageData)
	if err != nil {
		return nil, fmt.Errorf("failed to write right page: %w", err)
	}

	// Create mini-pages for split result
	leftMiniPage := node.NewMiniPage(miniPage.LeafPageID, uint32(len(leftPageData)))
	for _, rec := range leftRecords {
		leftMiniPage.Insert(rec.Key, rec.Value)
	}

	rightMiniPage := node.NewMiniPage(miniPage.LeafPageID+1, uint32(len(rightPageData)))
	for _, rec := range rightRecords {
		rightMiniPage.Insert(rec.Key, rec.Value)
	}

	return &MergeResult{
		LeftPage:  leftMiniPage,
		RightPage: rightMiniPage,
		SplitKey:  splitKey,
	}, nil
}

// Helper functions

func parseLeafPage(pageData []byte) ([]node.Record, error) {
	var records []node.Record
	offset := 0

	for offset < len(pageData) {
		key, value, newOffset, err := storage.ReadRecord(pageData, offset)
		if err != nil {
			if offset >= len(pageData)-4 {
				break // End of page
			}
			return nil, err
		}
		if len(key) == 0 {
			break // Empty record signals end
		}
		records = append(records, node.Record{Key: key, Value: value})
		offset = newOffset
	}

	return records, nil
}

func mergeRecords(leafRecords []node.Record, dirtyRecords []node.Record) []node.Record {
	// Create map of leaf records
	recordMap := make(map[string]node.Record)
	for _, rec := range leafRecords {
		if len(rec.Key) > 0 { // Skip empty keys
			recordMap[string(rec.Key)] = rec
		}
	}

	// Apply dirty records (updates/deletes)
	for _, dirty := range dirtyRecords {
		if len(dirty.Key) == 0 {
			continue
		}
		if dirty.Value == nil || len(dirty.Value) == 0 {
			// Tombstone - delete
			delete(recordMap, string(dirty.Key))
		} else {
			// Insert/Update
			recordMap[string(dirty.Key)] = dirty
		}
	}

	// Convert back to sorted slice
	result := make([]node.Record, 0, len(recordMap))
	for _, rec := range recordMap {
		result = append(result, rec)
	}

	// Sort records
	sortRecords(result)
	return result
}

func canFitInPage(records []node.Record) bool {
	size := 0
	for _, rec := range records {
		size += 4 + len(rec.Key) + len(rec.Value) // 4 bytes for lengths
	}
	return size <= storage.PageSize
}

func serializeRecords(records []node.Record) []byte {
	pageBuffer := make([]byte, storage.PageSize)
	offset := 0

	for _, rec := range records {
		newOffset, _ := storage.WriteRecord(pageBuffer, offset, rec.Key, rec.Value)
		offset = newOffset
	}

	return pageBuffer[:offset]
}

func splitRecords(records []node.Record) ([]byte, []node.Record, []node.Record) {
	mid := len(records) / 2
	if mid == 0 {
		mid = 1
	}

	splitKey := records[mid].Key
	leftRecords := records[:mid]
	rightRecords := records[mid:]

	return splitKey, leftRecords, rightRecords
}

func sortRecords(records []node.Record) {
	// Simple bubble sort for now
	for i := 0; i < len(records); i++ {
		for j := i + 1; j < len(records); j++ {
			if compare(records[j].Key, records[i].Key) < 0 {
				records[i], records[j] = records[j], records[i]
			}
		}
	}
}

func compare(a, b []byte) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] < b[i] {
			return -1
		} else if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	} else if len(a) > len(b) {
		return 1
	}
	return 0
}
