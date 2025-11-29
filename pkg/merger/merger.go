package merger

import (
	"fmt"

	"github.com/jeremytregunna/bftree/pkg/node"
	"github.com/jeremytregunna/bftree/pkg/storage"
)

// MergeResult contains the result of merging a mini-page with a leaf page
type MergeResult[K, V any] struct {
	LeftPage  *node.MiniPage[K, V]  // Left page after merge (always returned)
	RightPage *node.MiniPage[K, V]  // Right page if split occurred (nil if no split)
	SplitKey  K                      // Key that separates left and right pages
}

// Merger handles merging mini-pages with disk leaf pages
type Merger[K, V any] struct {
	store  *storage.Storage
	codecs *node.Codecs[K, V]
}

// NewMerger creates a new merger
func NewMerger[K, V any](store *storage.Storage, codecs *node.Codecs[K, V]) *Merger[K, V] {
	return &Merger[K, V]{
		store:  store,
		codecs: codecs,
	}
}

// MergeMiniPageToDisk merges a mini-page with its corresponding disk leaf page
func (m *Merger[K, V]) MergeMiniPageToDisk(miniPage *node.MiniPage[K, V], leafPageOffset uint64) (*MergeResult[K, V], error) {
	// Get dirty records from mini-page
	dirtyRecords := miniPage.GetDirtyRecords()
	if len(dirtyRecords) == 0 {
		// Nothing to merge, return mini-page as is
		var zeroKey K
		return &MergeResult[K, V]{
			LeftPage:  miniPage,
			RightPage: nil,
			SplitKey:  zeroKey,
		}, nil
	}

	// Read the existing leaf page from disk
	leafPageData, err := m.store.ReadPage(leafPageOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to read leaf page: %w", err)
	}

	// Parse leaf page records
	leafRecords, err := m.parseLeafPage(leafPageData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse leaf page: %w", err)
	}

	// Merge records: apply mini-page changes to leaf records
	mergedRecords := m.mergeRecords(leafRecords, dirtyRecords)

	// Check if merged page fits in single page
	if m.canFitInPage(mergedRecords) {
		// Write back to disk
		mergedPageData := m.serializeRecords(mergedRecords)
		_, err := m.store.WritePage(mergedPageData)
		if err != nil {
			return nil, fmt.Errorf("failed to write merged page: %w", err)
		}

		// Create new mini-page for the merged data
		newMiniPage := node.NewMiniPage[K, V](miniPage.LeafPageID, 4096, m.codecs)
		for _, rec := range mergedRecords {
			if !newMiniPage.InsertWithType(rec.Key, rec.Value, rec.Type) {
				// Grow if needed
				newMiniPage.Grow(8192)
				newMiniPage.InsertWithType(rec.Key, rec.Value, rec.Type)
			}
		}

		var zeroKey K
		return &MergeResult[K, V]{
			LeftPage:  newMiniPage,
			RightPage: nil,
			SplitKey:  zeroKey,
		}, nil
	}

	// Page too large, need to split
	splitKey, leftRecords, rightRecords := m.splitRecords(mergedRecords)

	// Write left page
	leftPageData := m.serializeRecords(leftRecords)
	_, err = m.store.WritePage(leftPageData)
	if err != nil {
		return nil, fmt.Errorf("failed to write left page: %w", err)
	}

	// Write right page
	rightPageData := m.serializeRecords(rightRecords)
	_, err = m.store.WritePage(rightPageData)
	if err != nil {
		return nil, fmt.Errorf("failed to write right page: %w", err)
	}

	// Create mini-pages for split result
	leftMiniPage := node.NewMiniPage[K, V](miniPage.LeafPageID, uint32(len(leftPageData)), m.codecs)
	for _, rec := range leftRecords {
		leftMiniPage.InsertWithType(rec.Key, rec.Value, rec.Type)
	}

	rightMiniPage := node.NewMiniPage[K, V](miniPage.LeafPageID+1, uint32(len(rightPageData)), m.codecs)
	for _, rec := range rightRecords {
		rightMiniPage.InsertWithType(rec.Key, rec.Value, rec.Type)
	}

	return &MergeResult[K, V]{
		LeftPage:  leftMiniPage,
		RightPage: rightMiniPage,
		SplitKey:  splitKey,
	}, nil
}

// ParseRecords deserializes records from raw page data
func (m *Merger[K, V]) ParseRecords(pageData []byte) ([]node.Record[K, V], error) {
	return m.parseLeafPage(pageData)
}

// Helper functions

func (m *Merger[K, V]) parseLeafPage(pageData []byte) ([]node.Record[K, V], error) {
	var records []node.Record[K, V]
	offset := 0

	for offset < len(pageData) {
		keyBytes, valueBytes, newOffset, err := storage.ReadRecord(pageData, offset)
		if err != nil {
			if offset >= len(pageData)-4 {
				break // End of page
			}
			return nil, err
		}
		if len(keyBytes) == 0 {
			break // Empty record signals end
		}

		// Deserialize key and value
		key, err := m.codecs.UnmarshalKey(keyBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal key: %w", err)
		}

		value, err := m.codecs.UnmarshalValue(valueBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal value: %w", err)
		}

		records = append(records, node.Record[K, V]{
			Key:   key,
			Value: value,
			Type:  node.Cache, // Records from disk are cached
		})
		offset = newOffset
	}

	return records, nil
}

func (m *Merger[K, V]) mergeRecords(leafRecords []node.Record[K, V], dirtyRecords []node.Record[K, V]) []node.Record[K, V] {
	// Create map of leaf records (keyed by string representation)
	recordMap := make(map[string]node.Record[K, V])
	for _, rec := range leafRecords {
		keyStr := string(m.codecs.MarshalKey(rec.Key))
		recordMap[keyStr] = rec
	}

	// Apply dirty records (updates/deletes)
	for _, dirty := range dirtyRecords {
		keyStr := string(m.codecs.MarshalKey(dirty.Key))

		if dirty.Type == node.Tombstone {
			// Tombstone - delete
			delete(recordMap, keyStr)
		} else {
			// Insert/Update
			recordMap[keyStr] = dirty
		}
	}

	// Convert back to sorted slice
	result := make([]node.Record[K, V], 0, len(recordMap))
	for _, rec := range recordMap {
		result = append(result, rec)
	}

	// Sort records
	m.sortRecords(result)
	return result
}

func (m *Merger[K, V]) canFitInPage(records []node.Record[K, V]) bool {
	size := 0
	for _, rec := range records {
		keyBytes := m.codecs.MarshalKey(rec.Key)
		valueBytes := m.codecs.MarshalValue(rec.Value)
		size += 4 + len(keyBytes) + len(valueBytes) // 4 bytes for lengths
	}
	return size <= storage.PageSize
}

func (m *Merger[K, V]) serializeRecords(records []node.Record[K, V]) []byte {
	pageBuffer := make([]byte, storage.PageSize)
	offset := 0

	for _, rec := range records {
		keyBytes := m.codecs.MarshalKey(rec.Key)
		valueBytes := m.codecs.MarshalValue(rec.Value)
		newOffset, _ := storage.WriteRecord(pageBuffer, offset, keyBytes, valueBytes)
		offset = newOffset
	}

	return pageBuffer[:offset]
}

func (m *Merger[K, V]) splitRecords(records []node.Record[K, V]) (K, []node.Record[K, V], []node.Record[K, V]) {
	mid := len(records) / 2
	if mid == 0 {
		mid = 1
	}

	splitKey := records[mid].Key
	leftRecords := records[:mid]
	rightRecords := records[mid:]

	return splitKey, leftRecords, rightRecords
}

func (m *Merger[K, V]) sortRecords(records []node.Record[K, V]) {
	// Simple bubble sort for now
	for i := 0; i < len(records); i++ {
		for j := i + 1; j < len(records); j++ {
			if m.codecs.Compare(records[j].Key, records[i].Key) < 0 {
				records[i], records[j] = records[j], records[i]
			}
		}
	}
}
