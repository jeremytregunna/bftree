package tree

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/jeremytregunna/bftree/pkg/buffer"
	"github.com/jeremytregunna/bftree/pkg/merger"
	"github.com/jeremytregunna/bftree/pkg/node"
	"github.com/jeremytregunna/bftree/pkg/storage"
)

// BfTree is the main B-Tree structure optimized for read-write workloads.
type BfTree struct {
	// Root node (pinned in memory)
	root *node.InnerNode

	// Buffer pool for mini-pages
	bufferPool *buffer.CircularBuffer

	// Buffer pool size limit
	bufferPoolSize uint64

	// Mapping table for page ID to physical address
	mappingTable *MappingTable

	// Storage for disk persistence
	store *storage.Storage

	// Merger for combining mini-pages with disk pages
	m *merger.Merger

	// Next page ID counter
	nextPageID uint64

	// Mini-pages index (pageID -> MiniPage)
	miniPages sync.Map

	// Memory chunks for mini-pages (pageID -> *buffer.MemoryChunk)
	miniPageChunks sync.Map

	// Total size of all mini-pages in memory for eviction tracking
	totalMiniPageSize uint64

	// Global lock for tree modifications
	treeMu sync.RWMutex
}

// NewBfTree creates a new Bf-Tree with the given buffer size and storage file.
func NewBfTree(bufferSizeBytes uint64, storePath string) (*BfTree, error) {
	// Create storage
	store, err := storage.NewStorage(storePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage: %w", err)
	}

	tree := &BfTree{
		root:           node.NewInnerNode(),
		bufferPool:     buffer.NewCircularBuffer(bufferSizeBytes),
		bufferPoolSize: bufferSizeBytes,
		mappingTable:   NewMappingTable(),
		store:          store,
		m:              merger.NewMerger(store),
		nextPageID:     1, // Start page IDs from 1
	}

	// Initialize root with first leaf page
	leafPageID := tree.allocatePageID()
	tree.mappingTable.Insert(leafPageID, 0) // Location 0 is disk offset

	// Add leaf page ID to root
	tree.root.LeafPageIDs[0] = leafPageID

	return tree, nil
}

// Get retrieves a value for the given key.
func (t *BfTree) Get(key []byte) ([]byte, error) {
	// Traverse the tree to find the leaf page
	leafPageID := t.traverse(key)
	if leafPageID == 0 {
		return nil, fmt.Errorf("key not found")
	}

	// Check mini-page first
	if mp, ok := t.miniPages.Load(leafPageID); ok {
		if value, found := mp.(*node.MiniPage).Search(key); found {
			return value, nil
		}
	}

	// Load from disk if not in mini-page
	mapEntry := t.mappingTable.Get(leafPageID)
	if mapEntry == nil {
		return nil, fmt.Errorf("key not found")
	}

	pageData, err := t.store.ReadPage(mapEntry.Location)
	if err != nil {
		return nil, fmt.Errorf("failed to read page from disk: %w", err)
	}

	// Parse records from page and search
	offset := 0
	for offset < len(pageData) {
		k, v, newOffset, err := storage.ReadRecord(pageData, offset)
		if err != nil {
			if offset >= len(pageData)-4 {
				break // End of page
			}
			return nil, fmt.Errorf("failed to read record: %w", err)
		}
		if len(k) == 0 {
			break // Empty record signals end
		}
		if compare(k, key) == 0 {
			return v, nil
		}
		offset = newOffset
	}

	return nil, fmt.Errorf("key not found")
}

// Insert inserts a key-value pair into the tree.
func (t *BfTree) Insert(key, value []byte) error {
	t.treeMu.Lock()
	defer t.treeMu.Unlock()

	// Traverse to find the leaf page
	leafPageID := t.traverse(key)
	if leafPageID == 0 {
		return fmt.Errorf("invalid leaf page")
	}

	// Get or create mini-page
	mp := t.getOrCreateMiniPage(leafPageID)

	// Try to insert into mini-page
	if mp.Insert(key, value) {
		return nil
	}

	// Mini-page is full, try to grow it
	if mp.Meta.NodeSize < 4096 {
		newSize := mp.Meta.NodeSize * 2
		if newSize > 4096 || newSize < mp.Meta.NodeSize {
			// Cap at 4096 or prevent overflow
			newSize = 4096
		}
		t.growMiniPageInBuffer(leafPageID, mp, newSize)

		if mp.Insert(key, value) {
			return nil
		}
	}

	// Mini-page too large, need to merge to disk
	mapEntry := t.mappingTable.Get(leafPageID)
	if mapEntry == nil {
		return fmt.Errorf("leaf page mapping not found")
	}

	result, err := t.m.MergeMiniPageToDisk(mp, mapEntry.Location)
	if err != nil {
		return fmt.Errorf("merge failed: %w", err)
	}

	// Update the merged mini-page
	t.miniPages.Store(leafPageID, result.LeftPage)

	// If split occurred, allocate new page ID and store right page
	var rightPageID uint64
	if result.RightPage != nil {
		rightPageID = t.allocatePageID()
		rightOffset := mapEntry.Location + uint64(storage.PageSize)
		t.mappingTable.Insert(rightPageID, rightOffset)
		t.miniPages.Store(rightPageID, result.RightPage)

		// Update inner node to reflect the split
		t.root.InsertLeafPageSplit(result.SplitKey, rightPageID)

		// Check if root needs to split
		t.splitRootIfFull()
	}

	// Try to insert the key into the appropriate page
	targetMP := result.LeftPage
	targetPageID := leafPageID
	if result.RightPage != nil && compare(key, result.SplitKey) >= 0 {
		targetMP = result.RightPage
		targetPageID = rightPageID
	}

	if !targetMP.Insert(key, value) {
		// Page is full after merge, try to grow it
		if targetMP.Meta.NodeSize < 4096 {
			newSize := targetMP.Meta.NodeSize * 2
			if newSize > 4096 || newSize < targetMP.Meta.NodeSize {
				newSize = 4096
			}
			t.growMiniPageInBuffer(targetPageID, targetMP, newSize)

			if !targetMP.Insert(key, value) {
				return fmt.Errorf("insert failed after merge and grow: page full")
			}
		} else {
			return fmt.Errorf("insert failed after merge: page full and max size reached")
		}
	}
	return nil
}

// Delete deletes a key from the tree by inserting a tombstone.
func (t *BfTree) Delete(key []byte) error {
	t.treeMu.Lock()
	defer t.treeMu.Unlock()

	// Traverse to find the leaf page
	leafPageID := t.traverse(key)
	if leafPageID == 0 {
		return fmt.Errorf("invalid leaf page")
	}

	// Get or create mini-page
	mp := t.getOrCreateMiniPage(leafPageID)

	// Insert tombstone
	mp.InsertWithType(key, nil, node.Tombstone)
	return nil
}

// Update updates a key-value pair in the tree.
func (t *BfTree) Update(key, value []byte) error {
	t.treeMu.Lock()
	defer t.treeMu.Unlock()

	// Traverse to find the leaf page
	leafPageID := t.traverse(key)
	if leafPageID == 0 {
		return fmt.Errorf("invalid leaf page")
	}

	// Get or create mini-page
	mp := t.getOrCreateMiniPage(leafPageID)

	// Try to update in mini-page
	if mp.InsertWithType(key, value, node.Insert) {
		return nil
	}

	// Mini-page is full, try to grow it
	if mp.Meta.NodeSize < 4096 {
		newSize := mp.Meta.NodeSize * 2
		if newSize > 4096 || newSize < mp.Meta.NodeSize {
			// Cap at 4096 or prevent overflow
			newSize = 4096
		}
		t.growMiniPageInBuffer(leafPageID, mp, newSize)

		if mp.InsertWithType(key, value, node.Insert) {
			return nil
		}
	}

	// Mini-page too large, need to merge to disk
	mapEntry := t.mappingTable.Get(leafPageID)
	if mapEntry == nil {
		return fmt.Errorf("leaf page mapping not found")
	}

	result, err := t.m.MergeMiniPageToDisk(mp, mapEntry.Location)
	if err != nil {
		return fmt.Errorf("merge failed: %w", err)
	}

	// Update the merged mini-page
	t.miniPages.Store(leafPageID, result.LeftPage)

	// If split occurred, allocate new page ID and store right page
	var rightPageID uint64
	if result.RightPage != nil {
		rightPageID = t.allocatePageID()
		rightOffset := mapEntry.Location + uint64(storage.PageSize)
		t.mappingTable.Insert(rightPageID, rightOffset)
		t.miniPages.Store(rightPageID, result.RightPage)

		// Update inner node to reflect the split
		t.root.InsertLeafPageSplit(result.SplitKey, rightPageID)

		// Check if root needs to split
		t.splitRootIfFull()
	}

	// Try to update the key into the appropriate page
	targetMP := result.LeftPage
	targetPageID := leafPageID
	if result.RightPage != nil && compare(key, result.SplitKey) >= 0 {
		targetMP = result.RightPage
		targetPageID = rightPageID
	}

	if !targetMP.InsertWithType(key, value, node.Insert) {
		// Page is full after merge, try to grow it
		if targetMP.Meta.NodeSize < 4096 {
			newSize := targetMP.Meta.NodeSize * 2
			if newSize > 4096 || newSize < targetMP.Meta.NodeSize {
				newSize = 4096
			}
			t.growMiniPageInBuffer(targetPageID, targetMP, newSize)

			if !targetMP.InsertWithType(key, value, node.Insert) {
				return fmt.Errorf("update failed after merge and grow: page full")
			}
		} else {
			return fmt.Errorf("update failed after merge: page full and max size reached")
		}
	}
	return nil
}

// Scan performs a range scan over keys in the given range [startKey, endKey).
func (t *BfTree) Scan(startKey, endKey []byte) ([]node.Record, error) {
	t.treeMu.RLock()
	defer t.treeMu.RUnlock()

	var results []node.Record
	scanned := make(map[uint64]bool) // Track scanned pages to avoid duplicates

	// Find the starting leaf page
	leafPageID := t.traverse(startKey)
	if leafPageID == 0 {
		return results, fmt.Errorf("invalid leaf page")
	}

	// Get the starting index in root's leaf page array
	startIndex := -1
	for i, id := range t.root.LeafPageIDs {
		if id == leafPageID {
			startIndex = i
			break
		}
	}
	if startIndex == -1 {
		// Fallback: scan just the first page if we can't find it
		startIndex = 0
	}

	// Scan from the starting page through all relevant leaf pages
	for pageIndex := startIndex; pageIndex < len(t.root.LeafPageIDs); pageIndex++ {
		currentPageID := t.root.LeafPageIDs[pageIndex]
		if scanned[currentPageID] {
			continue // Skip if already scanned
		}
		scanned[currentPageID] = true

		// Check if this page could contain records in our range
		// We can stop scanning if the page's minimum key is >= endKey
		// But since we don't track min keys, we conservatively scan all remaining pages
		// until we find no more data in range

		// Scan the mini-page if it exists
		if mp, ok := t.miniPages.Load(currentPageID); ok {
			miniPageRecords := mp.(*node.MiniPage).GetRecords()
			for _, record := range miniPageRecords {
				if compare(record.Key, startKey) >= 0 && compare(record.Key, endKey) < 0 {
					results = append(results, record)
				}
			}
		}

		// Scan disk page if it exists
		mapEntry := t.mappingTable.Get(currentPageID)
		if mapEntry != nil && t.store != nil {
			pageData, err := t.store.ReadPage(mapEntry.Location)
			if err == nil {
				offset := 0
				for offset < len(pageData) {
					k, v, newOffset, err := storage.ReadRecord(pageData, offset)
					if err != nil {
						if offset >= len(pageData)-4 {
							break // End of page
						}
					}
					if len(k) == 0 {
						break // Empty record signals end
					}

					if compare(k, startKey) >= 0 && compare(k, endKey) < 0 {
						// Check if this key is already in results
						found := false
						for _, r := range results {
							if compare(r.Key, k) == 0 {
								found = true
								break
							}
						}
						if !found {
							results = append(results, node.Record{Key: k, Value: v})
						}
					}
					offset = newOffset
				}
			}
		}
	}

	return results, nil
}

// compare compares two byte slices
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

// Private helper methods

// traverse traverses the tree to find the leaf page ID for a given key.
func (t *BfTree) traverse(key []byte) uint64 {
	current := t.root
	version := current.AcquireReadLock()
	defer current.ReleaseReadLock()

	// Traverse inner nodes until we reach the leaf level
	for {
		childIdx := current.Search(key)

		// Check if we should read the next inner node or go to leaf
		if childIdx >= len(current.Children) || current.Children[childIdx] == nil {
			// We've reached the leaf level
			if childIdx >= len(current.LeafPageIDs) {
				return 0
			}
			return current.LeafPageIDs[childIdx]
		}

		// Move to next inner node
		nextNode := current.Children[childIdx]
		if !nextNode.ValidateReadLock(version) {
			// Version mismatch, retry
			return t.traverse(key)
		}

		version = nextNode.AcquireReadLock()
		current.ReleaseReadLock()
		current = nextNode
	}
}

// getOrCreateMiniPage returns the mini-page for the given leaf page ID,
// creating one if it doesn't exist. Allocates kvData from circular buffer.
func (t *BfTree) getOrCreateMiniPage(leafPageID uint64) *node.MiniPage {
	// Check if mini-page already exists
	if mp, ok := t.miniPages.Load(leafPageID); ok {
		return mp.(*node.MiniPage)
	}

	// Check if we need to evict before creating new mini-page
	const initialSize = 64
	if atomic.LoadUint64(&t.totalMiniPageSize)+initialSize > t.bufferPoolSize {
		t.evictMiniPage()
	}

	// Allocate chunk from circular buffer for kvData
	chunk, err := t.bufferPool.Alloc(initialSize)
	if err != nil {
		// If allocation fails, try evicting and retry
		t.evictMiniPage()
		chunk, err = t.bufferPool.Alloc(initialSize)
		if err != nil {
			// Fall back to heap allocation (graceful degradation)
			mp := node.NewMiniPage(leafPageID, initialSize)
			actual, _ := t.miniPages.LoadOrStore(leafPageID, mp)
			atomic.AddUint64(&t.totalMiniPageSize, initialSize)
			return actual.(*node.MiniPage)
		}
	}

	// Create mini-page with buffer-allocated kvData
	mp := t.createMiniPageFromChunk(leafPageID, chunk)

	// Store in maps
	actual, _ := t.miniPages.LoadOrStore(leafPageID, mp)
	t.miniPageChunks.Store(leafPageID, chunk)

	// Track size
	atomic.AddUint64(&t.totalMiniPageSize, initialSize)

	return actual.(*node.MiniPage)
}

// createMiniPageFromChunk creates a mini-page using data from a circular buffer chunk
func (t *BfTree) createMiniPageFromChunk(leafPageID uint64, chunk *buffer.MemoryChunk) *node.MiniPage {
	metaSize := uint32(binary.Size(&node.NodeMeta{}))
	usableSize := uint32(chunk.Size - uint64(metaSize))

	// Create mini-page backed by buffer chunk (kvData is a slice into the chunk)
	kvDataSlice := chunk.Data[metaSize : metaSize+usableSize]
	return node.NewMiniPageFromBuffer(leafPageID, kvDataSlice, chunk.Size)
}

// growMiniPageInBuffer grows a mini-page with new buffer allocation.
func (t *BfTree) growMiniPageInBuffer(leafPageID uint64, mp *node.MiniPage, newSize uint32) {
	oldSize := mp.Meta.NodeSize
	if newSize <= oldSize {
		return
	}

	// Try to allocate new chunk from buffer
	newChunk, err := t.bufferPool.Alloc(uint64(newSize))
	if err != nil {
		// If allocation fails, try evicting and retrying
		t.evictMiniPage()
		newChunk, err = t.bufferPool.Alloc(uint64(newSize))
		if err != nil {
			// Fall back to heap growth
			mp.Grow(newSize)
			sizeIncrease := uint64(newSize - oldSize)
			atomic.AddUint64(&t.totalMiniPageSize, sizeIncrease)
			return
		}
	}

	// Copy old kvData to new chunk
	oldData := mp.KvData
	usedDataLen := mp.GetDataUsed() // Get how much data is actually used
	metaSize := uint32(binary.Size(&node.NodeMeta{}))
	newKvData := newChunk.Data[metaSize : metaSize+usedDataLen]
	copy(newKvData, oldData[:usedDataLen]) // Only copy used data

	// Deallocate old chunk if it exists
	if oldChunkIface, ok := t.miniPageChunks.Load(leafPageID); ok {
		t.bufferPool.Dealloc(oldChunkIface.(*buffer.MemoryChunk))
	}

	// Update mini-page with new data backing (full new capacity)
	newCapacity := newSize - metaSize
	mp.KvData = newChunk.Data[metaSize : metaSize+newCapacity]
	mp.Meta.NodeSize = newSize

	// Store new chunk
	t.miniPageChunks.Store(leafPageID, newChunk)

	// Track size increase
	sizeIncrease := uint64(newSize - oldSize)
	atomic.AddUint64(&t.totalMiniPageSize, sizeIncrease)

	// Check if we need to evict due to growth
	if atomic.LoadUint64(&t.totalMiniPageSize) > t.bufferPoolSize {
		t.evictMiniPage()
	}
}

// evictMiniPage finds and evicts the least-recently-used mini-page to disk
func (t *BfTree) evictMiniPage() error {
	var evictPageID uint64
	var evictMP *node.MiniPage

	// Scan mini-pages to find one with dirty records (needs persistence)
	t.miniPages.Range(func(key, value interface{}) bool {
		pageID := key.(uint64)
		mp := value.(*node.MiniPage)

		// Check if this mini-page has any records needing persistence
		dirtyRecords := mp.GetDirtyRecords()
		if len(dirtyRecords) > 0 {
			// This page has dirty data, it's a good candidate
			evictPageID = pageID
			evictMP = mp
			return false // Stop scanning, found one
		}
		return true
	})

	if evictPageID == 0 {
		return fmt.Errorf("no evictable mini-page found")
	}

	// Get the mapping entry for this leaf page
	mapEntry := t.mappingTable.Get(evictPageID)
	if mapEntry == nil {
		evictedSize := uint64(evictMP.Meta.NodeSize)
		t.miniPages.Delete(evictPageID)
		// Deallocate chunk if it exists
		if chunkIface, ok := t.miniPageChunks.LoadAndDelete(evictPageID); ok {
			t.bufferPool.Dealloc(chunkIface.(*buffer.MemoryChunk))
		}
		atomic.AddUint64(&t.totalMiniPageSize, ^(evictedSize - 1)) // Subtract size
		return nil
	}

	// Merge the mini-page to disk
	result, err := t.m.MergeMiniPageToDisk(evictMP, mapEntry.Location)
	if err != nil {
		return fmt.Errorf("eviction merge failed: %w", err)
	}

	// Deallocate the old chunk
	if oldChunkIface, ok := t.miniPageChunks.LoadAndDelete(evictPageID); ok {
		t.bufferPool.Dealloc(oldChunkIface.(*buffer.MemoryChunk))
	}

	// Update the merged mini-page and handle splits
	evictedSize := uint64(evictMP.Meta.NodeSize)
	newSize := uint64(result.LeftPage.Meta.NodeSize)
	atomic.AddUint64(&t.totalMiniPageSize, ^(evictedSize-1)) // Subtract old size
	atomic.AddUint64(&t.totalMiniPageSize, newSize)          // Add new size
	t.miniPages.Store(evictPageID, result.LeftPage)

	if result.RightPage != nil {
		rightPageID := t.allocatePageID()
		rightOffset := mapEntry.Location + uint64(storage.PageSize)
		t.mappingTable.Insert(rightPageID, rightOffset)
		t.miniPages.Store(rightPageID, result.RightPage)
		atomic.AddUint64(&t.totalMiniPageSize, uint64(result.RightPage.Meta.NodeSize))
		t.root.InsertLeafPageSplit(result.SplitKey, rightPageID)
		t.splitRootIfFull()
	}

	return nil
}

// allocatePageID allocates a new page ID.
func (t *BfTree) allocatePageID() uint64 {
	return atomic.AddUint64(&t.nextPageID, 1)
}

// Close closes the tree and releases resources
func (t *BfTree) Close() error {
	if t.store != nil {
		return t.store.Close()
	}
	return nil
}

// splitRootIfFull splits the root node if it exceeds capacity
func (t *BfTree) splitRootIfFull() {
	if !t.root.IsFull() {
		return // Root not full, no split needed
	}

	// Split the current root
	splitKey, rightNode := t.root.Split()
	if splitKey == nil || rightNode == nil {
		return // Split failed
	}

	// Create a new root node with proper structure
	newRoot := &node.InnerNode{
		Meta: &node.NodeMeta{
			PageType:  0, // Inner node
			SplitFlag: 0,
			RecordCnt: 1,
		},
		Keys:        [][]byte{splitKey},
		Children:    []*node.InnerNode{t.root, rightNode},
		LeafPageIDs: []uint64{t.root.LeafPageIDs[0], rightNode.LeafPageIDs[0]},
		Version:     0,
	}

	// Update tree root
	t.root = newRoot
}

// Stats returns statistics about the tree.
func (t *BfTree) Stats() map[string]interface{} {
	return map[string]interface{}{
		"next_page_id": atomic.LoadUint64(&t.nextPageID),
		"mini_pages_count": countMiniPages(t),
	}
}

func countMiniPages(t *BfTree) int {
	count := 0
	t.miniPages.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}
