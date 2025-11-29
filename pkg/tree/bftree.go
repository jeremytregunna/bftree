package tree

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/jeremytregunna/bftree/pkg/buffer"
	"github.com/jeremytregunna/bftree/pkg/merger"
	"github.com/jeremytregunna/bftree/pkg/node"
	"github.com/jeremytregunna/bftree/pkg/storage"
)

// BfTree is a generic Buffer-efficient B-Tree for key-value storage.
type BfTree[K, V any] struct {
	// Root node (pinned in memory)
	root *node.InnerNode[K]

	// Buffer pool for mini-pages
	bufferPool *buffer.CircularBuffer

	// Buffer pool size limit
	bufferPoolSize uint64

	// Mapping table for page ID to physical address
	mappingTable *MappingTable

	// Storage for disk persistence
	store *storage.Storage

	// Merger for combining mini-pages with disk pages
	m *merger.Merger[K, V]

	// Next page ID counter
	nextPageID uint64

	// Mini-pages index (pageID -> MiniPage)
	miniPages sync.Map

	// Memory chunks for mini-pages (pageID -> *buffer.MemoryChunk)
	miniPageChunks sync.Map

	// Total size of all mini-pages in memory for eviction tracking
	totalMiniPageSize uint64

	// Codecs for serialization
	codecs *node.Codecs[K, V]

	// Global lock for tree modifications
	treeMu sync.RWMutex
}

// NewBfTree creates a new generic Bf-Tree with given buffer size and storage file.
func NewBfTree[K, V any](bufferSizeBytes uint64, storePath string, codecs *node.Codecs[K, V]) (*BfTree[K, V], error) {
	// Create storage
	store, err := storage.NewStorage(storePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage: %w", err)
	}

	tree := &BfTree[K, V]{
		root:           node.NewInnerNode(codecs.Compare),
		bufferPool:     buffer.NewCircularBuffer(bufferSizeBytes),
		bufferPoolSize: bufferSizeBytes,
		mappingTable:   NewMappingTable(),
		store:          store,
		m:              merger.NewMerger(store, codecs),
		nextPageID:     1,
		codecs:         codecs,
	}

	// Initialize root with first leaf page
	leafPageID := tree.allocatePageID()
	tree.mappingTable.Insert(leafPageID, 0)
	tree.root.LeafPageIDs[0] = leafPageID

	return tree, nil
}

// Get retrieves a value for the given key.
func (t *BfTree[K, V]) Get(key K) (V, error) {
	// Traverse the tree to find the leaf page
	leafPageID := t.traverse(key)
	if leafPageID == 0 {
		var zero V
		return zero, fmt.Errorf("key not found")
	}

	// Check mini-page first
	if mp, ok := t.miniPages.Load(leafPageID); ok {
		if value, found := mp.(*node.MiniPage[K, V]).Search(key); found {
			return value, nil
		}
	}

	// Load from disk if not in mini-page
	mapEntry := t.mappingTable.Get(leafPageID)
	if mapEntry == nil {
		var zero V
		return zero, fmt.Errorf("key not found")
	}

	pageData, err := t.store.ReadPage(mapEntry.Location)
	if err != nil {
		var zero V
		return zero, fmt.Errorf("failed to read page from disk: %w", err)
	}

	// Parse records from page and search
	records, err := t.m.ParseRecords(pageData)
	if err != nil {
		var zero V
		return zero, err
	}

	for _, rec := range records {
		if t.codecs.Compare(rec.Key, key) == 0 {
			return rec.Value, nil
		}
	}

	var zero V
	return zero, fmt.Errorf("key not found")
}

// Insert inserts a key-value pair into the tree.
func (t *BfTree[K, V]) Insert(key K, value V) error {
	t.treeMu.Lock()
	defer t.treeMu.Unlock()

	// Find leaf page
	leafPageID := t.traverse(key)
	if leafPageID == 0 {
		return fmt.Errorf("failed to traverse tree")
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
	if result.RightPage != nil && t.codecs.Compare(key, result.SplitKey) >= 0 {
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
func (t *BfTree[K, V]) Delete(key K) error {
	t.treeMu.Lock()
	defer t.treeMu.Unlock()

	leafPageID := t.traverse(key)
	if leafPageID == 0 {
		return fmt.Errorf("key not found")
	}

	_ = t.getOrCreateMiniPage(leafPageID)

	var zeroValue V
	return t.insertRecord(leafPageID, key, zeroValue, node.Tombstone)
}

// Update updates an existing key with a new value.
func (t *BfTree[K, V]) Update(key K, value V) error {
	t.treeMu.Lock()
	defer t.treeMu.Unlock()

	leafPageID := t.traverse(key)
	if leafPageID == 0 {
		return fmt.Errorf("key not found")
	}

	return t.insertRecord(leafPageID, key, value, node.Insert)
}

// Scan returns all records in the range [startKey, endKey).
func (t *BfTree[K, V]) Scan(startKey, endKey K) ([]node.Record[K, V], error) {
	t.treeMu.RLock()
	defer t.treeMu.RUnlock()

	var results []node.Record[K, V]

	// Get starting leaf page
	leafPageID := t.traverse(startKey)
	if leafPageID == 0 {
		return results, nil
	}

	// Scan all leaf pages in range
	for leafPageID > 0 {
		// Check mini-page
		if mp, ok := t.miniPages.Load(leafPageID); ok {
			recs := mp.(*node.MiniPage[K, V]).GetRecords()
			for _, rec := range recs {
				if t.codecs.Compare(rec.Key, startKey) >= 0 && t.codecs.Compare(rec.Key, endKey) < 0 {
					results = append(results, rec)
				}
			}
		}

		// Check disk
		mapEntry := t.mappingTable.Get(leafPageID)
		if mapEntry != nil {
			pageData, err := t.store.ReadPage(mapEntry.Location)
			if err == nil {
				recs, err := t.m.ParseRecords(pageData)
				if err == nil {
					for _, rec := range recs {
						if t.codecs.Compare(rec.Key, startKey) >= 0 && t.codecs.Compare(rec.Key, endKey) < 0 {
							results = append(results, rec)
						}
					}
				}
			}
		}

		// Move to next leaf page
		idx := t.root.Search(startKey)
		if idx+1 < len(t.root.LeafPageIDs) {
			leafPageID = t.root.LeafPageIDs[idx+1]
		} else {
			break
		}
	}

	return results, nil
}

// Close closes the tree and releases resources
func (t *BfTree[K, V]) Close() error {
	if t.store != nil {
		return t.store.Close()
	}
	return nil
}

// Helper methods

func (t *BfTree[K, V]) insertRecord(leafPageID uint64, key K, value V, recType node.RecordType) error {
	mp := t.getOrCreateMiniPage(leafPageID)

	if mp.InsertWithType(key, value, recType) {
		return nil
	}

	if mp.Meta.NodeSize < 4096 {
		newSize := mp.Meta.NodeSize * 2
		if newSize > 4096 || newSize < mp.Meta.NodeSize {
			newSize = 4096
		}
		t.growMiniPageInBuffer(leafPageID, mp, newSize)

		if mp.InsertWithType(key, value, recType) {
			return nil
		}
	}

	mapEntry := t.mappingTable.Get(leafPageID)
	if mapEntry == nil {
		return fmt.Errorf("leaf page mapping not found")
	}

	result, err := t.m.MergeMiniPageToDisk(mp, mapEntry.Location)
	if err != nil {
		return fmt.Errorf("merge failed: %w", err)
	}

	// Deallocate the old chunk
	if oldChunkIface, ok := t.miniPageChunks.LoadAndDelete(leafPageID); ok {
		t.bufferPool.Dealloc(oldChunkIface.(*buffer.MemoryChunk))
	}

	// Update the merged mini-page and handle splits
	evictedSize := uint64(mp.Meta.NodeSize)
	newSize := uint64(result.LeftPage.Meta.NodeSize)
	atomic.AddUint64(&t.totalMiniPageSize, ^(evictedSize-1))
	atomic.AddUint64(&t.totalMiniPageSize, newSize)
	t.miniPages.Store(leafPageID, result.LeftPage)

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

func (t *BfTree[K, V]) traverse(key K) uint64 {
	current := t.root
	for len(current.Children) > 0 && current.Children[0] != nil {
		idx := current.Search(key)
		if idx >= len(current.Children) {
			idx = len(current.Children) - 1
		}

		if current.Children[idx] != nil {
			current = current.Children[idx]
		} else {
			break
		}
	}

	if len(current.LeafPageIDs) > 0 {
		idx := current.Search(key)
		if idx >= len(current.LeafPageIDs) {
			idx = len(current.LeafPageIDs) - 1
		}
		return current.LeafPageIDs[idx]
	}

	return 0
}

func (t *BfTree[K, V]) getOrCreateMiniPage(leafPageID uint64) *node.MiniPage[K, V] {
	// Check if mini-page already exists
	if mp, ok := t.miniPages.Load(leafPageID); ok {
		return mp.(*node.MiniPage[K, V])
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
			mp := node.NewMiniPage(leafPageID, initialSize, t.codecs)
			actual, _ := t.miniPages.LoadOrStore(leafPageID, mp)
			atomic.AddUint64(&t.totalMiniPageSize, initialSize)
			return actual.(*node.MiniPage[K, V])
		}
	}

	// Create mini-page with buffer-allocated kvData
	mp := node.NewMiniPage(leafPageID, initialSize, t.codecs)

	// Store in maps
	actual, _ := t.miniPages.LoadOrStore(leafPageID, mp)
	t.miniPageChunks.Store(leafPageID, chunk)

	// Track size
	atomic.AddUint64(&t.totalMiniPageSize, initialSize)

	return actual.(*node.MiniPage[K, V])
}

func (t *BfTree[K, V]) growMiniPageInBuffer(leafPageID uint64, mp *node.MiniPage[K, V], newSize uint32) {
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

	// Deallocate old chunk if it exists
	if oldChunkIface, ok := t.miniPageChunks.Load(leafPageID); ok {
		t.bufferPool.Dealloc(oldChunkIface.(*buffer.MemoryChunk))
	}

	mp.Grow(newSize)

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

func (t *BfTree[K, V]) evictMiniPage() error {
	var evictPageID uint64
	var evictMP *node.MiniPage[K, V]

	// Scan mini-pages to find one with dirty records (needs persistence)
	t.miniPages.Range(func(key, value interface{}) bool {
		pageID := key.(uint64)
		mp := value.(*node.MiniPage[K, V])

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
		atomic.AddUint64(&t.totalMiniPageSize, ^(evictedSize-1))
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
	atomic.AddUint64(&t.totalMiniPageSize, ^(evictedSize-1))
	atomic.AddUint64(&t.totalMiniPageSize, newSize)
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

func (t *BfTree[K, V]) splitRootIfFull() {
	if !t.root.IsFull() {
		return
	}

	splitKey, rightNode := t.root.Split()
	if rightNode == nil {
		return
	}

	newRoot := node.NewInnerNode(t.codecs.Compare)
	newRoot.Keys = append(newRoot.Keys, splitKey)
	newRoot.Children = append(newRoot.Children, t.root)
	newRoot.Children = append(newRoot.Children, rightNode)
	newRoot.LeafPageIDs = append(newRoot.LeafPageIDs, t.root.LeafPageIDs[0])
	newRoot.LeafPageIDs = append(newRoot.LeafPageIDs, rightNode.LeafPageIDs[0])
	newRoot.Meta.RecordCnt = 1

	t.root = newRoot
}

func (t *BfTree[K, V]) allocatePageID() uint64 {
	return atomic.AddUint64(&t.nextPageID, 1)
}
