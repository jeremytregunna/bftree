package node

import (
	"sync"
)

// InnerNode is a generic internal (non-leaf) node in the B-Tree.
// Inner nodes are pinned in memory and use direct pointer references.
type InnerNode[K any] struct {
	// Node metadata
	Meta *NodeMeta

	// Keys and child pointers
	// Keys: k1, k2, ... are stored in order
	// Children: c0, c1, c2, ... where c0 < k1, k1 <= c1 < k2, etc.
	Keys     []K
	Children []*InnerNode[K] // Direct pointers for pinned inner nodes

	// Leaf page children
	LeafPageIDs []uint64

	// Comparator for key comparison
	compare Comparator[K]

	// Version lock for optimistic latch coupling
	Version uint64

	mu sync.RWMutex
}

// NewInnerNode creates a new generic inner node.
func NewInnerNode[K any](compare Comparator[K]) *InnerNode[K] {
	return &InnerNode[K]{
		Meta: &NodeMeta{
			PageType:  0, // Non-leaf
			SplitFlag: 0,
			RecordCnt: 0,
		},
		Keys:        make([]K, 0),
		Children:    make([]*InnerNode[K], 1),
		LeafPageIDs: make([]uint64, 1),
		compare:     compare,
		Version:     0,
	}
}

// Search finds the child index for a given key using binary search.
func (in *InnerNode[K]) Search(key K) int {
	in.mu.RLock()
	defer in.mu.RUnlock()

	return in.searchLocked(key)
}

func (in *InnerNode[K]) searchLocked(key K) int {
	left, right := 0, len(in.Keys)

	for left < right {
		mid := (left + right) / 2
		if in.compare(in.Keys[mid], key) < 0 {
			left = mid + 1
		} else {
			right = mid
		}
	}

	return left
}

// Insert inserts a key with a right child and leaf page ID.
func (in *InnerNode[K]) Insert(key K, rightChild *InnerNode[K], leafPageID uint64) {
	in.mu.Lock()
	defer in.mu.Unlock()

	// Find insertion position
	pos := in.searchLocked(key)

	// Insert key
	in.Keys = append(in.Keys, key)
	copy(in.Keys[pos+1:], in.Keys[pos:len(in.Keys)-1])
	in.Keys[pos] = key

	// Insert child
	in.Children = append(in.Children, nil)
	copy(in.Children[pos+2:], in.Children[pos+1:len(in.Children)-1])
	in.Children[pos+1] = rightChild

	// Insert leaf page ID
	in.LeafPageIDs = append(in.LeafPageIDs, leafPageID)
	copy(in.LeafPageIDs[pos+2:], in.LeafPageIDs[pos+1:len(in.LeafPageIDs)-1])
	in.LeafPageIDs[pos+1] = leafPageID

	in.Meta.RecordCnt++
	in.Version++
}

// InsertLeafPageSplit inserts a split from a leaf page merge.
func (in *InnerNode[K]) InsertLeafPageSplit(key K, leafPageID uint64) {
	in.mu.Lock()
	defer in.mu.Unlock()

	pos := in.searchLocked(key)

	in.Keys = append(in.Keys, key)
	copy(in.Keys[pos+1:], in.Keys[pos:len(in.Keys)-1])
	in.Keys[pos] = key

	in.LeafPageIDs = append(in.LeafPageIDs, leafPageID)
	copy(in.LeafPageIDs[pos+1:], in.LeafPageIDs[pos:len(in.LeafPageIDs)-1])
	in.LeafPageIDs[pos] = leafPageID

	in.Meta.RecordCnt++
	in.Version++
}

// IsFull checks if node has exceeded 256-key capacity
func (in *InnerNode[K]) IsFull() bool {
	in.mu.RLock()
	defer in.mu.RUnlock()
	return len(in.Keys) >= 256
}

// Split splits the node at the midpoint
func (in *InnerNode[K]) Split() (K, *InnerNode[K]) {
	in.mu.Lock()
	defer in.mu.Unlock()

	if len(in.Keys) == 0 {
		var zero K
		return zero, nil
	}

	mid := len(in.Keys) / 2
	splitKey := in.Keys[mid]

	// Create right node
	rightNode := &InnerNode[K]{
		Meta: &NodeMeta{
			PageType:  0,
			SplitFlag: 0,
			RecordCnt: uint16(len(in.Keys) - mid - 1),
		},
		Keys:        make([]K, len(in.Keys)-mid-1),
		Children:    make([]*InnerNode[K], len(in.Children)-mid-1),
		LeafPageIDs: make([]uint64, len(in.LeafPageIDs)-mid-1),
		compare:     in.compare,
		Version:     0,
	}

	// Copy right half
	copy(rightNode.Keys, in.Keys[mid+1:])
	copy(rightNode.Children, in.Children[mid+1:])
	copy(rightNode.LeafPageIDs, in.LeafPageIDs[mid+1:])

	// Truncate left node
	in.Keys = in.Keys[:mid]
	in.Children = in.Children[:mid+1]
	in.LeafPageIDs = in.LeafPageIDs[:mid+1]
	in.Meta.RecordCnt = uint16(len(in.Keys))
	in.Version++

	return splitKey, rightNode
}

// AcquireReadLock acquires a read lock with version snapshot
func (in *InnerNode[K]) AcquireReadLock() uint64 {
	in.mu.RLock()
	return in.Version
}

// ValidateReadLock checks if version hasn't changed
func (in *InnerNode[K]) ValidateReadLock(version uint64) bool {
	defer in.mu.RUnlock()
	return in.Version == version
}

// AcquireWriteLock acquires a write lock
func (in *InnerNode[K]) AcquireWriteLock() {
	in.mu.Lock()
}

// ReleaseWriteLock releases the write lock
func (in *InnerNode[K]) ReleaseWriteLock() {
	in.mu.Unlock()
}
