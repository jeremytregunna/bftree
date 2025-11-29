package node

import (
	"sync"
)

// InnerNode represents an internal (non-leaf) node in the B-Tree.
// Inner nodes are pinned in memory and use direct pointer references.
type InnerNode struct {
	// Node metadata
	Meta *NodeMeta

	// Keys and child pointers
	// Keys: k1, k2, ... are stored in order
	// Children: c0, c1, c2, ... where c0 < k1, k1 <= c1 < k2, etc.
	Keys     [][]byte
	Children []*InnerNode // Direct pointers for pinned inner nodes

	// Leaf page children
	LeafPageIDs []uint64

	// Version lock for optimistic latch coupling
	Version uint64

	mu sync.RWMutex
}

// NewInnerNode creates a new inner node.
func NewInnerNode() *InnerNode {
	return &InnerNode{
		Meta: &NodeMeta{
			PageType:  0, // Non-leaf
			SplitFlag: 0,
			RecordCnt: 0,
		},
		Keys:        make([][]byte, 0),
		Children:    make([]*InnerNode, 1), // Start with one child pointer
		LeafPageIDs: make([]uint64, 1),
		Version:     0,
	}
}

// Search finds the child index for a given key using binary search.
// Returns the index of the child that may contain the key.
func (in *InnerNode) Search(key []byte) int {
	in.mu.RLock()
	defer in.mu.RUnlock()

	return in.searchLocked(key)
}

func (in *InnerNode) searchLocked(key []byte) int {
	left, right := 0, len(in.Keys)

	for left < right {
		mid := (left + right) / 2
		if compare(key, in.Keys[mid]) < 0 {
			right = mid
		} else {
			left = mid + 1
		}
	}

	return left
}

// GetChild returns the child node for the given index.
func (in *InnerNode) GetChild(index int) *InnerNode {
	in.mu.RLock()
	defer in.mu.RUnlock()

	if index < 0 || index >= len(in.Children) {
		return nil
	}
	return in.Children[index]
}

// GetLeafPageID returns the leaf page ID for the given index.
func (in *InnerNode) GetLeafPageID(index int) uint64 {
	in.mu.RLock()
	defer in.mu.RUnlock()

	if index < 0 || index >= len(in.LeafPageIDs) {
		return 0
	}
	return in.LeafPageIDs[index]
}

// Insert inserts a key and its right child into the inner node.
func (in *InnerNode) Insert(key []byte, rightChild *InnerNode, rightLeafPageID uint64) {
	in.mu.Lock()
	defer in.mu.Unlock()

	pos := in.searchLocked(key)

	// Insert key at position
	in.Keys = append(in.Keys, nil)
	copy(in.Keys[pos+1:], in.Keys[pos:])
	in.Keys[pos] = key

	// Insert child at position+1
	in.Children = append(in.Children, nil)
	copy(in.Children[pos+2:], in.Children[pos+1:])
	in.Children[pos+1] = rightChild

	// Insert leaf page ID at position+1
	in.LeafPageIDs = append(in.LeafPageIDs, 0)
	copy(in.LeafPageIDs[pos+2:], in.LeafPageIDs[pos+1:])
	in.LeafPageIDs[pos+1] = rightLeafPageID

	in.Meta.RecordCnt++
	in.Version++
}

// InsertLeafPageSplit inserts a leaf page split into the inner node.
// Used when a leaf page splits and we need to add the split key and right leaf page ID.
func (in *InnerNode) InsertLeafPageSplit(key []byte, rightLeafPageID uint64) {
	in.mu.Lock()
	defer in.mu.Unlock()

	pos := in.searchLocked(key)

	// Insert key at position
	in.Keys = append(in.Keys, nil)
	copy(in.Keys[pos+1:], in.Keys[pos:])
	in.Keys[pos] = key

	// Insert leaf page ID at position+1
	in.LeafPageIDs = append(in.LeafPageIDs, 0)
	copy(in.LeafPageIDs[pos+2:], in.LeafPageIDs[pos+1:])
	in.LeafPageIDs[pos+1] = rightLeafPageID

	// Ensure we have enough space in Children array
	if len(in.Children) <= pos+2 {
		in.Children = append(in.Children, nil)
	}

	in.Meta.RecordCnt++
	in.Version++
}

// AcquireReadLock acquires a read lock optimistically.
// Returns the current version for later validation.
func (in *InnerNode) AcquireReadLock() uint64 {
	in.mu.RLock()
	return in.Version
}

// ReleaseReadLock releases a read lock.
func (in *InnerNode) ReleaseReadLock() {
	in.mu.RUnlock()
}

// ValidateReadLock checks if the version has changed since the read lock was acquired.
func (in *InnerNode) ValidateReadLock(version uint64) bool {
	in.mu.RLock()
	defer in.mu.RUnlock()

	return in.Version == version
}

// AcquireWriteLock acquires a write lock on the node.
func (in *InnerNode) AcquireWriteLock() {
	in.mu.Lock()
}

// ReleaseWriteLock releases a write lock and increments the version.
func (in *InnerNode) ReleaseWriteLock() {
	in.Version++
	in.mu.Unlock()
}

// Split splits the inner node at the midpoint and returns the new right node.
// The middle key is returned separately.
func (in *InnerNode) Split() ([]byte, *InnerNode) {
	in.mu.Lock()
	defer in.mu.Unlock()

	mid := len(in.Keys) / 2
	if mid >= len(in.Keys) {
		return nil, nil
	}

	// Create new right node
	rightNode := &InnerNode{
		Meta: &NodeMeta{
			PageType:  0,
			SplitFlag: 0,
			RecordCnt: uint16(len(in.Keys) - mid - 1),
		},
		Keys:        make([][]byte, len(in.Keys[mid+1:])),
		Children:    make([]*InnerNode, len(in.Children[mid+1:])),
		LeafPageIDs: make([]uint64, len(in.LeafPageIDs[mid+1:])),
		Version:     0,
	}

	copy(rightNode.Keys, in.Keys[mid+1:])
	copy(rightNode.Children, in.Children[mid+1:])
	copy(rightNode.LeafPageIDs, in.LeafPageIDs[mid+1:])

	// Update left node
	midKey := in.Keys[mid]
	in.Keys = in.Keys[:mid]
	in.Children = in.Children[:mid+1]
	in.LeafPageIDs = in.LeafPageIDs[:mid+1]
	in.Meta.RecordCnt = uint16(len(in.Keys))
	in.Version++

	return midKey, rightNode
}

// IsFull checks if the node needs splitting.
func (in *InnerNode) IsFull() bool {
	in.mu.RLock()
	defer in.mu.RUnlock()

	// Assume a max of 256 keys per inner node
	return len(in.Keys) >= 256
}

// GetKeyCount returns the number of keys in the node.
func (in *InnerNode) GetKeyCount() int {
	in.mu.RLock()
	defer in.mu.RUnlock()

	return len(in.Keys)
}

// GetKeys returns a copy of all keys in the node.
func (in *InnerNode) GetKeys() [][]byte {
	in.mu.RLock()
	defer in.mu.RUnlock()

	keys := make([][]byte, len(in.Keys))
	copy(keys, in.Keys)
	return keys
}
