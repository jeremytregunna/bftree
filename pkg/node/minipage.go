package node

import (
	"encoding/binary"
	"sync"
)

// MiniPage is a generic in-memory leaf page buffer.
// It serves two purposes:
// 1. Buffer recent updates
// 2. Cache frequently accessed records
type MiniPage[K, V any] struct {
	// Page header
	Meta *NodeMeta

	// Typed key-value storage
	Keys   []K
	Values []V
	Types  []RecordType // Type of each record

	// Leaf page this mini-page corresponds to
	LeafPageID uint64

	// Codecs for serialization
	codecs *Codecs[K, V]

	// Lock for concurrent access
	mu sync.RWMutex
}

// NodeMeta contains metadata for a mini-page or leaf page
type NodeMeta struct {
	NodeSize   uint32 // Current size of this page
	PageType   uint8  // 0=leaf, 1=mini
	SplitFlag  uint8  // Whether page is full
	RecordCnt  uint16 // Number of records
	Reserved   uint32 // Padding
}

// RecordType indicates the type of record in the mini-page
type RecordType uint8

const (
	Insert   RecordType = iota // Inserted/updated by user
	Cache                       // Cached from disk
	Tombstone                   // Deleted (tombstone)
	Phantom                     // Negative search result
)

// NewMiniPage creates a new generic mini-page.
func NewMiniPage[K, V any](leafPageID uint64, initialSize uint32, codecs *Codecs[K, V]) *MiniPage[K, V] {
	if initialSize < 64 {
		initialSize = 64
	}

	mp := &MiniPage[K, V]{
		Meta: &NodeMeta{
			NodeSize:  initialSize,
			PageType:  1, // Mini page
			SplitFlag: 0,
			RecordCnt: 0,
		},
		Keys:       make([]K, 0),
		Values:     make([]V, 0),
		Types:      make([]RecordType, 0),
		LeafPageID: leafPageID,
		codecs:     codecs,
	}
	return mp
}

// Insert inserts a key-value pair with Insert type
func (mp *MiniPage[K, V]) Insert(key K, value V) bool {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	return mp.insertLocked(key, value, Insert)
}

// InsertWithType inserts a key-value pair with specified type
func (mp *MiniPage[K, V]) InsertWithType(key K, value V, recType RecordType) bool {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	return mp.insertLocked(key, value, recType)
}

func (mp *MiniPage[K, V]) insertLocked(key K, value V, recType RecordType) bool {
	// Check if key already exists
	for i, k := range mp.Keys {
		if mp.codecs.Compare(k, key) == 0 {
			// Replace existing record
			mp.Values[i] = value
			mp.Types[i] = recType
			return true
		}
	}

	// Check if we have space
	if len(mp.Keys) >= int((mp.Meta.NodeSize-uint32(binary.Size(&NodeMeta{})))/64) {
		return false // Rough space estimate
	}

	// Find insertion position (maintain sorted order)
	pos := mp.findInsertionPos(key)

	// Insert at position
	mp.Keys = append(mp.Keys, key)
	mp.Values = append(mp.Values, value)
	mp.Types = append(mp.Types, recType)

	copy(mp.Keys[pos+1:], mp.Keys[pos:len(mp.Keys)-1])
	mp.Keys[pos] = key
	copy(mp.Values[pos+1:], mp.Values[pos:len(mp.Values)-1])
	mp.Values[pos] = value
	copy(mp.Types[pos+1:], mp.Types[pos:len(mp.Types)-1])
	mp.Types[pos] = recType

	mp.Meta.RecordCnt++
	return true
}

// Search searches for a key and returns its value
func (mp *MiniPage[K, V]) Search(key K) (V, bool) {
	mp.mu.RLock()
	defer mp.mu.RUnlock()

	for i, k := range mp.Keys {
		if mp.codecs.Compare(k, key) == 0 {
			return mp.Values[i], true
		}
	}

	var zero V
	return zero, false
}

// GetRecords returns all non-tombstone records
func (mp *MiniPage[K, V]) GetRecords() []Record[K, V] {
	mp.mu.RLock()
	defer mp.mu.RUnlock()

	var records []Record[K, V]
	for i, k := range mp.Keys {
		if mp.Types[i] != Tombstone {
			records = append(records, Record[K, V]{
				Key:   k,
				Value: mp.Values[i],
				Type:  mp.Types[i],
			})
		}
	}
	return records
}

// GetDirtyRecords returns all non-phantom records (need persistence)
func (mp *MiniPage[K, V]) GetDirtyRecords() []Record[K, V] {
	mp.mu.RLock()
	defer mp.mu.RUnlock()

	var records []Record[K, V]
	for i, k := range mp.Keys {
		if mp.Types[i] != Phantom {
			records = append(records, Record[K, V]{
				Key:   k,
				Value: mp.Values[i],
				Type:  mp.Types[i],
			})
		}
	}
	return records
}

// IsFull checks if the mini-page is full
func (mp *MiniPage[K, V]) IsFull() bool {
	mp.mu.RLock()
	defer mp.mu.RUnlock()
	return len(mp.Keys) >= int((mp.Meta.NodeSize-uint32(binary.Size(&NodeMeta{})))/64)
}

// Grow resizes the mini-page to a larger size
func (mp *MiniPage[K, V]) Grow(newSize uint32) {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	if newSize <= mp.Meta.NodeSize {
		return
	}

	metaSize := uint32(binary.Size(&NodeMeta{}))
	if newSize <= metaSize {
		return
	}

	mp.Meta.NodeSize = newSize
}

// findInsertionPos finds where to insert a key to maintain sorted order
func (mp *MiniPage[K, V]) findInsertionPos(key K) int {
	left, right := 0, len(mp.Keys)

	for left < right {
		mid := (left + right) / 2
		if mp.codecs.Compare(mp.Keys[mid], key) < 0 {
			left = mid + 1
		} else {
			right = mid
		}
	}

	return left
}
