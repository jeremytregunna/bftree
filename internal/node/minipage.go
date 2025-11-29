package node

import (
	"encoding/binary"
	"sync"
)

// MiniPage is an in-memory slim version of a leaf page.
// It serves two purposes:
// 1. Buffer recent updates
// 2. Cache frequently accessed records
type MiniPage struct {
	// Page header
	Meta *NodeMeta

	// KV metadata and data storage
	KvMetas []*KVMeta
	KvData  []byte

	// kvDataLen tracks the actual used bytes in KvData
	// Never use len(KvData) - use kvDataLen instead to avoid append() escape
	kvDataLen uint32

	// Leaf page this mini-page corresponds to
	LeafPageID uint64

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

// KVMeta stores metadata for a key-value pair
type KVMeta struct {
	KeyLen    uint16
	ValueLen  uint16
	KeyOffset uint32
	ValueOffset uint32
	KeyType   uint8
	ValueType uint8
	IsFence   uint8      // Whether this is a fence key
	RefBit    uint8      // Reference bit for eviction
	RecordType RecordType // Type of record
	LookAhead [2]byte    // First 2 bytes of key for fast comparison
}

// NewMiniPage creates a new mini-page with minimal size.
func NewMiniPage(leafPageID uint64, initialSize uint32) *MiniPage {
	if initialSize < 64 {
		initialSize = 64 // Align with cache line
	}

	metaSize := uint32(binary.Size(&NodeMeta{}))
	kvDataCapacity := initialSize - metaSize

	mp := &MiniPage{
		Meta: &NodeMeta{
			NodeSize:  initialSize,
			PageType:  1, // Mini page
			SplitFlag: 0,
			RecordCnt: 0,
		},
		KvMetas:   make([]*KVMeta, 0),
		KvData:    make([]byte, kvDataCapacity), // Full capacity, no append() escapes
		kvDataLen: 0,                             // Start with 0 bytes used
		LeafPageID: leafPageID,
	}
	return mp
}

// NewMiniPageFromBuffer creates a mini-page backed by a pre-allocated buffer.
// This is used by the Bf-Tree to allocate mini-pages from the circular buffer.
// The kvData is a slice into the provided buffer, starting at offset metaSize.
func NewMiniPageFromBuffer(leafPageID uint64, kvData []byte, totalSize uint64) *MiniPage {
	mp := &MiniPage{
		Meta: &NodeMeta{
			NodeSize:  uint32(totalSize),
			PageType:  1, // Mini page
			SplitFlag: 0,
			RecordCnt: 0,
		},
		KvMetas:    make([]*KVMeta, 0),
		KvData:     kvData,
		kvDataLen:  0, // Start with 0 bytes used
		LeafPageID: leafPageID,
	}
	return mp
}

// Insert inserts a key-value pair into the mini-page with Insert record type.
// Returns true if successful, false if the page is full.
func (mp *MiniPage) Insert(key, value []byte) bool {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	return mp.insertLocked(key, value, Insert)
}

// InsertWithType inserts a key-value pair with the specified record type.
func (mp *MiniPage) InsertWithType(key, value []byte, recType RecordType) bool {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	return mp.insertLocked(key, value, recType)
}

func (mp *MiniPage) insertLocked(key, value []byte, recType RecordType) bool {

	// Check if key already exists
	for i := range mp.KvMetas {
		existingKey := mp.getKeyAt(i)
		if compare(key, existingKey) == 0 {
			// Replace existing record - append new value at end
			valueOffset := mp.kvDataLen
			requiredSpace := uint32(len(value))
			if valueOffset+requiredSpace > uint32(len(mp.KvData)) {
				return false // Not enough space for replacement
			}

			mp.KvMetas[i].ValueLen = uint16(len(value))
			mp.KvMetas[i].ValueOffset = valueOffset
			mp.KvMetas[i].RecordType = recType
			mp.KvMetas[i].RefBit = 1

			// Use copy instead of append to stay in pre-allocated buffer
			copy(mp.KvData[valueOffset:], value)
			mp.kvDataLen += requiredSpace

			return true
		}
	}

	// Check if we have space for new record in KvData
	requiredSize := uint32(len(key) + len(value))
	if mp.kvDataLen+requiredSize > uint32(len(mp.KvData)) {
		return false
	}

	// Find insertion position (maintain sorted order)
	pos := mp.findInsertionPos(key)

	// Prepare metadata with current offsets
	keyOffset := mp.kvDataLen
	valueOffset := mp.kvDataLen + uint32(len(key))

	kvMeta := &KVMeta{
		KeyLen:     uint16(len(key)),
		ValueLen:   uint16(len(value)),
		KeyOffset:  keyOffset,
		ValueOffset: valueOffset,
		RecordType: recType,
		RefBit:     1,
	}
	copy(kvMeta.LookAhead[:], key[:min(2, len(key))])

	// Add metadata (no append escape - KvMetas is separate heap allocation)
	mp.KvMetas = append(mp.KvMetas, nil)
	copy(mp.KvMetas[pos+1:], mp.KvMetas[pos:])
	mp.KvMetas[pos] = kvMeta

	// Copy key and value data using pre-allocated buffer
	copy(mp.KvData[keyOffset:], key)
	copy(mp.KvData[valueOffset:], value)
	mp.kvDataLen = valueOffset + uint32(len(value))

	mp.Meta.RecordCnt++

	return true
}

// Search performs a binary search for a key in the mini-page.
func (mp *MiniPage) Search(key []byte) ([]byte, bool) {
	mp.mu.RLock()
	defer mp.mu.RUnlock()

	return mp.searchLocked(key)
}

func (mp *MiniPage) searchLocked(key []byte) ([]byte, bool) {
	left, right := 0, len(mp.KvMetas)

	for left < right {
		mid := (left + right) / 2
		midKey := mp.getKeyAt(mid)

		cmp := compare(key, midKey)
		if cmp < 0 {
			right = mid
		} else if cmp > 0 {
			left = mid + 1
		} else {
			// Found - check if it's a valid record
			meta := mp.KvMetas[mid]
			meta.RefBit = 1

			// Tombstone means deleted
			if meta.RecordType == Tombstone {
				return nil, false
			}

			// Phantom means we already checked disk and it doesn't exist
			if meta.RecordType == Phantom {
				return nil, false
			}

			value := mp.getValueAt(mid)
			return value, true
		}
	}

	return nil, false
}

// Grow resizes the mini-page to a larger size.
func (mp *MiniPage) Grow(newSize uint32) {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	if newSize <= mp.Meta.NodeSize {
		return
	}

	metaSize := uint32(binary.Size(&NodeMeta{}))
	if newSize <= metaSize {
		// Can't allocate less than what meta needs
		return
	}

	newCapacity := newSize - metaSize

	// Ensure capacity is at least as large as current used length
	if newCapacity < mp.kvDataLen {
		return // Can't shrink capacity below current used length
	}

	// Create new buffer with full pre-allocated capacity
	newData := make([]byte, newCapacity)
	copy(newData, mp.KvData[:mp.kvDataLen]) // Copy only the used portion
	mp.KvData = newData
	mp.Meta.NodeSize = newSize
}

// GetRecords returns all records in the mini-page.
func (mp *MiniPage) GetRecords() []Record {
	mp.mu.RLock()
	defer mp.mu.RUnlock()

	records := make([]Record, len(mp.KvMetas))
	for i := range mp.KvMetas {
		records[i] = Record{
			Key:   mp.getKeyAt(i),
			Value: mp.getValueAt(i),
		}
	}
	return records
}

// IsFull checks if the mini-page is full.
func (mp *MiniPage) IsFull() bool {
	mp.mu.RLock()
	defer mp.mu.RUnlock()

	metaSize := uint32(binary.Size(&KVMeta{}))
	usedMeta := uint32(len(mp.KvMetas)) * metaSize
	usedData := mp.getUsedDataSizeLocked()

	return usedMeta+usedData >= mp.Meta.NodeSize
}

// GetDataUsed returns the number of bytes used in KvData
func (mp *MiniPage) GetDataUsed() uint32 {
	mp.mu.RLock()
	defer mp.mu.RUnlock()
	return mp.kvDataLen
}

// GetDirtyRecords returns all records that need to be persisted to disk
func (mp *MiniPage) GetDirtyRecords() []Record {
	mp.mu.RLock()
	defer mp.mu.RUnlock()

	var records []Record
	for i, meta := range mp.KvMetas {
		if meta.RecordType == Insert || meta.RecordType == Tombstone {
			records = append(records, Record{
				Key:   mp.getKeyAt(i),
				Value: mp.getValueAt(i),
			})
		}
	}
	return records
}

// Exists checks if a key exists and is not deleted
func (mp *MiniPage) Exists(key []byte) bool {
	mp.mu.RLock()
	defer mp.mu.RUnlock()

	_, found := mp.searchLocked(key)
	return found
}

// Helper functions

func (mp *MiniPage) getKeyAt(idx int) []byte {
	meta := mp.KvMetas[idx]
	return mp.KvData[meta.KeyOffset : meta.KeyOffset+uint32(meta.KeyLen)]
}

func (mp *MiniPage) getValueAt(idx int) []byte {
	meta := mp.KvMetas[idx]
	return mp.KvData[meta.ValueOffset : meta.ValueOffset+uint32(meta.ValueLen)]
}

func (mp *MiniPage) getUsedDataSizeLocked() uint32 {
	// Simply return how much of the pre-allocated buffer we're actually using
	return mp.kvDataLen
}

func (mp *MiniPage) findInsertionPos(key []byte) int {
	left, right := 0, len(mp.KvMetas)

	for left < right {
		mid := (left + right) / 2
		midKey := mp.getKeyAt(mid)

		if compare(key, midKey) < 0 {
			right = mid
		} else {
			left = mid + 1
		}
	}

	return left
}

// Utility functions

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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Record represents a key-value pair
type Record struct {
	Key   []byte
	Value []byte
}
