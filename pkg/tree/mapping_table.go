package tree

import (
	"sync"
)

// MappingEntry contains the mapping information for a page
type MappingEntry struct {
	// Physical location: memory address for mini-pages, disk offset for leaf pages
	Location uint64

	// Reader-writer lock for the page (16 bits)
	// State: 0=unlocked, >0=read locked, <0=write locked
	LockState int16

	// Reference to the mini-page if it exists
	MiniPagePtr interface{}
}

// MappingTable maps logical page IDs to physical locations.
// It's an indirection array for simplicity and performance.
type MappingTable struct {
	entries map[uint64]*MappingEntry
	mu      sync.RWMutex
}

// NewMappingTable creates a new mapping table.
func NewMappingTable() *MappingTable {
	return &MappingTable{
		entries: make(map[uint64]*MappingEntry),
	}
}

// Insert adds or updates a mapping for a page ID.
func (mt *MappingTable) Insert(pageID uint64, location uint64) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	if entry, ok := mt.entries[pageID]; ok {
		entry.Location = location
	} else {
		mt.entries[pageID] = &MappingEntry{
			Location:   location,
			LockState:  0,
			MiniPagePtr: nil,
		}
	}
}

// Get retrieves the mapping entry for a page ID.
func (mt *MappingTable) Get(pageID uint64) *MappingEntry {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	return mt.entries[pageID]
}

// SetMiniPage associates a mini-page with a page ID.
func (mt *MappingTable) SetMiniPage(pageID uint64, miniPagePtr interface{}) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	if entry, ok := mt.entries[pageID]; ok {
		entry.MiniPagePtr = miniPagePtr
	}
}

// GetMiniPage retrieves the mini-page for a page ID, if it exists.
func (mt *MappingTable) GetMiniPage(pageID uint64) interface{} {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	if entry, ok := mt.entries[pageID]; ok {
		return entry.MiniPagePtr
	}
	return nil
}

// Delete removes a page from the mapping table.
func (mt *MappingTable) Delete(pageID uint64) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	delete(mt.entries, pageID)
}

// AcquireReadLock acquires a read lock on a page.
// Returns true if successful, false if the page is write-locked.
func (mt *MappingTable) AcquireReadLock(pageID uint64) bool {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	entry, ok := mt.entries[pageID]
	if !ok {
		return false
	}

	if entry.LockState < 0 {
		// Write locked
		return false
	}

	entry.LockState++
	return true
}

// AcquireWriteLock acquires a write lock on a page.
// Returns true if successful, false if the page is already locked.
func (mt *MappingTable) AcquireWriteLock(pageID uint64) bool {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	entry, ok := mt.entries[pageID]
	if !ok {
		return false
	}

	if entry.LockState != 0 {
		return false
	}

	entry.LockState = -1
	return true
}

// ReleaseReadLock releases a read lock on a page.
func (mt *MappingTable) ReleaseReadLock(pageID uint64) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	if entry, ok := mt.entries[pageID]; ok && entry.LockState > 0 {
		entry.LockState--
	}
}

// ReleaseWriteLock releases a write lock on a page.
func (mt *MappingTable) ReleaseWriteLock(pageID uint64) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	if entry, ok := mt.entries[pageID]; ok && entry.LockState < 0 {
		entry.LockState = 0
	}
}
