package buffer

import (
	"fmt"
	"sync"
)

// CircularBuffer manages variable-length mini-pages using a circular buffer.
// It divides memory into three regions:
// - In-place-update region (90%): mini-pages can be modified in place
// - Copy-on-access region (10%): hot mini-pages copied to tail on access
type CircularBuffer struct {
	data []byte

	// Head points to the start of evictable memory
	head uint64
	// Tail points to the end of allocated memory
	tail uint64
	// SecondChance separates in-place-update and copy-on-access regions
	secondChance uint64

	// Free lists by size class for memory reuse
	freeLists map[uint32]*FreeList

	// Protects all fields
	mu sync.RWMutex

	// Size constraints
	totalSize     uint64
	inPlaceRatio  float64 // Default 0.9 (90%)
	copyOnAccess  float64 // Default 0.1 (10%)
}

// FreeList tracks freed memory chunks of a specific size class
type FreeList struct {
	chunks []*MemoryChunk
	mu     sync.Mutex
}

// MemoryChunk represents an allocated region of memory
type MemoryChunk struct {
	Offset uint64 // Offset in circular buffer
	Size   uint64 // Size of the chunk
	Data   []byte // Slice into the circular buffer
}

// NewCircularBuffer creates a new circular buffer with the given total size.
func NewCircularBuffer(totalSize uint64) *CircularBuffer {
	cb := &CircularBuffer{
		data:          make([]byte, totalSize),
		head:          0,
		tail:          0,
		totalSize:     totalSize,
		inPlaceRatio:  0.9,
		copyOnAccess:  0.1,
		freeLists:     make(map[uint32]*FreeList),
	}
	cb.secondChance = uint64(float64(totalSize) * cb.inPlaceRatio)
	return cb
}

// Alloc allocates a chunk of memory of the requested size.
// Returns the allocated chunk or an error if the buffer is full.
func (cb *CircularBuffer) Alloc(size uint64) (*MemoryChunk, error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if size == 0 {
		return nil, fmt.Errorf("cannot allocate 0 bytes")
	}

	// Try to allocate from free list first
	sizeClass := uint32(size)
	if fl, ok := cb.freeLists[sizeClass]; ok {
		if chunk := fl.pop(); chunk != nil {
			return chunk, nil
		}
	}

	// Allocate from tail
	if !cb.canAllocate(size) {
		return nil, fmt.Errorf("circular buffer full: need %d bytes, available %d", size, cb.availableSpace())
	}

	offset := cb.tail
	cb.tail += size

	// Handle wrap-around
	if cb.tail >= cb.totalSize {
		cb.tail = cb.tail % cb.totalSize
	}

	chunk := &MemoryChunk{
		Offset: offset,
		Size:   size,
		Data:   cb.data[offset : offset+size],
	}

	return chunk, nil
}

// Dealloc adds a memory chunk back to the free list for reuse.
func (cb *CircularBuffer) Dealloc(chunk *MemoryChunk) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	sizeClass := uint32(chunk.Size)
	if fl, ok := cb.freeLists[sizeClass]; ok {
		fl.push(chunk)
	} else {
		cb.freeLists[sizeClass] = &FreeList{chunks: []*MemoryChunk{chunk}}
	}
}

// Evict evicts mini-pages from the head of the buffer to make room.
// The callback is invoked for each evicted mini-page to persist it to disk.
// The callback should return the number of bytes freed from the mini-page.
func (cb *CircularBuffer) Evict(callback func(offset, size uint64) error) error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.head == cb.tail {
		return fmt.Errorf("circular buffer is empty")
	}

	// Find the first free list entry at the head
	// and evict it to make room
	for sizeClass, fl := range cb.freeLists {
		if len(fl.chunks) > 0 {
			// Evict the chunk at the head
			chunk := fl.chunks[0]
			if chunk.Offset < cb.secondChance {
				// Only evict from in-place-update region
				if err := callback(chunk.Offset, chunk.Size); err != nil {
					return fmt.Errorf("eviction callback failed: %w", err)
				}

				// Remove from free list
				fl.chunks = fl.chunks[1:]
				if len(fl.chunks) == 0 {
					delete(cb.freeLists, sizeClass)
				}

				// Advance head pointer
				if chunk.Offset+chunk.Size >= cb.head {
					cb.head = (cb.head + chunk.Size) % cb.totalSize
				}

				return nil
			}
		}
	}

	// No free list chunks to evict, return error
	return fmt.Errorf("no evictable chunks found")
}

// canAllocate checks if there's enough space to allocate the requested size.
func (cb *CircularBuffer) canAllocate(size uint64) bool {
	available := cb.availableSpace()
	return available >= size
}

// availableSpace returns the available space in the circular buffer.
func (cb *CircularBuffer) availableSpace() uint64 {
	if cb.tail >= cb.head {
		return cb.totalSize - (cb.tail - cb.head)
	}
	return cb.head - cb.tail
}

// Stats returns buffer statistics for verification and testing
func (cb *CircularBuffer) Stats() map[string]interface{} {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	totalFreed := uint64(0)
	freeCounts := make(map[uint32]int)
	for sizeClass, fl := range cb.freeLists {
		freeCounts[sizeClass] = len(fl.chunks)
		totalFreed += uint64(sizeClass) * uint64(len(fl.chunks))
	}

	return map[string]interface{}{
		"total_size":      cb.totalSize,
		"head":            cb.head,
		"tail":            cb.tail,
		"available_space": cb.availableSpace(),
		"used_space":      cb.totalSize - cb.availableSpace(),
		"in_place_end":    cb.secondChance,
		"free_list_counts": freeCounts,
		"total_freed_bytes": totalFreed,
	}
}

// FreeList helpers
func (fl *FreeList) push(chunk *MemoryChunk) {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	fl.chunks = append(fl.chunks, chunk)
}

func (fl *FreeList) pop() *MemoryChunk {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	if len(fl.chunks) == 0 {
		return nil
	}
	chunk := fl.chunks[len(fl.chunks)-1]
	fl.chunks = fl.chunks[:len(fl.chunks)-1]
	return chunk
}
