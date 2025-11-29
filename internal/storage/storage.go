package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

// PageSize is the standard disk page size
const PageSize = 4096

// Storage handles disk I/O for leaf pages
type Storage struct {
	file *os.File
	mu   sync.RWMutex

	// Offset allocation (next free offset)
	nextOffset uint64
}

// NewStorage creates a new storage instance backed by a file
func NewStorage(filepath string) (*Storage, error) {
	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open storage file: %w", err)
	}

	// Get current file size
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	return &Storage{
		file:       file,
		nextOffset: uint64(fileInfo.Size()),
	}, nil
}

// WritePage writes a page to disk and returns its offset
func (s *Storage) WritePage(data []byte) (uint64, error) {
	if len(data) > PageSize {
		return 0, fmt.Errorf("page size %d exceeds max size %d", len(data), PageSize)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Pad to page size
	padded := make([]byte, PageSize)
	copy(padded, data)

	offset := s.nextOffset
	_, err := s.file.WriteAt(padded, int64(offset))
	if err != nil {
		return 0, fmt.Errorf("failed to write page at offset %d: %w", offset, err)
	}

	s.nextOffset += uint64(PageSize)

	return offset, nil
}

// ReadPage reads a page from disk at the given offset
func (s *Storage) ReadPage(offset uint64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data := make([]byte, PageSize)
	n, err := s.file.ReadAt(data, int64(offset))
	if err != nil && err.Error() != "EOF" {
		return nil, fmt.Errorf("failed to read page at offset %d: %w", offset, err)
	}

	// Return actual data (may be less than PageSize)
	return data[:n], nil
}

// WriteRecord writes a single record (key-value pair) to a page buffer
// and returns the offset within the page
func WriteRecord(pageBuffer []byte, offset int, key, value []byte) (int, error) {
	if offset+4+len(key)+len(value) > len(pageBuffer) {
		return 0, fmt.Errorf("not enough space in page")
	}

	// Write key length
	binary.LittleEndian.PutUint16(pageBuffer[offset:], uint16(len(key)))
	offset += 2

	// Write value length
	binary.LittleEndian.PutUint16(pageBuffer[offset:], uint16(len(value)))
	offset += 2

	// Write key and value
	copy(pageBuffer[offset:], key)
	offset += len(key)

	copy(pageBuffer[offset:], value)
	offset += len(value)

	return offset, nil
}

// ReadRecord reads a single record from a page buffer
func ReadRecord(pageBuffer []byte, offset int) (key, value []byte, newOffset int, err error) {
	if offset+4 > len(pageBuffer) {
		return nil, nil, 0, fmt.Errorf("not enough space to read lengths")
	}

	// Read key length
	keyLen := int(binary.LittleEndian.Uint16(pageBuffer[offset:]))
	offset += 2

	// Read value length
	valueLen := int(binary.LittleEndian.Uint16(pageBuffer[offset:]))
	offset += 2

	if offset+keyLen+valueLen > len(pageBuffer) {
		return nil, nil, 0, fmt.Errorf("not enough space to read key and value")
	}

	// Read key and value
	key = make([]byte, keyLen)
	copy(key, pageBuffer[offset:offset+keyLen])
	offset += keyLen

	value = make([]byte, valueLen)
	copy(value, pageBuffer[offset:offset+valueLen])
	offset += valueLen

	return key, value, offset, nil
}

// Close closes the storage file
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file != nil {
		return s.file.Close()
	}
	return nil
}

// Sync syncs the file to disk
func (s *Storage) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file != nil {
		return s.file.Sync()
	}
	return nil
}
