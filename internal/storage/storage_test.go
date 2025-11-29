package storage

import (
	"os"
	"testing"
)

func TestStorageWriteRead(t *testing.T) {
	// Create temporary file
	tmpFile, err := os.CreateTemp("", "bftree-*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	// Open storage
	s, err := NewStorage(tmpFile.Name())
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer s.Close()

	// Write a page
	testData := []byte("hello world test data for storage")
	offset, err := s.WritePage(testData)
	if err != nil {
		t.Fatalf("failed to write page: %v", err)
	}

	if offset != 0 {
		t.Fatalf("expected offset 0, got %d", offset)
	}

	// Read the page
	readData, err := s.ReadPage(offset)
	if err != nil {
		t.Fatalf("failed to read page: %v", err)
	}

	// Verify data
	if string(readData[:len(testData)]) != string(testData) {
		t.Fatalf("data mismatch: got %v, expected %v", readData[:len(testData)], testData)
	}
}

func TestStorageMultiplePages(t *testing.T) {
	// Create temporary file
	tmpFile, err := os.CreateTemp("", "bftree-*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	// Open storage
	s, err := NewStorage(tmpFile.Name())
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer s.Close()

	// Write multiple pages
	offsets := make([]uint64, 3)
	testData := [][]byte{
		[]byte("page1 data"),
		[]byte("page2 data"),
		[]byte("page3 data"),
	}

	for i, data := range testData {
		offset, err := s.WritePage(data)
		if err != nil {
			t.Fatalf("failed to write page %d: %v", i, err)
		}
		offsets[i] = offset
	}

	// Verify offsets are sequential
	for i := 1; i < len(offsets); i++ {
		expectedOffset := offsets[i-1] + PageSize
		if offsets[i] != expectedOffset {
			t.Fatalf("expected offset %d, got %d", expectedOffset, offsets[i])
		}
	}

	// Read and verify all pages
	for i, offset := range offsets {
		readData, err := s.ReadPage(offset)
		if err != nil {
			t.Fatalf("failed to read page %d: %v", i, err)
		}

		if string(readData[:len(testData[i])]) != string(testData[i]) {
			t.Fatalf("data mismatch at page %d", i)
		}
	}
}

func TestRecordReadWrite(t *testing.T) {
	pageBuffer := make([]byte, 1024)

	key := []byte("test_key")
	value := []byte("test_value")

	// Write record
	offset, err := WriteRecord(pageBuffer, 0, key, value)
	if err != nil {
		t.Fatalf("failed to write record: %v", err)
	}

	// Read record
	readKey, readValue, newOffset, err := ReadRecord(pageBuffer, 0)
	if err != nil {
		t.Fatalf("failed to read record: %v", err)
	}

	if string(readKey) != string(key) {
		t.Fatalf("key mismatch: got %s, expected %s", string(readKey), string(key))
	}

	if string(readValue) != string(value) {
		t.Fatalf("value mismatch: got %s, expected %s", string(readValue), string(value))
	}

	if newOffset != offset {
		t.Fatalf("offset mismatch: got %d, expected %d", newOffset, offset)
	}
}
