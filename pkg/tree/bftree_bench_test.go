package tree

import (
	"fmt"
	"os"
	"testing"
)

func BenchmarkInsert(b *testing.B) {
	tree, tmpPath := createTempTree(&testing.T{})
	defer tree.Close()
	defer os.Remove(tmpPath)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%08d", i)
		value := fmt.Sprintf("val%08d", i)

		tree.Insert(key, value)
	}
}

func BenchmarkGet(b *testing.B) {
	tree, tmpPath := createTempTree(&testing.T{})
	defer tree.Close()
	defer os.Remove(tmpPath)

	// Populate with data first
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%08d", i)
		value := fmt.Sprintf("val%08d", i)
		tree.Insert(key, value)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%08d", i%1000)
		tree.Get(key)
	}
}

func BenchmarkDelete(b *testing.B) {
	tree, tmpPath := createTempTree(&testing.T{})
	defer tree.Close()
	defer os.Remove(tmpPath)

	// Populate with data first
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%08d", i)
		value := fmt.Sprintf("val%08d", i)
		tree.Insert(key, value)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%08d", i)
		tree.Delete(key)
	}
}

func BenchmarkScan(b *testing.B) {
	tree, tmpPath := createTempTree(&testing.T{})
	defer tree.Close()
	defer os.Remove(tmpPath)

	// Populate with data first
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%08d", i)
		value := fmt.Sprintf("val%08d", i)
		tree.Insert(key, value)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		startKey := fmt.Sprintf("key%08d", 1000)
		endKey := fmt.Sprintf("key%08d", 2000)
		tree.Scan(startKey, endKey)
	}
}

func BenchmarkMixedWorkload(b *testing.B) {
	tree, tmpPath := createTempTree(&testing.T{})
	defer tree.Close()
	defer os.Remove(tmpPath)

	b.ResetTimer()

	insertCount := 0
	for i := 0; i < b.N; i++ {
		op := i % 10 // 0-7: insert, 8: delete, 9: get
		key := fmt.Sprintf("key%08d", i)
		value := fmt.Sprintf("val%08d", i)

		switch {
		case op < 7:
			tree.Insert(key, value)
			insertCount++
		case op == 7:
			delKey := fmt.Sprintf("key%08d", (i-1)%insertCount)
			tree.Delete(delKey)
		case op == 8:
			getKey := fmt.Sprintf("key%08d", (i-1)%insertCount)
			tree.Get(getKey)
		}
	}
}
