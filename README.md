# Bf-Tree

A Go implementation of the Bf-Tree (Buffer-friendly B-Tree) data structure, based on the research paper:

**"Bf-Tree: A Byte-Level Friendly B-Tree for Efficient Key-Value Storage"**
https://vldb.org/pvldb/vol17/p3442-hao.pdf

## Overview

The Bf-Tree is a B-Tree variant optimized for modern hardware and read-write workloads. Key features:

- **Mini-pages**: Variable-sized in-memory record buffers that separate recent updates from disk pages
- **Circular buffer management**: All mini-page data lives in a managed circular buffer with automatic eviction
- **Lazy merging**: Mini-pages are merged with disk pages on demand or when memory pressure rises
- **No heap escapes**: Mini-page key-value data uses manual offset tracking to avoid Go slice append() escapes
- **Optimistic latch coupling**: Read locks on inner nodes using version-based validation

## Architecture

```
BfTree
├── Root (InnerNode, pinned)
├── Circular Buffer (memory-managed mini-page storage)
├── Mini-pages (indexed by leaf page ID)
├── Mapping Table (logical page ID → disk offset)
├── Merger (mini-page to disk merging)
└── Storage (disk I/O for leaf pages)
```

## Usage

```go
package main

import "github.com/jeremytregunna/bftree/pkg/tree"

func main() {
    // Create a tree with 256KB buffer and storage file
    t, err := tree.NewBfTree(256*1024, "tree.db")
    if err != nil {
        panic(err)
    }
    defer t.Close()

    // Insert key-value pairs
    t.Insert([]byte("key1"), []byte("value1"))
    t.Insert([]byte("key2"), []byte("value2"))

    // Retrieve values
    value, err := t.Get([]byte("key1"))
    if err == nil {
        println(string(value))
    }

    // Range scan
    records, err := t.Scan([]byte("key1"), []byte("key3"))
    for _, rec := range records {
        println(string(rec.Key), string(rec.Value))
    }

    // Delete (soft delete via tombstone)
    t.Delete([]byte("key1"))

    // Update
    t.Update([]byte("key2"), []byte("newvalue"))
}
```

## Implementation Details

- **Mini-pages**: Fixed-capacity buffers allocated from circular buffer. Data uses `kvDataLen` tracking instead of slice length to prevent heap escapes.
- **Buffer allocation**: Mini-pages allocate from `CircularBuffer.Alloc()` with automatic eviction on pressure.
- **Growth strategy**: When a mini-page grows, it reallocates a larger chunk from the buffer, copies used data, and deallocates the old chunk.
- **Eviction**: Dirty mini-pages are merged to disk via the merger, and their buffer chunks are returned to the free lists.
- **Memory efficiency**: Only active mini-pages live in memory; older updates are persisted to disk.

## Testing

All 23 tests pass, including:
- Basic operations (Insert, Get, Delete, Update, Scan)
- Disk persistence and merging
- Buffer allocation and eviction
- Verification that mini-page data actually lives in the circular buffer

Run tests with:
```bash
go test ./...
```

## Status

Complete implementation with all core Bf-Tree features working and verified.

## License

Copyright 2025 Jeremy Tregunna
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[https://www.apache.org/licenses/LICENSE-2.0](https://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
