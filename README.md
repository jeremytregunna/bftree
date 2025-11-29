# Bf-Tree

A Go implementation of the Bf-Tree (Buffer-friendly B-Tree) data structure, based on the research paper:

**"Bf-Tree: A Byte-Level Friendly B-Tree for Efficient Key-Value Storage"**
https://vldb.org/pvldb/vol17/p3442-hao.pdf

## Overview

The Bf-Tree is a B-Tree variant optimized for modern hardware and read-write workloads. Key features:

- **Generic type system**: Fully generic over `BfTree[K, V]` using Go 1.18+ generics
- **Codecs pattern**: Pluggable comparison and serialization functions for any key-value types
- **Mini-pages**: Variable-sized in-memory record buffers that separate recent updates from disk pages
- **Circular buffer management**: All mini-page data lives in a managed circular buffer with automatic eviction
- **Lazy merging**: Mini-pages are merged with disk pages on demand or when memory pressure rises
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

The Bf-Tree is generic and works with any key-value types. You provide a `Codecs` struct with comparison and serialization functions:

```go
package main

import (
    "github.com/jeremytregunna/bftree/pkg/node"
    "github.com/jeremytregunna/bftree/pkg/tree"
)

func main() {
    // Create codecs for string keys and values
    codecs := &node.Codecs[string, string]{
        Compare: func(a, b string) int {
            if a < b { return -1 }
            if a > b { return 1 }
            return 0
        },
        MarshalKey:     func(k string) []byte { return []byte(k) },
        UnmarshalKey:   func(b []byte) (string, error) { return string(b), nil },
        MarshalValue:   func(v string) []byte { return []byte(v) },
        UnmarshalValue: func(b []byte) (string, error) { return string(b), nil },
    }

    // Create tree with 256KB buffer
    t, err := tree.NewBfTree[string, string](256*1024, "tree.db", codecs)
    if err != nil {
        panic(err)
    }
    defer t.Close()

    // Insert key-value pairs
    t.Insert("key1", "value1")
    t.Insert("key2", "value2")

    // Retrieve values
    value, err := t.Get("key1")
    if err == nil {
        println(value)
    }

    // Range scan
    records, err := t.Scan("key1", "key3")
    for _, rec := range records {
        println(rec.Key, rec.Value)
    }

    // Delete (soft delete via tombstone)
    t.Delete("key1")

    // Update
    t.Update("key2", "newvalue")
}
```

## Implementation Details

- **Generics architecture**: All components generic - `InnerNode[K]`, `MiniPage[K, V]`, `BfTree[K, V]`, `Merger[K, V]`
- **Codecs pattern**: Separation of key comparison from serialization. Codecs struct provides `Compare`, `MarshalKey`, `UnmarshalKey`, `MarshalValue`, `UnmarshalValue`
- **Mini-pages**: Typed key-value storage with Record types (Insert, Cache, Tombstone, Phantom)
- **Buffer allocation**: Mini-pages allocate from `CircularBuffer.Alloc()` with automatic eviction on pressure
- **Merger strategy**: Deserializes disk pages using codecs, merges with in-memory records, re-serializes for disk storage
- **Memory efficiency**: Only active mini-pages live in memory; older updates are persisted to disk

## Testing

All 20+ tests pass with the generic type system, including:
- Basic operations with string codecs (Insert, Get, Delete, Update, Scan)
- Merger operations (simple merge, delete merge, split merge)
- Disk persistence and merging
- Buffer allocation and eviction
- Verification that mini-page data actually lives in the circular buffer

Run tests with:
```bash
go test ./pkg/...
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
