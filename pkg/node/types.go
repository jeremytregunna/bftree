package node

// Comparator defines how to compare two keys.
// Returns: -1 if a < b, 0 if a == b, 1 if a > b
type Comparator[K any] func(a, b K) int

// KeyMarshaler defines how to serialize a key.
type KeyMarshaler[K any] func(k K) []byte

// KeyUnmarshaler defines how to deserialize a key.
type KeyUnmarshaler[K any] func(b []byte) (K, error)

// ValueMarshaler defines how to serialize a value.
type ValueMarshaler[V any] func(v V) []byte

// ValueUnmarshaler defines how to deserialize a value.
type ValueUnmarshaler[V any] func(b []byte) (V, error)

// Record represents a key-value pair retrieved from the tree.
type Record[K, V any] struct {
	Key   K
	Value V
	Type  RecordType
}

// Codecs holds all marshaling/unmarshaling functions for a key-value type.
type Codecs[K, V any] struct {
	Compare      Comparator[K]
	MarshalKey   KeyMarshaler[K]
	UnmarshalKey KeyUnmarshaler[K]
	MarshalValue ValueMarshaler[V]
	UnmarshalValue ValueUnmarshaler[V]
}
