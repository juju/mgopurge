# lru
A Go implementation of a least-recently-used cache.

Use `lru.New(size)` to create a new LRU cache that will hold up to `size`
entries. Note that the LRU object is not thread safe, so you will need to add a
mutex if it is accessed from multiple goroutines.
