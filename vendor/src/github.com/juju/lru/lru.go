// Copyright 2018 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

// LRU implements a least-recently-used cache, which tracks what items have been accessed,
// and evicts them when adding a new item if it hasn't been used recently.
package lru

import (
	"container/list"
)

// LRU implements a least-recently-used cache, evicting items from the cache if they have not been accessed in a while.
type LRU struct {
	maxSize int
	keys    *list.List
	values  map[interface{}]cacheEntry
}

type cacheEntry struct {
	listElem *list.Element
	value    interface{}
}

// Create a new LRU cache that will hold no more than the given number of items,
func New(size int) *LRU {
	return &LRU{
		maxSize: size,
		keys:    list.New(),
		values:  make(map[interface{}]cacheEntry, 0),
	}
}

// Len gives the number of items in the cache
func (lru *LRU) Len() int {
	return len(lru.values)
}

// Add a new entry into the LRU cache
func (lru *LRU) Add(key, value interface{}) {
	entry, exists := lru.values[key]
	if exists {
		lru.keys.MoveToFront(entry.listElem)
		// Update the value
		entry.value = value
	} else {
		// We are adding an element, make sure there is room
		if lru.keys.Len() >= lru.maxSize {
			lru.removeLast()
		}
		elem := lru.keys.PushFront(key)
		entry = cacheEntry{
			listElem: elem,
			value:    value,
		}
	}
	lru.values[key] = entry
}

// Get returns the Value associated with key, and a boolean as to whether it actually exists in the cache.
// If it does exist in the cache, then it is treated as recently accessed.
func (lru *LRU) Get(key interface{}) (interface{}, bool) {
	entry, exists := lru.values[key]
	if exists {
		lru.keys.MoveToFront(entry.listElem)
		return entry.value, true
	} else {
		return nil, false
	}
}

// Peek is just like Get() except it doesn't affect if it was 'recently accessed'
func (lru *LRU) Peek(key interface{}) (interface{}, bool) {
	if entry, exists := lru.values[key]; exists {
		return entry.value, true
	}
	return nil, false
}

// removeLast removes the oldest entry from the queue
func (lru *LRU) removeLast() {
	last := lru.keys.Back()
	if last != nil {
		lru.keys.Remove(last)
	}
	// remember the "Value" in the list is actually a key
	delete(lru.values, last.Value)
}
