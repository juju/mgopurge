// Copyright 2018 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package lru

import (
	"fmt"
)

// StringCache tracks a limited number of strings.
// Use Intern() to get a saved version of the string, such that
//   x := cache.Intern(s1)
//   y := cache.Intern(s2)
// Now x and y will use the same underlying memory if s1 == s2.
// We track a map into a doubly linked list, moving accessed (or recently
// added) strings to the front of the list, and using the expiry at the end of
// the list to maintain the size of the cache.
// Note that StringCache is *not* thread safe, some form of mutex is necessary
// if you want to access it from multiple threads.
type StringCache struct {
	maxSize   int
	size      int
	hitCount  int64
	missCount int64
	root      stringElem
	values    map[string]*stringElem
}

// NewStringCache creates a cache for string objects that will hold no-more
// than 'size' strings.
func NewStringCache(size int) *StringCache {
	cache := &StringCache{
		maxSize: size,
	}
	cache.init()
	return cache
}

// stringElem represents a doubly linked list of elements, which allows us to
// move an entry from anywhere in the list to the 'front' of the list whenever
// it is accessed.
// For a complete double-linked list implementation, see the golang standard
// library "container/list". However, by inlining some of the functionality,
// we are able to save on storage size, and remove levels of indirection.
// The only function we need is 'MoveToFront', as we also never remove items,
// instead replacing the content when the old value is expired.
type stringElem struct {
	value      string
	prev, next *stringElem
}

func (sc *StringCache) init() {
	sc.values = make(map[string]*stringElem, sc.maxSize)
	sc.root.prev = &sc.root
	sc.root.next = &sc.root
	sc.size = 0
}

// Len returns how many strings are currently cached
func (sc *StringCache) Len() int {
	return sc.size
}

// HitCounts is used to track how well this cache is working
type HitCounts struct {
	Hit, Miss int64
}

// HitCounts gives information about accesses to the cache. The total number of
// calls to Intern can be computed by adding Hit and Miss.
func (sc *StringCache) HitCounts() HitCounts {
	return HitCounts{
		Hit:  sc.hitCount,
		Miss: sc.missCount,
	}
}

// realloc creates a slice of memory, and puts everything in order and
// simplifies the pointers.  This allocates into a single slab of memory,
// instead of being scattered around everywhere. We call this once our cache is
// full, and we know we won't be allocating any more elements.
func (sc *StringCache) realloc() {
	buff := make([]stringElem, sc.maxSize)
	cur := sc.root.next
	for i := 0; i < sc.maxSize; i++ {
		buff[i].value = cur.value
		if i > 0 {
			buff[i].prev = &buff[i-1]
		}
		if i < sc.maxSize-1 {
			buff[i].next = &buff[i+1]
		}
		sc.values[cur.value] = &buff[i]
		cur = cur.next
	}
	sc.root.next = &buff[0]
	sc.root.prev = &buff[sc.maxSize-1]
	buff[0].prev = &sc.root
	buff[sc.maxSize-1].next = &sc.root
}

// Validate checks invariants to make sure the double-linked list is properly
// linked, and that the values map to the correct element.
func (sc *StringCache) Validate() error {
	count := 0
	for cur := sc.root.next; cur != &sc.root; cur = cur.next {
		count++
		if cur.prev.next != cur {
			return fmt.Errorf("error at %#v, the value after prev is not this", cur)
		}
		if cur.next.prev != cur {
			return fmt.Errorf("error at %#v, the value before next is not this", cur)
		}
		v := cur.value
		if sc.values[v] != cur {
			return fmt.Errorf("error at %q, %p %# v, the map doesn't point to cur it points to: %p %# v",
				v, cur, cur, sc.values[v], sc.values[v])
		}
	}
	if count != sc.size {
		return fmt.Errorf("incorrect count, expected %d got %d", sc.size, count)
	}
	if len(sc.values) != sc.size {
		return fmt.Errorf("value map has wrong count, expected %d got %d", sc.size, len(sc.values))
	}
	return nil
}

// Intern takes a string, and returns either the cached copy of the string, or
// caches the string and returns it back.  It also updates how recently the
// string was seen, so that strings aren't cached forever.
func (sc *StringCache) Intern(v string) string {
	if elem, ok := sc.values[v]; ok {
		sc.moveToFront(elem)
		v := elem.value
		sc.hitCount++
		return v
	}
	sc.missCount++
	var elem *stringElem
	if sc.size < sc.maxSize {
		elem = &stringElem{value: v}
		sc.size++
		if sc.size == sc.maxSize {
			sc.moveToFront(elem)
			sc.realloc()
			return v
		}
	} else {
		elem = sc.root.prev
		delete(sc.values, elem.value)
		elem.value = v
	}
	sc.moveToFront(elem)
	sc.values[v] = elem
	return v
}

// Contains returns true if the string is in the cache. It does not change
// information about recently-used.
func (sc *StringCache) Contains(v string) bool {
	_, ok := sc.values[v]
	return ok
}

func (sc *StringCache) moveToFront(elem *stringElem) {
	if sc.root.next == elem {
		// we're already at the front
		return
	}
	if elem.prev != nil {
		// remove it from its current spot
		elem.prev.next = elem.next
		elem.next.prev = elem.prev
	}
	next := sc.root.next
	sc.root.next = elem
	elem.prev = &sc.root
	elem.next = next
	next.prev = elem
}
