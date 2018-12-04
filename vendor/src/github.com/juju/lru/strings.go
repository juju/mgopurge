// Copyright 2018 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package lru

import (
	"fmt"
)

type stringElem struct {
	value      string
	prev, next *stringElem
}

// StringCache tracks a limited number of strings.
// Use Intern() to get a saved version of the string, such that
//   x := cache.Intern(s1)
//   y := cache.Intern(s2)
// Now x and y will use the same underlying memory if s1 == s2.
type StringCache struct {
	maxSize int
	root    stringElem
	len     int
	values  map[string]*stringElem
}

func NewStringCache(size int) *StringCache {
	cache := &StringCache{
		maxSize: size,
	}
	cache.init()
	return cache
}

func (sc *StringCache) init() {
	sc.values = make(map[string]*stringElem, sc.maxSize)
	sc.root.prev = &sc.root
	sc.root.next = &sc.root
	sc.len = 0
}

// Len returns how many strings are currently cached
func (sc *StringCache) Len() int {
	return sc.len
}

// realloc creates a slice of memory, and puts everything in order and simplifies the pointers.
// this allocates into a single slab of memory, instead of being scattered around everywhere
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
	if count != sc.len {
		return fmt.Errorf("incorrect count, expected %d got %d", sc.len, count)
	}
	if len(sc.values) != sc.len {
		return fmt.Errorf("value map has wrong count, expected %d got %d", sc.len, len(sc.values))
	}
	return nil
}

// Intern takes a string, and returns either the cached copy of the string, or caches the string and returns it back.
// It also updates how recently the string was seen, so that strings aren't cached forever.
func (sc *StringCache) Intern(v string) string {
	if elem, ok := sc.values[v]; ok {
		sc.moveToFront(elem)
		return elem.value
	}
	var elem *stringElem
	if sc.len < sc.maxSize {
		elem = &stringElem{value: v}
		sc.len++
		if sc.len == sc.maxSize {
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

// Contains returns true if the string is in the cache. It does not change information about recently-used.
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
