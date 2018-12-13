// Copyright 2018 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package lru_test

import (
	"testing"

	gc "gopkg.in/check.v1"

	"github.com/juju/lru"
)

func TestAll(t *testing.T) {
	// Pass nil for Certs because we don't need SSL
	gc.TestingT(t)
}

type LRUSuite struct{}

var _ = gc.Suite(&LRUSuite{})

func checkPeekExists(c *gc.C, cache *lru.LRU, key, value interface{}) {
	checkedValue, checkedExists := cache.Peek(key)
	c.Check(checkedExists, gc.Equals, true,
		gc.Commentf("key %#v did not exist in cache", key))
	if checkedExists {
		c.Check(checkedValue, gc.Equals, value)
	}
}

func checkPeekMissing(c *gc.C, cache *lru.LRU, key interface{}) {
	checkedValue, checkedExists := cache.Peek(key)
	c.Check(checkedExists, gc.Equals, false,
		gc.Commentf("key %#v shouldn't have been present in cache with value %v", key, checkedValue))
}

func checkGet(c *gc.C, cache *lru.LRU, key, value interface{}, exists bool) {
	checkedValue, checkedExists := cache.Get(key)
	c.Check(checkedValue, gc.Equals, value)
	c.Check(checkedExists, gc.Equals, exists)
}

func simpleFullCache() *lru.LRU {
	cache := lru.New(10)
	cache.Add(1, "a")
	cache.Add(2, "b")
	cache.Add(3, "c")
	cache.Add(4, "d")
	cache.Add(5, "e")
	cache.Add(6, "f")
	cache.Add(7, "g")
	cache.Add(8, "h")
	cache.Add(9, "i")
	cache.Add(0, "j")
	return cache
}

func (s *LRUSuite) TestLRUAdd(c *gc.C) {
	cache := lru.New(128)
	cache.Add("foo", "bar")
	checkPeekExists(c, cache, "foo", "bar")
	checkPeekMissing(c, cache, "bar")
}

func (s *LRUSuite) TestLRUAddReplaces(c *gc.C) {
	cache := simpleFullCache()
	cache.Add(1, "bar")
	checkPeekExists(c, cache, 1, "bar")
}

func (s *LRUSuite) TestLRUAddEvicts(c *gc.C) {
	cache := simpleFullCache()
	cache.Add("a", "k")
	// 1 being the least recent, should be evicted
	checkPeekMissing(c, cache, 1)
	checkPeekExists(c, cache, 2, "b")
	checkPeekExists(c, cache, 3, "c")
	checkPeekExists(c, cache, 4, "d")
	checkPeekExists(c, cache, 5, "e")
	checkPeekExists(c, cache, 6, "f")
	checkPeekExists(c, cache, 7, "g")
	checkPeekExists(c, cache, 8, "h")
	checkPeekExists(c, cache, 9, "i")
	checkPeekExists(c, cache, 0, "j")
	checkPeekExists(c, cache, "a", "k")
}

func (s *LRUSuite) TestLRUGetNoExists(c *gc.C) {
	cache := simpleFullCache()
	checkGet(c, cache, "nope", nil, false)
}

func (s *LRUSuite) TestLRUGetUpdatesEvict(c *gc.C) {
	cache := simpleFullCache()
	checkGet(c, cache, 1, "a", true)
	checkGet(c, cache, 2, "b", true)
	// Add a value, which should evict 3, because 1 and 2 have been accessed
	cache.Add("q", "blah")
	checkPeekExists(c, cache, 1, "a")
	checkPeekExists(c, cache, 2, "b")
	checkPeekMissing(c, cache, 3)
	checkPeekExists(c, cache, 4, "d")
	checkPeekExists(c, cache, 5, "e")
	checkPeekExists(c, cache, 6, "f")
	checkPeekExists(c, cache, 7, "g")
	checkPeekExists(c, cache, 8, "h")
	checkPeekExists(c, cache, 9, "i")
	checkPeekExists(c, cache, 0, "j")
	checkPeekExists(c, cache, "q", "blah")
}

func (s *LRUSuite) TestLRULen(c *gc.C) {
	cache := lru.New(20)
	for i := 1; i <= 25; i++ {
		cache.Add(i, i)
		if i < 20 {
			c.Check(cache.Len(), gc.Equals, i)
		} else {
			c.Check(cache.Len(), gc.Equals, 20)
		}
	}
}
