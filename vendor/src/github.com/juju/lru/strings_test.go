// Copyright 2018 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package lru_test

import (
	"fmt"
	"math/rand"
	"sync"
	"unsafe"

	gc "gopkg.in/check.v1"

	"github.com/juju/lru"
)

type StringsSuite struct{}

// this is taken from runtime/string.go
type stringStruct struct {
	str unsafe.Pointer
	len int
}

var _ = gc.Suite(&StringsSuite{})

func isSameStr(s1, s2 string) bool {
	if len(s1) != len(s2) {
		return false
	}
	ss1 := *(*stringStruct)(unsafe.Pointer(&s1))
	ss2 := *(*stringStruct)(unsafe.Pointer(&s2))
	return ss1.str == ss2.str
}

func (*StringsSuite) TestIntern(c *gc.C) {
	str1 := fmt.Sprintf("foo%s", "bar")
	str2 := fmt.Sprintf("foo%s", "bar")
	c.Check(isSameStr(str1, str2), gc.Equals, false)

	cache := lru.NewStringCache(100)
	str3 := cache.Intern(str1)
	c.Check(isSameStr(str1, str3), gc.Equals, true)
	str4 := cache.Intern(str2)
	c.Check(isSameStr(str1, str4), gc.Equals, true)
	c.Check(cache.Len(), gc.Equals, 1)
	c.Check(cache.Contains(str1), gc.Equals, true)
}

func (*StringsSuite) TestInternMaxSize(c *gc.C) {
	cache := lru.NewStringCache(5)
	for i := 0; i < 30; i++ {
		s := fmt.Sprint(i)
		res := cache.Intern(s)
		c.Check(res, gc.Equals, s)
		c.Check(isSameStr(s, res), gc.Equals, true)
		c.Check(cache.Contains(fmt.Sprint(i)), gc.Equals, true,
			gc.Commentf("%d should immediately be cached", i))
		err := cache.Validate()
		c.Assert(err, gc.IsNil, gc.Commentf("Validate adding %d failed with: %s", i, err))
	}
	c.Check(cache.Len(), gc.Equals, 5)
	for i := 0; i < 25; i++ {
		c.Check(cache.Contains(fmt.Sprint(i)), gc.Equals, false,
			gc.Commentf("%d was not supposed to be cached", i))
	}
	for i := 25; i < 30; i++ {
		c.Check(cache.Contains(fmt.Sprint(i)), gc.Equals, true,
			gc.Commentf("%d was supposed to be cached", i))
	}
}

func (*StringsSuite) TestInternAbuse(c *gc.C) {
	const totalKeys = 100000
	const totalUniqueKeys = 1000
	keys := make([]string, totalKeys)
	for i := 0; i < totalKeys; i++ {
		keys[i] = fmt.Sprint((i % totalUniqueKeys) + 1000000)
	}
	rand.Shuffle(c.N, func(i, j int) { keys[j], keys[i] = keys[i], keys[j] })
	var size int = totalUniqueKeys * 0.75
	c.Logf("using size: %d", size)
	cache := lru.NewStringCache(size)
	for _, k := range keys {
		v := cache.Intern(k)
		c.Assert(v, gc.Equals, k)
	}
	c.Check(cache.Len(), gc.Equals, size)
}

func (*StringsSuite) TestHitCount(c *gc.C) {
	cache := lru.NewStringCache(5)
	cache.Intern("a")
	cache.Intern("b")
	cache.Intern("c")
	cache.Intern("d")
	cache.Intern("d")
	cache.Intern("d")
	cache.Intern("d")
	c.Check(cache.HitCounts(), gc.Equals, lru.HitCounts{Hit: 3, Miss: 4})
	cache.Intern("e")
	cache.Intern("f")
	cache.Intern("a")
	// we overflowed, so everything misses
	c.Check(cache.HitCounts(), gc.Equals, lru.HitCounts{Hit: 3, Miss: 7})
}

func (*StringsSuite) TestInternMultithreaded(c *gc.C) {
	const totalKeys = 100000
	const totalUniqueKeys = 1000
	const threads = 10
	keys := make([]string, totalKeys)
	for i := 0; i < totalKeys; i++ {
		keys[i] = fmt.Sprint((i % totalUniqueKeys) + 1000000)
	}
	var size int = totalUniqueKeys * 0.75
	c.Logf("using size: %d", size)
	cache := lru.NewStringCache(size)
	var wg sync.WaitGroup
	var mu sync.Mutex
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localKeys := keys[:]
			rand.Shuffle(c.N, func(i, j int) { localKeys[j], localKeys[i] = localKeys[i], localKeys[j] })
			for _, k := range keys {
				mu.Lock()
				v := cache.Intern(k)
				mu.Unlock()
				if v != k {
					c.Errorf("key %q mapped to %q", k, v)
					return
				}
			}
		}()
	}
	wg.Wait()
	c.Check(cache.Len(), gc.Equals, size)
	hitCount := cache.HitCounts()
	c.Logf("hit count: %# v", hitCount)
	c.Check(hitCount.Hit+hitCount.Miss, gc.Equals, int64(totalKeys*threads))
}

var _ = gc.Suite(&BenchmarkStrings{})

type BenchmarkStrings struct{}

func (*BenchmarkStrings) BenchmarkInternRand0000010(c *gc.C) {
	benchmarkIntern(c, 10, true)
}

func (*BenchmarkStrings) BenchmarkInternRand0000020(c *gc.C) {
	benchmarkIntern(c, 20, true)
}

func (*BenchmarkStrings) BenchmarkInternRand0000050(c *gc.C) {
	benchmarkIntern(c, 50, true)
}

func (*BenchmarkStrings) BenchmarkInternRand0000100(c *gc.C) {
	benchmarkIntern(c, 100, true)
}

func (*BenchmarkStrings) BenchmarkInternRand0000200(c *gc.C) {
	benchmarkIntern(c, 200, true)
}

func (*BenchmarkStrings) BenchmarkInternRand0000500(c *gc.C) {
	benchmarkIntern(c, 500, true)
}

func (*BenchmarkStrings) BenchmarkInternRand0001000(c *gc.C) {
	benchmarkIntern(c, 1000, true)
}

func (*BenchmarkStrings) BenchmarkInternRand0002000(c *gc.C) {
	benchmarkIntern(c, 2000, true)
}

func (*BenchmarkStrings) BenchmarkInternRand0005000(c *gc.C) {
	benchmarkIntern(c, 5000, true)
}

func (*BenchmarkStrings) BenchmarkInternRand0010000(c *gc.C) {
	benchmarkIntern(c, 10000, true)
}

func (*BenchmarkStrings) BenchmarkInternRand0020000(c *gc.C) {
	benchmarkIntern(c, 20000, true)
}

func (*BenchmarkStrings) BenchmarkInternRand0050000(c *gc.C) {
	benchmarkIntern(c, 50000, true)
}

func (*BenchmarkStrings) BenchmarkInternRand0100000(c *gc.C) {
	benchmarkIntern(c, 100000, true)
}

func (*BenchmarkStrings) BenchmarkInternRand0200000(c *gc.C) {
	benchmarkIntern(c, 200000, true)
}

func (*BenchmarkStrings) BenchmarkInternRand0500000(c *gc.C) {
	benchmarkIntern(c, 500000, true)
}

func (*BenchmarkStrings) BenchmarkInternRand1000000(c *gc.C) {
	benchmarkIntern(c, 1000000, true)
}

func (*BenchmarkStrings) BenchmarkInternRand2000000(c *gc.C) {
	benchmarkIntern(c, 2000000, true)
}

func benchmarkIntern(c *gc.C, size int, randomize bool) {
	strs := make([]string, c.N)
	for i := 0; i < c.N; i++ {
		// We want reasonably long strings
		strs[i] = fmt.Sprint(i + 10000000)
	}
	if randomize {
		rand.Shuffle(c.N, func(i, j int) { strs[j], strs[i] = strs[i], strs[j] })
	}
	cache := lru.NewStringCache(size)
	c.ResetTimer()
	for i := 0; i < c.N; i++ {
		cache.Intern(strs[i])
	}
	expectLen := size
	if c.N < expectLen {
		expectLen = c.N
	}
	c.Assert(cache.Len(), gc.Equals, expectLen)
}

func (*BenchmarkStrings) BenchmarkIntern0000010(c *gc.C) {
	benchmarkIntern(c, 10, false)
}

func (*BenchmarkStrings) BenchmarkIntern0000020(c *gc.C) {
	benchmarkIntern(c, 20, false)
}

func (*BenchmarkStrings) BenchmarkIntern0000050(c *gc.C) {
	benchmarkIntern(c, 50, false)
}

func (*BenchmarkStrings) BenchmarkIntern0000100(c *gc.C) {
	benchmarkIntern(c, 100, false)
}

func (*BenchmarkStrings) BenchmarkIntern0000200(c *gc.C) {
	benchmarkIntern(c, 200, false)
}

func (*BenchmarkStrings) BenchmarkIntern0000500(c *gc.C) {
	benchmarkIntern(c, 500, false)
}

func (*BenchmarkStrings) BenchmarkIntern0001000(c *gc.C) {
	benchmarkIntern(c, 1000, false)
}

func (*BenchmarkStrings) BenchmarkIntern0002000(c *gc.C) {
	benchmarkIntern(c, 2000, false)
}

func (*BenchmarkStrings) BenchmarkIntern0005000(c *gc.C) {
	benchmarkIntern(c, 5000, false)
}

func (*BenchmarkStrings) BenchmarkIntern0010000(c *gc.C) {
	benchmarkIntern(c, 10000, false)
}

func (*BenchmarkStrings) BenchmarkIntern0020000(c *gc.C) {
	benchmarkIntern(c, 20000, false)
}

func (*BenchmarkStrings) BenchmarkIntern0050000(c *gc.C) {
	benchmarkIntern(c, 50000, false)
}

func (*BenchmarkStrings) BenchmarkIntern0100000(c *gc.C) {
	benchmarkIntern(c, 100000, false)
}

func (*BenchmarkStrings) BenchmarkIntern0200000(c *gc.C) {
	benchmarkIntern(c, 200000, false)
}

func (*BenchmarkStrings) BenchmarkIntern0500000(c *gc.C) {
	benchmarkIntern(c, 500000, false)
}

func (*BenchmarkStrings) BenchmarkIntern1000000(c *gc.C) {
	benchmarkIntern(c, 1000000, false)
}

func (*BenchmarkStrings) BenchmarkIntern2000000(c *gc.C) {
	benchmarkIntern(c, 2000000, false)
}

func (*BenchmarkStrings) BenchmarkInternMemSize(c *gc.C) {
	keys := make([]string, c.N)
	for i := 0; i < c.N; i++ {
		keys[i] = fmt.Sprint(i + 1e7)
	}
	rand.Shuffle(c.N, func(i, j int) { keys[j], keys[i] = keys[i], keys[j] })
	c.ResetTimer()
	cache := lru.NewStringCache(c.N)
	for i := 0; i < c.N; i++ {
		cache.Intern(keys[i])
	}
}
