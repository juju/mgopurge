// Copyright 2018 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package lru_test

import (
	gc "gopkg.in/check.v1"

	"github.com/juju/lru"
)

type BenchmarkSuite struct{}

var _ = gc.Suite(&BenchmarkSuite{})

func (*BenchmarkSuite) BenchmarkAddAndEvict1000(c *gc.C) {
	cache := lru.New(1000)
	for i := 0; i < c.N; i++ {
		cache.Add(i, i)
	}
	expectLen := 1000
	if c.N < expectLen {
		expectLen = c.N
	}
	c.Assert(cache.Len(), gc.Equals, expectLen)
}
