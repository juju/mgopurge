// Copyright 2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.
package main

import (
	gc "gopkg.in/check.v1"
	jc "github.com/juju/testing/checkers"
	"github.com/juju/testing"
	"gopkg.in/mgo.v2/txn"
	"gopkg.in/mgo.v2/bson"
)

type TrimSuite struct {
	testing.IsolatedMgoSuite
}

var _ = gc.Suite(&TrimSuite{})

func (s *TrimSuite) TestTrimSimpleTest(c *gc.C) {
	db := s.Session.DB("test")
	txns := db.C("txns")
	coll := db.C("coll")
	runner := txn.NewRunner(txns)
	err := runner.Run([]txn.Op{{
		C: "coll",
		Id: 0,
		Insert: bson.M{"foo": "bar"},
	}}, "", nil)
	c.Assert(err, jc.ErrorIsNil)
	var result bson.M
	err = coll.FindId(0).One(&result)
	c.Assert(err, jc.ErrorIsNil)
	c.Check(result["foo"], gc.Equals, "bar")
}
