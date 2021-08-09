// Copyright 2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.
package main

import (
	"github.com/juju/mgo/v2"
	"github.com/juju/mgo/v2/bson"
	"github.com/juju/mgo/v2/txn"
	"github.com/juju/testing"
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
)

type InvalidTxnReferenceCleanerSuite struct {
	testing.IsolatedMgoSuite

	runner *txn.Runner
	db     *mgo.Database
	coll   *mgo.Collection
	txns   *mgo.Collection
}

var _ = gc.Suite(&InvalidTxnReferenceCleanerSuite{})

func (s *InvalidTxnReferenceCleanerSuite) SetUpTest(c *gc.C) {
	s.IsolatedMgoSuite.SetUpTest(c)
	s.db = s.Session.DB("test")
	s.txns = s.db.C("txns")
	s.coll = s.db.C("coll")
	s.runner = txn.NewRunner(s.txns)
}

func (s *InvalidTxnReferenceCleanerSuite) TestNoMissingDoc(c *gc.C) {
	err := s.runner.Run([]txn.Op{{
		C:      "coll",
		Id:     0,
		Insert: bson.M{"foo": "bar"},
	}}, "", nil)
	c.Assert(err, jc.ErrorIsNil)
	var result bson.M
	err = s.coll.FindId(0).One(&result)
	c.Assert(err, jc.ErrorIsNil)
	c.Check(result["foo"], gc.Equals, "bar")
	c.Check(result["txn-queue"], gc.HasLen, 1)
	err = CleanupInvalidTxnReferences(s.txns)
	c.Assert(err, jc.ErrorIsNil)
	c.Check(result["foo"], gc.Equals, "bar")
	c.Check(result["txn-queue"], gc.HasLen, 1)
}

func tokenToObjectId(token string) bson.ObjectId {
	return bson.ObjectIdHex(token[:24])
}
func (s *InvalidTxnReferenceCleanerSuite) TestTxnWithNoDoc(c *gc.C) {
	err := s.runner.Run([]txn.Op{{
		C:      "coll",
		Id:     0,
		Insert: bson.M{"foo": "bar"},
	}}, "", nil)
	c.Assert(err, jc.ErrorIsNil)
	txn.SetChaos(txn.Chaos{
		KillChance: 1,
		Breakpoint: "set-applying",
	})
	defer txn.SetChaos(txn.Chaos{})
	ops := []txn.Op{{
		C:      s.coll.Name,
		Id:     0,
		Assert: bson.M{"foo": "bar"}, // assert that the document still says "bar"
		Update: bson.M{"$set": bson.M{"foo": "baz"}},
	}}
	oid := bson.NewObjectId()
	c.Assert(s.runner.Run(ops, oid, nil), gc.Equals, txn.ErrChaos)
	var result bson.M
	err = s.coll.FindId(0).One(&result)
	c.Assert(err, jc.ErrorIsNil)
	c.Check(result["foo"], gc.Equals, "bar")
	queue := result["txn-queue"].([]interface{})
	c.Check(queue, gc.HasLen, 2)
	token := queue[1].(string)
	c.Check(tokenToObjectId(token), gc.Equals, oid)
	s.coll.RemoveId(0)
	var raw rawTransaction
	c.Assert(s.txns.FindId(oid).One(&raw), jc.ErrorIsNil)
	c.Check(raw.State, gc.Equals, 2) // prepared
	err = CleanupInvalidTxnReferences(s.txns)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(s.txns.FindId(oid).One(&raw), jc.ErrorIsNil)
	c.Check(raw.State, gc.Equals, 1) // preparing
	c.Assert(s.runner.ResumeAll(), jc.ErrorIsNil)
	c.Assert(s.txns.FindId(oid).One(&raw), jc.ErrorIsNil)
	c.Check(raw.State, gc.Equals, 5) // cancelled
	c.Assert(s.coll.FindId(0).One(&result), gc.Equals, mgo.ErrNotFound)
}
