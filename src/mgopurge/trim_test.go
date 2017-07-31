// Copyright 2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.
package main

import (
	"github.com/juju/testing"
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/mgo.v2/txn"
)

type TrimSuite struct {
	testing.IsolatedMgoSuite

	runner *txn.Runner
	db     *mgo.Database
	coll   *mgo.Collection
	txns   *mgo.Collection
}

var _ = gc.Suite(&TrimSuite{})

func (s *TrimSuite) SetUpTest(c *gc.C) {
	s.IsolatedMgoSuite.SetUpTest(c)
	s.db = s.Session.DB("test")
	s.txns = s.db.C("txns")
	s.coll = s.db.C("coll")
	s.runner = txn.NewRunner(s.txns)
}

func (s *TrimSuite) TestTrimSimpleTest(c *gc.C) {
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
	err = TrimLongTransactionQueues(s.txns, 100, "coll")
	c.Assert(err, jc.ErrorIsNil)
	// untouched
	err = s.coll.FindId(0).One(&result)
	c.Check(result["foo"], gc.Equals, "bar")
	c.Check(result["txn-queue"], gc.HasLen, 1)
}

func (s *TrimSuite) createDocWith51Txns(c *gc.C) {
	err := s.runner.Run([]txn.Op{{
		C:      s.coll.Name,
		Id:     0,
		Insert: bson.M{"foo": "bar"},
	}}, "", nil)
	c.Assert(err, jc.ErrorIsNil)
	// queue up a bunch of txns
	txn.SetChaos(txn.Chaos{
		KillChance: 1,
		Breakpoint: "set-applying",
	})
	defer txn.SetChaos(txn.Chaos{})
	ops := []txn.Op{{
		C:      s.coll.Name,
		Id:     0,
		Update: bson.M{"$set": bson.M{"foo": "baz"}},
	}}
	for i := 0; i < 50; i++ {
		c.Assert(s.runner.Run(ops, "", nil), gc.Equals, txn.ErrChaos)
	}
	var result bson.M
	err = s.coll.FindId(0).One(&result)
	c.Assert(err, jc.ErrorIsNil)
	c.Check(result["foo"], gc.Equals, "bar")
	c.Check(result["txn-queue"], gc.HasLen, 51)
}

func (s *TrimSuite) TestTrimNotLongEnough(c *gc.C) {
	s.createDocWith51Txns(c)
	err := TrimLongTransactionQueues(s.txns, 100, "coll")
	c.Assert(err, jc.ErrorIsNil)
	// untouched
	var result bson.M
	err = s.coll.FindId(0).One(&result)
	c.Check(result["foo"], gc.Equals, "bar")
	c.Check(result["txn-queue"], gc.HasLen, 51)
}

func (s *TrimSuite) TestTrimmedSingleDoc(c *gc.C) {
	s.createDocWith51Txns(c)
	trimmer := &LongTxnTrimmer{
		txns:         s.txns,
		longTxnSize:  50,
		txnBatchSize: 5,
	}
	count, err := s.txns.Count()
	c.Assert(err, jc.ErrorIsNil)
	c.Check(count, gc.Equals, 51)
	err = trimmer.Trim([]string{s.coll.Name})
	c.Assert(err, jc.ErrorIsNil)
	// All of the Prepared but not completed txns should be removed
	var result bson.M
	err = s.coll.FindId(0).One(&result)
	c.Check(result["foo"], gc.Equals, "bar")
	c.Check(result["txn-queue"], gc.HasLen, 1)
	c.Check(trimmer.docCleanupCount, gc.Equals, 1)
	count, err = s.txns.Count()
	c.Assert(err, jc.ErrorIsNil)
	c.Check(count, gc.Equals, 1)
}

func (s *TrimSuite) createMultiDocTxns(c *gc.C) {
	err := s.runner.Run([]txn.Op{{
		C:      s.coll.Name,
		Id:     0,
		Insert: bson.M{"foo": "bar"},
	}}, "", nil)
	c.Assert(err, jc.ErrorIsNil)
	err = s.runner.Run([]txn.Op{{
		C:      s.coll.Name,
		Id:     1,
		Insert: bson.M{"foo": "boing"},
	}}, "", nil)
	c.Assert(err, jc.ErrorIsNil)
	// queue up a bunch of txns
	txn.SetChaos(txn.Chaos{
		KillChance: 1,
		Breakpoint: "set-applying",
	})
	defer txn.SetChaos(txn.Chaos{})
	ops := []txn.Op{{
		C:      s.coll.Name,
		Id:     0,
		Update: bson.M{"$set": bson.M{"foo": "baz"}},
	}, {
		C:      s.coll.Name,
		Id:     1,
		Update: bson.M{"$set": bson.M{"foo": "bling"}},
	}}
	for i := 0; i < 50; i++ {
		c.Assert(s.runner.Run(ops, "", nil), gc.Equals, txn.ErrChaos)
	}
	var result bson.M
	err = s.coll.FindId(0).One(&result)
	c.Assert(err, jc.ErrorIsNil)
	c.Check(result["foo"], gc.Equals, "bar")
	c.Check(result["txn-queue"], gc.HasLen, 51)
	err = s.coll.FindId(1).One(&result)
	c.Assert(err, jc.ErrorIsNil)
	c.Check(result["foo"], gc.Equals, "boing")
	c.Check(result["txn-queue"], gc.HasLen, 51)
}

func (s *TrimSuite) TestTrimMultiDoc(c *gc.C) {
	s.createMultiDocTxns(c)
	trimmer := &LongTxnTrimmer{
		txns:         s.txns,
		longTxnSize:  50,
		txnBatchSize: 13,
	}
	count, err := s.txns.Count()
	c.Assert(err, jc.ErrorIsNil)
	c.Check(count, gc.Equals, 52)
	err = trimmer.Trim([]string{s.coll.Name})
	c.Assert(err, jc.ErrorIsNil)
	// All of the Prepared but not completed txns should be removed
	var result bson.M
	err = s.coll.FindId(0).One(&result)
	c.Check(result["foo"], gc.Equals, "bar")
	c.Check(result["txn-queue"], gc.HasLen, 1)
	err = s.coll.FindId(1).One(&result)
	c.Check(result["foo"], gc.Equals, "boing")
	c.Check(result["txn-queue"], gc.HasLen, 1)
	c.Check(trimmer.docCleanupCount, gc.Equals, 2)
	count, err = s.txns.Count()
	c.Assert(err, jc.ErrorIsNil)
	c.Check(count, gc.Equals, 2)
}

func (s *TrimSuite) TestTrimMultiDocWithExtras(c *gc.C) {
	s.createMultiDocTxns(c)
	// Now we also include another document that intersect with
	// those documents whose queues are way too long.
	// These transactions should not be removed
	txn.SetChaos(txn.Chaos{
		KillChance: 1,
		Breakpoint: "set-applying",
	})
	defer txn.SetChaos(txn.Chaos{})
	err := s.runner.Run([]txn.Op{{
		C:      s.coll.Name,
		Id:     0,
		Update: bson.M{"$set": bson.M{"baz": "bling"}},
	}, {
		C:      s.coll.Name,
		Id:     4,
		Insert: bson.M{"new": "stuff"},
	}}, "", nil)
	c.Assert(err, gc.Equals, txn.ErrChaos)
	trimmer := &LongTxnTrimmer{
		txns:         s.txns,
		longTxnSize:  50,
		txnBatchSize: 17,
	}
	count, err := s.txns.Count()
	c.Assert(err, jc.ErrorIsNil)
	c.Check(count, gc.Equals, 53)
	err = trimmer.Trim([]string{s.coll.Name})
	c.Assert(err, jc.ErrorIsNil)
	// All of the Prepared but not completed txns should be removed
	var result bson.M
	err = s.coll.FindId(0).One(&result)
	c.Check(result["foo"], gc.Equals, "bar")
	c.Check(result["txn-queue"], gc.HasLen, 2)
	err = s.coll.FindId(1).One(&result)
	c.Check(result["foo"], gc.Equals, "boing")
	c.Check(result["txn-queue"], gc.HasLen, 1)
	c.Check(trimmer.docCleanupCount, gc.Equals, 2)
	count, err = s.txns.Count()
	c.Assert(err, jc.ErrorIsNil)
	c.Check(count, gc.Equals, 3)
}
