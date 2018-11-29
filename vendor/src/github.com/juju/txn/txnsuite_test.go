// Copyright 2017 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package txn_test

import (
	"sync/atomic"
	"time"

	"github.com/juju/testing"
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/mgo.v2/txn"
)

type TxnSuite struct {
	testing.IsolationSuite
	testing.MgoSuite
	db     *mgo.Database
	txns   *mgo.Collection
	runner *txn.Runner
}

func (s *TxnSuite) SetUpSuite(c *gc.C) {
	s.IsolationSuite.SetUpSuite(c)
	s.MgoSuite.SetUpSuite(c)
}

func (s *TxnSuite) TearDownSuite(c *gc.C) {
	// Make sure we've removed any Chaos
	txn.SetChaos(txn.Chaos{})
	s.MgoSuite.TearDownSuite(c)
	s.IsolationSuite.TearDownSuite(c)
}

func (s *TxnSuite) SetUpTest(c *gc.C) {
	s.IsolationSuite.SetUpTest(c)
	s.MgoSuite.SetUpTest(c)
	txn.SetChaos(txn.Chaos{})

	s.db = s.Session.DB("mgo-test")
	s.txns = s.db.C("txns")
	s.runner = txn.NewRunner(s.txns)
}

func (s *TxnSuite) TearDownTest(c *gc.C) {
	// Make sure we've removed any Chaos
	txn.SetChaos(txn.Chaos{})
	s.MgoSuite.TearDownTest(c)
	s.IsolationSuite.TearDownTest(c)
}

func (s *TxnSuite) runTxn(c *gc.C, ops ...txn.Op) bson.ObjectId {
	txnId := bson.NewObjectId()
	err := s.runner.Run(ops, txnId, nil)
	c.Assert(err, jc.ErrorIsNil)
	return txnId
}

var objectIdCounter uint32

// timestampBasedTxnId allows us to create an ObjectId that embeds an exact
// timestamp. This isn't guaranteed unique always like bson.NewObjectId, but is
// good enough for testing.
func timestampBasedTxnId(timestamp time.Time) bson.ObjectId {
	baseTxnId := bson.NewObjectIdWithTime(timestamp)
	// We increment a counter so all ObjectIds will be distinct.
	i := atomic.AddUint32(&objectIdCounter, 1)
	asBytes := []byte(baseTxnId)
	asBytes[9] = byte(i >> 16)
	asBytes[10] = byte(i >> 8)
	asBytes[11] = byte(i)
	return bson.ObjectId(asBytes)
}

// runTxnWithTimestamp is the same as runTxn but forces the ObjectId of this
// transaction to be at a particular timestamp.
func (s *TxnSuite) runTxnWithTimestamp(c *gc.C, expectedErr error, timestamp time.Time, ops ...txn.Op) bson.ObjectId {
	txnId := timestampBasedTxnId(timestamp)
	c.Logf("generated txn %v from timestamp %v", txnId, timestamp)
	err := s.runner.Run(ops, txnId, nil)
	c.Assert(err, gc.Equals, expectedErr)
	return txnId
}

func (s *TxnSuite) runFailingTxn(c *gc.C, expectedErr error, ops ...txn.Op) bson.ObjectId {
	txnId := bson.NewObjectId()
	err := s.runner.Run(ops, txnId, nil)
	c.Assert(err, gc.Equals, expectedErr)
	return txnId
}

// runInterruptedTxn starts a transaction, but interrupts it just before it gets applied.
func (s *TxnSuite) runInterruptedTxn(c *gc.C, ops ...txn.Op) bson.ObjectId {
	txn.SetChaos(txn.Chaos{
		KillChance: 1,
		Breakpoint: "set-applying",
	})
	txnId := s.runFailingTxn(c, txn.ErrChaos, ops...)
	txn.SetChaos(txn.Chaos{})
	return txnId
}

func (s *TxnSuite) assertTxns(c *gc.C, expectedIds ...bson.ObjectId) {
	var actualIds []bson.ObjectId
	var txnDoc struct {
		Id bson.ObjectId `bson:"_id"`
	}
	iter := s.txns.Find(nil).Select(bson.M{"_id": 1}).Iter()
	for iter.Next(&txnDoc) {
		actualIds = append(actualIds, txnDoc.Id)
	}
	c.Assert(actualIds, jc.SameContents, expectedIds)
}

func checkTxnIds(c *gc.C, expectedIds []bson.ObjectId, actualIds []bson.ObjectId) {
	expectedHex := make([]string, len(expectedIds))
	for i, id := range expectedIds {
		expectedHex[i] = id.Hex()
	}
	actualHex := make([]string, len(actualIds))
	for i, id := range actualIds {
		actualHex[i] = id.Hex()
	}
	c.Check(actualHex, jc.SameContents, expectedHex)
}

func (s *TxnSuite) assertDocQueue(c *gc.C, collection string, id interface{}, expectedIds ...bson.ObjectId) {
	coll := s.db.C(collection)
	var queueDoc struct {
		Queue []string `bson:"txn-queue"`
	}
	err := coll.FindId(id).One(&queueDoc)
	c.Assert(err, jc.ErrorIsNil)
	txnIdsHex := make([]string, len(queueDoc.Queue))
	for i, token := range queueDoc.Queue {
		// strip of the _nonce
		txnIdsHex[i] = token[:24]
	}
	expectedHex := make([]string, len(expectedIds))
	for i, id := range expectedIds {
		expectedHex[i] = id.Hex()
	}
	c.Check(txnIdsHex, gc.DeepEquals, expectedHex)
}

func (s *TxnSuite) assertStashDocQueue(c *gc.C, collection string, id interface{}, expectedIds ...bson.ObjectId) {
	// Assert a pending/removed document that is currently in the stash.
	// We identify it by the collection it would have been in.
	stashId := bson.D{{"c", collection}, {"id", id}}
	s.assertDocQueue(c, "txns.stash", stashId, expectedIds...)
}

func (s *TxnSuite) assertCollCount(c *gc.C, collName string, expectedCount int) {
	count := s.getCollCount(c, collName)
	c.Assert(count, gc.Equals, expectedCount)
}

func (s *TxnSuite) getCollCount(c *gc.C, collName string) int {
	n, err := s.db.C(collName).Count()
	c.Assert(err, jc.ErrorIsNil)
	return n
}
