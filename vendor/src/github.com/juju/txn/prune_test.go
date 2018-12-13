// Copyright 2015 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package txn_test

import (
	"time"

	"github.com/juju/clock/testclock"
	"github.com/juju/loggo"
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/mgo.v2/txn"

	jujutxn "github.com/juju/txn"
)

type PruneSuite struct {
	TxnSuite
}

var _ = gc.Suite(&PruneSuite{})

func (s *PruneSuite) maybePrune(c *gc.C, pruneFactor float32) {
	// A 'zero' timestamp means to ignore the time threshold
	s.maybePruneWithTimestamp(c, pruneFactor, time.Time{})
}

func (s *PruneSuite) maybePruneWithTimestamp(c *gc.C, pruneFactor float32, timestamp time.Time) {
	r := jujutxn.NewRunner(jujutxn.RunnerParams{
		Database:                  s.db,
		TransactionCollectionName: s.txns.Name,
		ChangeLogName:             s.txns.Name + ".log",
		Clock:                     testclock.NewClock(time.Now()),
	})
	err := r.MaybePruneTransactions(jujutxn.PruneOptions{
		PruneFactor:        pruneFactor,
		MinNewTransactions: 1,
		MaxNewTransactions: 1000,
		MaxTime:            timestamp,
	})
	c.Assert(err, jc.ErrorIsNil)
}

func (s *PruneSuite) TestSingleCollection(c *gc.C) {
	// Create some simple transactions, keeping track of the last
	// transaction id for each document.
	const numDocs = 5
	const updatesPerDoc = 3

	for id := 0; id < numDocs; id++ {
		s.runTxn(c, txn.Op{
			C:      "coll",
			Id:     id,
			Insert: bson.M{},
		})

		for txnNum := 0; txnNum < updatesPerDoc-1; txnNum++ {
			s.runTxn(c, txn.Op{
				C:      "coll",
				Id:     id,
				Update: bson.M{},
			})
		}
	}
	// Now create a transaction for each document that is interrupted
	lastTxnIds := make([]bson.ObjectId, numDocs)
	for id := 0; id < numDocs; id++ {
		lastTxnIds[id] = s.runInterruptedTxn(c, txn.Op{
			C:      "coll",
			Id:     id,
			Insert: bson.M{},
		})
	}

	// Ensure that expected number of transactions were created.
	s.assertCollCount(c, "txns", numDocs+(numDocs*updatesPerDoc))

	s.maybePrune(c, 1)

	// Confirm that only the records for the most recent transactions
	// for each document were kept.
	s.assertTxns(c, lastTxnIds...)

	for id := 0; id < numDocs; id++ {
		s.assertDocQueue(c, "coll", id, lastTxnIds[id])
	}

	// Run another transaction on each of the docs to ensure mgo/txn
	// is happy.
	for id := 0; id < numDocs; id++ {
		s.runTxn(c, txn.Op{
			C:      "coll",
			Id:     id,
			Update: bson.M{},
		})
	}
}

func (s *PruneSuite) TestMultipleDocumentsInOneTxn(c *gc.C) {
	// Create two documents each in their own txn.
	s.runTxn(c, txn.Op{
		C:      "coll",
		Id:     0,
		Insert: bson.M{},
	})
	s.runTxn(c, txn.Op{
		C:      "coll",
		Id:     1,
		Insert: bson.M{},
	})

	// Now update both documents in one transaction.
	txnId := s.runInterruptedTxn(c, txn.Op{
		C:      "coll",
		Id:     0,
		Update: bson.M{},
	}, txn.Op{
		C:      "coll",
		Id:     1,
		Update: bson.M{},
	})

	s.maybePrune(c, 1)

	// Only the last transaction should be left.
	s.assertTxns(c, txnId)
	s.assertDocQueue(c, "coll", 0, txnId)
	s.assertDocQueue(c, "coll", 1, txnId)
}

func (s *PruneSuite) TestMultipleCollections(c *gc.C) {
	var lastTxnIds []bson.ObjectId

	// Create a single document.
	s.runTxn(c, txn.Op{
		C:      "coll0",
		Id:     0,
		Insert: bson.M{},
	})

	// Update that document and create two more in other collections,
	// all in one txn. The reference will be removed, so this will be pruned.
	s.runTxn(c, txn.Op{
		C:      "coll0",
		Id:     0,
		Update: bson.M{},
	}, txn.Op{
		C:      "coll1",
		Id:     0,
		Insert: bson.M{},
	}, txn.Op{
		C:      "coll2",
		Id:     0,
		Insert: bson.M{},
	})

	// Update coll1 and coll2 docs together. This will be touching
	// coll1/0 and coll2/0 so it should not be pruned.
	txnId1 := s.runInterruptedTxn(c, txn.Op{
		C:      "coll1",
		Id:     0,
		Update: bson.M{},
	}, txn.Op{
		C:      "coll2",
		Id:     0,
		Update: bson.M{},
	})
	lastTxnIds = append(lastTxnIds, txnId1)

	// Insert more documents into coll0 and coll1.
	txnId2 := s.runInterruptedTxn(c, txn.Op{
		C:      "coll0",
		Id:     1,
		Insert: bson.M{},
	}, txn.Op{
		C:      "coll1",
		Id:     1,
		Insert: bson.M{},
	})

	// Modifies existing documents that have pending transactions
	txnId3 := s.runInterruptedTxn(c, txn.Op{
		C:      "coll1",
		Id:     0,
		Update: bson.M{},
	}, txn.Op{
		C:      "coll0",
		Id:     1,
		Update: bson.M{},
	})
	s.maybePrune(c, 1)
	s.assertTxns(c, txnId1, txnId2, txnId3)
	// check that the transactions have been removed from documents
	s.assertDocQueue(c, "coll1", 0, txnId1, txnId3)
	s.assertDocQueue(c, "coll2", 0, txnId1)
	// These documents are still in the stash because their insert got
	// interrupted
	s.assertStashDocQueue(c, "coll0", 1, txnId2, txnId3)
	s.assertStashDocQueue(c, "coll1", 1, txnId2)
}

func (s *PruneSuite) TestWithStashReference(c *gc.C) {
	// Ensure that txns referenced in the stash are not pruned from
	// the txns collection.

	// An easy way to get something into the stash is to interrupt creating a document
	txnId := s.runInterruptedTxn(c, txn.Op{
		C:      "coll",
		Id:     0,
		Insert: bson.M{},
	})
	s.assertCollCount(c, "txns.stash", 1)

	s.maybePrune(c, 1)
	s.assertTxns(c, txnId)
	// And the doc still refers to the transaction that is creating it
	s.assertStashDocQueue(c, "coll", 0, txnId)
}

func (s *PruneSuite) TestStashCleanedUp(c *gc.C) {
	// Test that items that were created and removed have been put into the
	// stash, but now we're able to clean them up if all of their
	// transactions have been marked completed.
	add0Id := s.runTxn(c, txn.Op{
		C:      "coll",
		Id:     0,
		Insert: bson.M{},
	})
	add1Id := s.runTxn(c, txn.Op{
		C:      "coll",
		Id:     1,
		Insert: bson.M{},
	})
	remove1Id := s.runTxn(c, txn.Op{
		C:      "coll",
		Id:     1,
		Remove: true,
	})
	s.assertTxns(c, add0Id, add1Id, remove1Id)
	s.assertCollCount(c, "txns.stash", 1)
	s.maybePrune(c, 1.0)
	s.assertCollCount(c, "txns.stash", 0)
	// The document 1 should be removed from the stash, no longer referencing either the
	// insert or the remove which allows us to remove the transactions
	s.assertTxns(c)
	s.assertDocQueue(c, "coll", 0)
	// we can't assert the queue on id=1 because the doc no longer exists.
}

func (s *PruneSuite) TestInProgressInsertNotPruned(c *gc.C) {
	// Create an incomplete insert transaction.
	txnId := s.runInterruptedTxn(c, txn.Op{
		C:      "coll",
		Id:     0,
		Insert: bson.M{},
	})

	// There will be in-progress txns and txns.stash entries for the
	// new document now. Remove the stash entry to simulate the point
	// in time where the txns doc has been inserted but the txns.stash
	// doc hasn't yet.
	err := s.db.C("txns.stash").RemoveId(bson.D{
		{"c", "coll"},
		{"id", 0},
	})
	c.Assert(err, jc.ErrorIsNil)

	s.maybePrune(c, 1)
	s.assertTxns(c, txnId)
}

func (s *PruneSuite) TestInProgressUpdateNotPruned(c *gc.C) {
	// Create an insert transaction and then in-progress update
	// transaction.
	s.runTxn(c, txn.Op{
		C:      "coll",
		Id:     0,
		Insert: bson.M{},
	})

	txn.SetChaos(txn.Chaos{
		KillChance: 1,
		Breakpoint: "set-prepared",
	})
	txnIdUpdate := s.runFailingTxn(c, txn.ErrChaos, txn.Op{
		C:      "coll",
		Id:     0,
		Update: bson.M{},
	})

	// Remove reference to the update transaction from the doc to
	// simulate the point in time where the txns doc has been created
	// but it's not referenced from the doc being updated yet.
	coll := s.db.C("coll")
	err := coll.UpdateId(0, bson.M{
		"$pull": bson.M{"txn-queue": bson.M{"$regex": "^" + txnIdUpdate.Hex() + "_*"}},
	})
	c.Assert(err, jc.ErrorIsNil)

	s.maybePrune(c, 1)
	s.assertTxns(c, txnIdUpdate)
}

func (s *PruneSuite) TestInProgressUpdateNotCleaned(c *gc.C) {
	// Create an insert transaction and then in-progress update
	// transaction.
	s.runTxn(c, txn.Op{
		C:      "coll",
		Id:     0,
		Insert: bson.M{},
	})

	txnIdUpdate := s.runInterruptedTxn(c, txn.Op{
		C:      "coll",
		Id:     0,
		Update: bson.M{},
	})

	s.maybePrune(c, 1)
	s.assertTxns(c, txnIdUpdate)
	s.assertDocQueue(c, "coll", 0, txnIdUpdate)
}

func (s *PruneSuite) TestAbortedTxnsArePruned(c *gc.C) {
	// Create an insert transaction, then an aborted transaction
	// and then a successful transaction, all for same doc.
	s.runTxn(c, txn.Op{
		C:      "coll",
		Id:     0,
		Insert: bson.M{},
	})
	s.runFailingTxn(c, txn.ErrAborted, txn.Op{
		C:      "coll",
		Id:     0,
		Assert: txn.DocMissing, // Aborts because doc is already there.
		Insert: bson.M{},
	})
	txnId := s.runInterruptedTxn(c, txn.Op{
		C:      "coll",
		Id:     0,
		Update: bson.M{},
	})

	s.maybePrune(c, 1)
	s.assertTxns(c, txnId)
	s.assertDocQueue(c, "coll", 0, txnId)
}

func (s *PruneSuite) TestManyTxnRemovals(c *gc.C) {
	// This is mainly to test the chunking of removals.
	s.runTxn(c, txn.Op{
		C:      "coll",
		Id:     0,
		Insert: bson.M{},
	})
	for i := 0; i < 3000; i++ {
		s.runTxn(c, txn.Op{
			C:      "coll",
			Id:     0,
			Update: bson.M{},
		})
	}
	s.assertCollCount(c, "txns", 3001)

	s.maybePrune(c, 1)
	s.assertTxns(c)
	s.assertDocQueue(c, "coll", 0)
}

func (s *PruneSuite) TestIgnoresNewTxns(c *gc.C) {
	baseTime, err := time.Parse("2006-01-02 15:04:05", "2017-01-01 12:00:00")
	c.Assert(err, jc.ErrorIsNil)
	s.runTxnWithTimestamp(c, nil, baseTime.Add(-30*time.Second), txn.Op{
		C:      "coll",
		Id:     0,
		Insert: bson.M{},
	})
	txnId2 := s.runTxnWithTimestamp(c, nil, baseTime, txn.Op{
		C:      "coll",
		Id:     0,
		Update: bson.M{},
	})
	txnId3 := s.runTxnWithTimestamp(c, nil, baseTime.Add(30*time.Second), txn.Op{
		C:      "col",
		Id:     0,
		Remove: true,
	})
	s.maybePruneWithTimestamp(c, 1, baseTime)
	s.assertTxns(c, txnId2, txnId3)
}

func (s *PruneSuite) TestFirstRun(c *gc.C) {
	// When there's no pruning stats recorded pruning should always
	// happen.
	s.makeTxnsForNewDoc(c, 10)
	s.assertCollCount(c, "txns", 10)

	s.maybePrune(c, 2.0)

	s.assertCollCount(c, "txns", 0)
	s.assertLastPruneStats(c, 10, 0)
	s.assertPruneStatCount(c, 1)
}

func (s *PruneSuite) TestPruningRequired(c *gc.C) {
	s.makeTxnsForNewDoc(c, 5)
	s.makeTxnsForNewDoc(c, 5)
	s.assertCollCount(c, "txns", 10)

	// Fake that the last txns size was 3 documents so that pruning
	// should be triggered (10 >= 3 * 2.0).
	s.setLastPruneCount(c, 3)

	s.maybePrune(c, 2.0)

	s.assertCollCount(c, "txns", 0)
	s.assertLastPruneStats(c, 10, 0)
	s.assertPruneStatCount(c, 2)
}

func (s *PruneSuite) TestPruningNotRequired(c *gc.C) {
	s.makeTxnsForNewDoc(c, 10)
	s.assertCollCount(c, "txns", 10)

	// Set the last txns count such that pruning won't be triggered
	// with a factor of 2.0  (6 * 2.0 > 10).
	s.setLastPruneCount(c, 6)

	s.maybePrune(c, 2.0)

	// Pruning shouldn't have happened.
	s.assertCollCount(c, "txns", 10)
	s.assertPruneStatCount(c, 1)
}

func (s *PruneSuite) TestPruningStatsHistory(c *gc.C) {
	s.maybePrune(c, 2.0)
	s.assertLastPruneStats(c, 0, 0)
	s.assertPruneStatCount(c, 1)

	s.makeTxnsForNewDoc(c, 5)

	s.maybePrune(c, 2.0)
	s.assertLastPruneStats(c, 5, 0)
	s.assertPruneStatCount(c, 2)

	s.makeTxnsForNewDoc(c, 11)

	s.maybePrune(c, 2.0)
	s.assertLastPruneStats(c, 11, 0)
	s.assertPruneStatCount(c, 3)

	s.makeTxnsForNewDoc(c, 5)

	s.maybePrune(c, 2.0)
	s.assertLastPruneStats(c, 5, 0)
	s.assertPruneStatCount(c, 4)
}

func (s *PruneSuite) TestPruningStatsBrokenLastPointer(c *gc.C) {
	// Create an initial pruning stats record.
	s.maybePrune(c, 2.0)
	s.assertLastPruneStats(c, 0, 0)
	s.assertPruneStatCount(c, 1)

	// Point the "last" pointer to a non-existent id.
	err := s.db.C("txns.prune").UpdateId("last", bson.M{
		"$set": bson.M{"id": bson.NewObjectId()},
	})
	c.Assert(err, jc.ErrorIsNil)

	var tw loggo.TestWriter
	c.Assert(loggo.RegisterWriter("test", loggo.NewMinimumLevelWriter(&tw, loggo.WARNING)), gc.IsNil)
	defer loggo.RemoveWriter("test")

	// Pruning should occur when "last" pointer is broken.
	s.maybePrune(c, 2.0)
	s.assertPruneStatCount(c, 2) // Note the new pruning stats record.
	c.Assert(tw.Log(), jc.LogMatches,
		[]jc.SimpleMessage{{loggo.WARNING, `pruning stats pointer was broken .+`}})
}

func (s *PruneSuite) makeTxnsForNewDoc(c *gc.C, count int) {
	id := bson.NewObjectId()
	s.runTxn(c, txn.Op{
		C:      "coll",
		Id:     id,
		Insert: bson.M{},
	})
	for i := 0; i < count-1; i++ {
		s.runTxn(c, txn.Op{
			C:      "coll",
			Id:     id,
			Update: bson.M{},
		})
	}
}

func (s *PruneSuite) setLastPruneCount(c *gc.C, count int) {
	id := bson.NewObjectId()
	err := s.db.C("txns.prune").Insert(bson.M{
		"_id":        id,
		"txns-after": count,
	})
	c.Assert(err, jc.ErrorIsNil)

	err = s.db.C("txns.prune").Insert(bson.M{
		"_id": "last",
		"id":  id,
	})
	c.Assert(err, jc.ErrorIsNil)
}

func (s *PruneSuite) assertLastPruneStats(c *gc.C, txnsBefore, txnsAfter int) {
	txnsPrune := s.db.C("txns.prune")
	var doc bson.M

	err := txnsPrune.FindId("last").One(&doc)
	c.Assert(err, jc.ErrorIsNil)

	err = txnsPrune.FindId(doc["id"]).One(&doc)
	c.Assert(err, jc.ErrorIsNil)

	c.Assert(doc["txns-before"].(int), gc.Equals, txnsBefore)
	c.Assert(doc["txns-after"].(int), gc.Equals, txnsAfter)

	started := doc["started"].(time.Time)
	completed := doc["completed"].(time.Time)
	c.Assert(completed.Sub(started) >= time.Duration(0), jc.IsTrue)
	assertTimeIsRecent(c, started)
	assertTimeIsRecent(c, completed)
}

func (s *PruneSuite) assertPruneStatCount(c *gc.C, expected int) {
	txnsPrune := s.db.C("txns.prune")
	actual, err := txnsPrune.Count()
	c.Assert(err, jc.ErrorIsNil)

	actual-- // Ignore "last" pointer document
	c.Assert(actual, gc.Equals, expected)
}

func assertTimeIsRecent(c *gc.C, t time.Time) {
	c.Assert(time.Now().Sub(t), jc.LessThan, time.Hour)
}
