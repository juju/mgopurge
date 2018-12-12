// Copyright 2018 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package txn

import (
	"fmt"
	"time"

	"github.com/juju/errors"
	"github.com/juju/testing"
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/mgo.v2/txn"
)

var _ = gc.Suite(&IncrementalPruneSuite{})

type IncrementalPruneSuite struct {
	TxnSuite
}

func (s *IncrementalPruneSuite) TestPruneAlsoCleans(c *gc.C) {
	s.runTxn(c, txn.Op{
		C:      "docs",
		Id:     "1",
		Insert: bson.M{"key": "value"},
	})
	var doc docWithQueue
	c.Assert(s.db.C("docs").FindId("1").One(&doc), jc.ErrorIsNil)
	c.Check(doc.Queue, gc.HasLen, 1)
	pruner := NewIncrementalPruner(IncrementalPruneArgs{})
	stats, err := pruner.Prune(s.txns)
	c.Assert(err, jc.ErrorIsNil)
	c.Check(stats.TxnsRemoved, gc.Equals, int64(1))
	c.Check(stats.DocReads, gc.Equals, int64(1))
	c.Check(stats.DocQueuesCleaned, gc.Equals, int64(1))
	c.Check(stats.DocTokensCleaned, gc.Equals, int64(1))
	// We should have cleaned the document, as well as deleting the transaction
	c.Assert(s.db.C("docs").FindId("1").One(&doc), jc.ErrorIsNil)
	c.Check(doc.Queue, gc.DeepEquals, []string{})
	count, err := s.txns.Count()
	c.Assert(err, jc.ErrorIsNil)
	c.Check(count, gc.Equals, 0)
}

func (s *IncrementalPruneSuite) TestPruneHandlesMissingDocs(c *gc.C) {
	s.runTxn(c, txn.Op{
		C:      "docs",
		Id:     "1",
		Insert: bson.M{"key": "value"},
	})
	// Now that we have the doc, we forcefully delete it
	res, err := s.db.C("docs").RemoveAll(nil)
	c.Assert(err, jc.ErrorIsNil)
	c.Check(res.Removed, gc.Equals, 1)
	pruner := NewIncrementalPruner(IncrementalPruneArgs{})
	stats, err := pruner.Prune(s.txns)
	c.Assert(err, jc.ErrorIsNil)
	c.Check(stats.TxnsRemoved, gc.Equals, int64(1))
	c.Check(stats.DocReads, gc.Equals, int64(0))
	c.Check(stats.DocQueuesCleaned, gc.Equals, int64(0))
	c.Check(stats.DocTokensCleaned, gc.Equals, int64(0))
	c.Check(stats.DocsMissing, gc.Equals, int64(1))
	// The txn gets cleaned up since the docs are missing, thus nothing refers to it.
	count, err := s.txns.Count()
	c.Assert(err, jc.ErrorIsNil)
	c.Check(count, gc.Equals, 0)
}

func (s *IncrementalPruneSuite) TestPruneIgnoresPendingTransactions(c *gc.C) {
	s.runTxn(c, txn.Op{
		C:      "docs",
		Id:     "1",
		Insert: bson.M{"key": "value"},
	})
	s.runInterruptedTxn(c, "set-applying", txn.Op{
		C:      "docs",
		Id:     "1",
		Update: bson.M{"$set": bson.M{"key": "newvalue"}},
	})
	count, err := s.txns.Count()
	c.Assert(err, jc.ErrorIsNil)
	c.Check(count, gc.Equals, 2)
	var doc docWithQueue
	c.Assert(s.db.C("docs").FindId("1").One(&doc), jc.ErrorIsNil)
	c.Check(doc.Queue, gc.HasLen, 2)
	pruner := NewIncrementalPruner(IncrementalPruneArgs{})
	stats, err := pruner.Prune(s.txns)
	c.Assert(err, jc.ErrorIsNil)
	c.Check(stats.TxnsRemoved, gc.Equals, int64(1))
	c.Check(stats.DocReads, gc.Equals, int64(1))
	c.Check(stats.DocQueuesCleaned, gc.Equals, int64(1))
	c.Check(stats.DocTokensCleaned, gc.Equals, int64(1))
	c.Check(stats.DocsMissing, gc.Equals, int64(0))
	count, err = s.txns.Count()
	c.Assert(err, jc.ErrorIsNil)
	c.Check(count, gc.Equals, 1)
	c.Assert(s.db.C("docs").FindId("1").One(&doc), jc.ErrorIsNil)
	c.Check(doc.Queue, gc.HasLen, 1)
}

func (s *IncrementalPruneSuite) TestPruneIgnoresRecentTxns(c *gc.C) {
	s.runTxn(c, txn.Op{
		C:      "docs",
		Id:     "1",
		Insert: bson.M{"key": "value"},
	})
	count, err := s.txns.Count()
	c.Assert(err, jc.ErrorIsNil)
	c.Check(count, gc.Equals, 1)
	var doc docWithQueue
	c.Assert(s.db.C("docs").FindId("1").One(&doc), jc.ErrorIsNil)
	c.Check(doc.Queue, gc.HasLen, 1)
	pruner := NewIncrementalPruner(IncrementalPruneArgs{
		MaxTime: time.Now().Add(-time.Hour),
	})
	// Nothing is touched because the txn is newer than 1 hour old
	stats, err := pruner.Prune(s.txns)
	c.Assert(err, jc.ErrorIsNil)
	c.Check(stats.TxnsRemoved, gc.Equals, int64(0))
	c.Check(stats.DocReads, gc.Equals, int64(0))
	c.Check(stats.DocQueuesCleaned, gc.Equals, int64(0))
	c.Check(stats.DocTokensCleaned, gc.Equals, int64(0))
	c.Check(stats.DocsMissing, gc.Equals, int64(0))
	count, err = s.txns.Count()
	c.Assert(err, jc.ErrorIsNil)
	c.Check(count, gc.Equals, 1)
	c.Assert(s.db.C("docs").FindId("1").One(&doc), jc.ErrorIsNil)
	c.Check(doc.Queue, gc.HasLen, 1)
}

func (s *IncrementalPruneSuite) TestPruneCleansUpStash(c *gc.C) {
	s.runTxn(c, txn.Op{
		C:      "docs",
		Id:     "1",
		Insert: bson.M{"key": "value"},
	})
	s.runTxn(c, txn.Op{
		C:      "docs",
		Id:     "1",
		Remove: true,
	})
	count, err := s.txns.Count()
	c.Assert(err, jc.ErrorIsNil)
	c.Check(count, gc.Equals, 2)
	var doc docWithQueue
	// The doc should be in the stash
	c.Assert(errors.Cause(s.db.C("docs").FindId("1").One(&doc)), gc.Equals, mgo.ErrNotFound)
	txnsStash := s.db.C("txns.stash")
	stashId := bson.D{{"c", "docs"}, {"id", "1"}}
	c.Assert(txnsStash.FindId(stashId).One(&doc), jc.ErrorIsNil)
	c.Check(doc.Queue, gc.HasLen, 1)
	pruner := NewIncrementalPruner(IncrementalPruneArgs{})
	stats, err := pruner.Prune(s.txns)
	c.Assert(err, jc.ErrorIsNil)
	c.Check(stats.TxnsRemoved, gc.Equals, int64(2))
	c.Check(stats.DocReads, gc.Equals, int64(0))
	c.Check(stats.DocQueuesCleaned, gc.Equals, int64(1))
	c.Check(stats.DocTokensCleaned, gc.Equals, int64(1))
	c.Check(stats.DocsMissing, gc.Equals, int64(0))
	c.Check(stats.StashDocReads, gc.Equals, int64(1))
	c.Check(stats.StashDocsRemoved, gc.Equals, int64(1))
	err = txnsStash.FindId(stashId).One(&doc)
	c.Assert(errors.Cause(err), gc.Equals, mgo.ErrNotFound)
	count, err = s.txns.Count()
	c.Assert(err, jc.ErrorIsNil)
	c.Check(count, gc.Equals, 0)
	count, err = txnsStash.Count()
	c.Assert(err, jc.ErrorIsNil)
	c.Check(count, gc.Equals, 0)
}

func (s *IncrementalPruneSuite) TestPruneDoesntRereadCachedDocs(c *gc.C) {
	// We create a lot of trnansactions updating the same doc
	s.runTxn(c, txn.Op{
		C:      "docs",
		Id:     "1",
		Insert: bson.M{"key": "value"},
	})
	for i := 0; i < 20; i++ {
		s.runTxn(c, txn.Op{
			C:      "docs",
			Id:     "1",
			Update: bson.M{"$set": bson.M{"key": fmt.Sprint(i)}},
		})
	}
	count, err := s.txns.Count()
	c.Assert(err, jc.ErrorIsNil)
	c.Check(count, gc.Equals, 21)
	var doc docWithQueue
	// The doc should be in the stash
	c.Assert(errors.Cause(s.db.C("docs").FindId("1").One(&doc)), jc.ErrorIsNil)
	c.Check(doc.Queue, gc.HasLen, 1)
	pruner := NewIncrementalPruner(IncrementalPruneArgs{})
	stats, err := pruner.Prune(s.txns)
	c.Assert(err, jc.ErrorIsNil)
	c.Check(stats.TxnsRemoved, gc.Equals, int64(21))
	c.Check(stats.DocReads, gc.Equals, int64(1))
	c.Check(stats.DocQueuesCleaned, gc.Equals, int64(1))
	c.Check(stats.DocTokensCleaned, gc.Equals, int64(1))
	c.Check(stats.DocsMissing, gc.Equals, int64(0))
	c.Check(stats.StashDocReads, gc.Equals, int64(0))
	c.Check(stats.StashDocsRemoved, gc.Equals, int64(0))
	c.Assert(errors.Cause(s.db.C("docs").FindId("1").One(&doc)), jc.ErrorIsNil)
	c.Check(doc.Queue, gc.HasLen, 0)
	count, err = s.txns.Count()
	c.Assert(err, jc.ErrorIsNil)
	c.Check(count, gc.Equals, 0)
}

func (s *IncrementalPruneSuite) TestPruneLeavesIncompleteStashAlone(c *gc.C) {
	s.runTxn(c, txn.Op{
		C:      "docs",
		Id:     "1",
		Insert: bson.M{"key": "value"},
	})
	s.runTxn(c, txn.Op{
		C:      "docs",
		Id:     "1",
		Remove: true,
	})
	s.runInterruptedTxn(c, "set-applying", txn.Op{
		C:      "docs",
		Id:     "1",
		Insert: bson.M{"key": "new-value"},
	})
	// Inserting the document again will try to restore it from the stash, but
	// leave a pending txn
	count, err := s.txns.Count()
	c.Assert(err, jc.ErrorIsNil)
	c.Check(count, gc.Equals, 3)
	var doc docWithQueue
	// The doc should be in the stash
	c.Assert(errors.Cause(s.db.C("docs").FindId("1").One(&doc)), gc.Equals, mgo.ErrNotFound)
	txnsStash := s.db.C("txns.stash")
	stashId := bson.D{{"c", "docs"}, {"id", "1"}}
	c.Assert(txnsStash.FindId(stashId).One(&doc), jc.ErrorIsNil)
	c.Check(doc.Queue, gc.HasLen, 2)
	pruner := NewIncrementalPruner(IncrementalPruneArgs{})
	stats, err := pruner.Prune(s.txns)
	c.Assert(err, jc.ErrorIsNil)
	// We can remove the finished txns, but not the incomplete one, and we must
	// leave the doc in txns.stash
	c.Check(stats.TxnsRemoved, gc.Equals, int64(2))
	c.Check(stats.DocReads, gc.Equals, int64(0))
	c.Check(stats.DocQueuesCleaned, gc.Equals, int64(1))
	c.Check(stats.DocTokensCleaned, gc.Equals, int64(1))
	c.Check(stats.DocsMissing, gc.Equals, int64(0))
	c.Check(stats.StashDocReads, gc.Equals, int64(1))
	c.Check(stats.StashDocsRemoved, gc.Equals, int64(0))
	c.Assert(txnsStash.FindId(stashId).One(&doc), jc.ErrorIsNil)
	c.Check(doc.Queue, gc.HasLen, 1)
	count, err = s.txns.Count()
	c.Assert(err, jc.ErrorIsNil)
	c.Check(count, gc.Equals, 1)
	count, err = txnsStash.Count()
	c.Assert(err, jc.ErrorIsNil)
	c.Check(count, gc.Equals, 1)
}

type TxnSuite struct {
	testing.IsolatedMgoSuite
	db     *mgo.Database
	txns   *mgo.Collection
	runner *txn.Runner
}

func (s *TxnSuite) SetUpTest(c *gc.C) {
	s.IsolatedMgoSuite.SetUpTest(c)
	txn.SetChaos(txn.Chaos{})

	s.db = s.Session.DB("mgo-test")
	s.txns = s.db.C("txns")
	s.runner = txn.NewRunner(s.txns)
}

func (s *TxnSuite) TearDownTest(c *gc.C) {
	// Make sure we've removed any Chaos
	txn.SetChaos(txn.Chaos{})
	s.IsolatedMgoSuite.TearDownTest(c)
}

func (s *TxnSuite) runTxn(c *gc.C, ops ...txn.Op) bson.ObjectId {
	txnId := bson.NewObjectId()
	err := s.runner.Run(ops, txnId, nil)
	c.Assert(err, jc.ErrorIsNil)
	return txnId
}

// runInterruptedTxn starts a txn but uses Chaos to force that txn to not complete
func (s *TxnSuite) runInterruptedTxn(c *gc.C, breakpoint string, ops ...txn.Op) bson.ObjectId {
	txnId := bson.NewObjectId()
	txn.SetChaos(txn.Chaos{
		KillChance: 1,
		Breakpoint: breakpoint,
	})
	err := s.runner.Run(ops, txnId, nil)
	c.Assert(err, gc.Equals, txn.ErrChaos)
	txn.SetChaos(txn.Chaos{})
	return txnId
}

type PrunerStatsSuite struct {
	testing.IsolationSuite
}

var _ = gc.Suite(&PrunerStatsSuite{})

func (*PrunerStatsSuite) TestBaseString(c *gc.C) {
	v1 := PrunerStats{}
	c.Check(v1.String(), gc.Equals, `
PrunerStats(
     CacheLookupTime: 0.000
         DocReadTime: 0.000
       DocLookupTime: 0.000
      DocCleanupTime: 0.000
     StashLookupTime: 0.000
     StashRemoveTime: 0.000
         TxnReadTime: 0.000
       TxnRemoveTime: 0.000
        DocCacheHits: 0
      DocCacheMisses: 0
  DocMissingCacheHit: 0
         DocsMissing: 0
   CollectionQueries: 0
            DocReads: 0
     DocStillMissing: 0
        StashQueries: 0
       StashDocReads: 0
    StashDocsRemoved: 0
    DocQueuesCleaned: 0
    DocTokensCleaned: 0
    DocsAlreadyClean: 0
         TxnsRemoved: 0
      TxnsNotRemoved: 0
        StrCacheHits: 0
      StrCacheMisses: 0
)`[1:])
}

func (*PrunerStatsSuite) TestAlignedTimes(c *gc.C) {
	v1 := PrunerStats{
		CacheLookupTime: time.Duration(12345 * time.Millisecond),
		DocReadTime:     time.Duration(23456789 * time.Microsecond),
		StashLookupTime: time.Duration(200 * time.Millisecond),
	}
	c.Check(v1.String(), gc.Equals, `
PrunerStats(
     CacheLookupTime: 12.345
         DocReadTime: 23.457
       DocLookupTime:  0.000
      DocCleanupTime:  0.000
     StashLookupTime:  0.200
     StashRemoveTime:  0.000
         TxnReadTime:  0.000
       TxnRemoveTime:  0.000
        DocCacheHits: 0
      DocCacheMisses: 0
  DocMissingCacheHit: 0
         DocsMissing: 0
   CollectionQueries: 0
            DocReads: 0
     DocStillMissing: 0
        StashQueries: 0
       StashDocReads: 0
    StashDocsRemoved: 0
    DocQueuesCleaned: 0
    DocTokensCleaned: 0
    DocsAlreadyClean: 0
         TxnsRemoved: 0
      TxnsNotRemoved: 0
        StrCacheHits: 0
      StrCacheMisses: 0
)`[1:])
}

func (*PrunerStatsSuite) TestAlignedValues(c *gc.C) {
	v1 := PrunerStats{
		StashDocsRemoved: 1000,
		StashDocReads:    12345,
	}
	c.Check(v1.String(), gc.Equals, `
PrunerStats(
     CacheLookupTime: 0.000
         DocReadTime: 0.000
       DocLookupTime: 0.000
      DocCleanupTime: 0.000
     StashLookupTime: 0.000
     StashRemoveTime: 0.000
         TxnReadTime: 0.000
       TxnRemoveTime: 0.000
        DocCacheHits:     0
      DocCacheMisses:     0
  DocMissingCacheHit:     0
         DocsMissing:     0
   CollectionQueries:     0
            DocReads:     0
     DocStillMissing:     0
        StashQueries:     0
       StashDocReads: 12345
    StashDocsRemoved:  1000
    DocQueuesCleaned:     0
    DocTokensCleaned:     0
    DocsAlreadyClean:     0
         TxnsRemoved:     0
      TxnsNotRemoved:     0
        StrCacheHits:     0
      StrCacheMisses:     0
)`[1:])
}
