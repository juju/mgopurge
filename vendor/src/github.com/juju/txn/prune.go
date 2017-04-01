// Copyright 2015 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package txn

import (
	"fmt"
	"strings"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	// Transaction states copied from mgo/txn.
	taborted = 5 // Pre-conditions failed, nothing done
	tapplied = 6 // All changes applied

	// maxBatchDocs defines the maximum MongoDB batch size.
	maxBatchDocs = 1616

	// maxBulkOps defines the maximum number of operations in a bulk
	// operation.
	maxBulkOps = 1000

	// logInterval defines often to report progress during long
	// operations.
	logInterval = 15 * time.Second
)

type pruneStats struct {
	Id         bson.ObjectId `bson:"_id"`
	Started    time.Time     `bson:"started"`
	Completed  time.Time     `bson:"completed"`
	TxnsBefore int           `bson:"txns-before"`
	TxnsAfter  int           `bson:"txns-after"`
}

func maybePrune(db *mgo.Database, txnsName string, pruneFactor float32) error {
	txnsPrune := db.C(txnsPruneC(txnsName))
	txns := db.C(txnsName)

	txnsCount, err := txns.Count()
	if err != nil {
		return fmt.Errorf("failed to retrieve starting txns count: %v", err)
	}
	lastTxnsCount, err := getPruneLastTxnsCount(txnsPrune)
	if err != nil {
		return fmt.Errorf("failed to retrieve pruning stats: %v", err)
	}

	required := lastTxnsCount == 0 || float32(txnsCount) >= float32(lastTxnsCount)*pruneFactor
	logger.Infof("txns after last prune: %d, txns now: %d, pruning required: %v", lastTxnsCount, txnsCount, required)

	if required {
		started := time.Now()
		err := PruneTxns(txnsPrune.Database, txns)
		if err != nil {
			return err
		}
		completed := time.Now()

		txnsCountAfter, err := txns.Count()
		if err != nil {
			return fmt.Errorf("failed to retrieve final txns count: %v", err)
		}
		logger.Infof("txn pruning complete. txns now: %d", txnsCountAfter)
		return writePruneTxnsCount(txnsPrune, started, completed, txnsCount, txnsCountAfter)
	}

	return nil
}

func getPruneLastTxnsCount(txnsPrune *mgo.Collection) (int, error) {
	// Retrieve the doc which points to the latest stats entry.
	var ptrDoc bson.M
	err := txnsPrune.FindId("last").One(&ptrDoc)
	if err == mgo.ErrNotFound {
		return 0, nil
	} else if err != nil {
		return -1, fmt.Errorf("failed to load pruning stats pointer: %v", err)
	}

	// Get the stats.
	var doc pruneStats
	err = txnsPrune.FindId(ptrDoc["id"]).One(&doc)
	if err == mgo.ErrNotFound {
		// Pointer was broken. Recover by returning 0 which will force
		// pruning.
		logger.Warningf("pruning stats pointer was broken - will recover")
		return 0, nil
	} else if err != nil {
		return -1, fmt.Errorf("failed to load pruning stats: %v", err)
	}
	return doc.TxnsAfter, nil
}

func writePruneTxnsCount(
	txnsPrune *mgo.Collection,
	started, completed time.Time,
	txnsBefore, txnsAfter int,
) error {
	id := bson.NewObjectId()
	err := txnsPrune.Insert(pruneStats{
		Id:         id,
		Started:    started,
		Completed:  completed,
		TxnsBefore: txnsBefore,
		TxnsAfter:  txnsAfter,
	})
	if err != nil {
		return fmt.Errorf("failed to write prune stats: %v", err)
	}

	// Set pointer to latest stats document.
	_, err = txnsPrune.UpsertId("last", bson.M{"$set": bson.M{"id": id}})
	if err != nil {
		return fmt.Errorf("failed to write prune stats pointer: %v", err)
	}
	return nil
}

func txnsPruneC(txnsName string) string {
	return txnsName + ".prune"
}

// PruneTxns removes applied and aborted entries from the txns
// collection that are no longer referenced by any document.
//
// Warning: this is a fairly heavyweight activity and therefore should
// be done infrequently.
//
// PruneTxns is the low-level pruning function that does the actual
// pruning work. It only exposed for external utilities to
// call. Typical usage should be via Runner.MaybePruneTransactions
// which wraps PruneTxns, only calling it when really necessary.
//
// TODO(mjs) - this knows way too much about mgo/txn's internals and
// with a bit of luck something like this will one day be part of
// mgo/txn.
func PruneTxns(db *mgo.Database, txns *mgo.Collection) error {
	workingSetName := txns.Name + ".prunetemp"
	workingSet := db.C(workingSetName)
	defer workingSet.DropCollection()

	// Load the ids of all completed and aborted txns into a separate
	// temporary collection.
	logger.Debugf("loading all completed transactions")
	pipe := txns.Pipe([]bson.M{
		// This used to use $in but that's much slower than $gte.
		{"$match": bson.M{"s": bson.M{"$gte": taborted}}},
		{"$project": bson.M{"_id": 1}},
		{"$out": workingSetName},
	})
	pipe.Batch(maxBatchDocs)
	pipe.AllowDiskUse()
	if err := pipe.All(&bson.D{}); err != nil {
		return fmt.Errorf("reading completed txns: %v", err)
	}

	count, err := workingSet.Count()
	if err != nil {
		return fmt.Errorf("getting txn count: %v", err)
	}
	logger.Debugf("%d completed txns found", count)

	collNames, err := db.CollectionNames()
	if err != nil {
		return fmt.Errorf("reading collection names: %v", err)
	}
	collNames = txnCollections(collNames, txns.Name)
	logger.Debugf("%d collections with txns to examine", len(collNames))

	// Now remove the txn ids referenced by any document in any
	// txn-using collection from the set of known txn ids.
	//
	// Working the other way - starting with the set of txns
	// referenced by documents and then removing any not in that set
	// from the txns collection - is unsafe as it will result in the
	// removal of transactions created during the pruning process.
	t := newSimpleTimer(logInterval)
	remover := newBulkRemover(workingSet)
	for _, collName := range collNames {
		logger.Tracef("checking %s for txn references", collName)
		coll := db.C(collName)
		var tDoc struct {
			Queue []string `bson:"txn-queue"`
		}
		query := coll.Find(nil).Select(bson.M{"txn-queue": 1})
		query.Batch(maxBatchDocs)
		iter := query.Iter()
		for iter.Next(&tDoc) {
			for _, token := range tDoc.Queue {
				if err := remover.remove(txnTokenToId(token)); err != nil {
					return fmt.Errorf("handling completed txns: %v", err)
				}
				if t.isAfter() {
					logger.Debugf("%d referenced txns found so far", remover.removed)
				}
			}
		}
		if err := iter.Close(); err != nil {
			return fmt.Errorf("failed to read docs: %v", err)
		}
	}
	if err := remover.flush(); err != nil {
		return fmt.Errorf("handling completed txns: %v", err)
	}
	logger.Debugf("%d txns are still referenced and will be kept", remover.removed)

	// Remove the no-longer-referenced transactions from the txns collection.
	t = newSimpleTimer(logInterval)
	remover = newBulkRemover(txns)
	query := workingSet.Find(nil).Batch(maxBatchDocs)
	iter := query.Iter()
	var doc struct {
		ID bson.ObjectId `bson:"_id"`
	}
	for iter.Next(&doc) {
		if err := remover.remove(doc.ID); err != nil {
			return fmt.Errorf("removing txns: %v", err)
		}
		if t.isAfter() {
			logger.Debugf("%d completed txns pruned so far", remover.removed)
		}
	}
	if err := remover.flush(); err != nil {
		return fmt.Errorf("removing txns: %v", err)
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("iterating through unreferenced txns: %v", err)
	}

	logger.Debugf("pruning completed: removed %d txns", remover.removed)
	return nil
}

// txnCollections takes the list of all collections in a database and
// filters them to just the ones that may have txn references.
func txnCollections(inNames []string, txnsName string) []string {
	// hasTxnReferences returns true if a collection may have
	// references to txns.
	hasTxnReferences := func(name string) bool {
		switch {
		case name == txnsName+".stash":
			return true // Need to look in the stash.
		case name == txnsName, strings.HasPrefix(name, txnsName+"."):
			// The txns collection and its childen shouldn't be considered.
			return false
		case strings.HasPrefix(name, "system."):
			// Don't look in system collections.
			return false
		default:
			// Everything else needs to be considered.
			return true
		}
	}

	outNames := make([]string, 0, len(inNames))
	for _, name := range inNames {
		if hasTxnReferences(name) {
			outNames = append(outNames, name)
		}
	}
	return outNames
}

func txnTokenToId(token string) bson.ObjectId {
	// mgo/txn transaction tokens are the 24 character txn id
	// followed by "_<nonce>"
	return bson.ObjectIdHex(token[:24])
}

func newBulkRemover(coll *mgo.Collection) *bulkRemover {
	r := &bulkRemover{coll: coll}
	r.newChunk()
	return r
}

type bulkRemover struct {
	coll      *mgo.Collection
	chunk     *mgo.Bulk
	chunkSize int
	removed   int
}

func (r *bulkRemover) newChunk() {
	r.chunk = r.coll.Bulk()
	r.chunk.Unordered()
	r.chunkSize = 0
}

func (r *bulkRemover) remove(id interface{}) error {
	r.chunk.Remove(bson.D{{"_id", id}})
	r.chunkSize++
	if r.chunkSize >= maxBulkOps {
		return r.flush()
	}
	return nil
}

func (r *bulkRemover) flush() error {
	if r.chunkSize < 1 {
		return nil // Nothing to do
	}
	switch result, err := r.chunk.Run(); err {
	case nil, mgo.ErrNotFound:
		// It's OK for txns to no longer exist. Another process
		// may have concurrently pruned them.
		r.removed += result.Matched
		r.newChunk()
		return nil
	default:
		return err
	}
}

func newSimpleTimer(interval time.Duration) *simpleTimer {
	return &simpleTimer{
		interval: interval,
		next:     time.Now().Add(interval),
	}
}

type simpleTimer struct {
	interval time.Duration
	next     time.Time
}

func (t *simpleTimer) isAfter() bool {
	now := time.Now()
	if now.After(t.next) {
		t.next = now.Add(t.interval)
		return true
	}
	return false
}
