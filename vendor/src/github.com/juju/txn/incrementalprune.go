// Copyright 2018 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package txn

import (
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/juju/lru"
	"github.com/kr/pretty"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const pruneTxnBatchSize = 1000
const queryDocBatchSize = 100
const pruneDocCacheSize = 10000

// IncrementalPruner reads the transzaction table incrementally, seeing if it can remove the current set of transactions,
// and then moves on to newer transactions. It only thinks about 1k txns at a time, because that is the batch size that
// can be deleted. Instead, it caches documents that it has seen.
type IncrementalPruner struct {
	docCache     *lru.LRU
	missingCache *lru.LRU
	stats        PrunerStats
}

// PrunerStats collects statistics about how the prune progressed
type PrunerStats struct {
	DocCacheHits       int
	DocCacheMisses     int
	DocMissingCacheHit int
	DocsMissing        int
	CollectionQueries  int
	DocReads           int
	DocStillMissing    int
	StashQueries       int
	StashReads         int
	DocQueuesCleaned   int
	DocTokensCleaned   int
	DocsAlreadyClean   int
	TxnsRemoved        int
	TxnsNotRemoved     int
}

// lookupDocs searches the cache and then looks in the database for the txn-queue of all the referenced document keys.
func (p *IncrementalPruner) lookupDocs(keys docKeySet, txnsStash *mgo.Collection) (docMap, error) {
	db := txnsStash.Database
	docs := make(docMap, len(docKeySet{}))
	docsByCollection := make(map[string][]interface{}, 0)
	for key, _ := range keys {
		cacheDoc, exists := p.docCache.Get(key)
		if exists {
			// Found in cache.
			// Note that it is possible we'll actually be looking at a document that has since been updated.
			// However, it is ok for new transactions to be added to the queue, and for completed transactions
			// to be removed.
			// The key for us is that we're only processing very old completed transactions, so the old information
			// we are looking at won't be changing. At worst we'll try to cleanup a document that has already been
			// cleaned up. But since we only process completed transaction we can't miss a document that has the txn
			// added to it.
			docs[key] = cacheDoc.(docWithQueue)
			p.stats.DocCacheHits++
		} else {
			p.stats.DocCacheMisses++
			docsByCollection[key.Collection] = append(docsByCollection[key.Collection], key.DocId)
		}
	}
	missingKeys := make(map[stashDocKey]struct{}, 0)
	for collection, ids := range docsByCollection {
		missing := make(map[interface{}]struct{}, len(ids))
		for _, id := range ids {
			missing[id] = struct{}{}
		}
		coll := db.C(collection)
		query := coll.Find(bson.M{"_id": bson.M{"$in": ids}})
		query.Select(bson.M{"_id": 1, "txn-queue": 1})
		query.Batch(queryDocBatchSize)
		iter := query.Iter()
		p.stats.CollectionQueries++
		var doc docWithQueue
		for iter.Next(&doc) {
			key := docKey{Collection: collection, DocId: doc.Id}
			p.docCache.Add(key, doc)
			docs[key] = doc
			p.stats.DocReads++
			delete(missing, doc.Id)
		}
		p.stats.DocStillMissing += len(missing)
		for id, _ := range missing {
			stashKey := stashDocKey{Collection: collection, Id: id}
			missingKeys[stashKey] = struct{}{}
		}

		if err := iter.Close(); err != nil {
			return nil, errors.Trace(err)
		}
	}
	// Note: there is some danger that new transactions will be adding and removing a document that we
	// reference in an old transaction. If that is happening fast enough, it is possible that we won't be able to see
	// the document in either place, and thus won't be able to verify that the old transaction is not actually
	// referenced. However, the act of adding or remove a document should be cleaning up the txn queue anyway,
	// which means it is safe to delete the document
	if len(missingKeys) > 0 {
		// TODO(jam): 2018-11-29, we know that the metrics collection is frequently cleaned by bulk removal, rather
		// than  by removing with a txn.Op. Which means all those docs get looked up in stash even though they aren't
		// there. Is there something better we could do?
		// For all the other documents, now we need to check txns.stash
		foundMissingKeys := make(map[stashDocKey]struct{}, len(missingKeys))
		p.stats.StashQueries++
		missingSlice := make([]stashDocKey, 0, len(missingKeys))
		for key := range missingKeys {
			missingSlice = append(missingSlice, key)
		}
		query := txnsStash.Find(bson.M{"_id": bson.M{"$in": missingSlice}})
		query.Select(bson.M{"_id": 1, "txn-queue": 1})
		query.Batch(queryDocBatchSize)
		iter := query.Iter()
		var doc stashEntry
		for iter.Next(&doc) {
			key := docKey{Collection: doc.Id.Collection, DocId: doc.Id.Id}
			qDoc := docWithQueue{Id: doc.Id.Id, Queue: doc.Queue, foundInStash: true}
			p.docCache.Add(key, qDoc)
			docs[key] = qDoc
			p.stats.StashReads++
			foundMissingKeys[doc.Id] = struct{}{}
		}
		if err := iter.Close(); err != nil {
			return nil, errors.Trace(err)
		}
		for stashKey := range missingKeys {
			if _, exists := foundMissingKeys[stashKey]; exists {
				continue
			}
			// Note: we don't track docKeys that are still missing, they are found by the caller when they aren't in docMap
		}
	}

	return docs, nil
}

func (p *IncrementalPruner) pruneNextBatch(iter *mgo.Iter, txnsStash *mgo.Collection, removeCh chan []bson.ObjectId) (bool, error) {
	done := false
	// First, read all the txns to find the document identities we might care about
	txns := make([]txnDoc, 0, pruneTxnBatchSize)
	// We expect a doc in each txn
	docsToCheck := make(docKeySet, pruneTxnBatchSize)
	txnsBeingCleaned := make(map[bson.ObjectId]struct{})
	for count := 0; count < pruneTxnBatchSize; count++ {
		var txn txnDoc
		if iter.Next(&txn) {
			// TODO: Make sure we don't need a copy here, especially because of Ops being a slice
			txns = append(txns, txn)
			for _, key := range txn.Ops {
				if _, ok := p.missingCache.Get(key); ok {
					// known to be missing, don't bother
					p.stats.DocMissingCacheHit++
					continue
				}
				docsToCheck[key] = struct{}{}
			}
			txnsBeingCleaned[txn.Id] = struct{}{}
		} else {
			done = true
		}
	}
	db := txnsStash.Database
	// Now that we have a bunch of documents we want to look at, load them from the collections
	docMap, err := p.lookupDocs(docsToCheck, txnsStash)
	if err != nil {
		return done, errors.Trace(err)
	}
	txnsToDelete := make([]bson.ObjectId, 0, pruneTxnBatchSize)
	// TODO: Maybe we should use a Batch pull here, rather than a single query per doc?
	// Potentially we should be sorting by collection
	for _, txn := range txns {
		txnCanBeRemoved := true
		for _, docKey := range txn.Ops {
			doc, ok := docMap[docKey]
			if !ok {
				p.stats.DocsMissing++
				if docKey.Collection == "metrics" {
					// XXX: This is a special case. Metrics are *known* to violate the transaction guarantees
					// by removing documents directly from the collection, without using a transaction. Even
					// though they are *created* with transactions... bad metrics, bad dog
					logger.Tracef("ignoring missing metrics doc: %v", docKey)
					p.missingCache.Add(docKey, nil)
				} else if docKey.Collection == "cloudimagemetadata" {
					// There is an upgrade step in 2.3.4 that bulk deletes all cloudimagemetadata that have particular
					// attributes, ignoring transactions...
					logger.Tracef("ignoring missing cloudimagemetadat doc: %v", docKey)
					p.missingCache.Add(docKey, nil)
				} else {
					logger.Warningf("transaction %q referenced document %v but it could not be found",
						txn.Id.Hex(), docKey)
					// This is usually a sign of corruption, but for the purposes of pruning, we'll just treat it as a
					// transaction that cannot be cleaned up.
					txnCanBeRemoved = false
				}
			}
			tokensToPull := make([]string, 0)
			newQueue := make([]string, 0)
			for _, token := range doc.Queue {
				txnId := txnTokenToId(token)
				if _, isCleaned := txnsBeingCleaned[txnId]; isCleaned {
					tokensToPull = append(tokensToPull, token)
				} else {
					newQueue = append(newQueue, token)
				}
			}
			if len(tokensToPull) > 0 {
				p.stats.DocTokensCleaned += len(tokensToPull)
				p.stats.DocQueuesCleaned++
				coll := db.C(docKey.Collection)
				pull := bson.M{"$pullAll": bson.M{"txn-queue": tokensToPull}}
				err := coll.UpdateId(docKey.DocId, pull)
				if err != nil {
					if err != mgo.ErrNotFound {
						return done, errors.Trace(err)
					}
					// Look in txns.stash
					err := txnsStash.UpdateId(stashDocKey{
						Collection: docKey.Collection,
						Id:         docKey.DocId,
					}, pull)
					if err != nil {
						if err == mgo.ErrNotFound {
							logger.Warningf("trying to cleanup doc %v, could not be found in collection nor stash",
								docKey)
							txnCanBeRemoved = false
							// We don't treat this as a fatal error, just a txn that cannot be cleaned up.
						}
						return done, errors.Trace(err)
					}
				}
				// Update the known Queue of the document, since we cleaned it.
				doc.Queue = newQueue
				p.docCache.Add(docKey, doc)
			} else {
				// already clean of transactions we are currently processing
				p.stats.DocsAlreadyClean++
			}
		}
		if txnCanBeRemoved {
			txnsToDelete = append(txnsToDelete, txn.Id)
		} else {
			p.stats.TxnsNotRemoved++
		}
	}
	// TODO(jam): 2018-11-29 Bare channel send, support a shutdown of some sort
	removeCh <- txnsToDelete
	return done, nil
}

type removeResult struct {
	count int
	err   error
}

func (p *IncrementalPruner) txnRemoveThread(txns *mgo.Collection, ch chan []bson.ObjectId, resultCh chan removeResult) {
	s := txns.Database.Session.Clone()
	defer s.Close()
	txns = txns.With(s)
	for batch := range ch {
		if len(batch) == 0 {
			resultCh <- removeResult{count: 0}
			continue
		}
		// Is Bulk better than doing it directly?
		results, err := txns.RemoveAll(bson.M{"_id": bson.M{"$in": batch}})
		if err == nil {
			resultCh <- removeResult{count: results.Removed}
		} else {
			err = errors.Trace(err)
			// TODO(jam): 2018-11-29 Bare channel send, support a shutdown of some sort
			resultCh <- removeResult{
				count: len(batch),
				err:   err,
			}
		}
	}
}

func (p *IncrementalPruner) Prune(args CleanAndPruneArgs) (PrunerStats, error) {
	tStart := time.Now()
	txns := args.Txns
	db := txns.Database
	txnsStashName := args.Txns.Name + ".stash"
	txnsStash := db.C(txnsStashName)
	query := txns.Find(completedOldTransactionMatch(args.MaxTime))
	query.Select(bson.M{
		"_id": 1,
		"o.c": 1,
		"o.d": 1,
	})
	query.Sort("_id")
	timer := newSimpleTimer(15 * time.Second)
	query.Batch(pruneTxnBatchSize)
	removeChan := make(chan []bson.ObjectId, 0)
	resultChan := make(chan removeResult, 0)
	errs := []error{}
	var wg sync.WaitGroup
	go func() {
		// collect the results
		for {
			res, ok := <-resultChan
			if !ok {
				break
			}
			p.stats.TxnsRemoved += res.count
			if res.err != nil {
				errs = append(errs, res.err)
			}
			wg.Done()
		}
	}()
	go p.txnRemoveThread(txns, removeChan, resultChan)
	iter := query.Iter()
	for {
		// There is one Wait for each pruneNextBatch iteration
		wg.Add(1)
		done, err := p.pruneNextBatch(iter, txnsStash, removeChan)
		if err != nil {
			iterErr := iter.Close()
			if iterErr != nil {
				logger.Warningf("ignoring iteration close error: %v", iterErr)
			}
			wg.Done()
			return p.stats, errors.Trace(err)
		}
		if done {
			break
		}
		if timer.isAfter() {
			logger.Debugf("pruning has removed %d txns, handling %d docs (%d in cache)",
				p.stats.TxnsRemoved, p.stats.DocCacheHits+p.stats.DocCacheMisses, p.stats.DocCacheHits)
		}
	}
	if err := iter.Close(); err != nil {
		return p.stats, errors.Trace(err)
	}
	wg.Wait()
	close(removeChan)
	close(resultChan)
	if len(errs) != 0 {
		return p.stats, errors.Trace(errs[0])
	}
	// TODO: Now we should iterate over txns.Stash and remove documents that aren't referenced by any transactions.
	// Maybe we can just remove anything that
	logger.Infof("pruning removed %d txns and cleaned %d docs in %s.",
		p.stats.TxnsRemoved, p.stats.DocQueuesCleaned, time.Since(tStart).Round(time.Millisecond))
	logger.Debugf("prune stats: %s", pretty.Sprint(p.stats))
	return p.stats, nil
}

// docWithQueue is used to serialize a Mongo document that has a txn-queue
type docWithQueue struct {
	Id           interface{} `bson:"_id"`
	Queue        []string    `bson:"txn-queue"`
	foundInStash bool        `bson:"-"`
}

// these are only the fields of txnDoc that we care about
type txnDoc struct {
	Id  bson.ObjectId `bson:"_id"`
	Ops []docKey      `bson:"o"`
}

type docKey struct {
	Collection string      `bson:"c"`
	DocId      interface{} `bson:"d"`
}

type stashDocKey struct {
	Collection string      `bson:"c"`
	Id         interface{} `bson:"id"`
}

type stashEntry struct {
	Id    stashDocKey `bson:"_id"`
	Queue []string    `bson:"txn-queue"`
}

type docKeySet map[docKey]struct{}

type docMap map[docKey]docWithQueue
