// Copyright 2018 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package txn

import (
	"sync"
	"sync/atomic"
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
const missingKeyCacheSize = 2000
const strCacheSize = 10000

// IncrementalPruner reads the transzaction table incrementally, seeing if it can remove the current set of transactions,
// and then moves on to newer transactions. It only thinks about 1k txns at a time, because that is the batch size that
// can be deleted. Instead, it caches documents that it has seen.
type IncrementalPruner struct {
	docCache     DocCache
	missingCache MissingKeyCache
	strCache     *lru.StringCache
	stats        PrunerStats
}

// IncrementalPruneArgs specifies the parameters for running incremental cleanup steps..
type IncrementalPruneArgs struct {
	// Txns is the collection that holds all of the transactions that we
	// might want to prune. We will also make use of Txns.Database to find
	// all of the collections that might make use of transactions from that
	// collection.
	Txns *mgo.Collection

	// MaxTime is a timestamp that provides a threshold of transactions
	// that we will actually prune. Only transactions that were created
	// before this threshold will be pruned.
	MaxTime time.Time

	// MultipleCleanups can be set to true to trigger multiple goroutines cleaning up different transactions.
	MultipleCleanups bool
}

// PrunerStats collects statistics about how the prune progressed
type PrunerStats struct {
	CacheLookupTime    time.Duration
	DocLookupTime      time.Duration
	DocCleanupTime     time.Duration
	DocReadTime        time.Duration
	StashLookupTime    time.Duration
	TxnReadTime        time.Duration
	TxnRemoveTime      time.Duration
	DocCacheHits       int64
	DocCacheMisses     int64
	DocMissingCacheHit int64
	DocsMissing        int64
	CollectionQueries  int64
	DocReads           int64
	DocStillMissing    int64
	StashQueries       int64
	StashDocReads      int64
	DocQueuesCleaned   int64
	DocTokensCleaned   int64
	DocsAlreadyClean   int64
	TxnsRemoved        int64
	TxnsNotRemoved     int64
}

func (p *IncrementalPruner) startReportingThread(stop <-chan struct{}) {
	tStart := time.Now()
	next := time.After(15 * time.Second)
	go func() {
		select {
		case <-stop:
			return
		case <-next:
			hits := atomic.LoadInt64(&p.stats.DocCacheHits)
			totalDocs := hits + atomic.LoadInt64(&p.stats.DocCacheMisses)
			cachePercent := 0.0
			if totalDocs > 0 {
				cachePercent = 100.0 * float64(hits) / float64(totalDocs)
			}
			txnRate := 0.0
			since := time.Since(tStart).Seconds()
			removed := atomic.LoadInt64(&p.stats.TxnsRemoved)
			if since > 0 {
				txnRate = float64(removed) / since
			}
			logger.Debugf("pruning has removed %d txns looking at %d docs (%.1f%% in cache)%.0ftxn/s",
				removed, totalDocs, cachePercent, txnRate)
			next = time.After(15 * time.Second)
		}
	}()
}

func (p *IncrementalPruner) pruneThread(
	stop <-chan struct{},
	errorCh chan error,
	wg *sync.WaitGroup,
	txns *mgo.Collection,
	maxTime time.Time,
	reverse bool,
) {
	db := txns.Database
	session := db.Session.Copy()
	defer session.Close()
	txnsStashName := txns.Name + ".stash"
	txnsStash := db.C(txnsStashName)
	query := txns.Find(completedOldTransactionMatch(maxTime))
	query.Select(bson.M{
		"_id": 1,
		"o.c": 1,
		"o.d": 1,
	})
	// Sorting by _id helps make sure that we are grouping the transactions close to each other for removals
	if reverse {
		query.Sort("-_id")
	} else {
		query.Sort("_id")
	}
	query.Batch(pruneTxnBatchSize)
	iter := query.Iter()
	done := false
	for !done {
		var err error
		done, err = p.pruneNextBatch(iter, txns, txnsStash, errorCh, wg)
		if err != nil {
			done = true
			iterErr := iter.Close()
			if iterErr != nil {
				logger.Warningf("ignoring iteration close error: %v", iterErr)
			}
			errorCh <- errors.Trace(err)
			break
		}
		select {
		case <-stop:
			done = true
		default:
			// do nothing
		}
	}
	if err := iter.Close(); err != nil {
		errorCh <- errors.Trace(err)
	}
	wg.Done()
}

func (p *IncrementalPruner) Prune(args IncrementalPruneArgs) (PrunerStats, error) {
	tStart := time.Now()
	stop := make(chan struct{}, 0)
	p.startReportingThread(stop)
	errorCh := make(chan error, 100)
	var wg sync.WaitGroup

	wg.Add(1)
	go p.pruneThread(stop, errorCh, &wg, args.Txns, args.MaxTime, false)

	wg.Wait()
	close(stop)
	select {
	case err := <-errorCh:
		if err != nil {
			return p.stats, errors.Trace(err)
		}
	default:
	}
	// TODO: Now we should iterate over txns.Stash and remove documents that aren't referenced by any transactions.
	// Essentially, any document in txns.Stash that has 0 entries in its txn-queue.
	logger.Infof("pruning removed %d txns and cleaned %d docs in %s.",
		atomic.LoadInt64(&p.stats.TxnsRemoved),
		atomic.LoadInt64(&p.stats.DocQueuesCleaned),
		time.Since(tStart).Round(time.Millisecond))
	logger.Debugf("prune stats: %s", pretty.Sprint(p.stats))
	return p.stats, nil
}

func checkTime(toAdd *time.Duration) func() {
	tStart := time.Now()
	return func() {
		atomic.AddInt64((*int64)(toAdd), int64(time.Since(tStart)))
	}
}

type removeResult struct {
	TxnRemoveCount int
	TxnTime        time.Duration
	err            error
}

func (p *IncrementalPruner) pruneNextBatch(iter *mgo.Iter, txnsColl, txnsStash *mgo.Collection, errorCh chan error, wg *sync.WaitGroup) (bool, error) {
	done, txns, txnsBeingCleaned, docsToCheck := p.findTxnsAndDocsToLookup(iter)
	// Now that we have a bunch of documents we want to look at, load them from the collections
	foundDocs, err := p.lookupDocs(docsToCheck, txnsStash)
	if err != nil {
		return done, errors.Trace(err)
	}
	txnsToDelete, err := p.cleanupDocs(docsToCheck, foundDocs, txns, txnsBeingCleaned, txnsColl.Database, txnsStash)
	if err != nil {
		return done, errors.Trace(err)
	}
	if len(txnsToDelete) > 0 {
		p.removeTxns(txnsToDelete, txnsColl, errorCh, wg)
	}
	return done, nil
}

// lookupDocs searches the cache and then looks in the database for the txn-queue of all the referenced document keys.
func (p *IncrementalPruner) lookupDocs(keys docKeySet, txnsStash *mgo.Collection) (docMap, error) {
	defer checkTime(&p.stats.DocLookupTime)()
	docs, docsByCollection := p.lookupDocsInCache(keys)
	missingKeys, err := p.updateDocsFromCollections(docs, docsByCollection, txnsStash.Database)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(missingKeys) > 0 {
		err := p.updateDocsFromStash(docs, missingKeys, txnsStash)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	return docs, nil
}

func (p *IncrementalPruner) findTxnsAndDocsToLookup(iter *mgo.Iter) (bool, []txnDoc, map[bson.ObjectId]struct{}, docKeySet) {
	defer checkTime(&p.stats.TxnReadTime)()
	done := false
	// First, read all the txns to find the document identities we might care about
	txns := make([]txnDoc, 0, pruneTxnBatchSize)
	// We expect a doc in each txn
	docsToCheck := make(docKeySet, pruneTxnBatchSize)
	txnsBeingCleaned := make(map[bson.ObjectId]struct{})
	for count := 0; count < pruneTxnBatchSize; count++ {
		var txn txnDoc
		if iter.Next(&txn) {
			txn.Id = p.cacheTxnId(txn.Id)
			for i := range txn.Ops {
				txn.Ops[i] = p.cacheKey(txn.Ops[i])
			}
			txns = append(txns, txn)
			for _, key := range txn.Ops {
				if p.missingCache.IsMissing(key) {
					// known to be missing, don't bother
					atomic.AddInt64(&p.stats.DocMissingCacheHit, 1)
					continue
				}
				docsToCheck[key] = struct{}{}
			}
			txnsBeingCleaned[txn.Id] = struct{}{}
		} else {
			done = true
		}
	}
	return done, txns, txnsBeingCleaned, docsToCheck
}

func (p *IncrementalPruner) lookupDocsInCache(keys docKeySet) (docMap, map[string][]interface{}) {
	defer checkTime(&p.stats.CacheLookupTime)()
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
			docs[key] = cacheDoc
			atomic.AddInt64(&p.stats.DocCacheHits, 1)
		} else {
			atomic.AddInt64(&p.stats.DocCacheMisses, 1)
			docsByCollection[key.Collection] = append(docsByCollection[key.Collection], key.DocId)
		}
	}
	return docs, docsByCollection
}

func (p *IncrementalPruner) cacheString(s string) string {
	return p.strCache.Intern(s)
}

func (p *IncrementalPruner) cacheObj(obj interface{}) interface{} {
	// technically this might not be a string, but 90% of the time it is.
	if s, ok := obj.(string); ok {
		return p.strCache.Intern(s)
	}
	return obj
}

func (p *IncrementalPruner) cacheKey(key docKey) docKey {
	// TODO(jam): Is it worth caching the docKey object itself?, we aren't using pointers to the docKey
	// it does save a double lookup on both Collection and DocId
	key.Collection = p.cacheString(key.Collection)
	// note that DocId is 99% of the time just a string
	key.DocId = p.cacheObj(key.DocId)
	return key
}

func (p *IncrementalPruner) cacheTxnId(objId bson.ObjectId) bson.ObjectId {
	return bson.ObjectId(p.strCache.Intern(string(objId)))
}

func (p *IncrementalPruner) cacheDoc(collection string, docId interface{}, queue []string, docs docMap) docWithQueue {
	docId = p.cacheObj(docId)
	key := docKey{Collection: p.cacheString(collection), DocId: docId}
	for i := range queue {
		queue[i] = p.cacheString(queue[i])
	}
	txns := p.txnsFromTokens(queue)
	doc := docWithQueue{
		Id:    docId,
		Queue: queue,
		txns:  txns,
	}
	p.docCache.Add(key, doc)
	docs[key] = doc
	return doc
}

// DocCache is a type-aware LRU Cache
type DocCache struct {
	cache *lru.LRU
}

func (dc *DocCache) Get(key docKey) (docWithQueue, bool) {
	res, exists := dc.cache.Get(key)
	if exists {
		return res.(docWithQueue), true
	}
	return docWithQueue{}, false
}

func (dc *DocCache) Add(key docKey, doc docWithQueue) {
	dc.cache.Add(key, doc)
}

// MissingKeyCache is a simplified LRU cache tracking missing keys
type MissingKeyCache struct {
	cache *lru.LRU
}

func (mkc *MissingKeyCache) KnownMissing(key docKey) {
	mkc.cache.Add(key, nil)
}

func (mkc *MissingKeyCache) IsMissing(key docKey) bool {
	_, present := mkc.cache.Get(key)
	return present
}

func (p *IncrementalPruner) updateDocsFromCollections(
	docs docMap,
	docsByCollection map[string][]interface{},
	db *mgo.Database,
) (map[stashDocKey]struct{}, error) {
	defer checkTime(&p.stats.DocReadTime)()
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
		atomic.AddInt64(&p.stats.CollectionQueries, 1)
		var doc docWithQueue
		for iter.Next(&doc) {
			doc = p.cacheDoc(collection, doc.Id, doc.Queue, docs)
			atomic.AddInt64(&p.stats.DocReads, 1)
			delete(missing, doc.Id)
		}
		atomic.AddInt64(&p.stats.DocStillMissing, int64(len(missing)))
		for id, _ := range missing {
			stashKey := stashDocKey{Collection: collection, Id: id}
			missingKeys[stashKey] = struct{}{}
		}

		if err := iter.Close(); err != nil {
			return nil, errors.Trace(err)
		}
	}
	return missingKeys, nil
}

// Txns returns the Transaction ObjectIds associated with each token.
// These are cached on the doc object, so that we don't have to convert repeatedly.
func (p *IncrementalPruner) txnsFromTokens(tokens []string) []bson.ObjectId {
	txns := make([]bson.ObjectId, len(tokens))
	for i := range tokens {
		txns[i] = p.cacheTxnId(txnTokenToId(tokens[i]))
	}
	return txns
}

func (p *IncrementalPruner) updateDocsFromStash(
	docs docMap,
	missingKeys map[stashDocKey]struct{},
	txnsStash *mgo.Collection,
) error {
	defer checkTime(&p.stats.StashLookupTime)()
	// Note: there is some danger that new transactions will be adding and removing a document that we
	// reference in an old transaction. If that is happening fast enough, it is possible that we won't be able to see
	// the document in either place, and thus won't be able to verify that the old transaction is not actually
	// referenced. However, the act of adding or remove a document should be cleaning up the txn queue anyway,
	// which means it is safe to delete the document
	// For all the other documents, now we need to check txns.stash
	atomic.AddInt64(&p.stats.StashQueries, 1)
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
		p.cacheDoc(doc.Id.Collection, doc.Id.Id, doc.Queue, docs)
		atomic.AddInt64(&p.stats.StashDocReads, 1)
	}
	if err := iter.Close(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (p *IncrementalPruner) cleanupDocs(
	docsToCheck docKeySet,
	foundDocs docMap,
	txns []txnDoc,
	txnsBeingCleaned map[bson.ObjectId]struct{},
	db *mgo.Database,
	txnsStash *mgo.Collection,
) ([]bson.ObjectId, error) {
	defer checkTime(&p.stats.DocCleanupTime)()
	txnsToDelete := make([]bson.ObjectId, 0, pruneTxnBatchSize)
	// TODO(jam): 2018-11-30 Currently this operates in txn order, iterating all the txns, finding docs to cleanup.
	// We could, instead, iterate the txn order, then build up documents in each collection to clean, and then issue
	// a single cleanup per collection.
	// However, I'm not sure how that interacts with txns.stash, as we need to know which documents we failed to update.
	// At least this code pulls all txns in the current batch in each pass. Though if you have the same docs over and
	// over, you end up iterating the list to find there is nothing to pull multiple times.
	for _, txn := range txns {
		txnCanBeRemoved := true
		for _, docKey := range txn.Ops {
			doc, ok := foundDocs[docKey]
			if !ok {
				atomic.AddInt64(&p.stats.DocsMissing, 1)
				p.missingCache.KnownMissing(docKey)
				if docKey.Collection == "metrics" {
					// XXX: This is a special case. Metrics are *known* to violate the transaction guarantees
					// by removing documents directly from the collection, without using a transaction. Even
					// though they are *created* with transactions... bad metrics, bad dog
					logger.Tracef("ignoring missing metrics doc: %v", docKey)
				} else if docKey.Collection == "cloudimagemetadata" {
					// There is an upgrade step in 2.3.4 that bulk deletes all cloudimagemetadata that have particular
					// attributes, ignoring transactions...
					logger.Tracef("ignoring missing cloudimagemetadat doc: %v", docKey)
				} else {
					logger.Warningf("transaction %q referenced document %v but it could not be found",
						txn.Id.Hex(), docKey)
					// This is usually a sign of corruption, but for the purposes of pruning, we'll just treat it as a
					// transaction that cannot be cleaned up.
					txnCanBeRemoved = false
				}
				continue
			}
			tokensToPull, newQueue, newTxns := p.findTxnsToPull(doc, txnsBeingCleaned)
			if len(tokensToPull) > 0 {
				atomic.AddInt64(&p.stats.DocTokensCleaned, int64(len(tokensToPull)))
				atomic.AddInt64(&p.stats.DocQueuesCleaned, 1)
				coll := db.C(docKey.Collection)
				pull := bson.M{"$pullAll": bson.M{"txn-queue": tokensToPull}}
				err := coll.UpdateId(docKey.DocId, pull)
				if err != nil {
					if err != mgo.ErrNotFound {
						return nil, errors.Trace(err)
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
						return nil, errors.Trace(err)
					}
				}
				// Update the known Queue of the document, since we cleaned it.
				doc.Queue = newQueue
				doc.txns = newTxns
				p.docCache.Add(docKey, doc)
			} else {
				// already clean of transactions we are currently processing
				atomic.AddInt64(&p.stats.DocsAlreadyClean, 1)
			}
		}
		if txnCanBeRemoved {
			txnsToDelete = append(txnsToDelete, txn.Id)
		} else {
			atomic.AddInt64(&p.stats.TxnsNotRemoved, 1)
		}
	}
	return txnsToDelete, nil
}

func (p *IncrementalPruner) findTxnsToPull(doc docWithQueue, txnsBeingCleaned map[bson.ObjectId]struct{}) ([]string, []string, []bson.ObjectId) {
	// We expect that *most* of the time, we won't pull any txns, because old txns will already have been removed
	// Because of this, we actually do 2 passes over the data. The first time we are seeing if there is anything that
	// might be pulled, and the second actually builds the new lists
	// DocQueuesCleaned:253,438,
	// DocsAlreadyClean:29,103,909
	// So about 100x more likely to not have anything to do. No need to allocate the slices we won't use.
	hasChanges := false
	for _, txnId := range doc.txns {
		if _, isCleaned := txnsBeingCleaned[txnId]; isCleaned {
			hasChanges = true
			break
		}
	}
	if !hasChanges {
		// No changes to make
		return nil, nil, nil
	}
	tokensToPull := make([]string, 0)
	newQueue := make([]string, 0, len(doc.Queue))
	newTxns := make([]bson.ObjectId, 0, len(doc.Queue))
	for i := range doc.Queue {
		token := doc.Queue[i]
		txnId := doc.txns[i]
		if _, isCleaned := txnsBeingCleaned[txnId]; isCleaned {
			tokensToPull = append(tokensToPull, token)
		} else {
			newQueue = append(newQueue, token)
			newTxns = append(newTxns, txnId)
		}
	}
	return tokensToPull, newQueue, newTxns
}

func (p *IncrementalPruner) removeTxns(txnsToDelete []bson.ObjectId, txns *mgo.Collection, errorCh chan error, wg *sync.WaitGroup) error {
	// TODO(jam): 2018-11-29 Evaluate if txnsColl.Bulk().RemoveAll is any better than txnsColl.RemoveAll, we especially want
	// to be using Unordered()
	// The other option is lots of Bulk.Remove() calls.
	// Bulk().Remove seems to be slower than RemoveAll
	wg.Add(1)
	go func() {
		tStart := time.Now()
		session := txns.Database.Session.Copy()
		txns = txns.With(session)
		defer session.Close()
		results, err := txns.RemoveAll(bson.M{
			"_id": bson.M{"$in": txnsToDelete},
		})
		logger.Tracef("removing %d txns removed %d", len(txnsToDelete), results.Removed)
		// TODO: add a stop channel
		atomic.AddInt64(&p.stats.TxnsRemoved, int64(results.Removed))
		atomic.AddInt64((*int64)(&p.stats.TxnRemoveTime), int64(time.Since(tStart)))
		if err != nil {
			errorCh <- errors.Trace(err)
		}
		wg.Done()
	}()
	return nil
}

// docWithQueue is used to serialize a Mongo document that has a txn-queue
type docWithQueue struct {
	Id    interface{}     `bson:"_id"`
	Queue []string        `bson:"txn-queue"`
	txns  []bson.ObjectId `bson:"-"`
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
