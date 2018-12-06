// Copyright 2018 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package txn

import (
	"sort"
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
const missingKeyCacheSize = 2000
const strCacheSize = 10000

// IncrementalPruner reads the transzaction table incrementally, seeing if it can remove the current set of transactions,
// and then moves on to newer transactions. It only thinks about 1k txns at a time, because that is the batch size that
// can be deleted. Instead, it caches documents that it has seen.
type IncrementalPruner struct {
	maxTime      time.Time
	reverse      bool
	ProgressChan chan ProgressMessage
	docCache     docCache
	missingCache missingKeyCache
	strCache     *lru.StringCache
	strMu        sync.Mutex
	stats        PrunerStats
}

type ProgressMessage struct {
	TxnsRemoved int
	DocsCleaned int
}

// IncrementalPruneArgs specifies the parameters for running incremental cleanup steps..
type IncrementalPruneArgs struct {
	// MaxTime is a timestamp that provides a threshold of transactions
	// that we will actually prune. Only transactions that were created
	// before this threshold will be pruned.
	// MaxTime can be set to the Zero value to indicate all transactions.
	MaxTime time.Time

	// If ProgressChannel is not nil, this will send updates when documents are
	// processed and transactions are pruned.
	ProgressChannel chan ProgressMessage

	// ReverseOrder indicates we should process transactions from newest to
	// oldest instead of form oldest to newest.
	ReverseOrder bool
}

// PrunerStats collects statistics about how the prune progressed
type PrunerStats struct {
	CacheLookupTime    time.Duration
	DocLookupTime      time.Duration
	DocCleanupTime     time.Duration
	DocReadTime        time.Duration
	StashLookupTime    time.Duration
	StashRemoveTime    time.Duration
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
	StashDocsRemoved   int64
	DocQueuesCleaned   int64
	DocTokensCleaned   int64
	DocsAlreadyClean   int64
	TxnsRemoved        int64
	TxnsNotRemoved     int64
	ObjCacheHits       int64
	ObjCacheMisses     int64
}

func CombineStats(a, b PrunerStats) PrunerStats {
	return PrunerStats{
		CacheLookupTime:    a.CacheLookupTime + b.CacheLookupTime,
		DocLookupTime:      a.DocLookupTime + b.DocLookupTime,
		DocCleanupTime:     a.DocCleanupTime + b.DocCleanupTime,
		DocReadTime:        a.DocReadTime + b.DocReadTime,
		StashLookupTime:    a.StashLookupTime + b.StashLookupTime,
		StashRemoveTime:    a.StashRemoveTime + b.StashRemoveTime,
		TxnReadTime:        a.TxnReadTime + b.TxnReadTime,
		TxnRemoveTime:      a.TxnRemoveTime + b.TxnRemoveTime,
		DocCacheHits:       a.DocCacheHits + b.DocCacheHits,
		DocCacheMisses:     a.DocCacheMisses + b.DocCacheMisses,
		DocMissingCacheHit: a.DocMissingCacheHit + b.DocMissingCacheHit,
		DocsMissing:        a.DocsMissing + b.DocsMissing,
		CollectionQueries:  a.CollectionQueries + b.CollectionQueries,
		DocReads:           a.DocReads + b.DocReads,
		DocStillMissing:    a.DocStillMissing + b.DocStillMissing,
		StashQueries:       a.StashQueries + b.StashQueries,
		StashDocReads:      a.StashDocReads + b.StashDocReads,
		StashDocsRemoved:   a.StashDocsRemoved + b.StashDocsRemoved,
		DocQueuesCleaned:   a.DocQueuesCleaned + b.DocQueuesCleaned,
		DocTokensCleaned:   a.DocTokensCleaned + b.DocTokensCleaned,
		DocsAlreadyClean:   a.DocsAlreadyClean + b.DocsAlreadyClean,
		TxnsRemoved:        a.TxnsRemoved + b.TxnsRemoved,
		TxnsNotRemoved:     a.TxnsNotRemoved + b.TxnsNotRemoved,
		ObjCacheHits:       a.ObjCacheHits + b.ObjCacheHits,
		ObjCacheMisses:     a.ObjCacheMisses + b.ObjCacheMisses,
	}
}

func NewIncrementalPruner(args IncrementalPruneArgs) *IncrementalPruner {
	return &IncrementalPruner{
		maxTime:      args.MaxTime,
		reverse:      args.ReverseOrder,
		ProgressChan: args.ProgressChannel,
		docCache:     docCache{cache: lru.New(pruneDocCacheSize)},
		missingCache: missingKeyCache{cache: lru.New(missingKeyCacheSize)},
		strCache:     lru.NewStringCache(strCacheSize),
	}
}

func (p *IncrementalPruner) Prune(txns *mgo.Collection) (PrunerStats, error) {
	session := txns.Database.Session.Copy()
	defer session.Close()
	txns = txns.With(session)
	txnsStashName := txns.Name + ".stash"
	txnsStash := txns.Database.C(txnsStashName)
	errorCh := make(chan error, 100)
	var wg sync.WaitGroup

	iter := p.findTxnsQuery(txns)
	done := false
	for !done {
		var err error
		done, err = p.pruneNextBatch(iter, txns, txnsStash, errorCh, &wg)
		if err != nil {
			done = true
			// It is a little weird to buffer an error to our own loop, but we have
			// to process the error chan in case the txn deletion routines
			// also encounter errors.
			errorCh <- errors.Trace(err)
		}
	}
	if err := iter.Close(); err != nil {
		logger.Warningf("error closing iteration: %v", err)
		errorCh <- errors.Trace(err)
	}
	// Wait for all txn.Remove to be finished
	wg.Wait()
	var firstErr error
	empty := false
	for !empty {
		select {
		case err := <-errorCh:
			if firstErr == nil {
				firstErr = err
			} else {
				logger.Warningf("error while processing: %v", err)
			}
		default:
			empty = true
		}
	}
	hits := p.strCache.HitCounts()
	p.stats.ObjCacheHits = hits.Hit
	p.stats.ObjCacheMisses = hits.Miss
	if firstErr == nil {
		firstErr = p.cleanupStash(txnsStash)
	}
	logger.Debugf("pruneStats: %# v", pretty.Sprint(p.stats))
	return p.stats, errors.Trace(firstErr)
}

func (p *IncrementalPruner) findTxnsQuery(txns *mgo.Collection) *mgo.Iter {
	if !p.maxTime.IsZero() {
		logger.Debugf("looking for completed transactions older than %s", p.maxTime)
	} else {
		logger.Debugf("looking for all completed transactions")
	}
	query := txns.Find(completedOldTransactionMatch(p.maxTime))
	query.Select(bson.M{
		"_id": 1,
		"o.c": 1,
		"o.d": 1,
	})
	// Sorting by _id helps make sure that we are grouping the transactions close to each other for removals
	if p.reverse {
		query.Sort("-_id")
	} else {
		query.Sort("_id")
	}
	query.Batch(pruneTxnBatchSize)
	return query.Iter()

}

func checkTime(toAdd *time.Duration) func() {
	tStart := time.Now()
	return func() {
		(*toAdd) += time.Since(tStart)
	}
}

func (p *IncrementalPruner) cleanupStash(txnsStash *mgo.Collection) error {
	tStart := time.Now()
	info, err := txnsStash.RemoveAll(
		bson.M{"txn-queue.0": bson.M{"$exists": 0}},
	)
	p.stats.StashRemoveTime = time.Since(tStart)
	p.stats.StashDocsRemoved = int64(info.Removed)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (p *IncrementalPruner) pruneNextBatch(iter *mgo.Iter, txnsColl, txnsStash *mgo.Collection, errorCh chan error, wg *sync.WaitGroup) (bool, error) {
	done, txns, txnsBeingCleaned, docsToCheck := p.findTxnsAndDocsToLookup(iter)
	// Now that we have a bunch of documents we want to look at, load them from the collections
	foundDocs, err := p.lookupDocs(docsToCheck, txnsStash)
	if err != nil {
		return done, errors.Trace(err)
	}

	if err := p.cleanupDocs(foundDocs, txns, txnsBeingCleaned, txnsColl.Database, txnsStash); err != nil {
		return done, errors.Trace(err)
	}
	if len(txns) > 0 {
		txnsToRemove := make([]bson.ObjectId, len(txns))
		for i, txn := range txns {
			txnsToRemove[i] = txn.Id
		}
		p.removeTxns(txnsToRemove, txnsColl, errorCh, wg)
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
			p.stats.DocCacheHits++
		} else {
			p.stats.DocCacheMisses++
			docsByCollection[key.Collection] = append(docsByCollection[key.Collection], key.DocId)
		}
	}
	return docs, docsByCollection
}

func (p *IncrementalPruner) cacheString(s string) string {
	p.strMu.Lock()
	s = p.strCache.Intern(s)
	p.strMu.Unlock()
	return s
}

func (p *IncrementalPruner) cacheObj(obj interface{}) interface{} {
	// technically this might not be a string, but 90% of the time it is.
	if s, ok := obj.(string); ok {
		return p.cacheString(s)
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
	return bson.ObjectId(p.cacheString(string(objId)))
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

// docCache is a type-aware LRU Cache
type docCache struct {
	cache *lru.LRU
	mu    sync.Mutex
}

func (dc *docCache) Get(key docKey) (docWithQueue, bool) {
	dc.mu.Lock()
	res, exists := dc.cache.Get(key)
	dc.mu.Unlock()
	if exists {
		return res.(docWithQueue), true
	}
	return docWithQueue{}, false
}

func (dc *docCache) Add(key docKey, doc docWithQueue) {
	dc.mu.Lock()
	dc.cache.Add(key, doc)
	dc.mu.Unlock()
}

// missingKeyCache is a simplified LRU cache tracking missing keys
type missingKeyCache struct {
	cache *lru.LRU
	mu    sync.Mutex
}

func (mkc *missingKeyCache) KnownMissing(key docKey) {
	mkc.mu.Lock()
	mkc.cache.Add(key, nil)
	mkc.mu.Unlock()
}

func (mkc *missingKeyCache) IsMissing(key docKey) bool {
	mkc.mu.Lock()
	_, present := mkc.cache.Get(key)
	mkc.mu.Unlock()
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
		p.stats.CollectionQueries++
		var doc docWithQueue
		for iter.Next(&doc) {
			doc = p.cacheDoc(collection, doc.Id, doc.Queue, docs)
			p.stats.DocReads++
			delete(missing, doc.Id)
		}
		p.stats.DocStillMissing += int64(len(missing))
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
		p.cacheDoc(doc.Id.Collection, doc.Id.Id, doc.Queue, docs)
		p.stats.StashDocReads++
	}
	if err := iter.Close(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (p *IncrementalPruner) groupDocsByCollection(foundDocs docMap, txns []txnDoc) map[string]map[interface{}]docWithQueue {
	docsByCollectionAndId := make(map[string]map[interface{}]docWithQueue)
	// Iterate the the transactions, and group the docs to clean by collection
	for _, txn := range txns {
		missingDocKeys := make([]docKey, 0)
		for _, docKey := range txn.Ops {
			collMap := docsByCollectionAndId[docKey.Collection]
			if collMap == nil {
				collMap = make(map[interface{}]docWithQueue)
				docsByCollectionAndId[docKey.Collection] = collMap
			}
			if _, ok := collMap[docKey.DocId]; ok {
				// already queued this doc
				continue
			}
			doc, ok := foundDocs[docKey]
			if !ok {
				if p.missingCache.IsMissing(docKey) {
					continue
				}
				p.stats.DocsMissing++
				p.missingCache.KnownMissing(docKey)
				if docKey.Collection == "metrics" {
					// XXX: This is a special case. Metrics are *known* to violate the transaction guarantees
					// by removing documents directly from the collection, without using a transaction. Even
					// though they are *created* with transactions... bad metrics, bad dog
					logger.Tracef("ignoring missing metrics doc: %v", docKey)
				} else {
					missingDocKeys = append(missingDocKeys, docKey)
				}
				continue
			}
			collMap[docKey.DocId] = doc
		}
		if len(missingDocKeys) > 0 {
			// This might be corruption, or might be an issue, but humans probably can't do anything about it anyway
			logger.Infof("transaction %q referenced documents that could not be found: %v",
				txn.Id.Hex(), missingDocKeys)
		}
	}
	return docsByCollectionAndId
}

type docToCleanup struct {
	id           interface{}
	tokensToPull []string
	newQueue     []string
	newTxnIds    []bson.ObjectId
}
type collectionToCleanup struct {
	collection    string
	docsToCleanup []docToCleanup
}

func (p *IncrementalPruner) sortDocsToProcess(
	txnsBeingCleaned map[bson.ObjectId]struct{},
	docsByCollectionAndId map[string]map[interface{}]docWithQueue,
) []collectionToCleanup {
	todo := make([]collectionToCleanup, 0, len(docsByCollectionAndId))
	// Now we have all the documents we want to handle, sort them nicely and remove ones that don't have any updates
	// We try to sort by the string form of their Id(), but if it isn't a string, we don't bother
	for collection, mappedDocs := range docsByCollectionAndId {
		keys := make([]string, 0, len(mappedDocs))
		byId := make(map[string]docToCleanup)
		collCleanup := collectionToCleanup{
			collection: collection,
		}
		for _, doc := range mappedDocs {
			if len(doc.Queue) == 0 {
				// nothing to do
				p.stats.DocsAlreadyClean++
				continue
			}
			tokensToPull, newQueue, newTxns := p.findTxnsToPull(doc, txnsBeingCleaned)
			if len(tokensToPull) == 0 {
				// Nothing to do for this doc
				p.stats.DocsAlreadyClean++
				continue
			}
			cleanup := docToCleanup{
				id:           doc.Id,
				tokensToPull: tokensToPull,
				newQueue:     newQueue,
				newTxnIds:    newTxns,
			}
			if s, ok := doc.Id.(string); ok {
				keys = append(keys, s)
				byId[s] = cleanup
			} else {
				collCleanup.docsToCleanup = append(collCleanup.docsToCleanup, cleanup)
			}
		}
		sort.Strings(keys)
		for _, key := range keys {
			collCleanup.docsToCleanup = append(collCleanup.docsToCleanup, byId[key])
		}
		if len(collCleanup.docsToCleanup) > 0 {
			todo = append(todo, collCleanup)
		}
	}
	return todo
}

func (p *IncrementalPruner) cleanupDocs(
	foundDocs docMap,
	txns []txnDoc,
	txnsBeingCleaned map[bson.ObjectId]struct{},
	db *mgo.Database,
	txnsStash *mgo.Collection,
) error {
	defer checkTime(&p.stats.DocCleanupTime)()
	docsByCollection := p.groupDocsByCollection(foundDocs, txns)
	todo := p.sortDocsToProcess(txnsBeingCleaned, docsByCollection)

	docsCleanedUp := 0
	for _, collectionCleanup := range todo {
		coll := db.C(collectionCleanup.collection)
		for _, docCleanup := range collectionCleanup.docsToCleanup {
			p.stats.DocTokensCleaned += int64(len(docCleanup.tokensToPull))
			p.stats.DocQueuesCleaned++
			pull := bson.M{"$pullAll": bson.M{"txn-queue": docCleanup.tokensToPull}}
			err := coll.UpdateId(docCleanup.id, pull)
			if err != nil {
				if err != mgo.ErrNotFound {
					return errors.Trace(err)
				}
				// Look in txns.stash. One option here is to just delete the document if there are no more
				// references in the queue.
				err := txnsStash.UpdateId(stashDocKey{
					Collection: collectionCleanup.collection,
					Id:         docCleanup.id,
				}, pull)
				if err != nil {
					if err == mgo.ErrNotFound {
						logger.Warningf("trying to cleanup doc %v, could not be found in collection %q nor stash",
							docCleanup.id, collectionCleanup.collection)
					} else {
						return errors.Trace(err)
					}
				}
			}
			docsCleanedUp++
			dKey := docKey{
				Collection: collectionCleanup.collection,
				DocId:      docCleanup.id,
			}
			// Update the known Queue of the document, since we cleaned it.
			p.docCache.Add(dKey, docWithQueue{
				Id:    docCleanup.id,
				Queue: docCleanup.newQueue,
				txns:  docCleanup.newTxnIds,
			})
		}
	}
	if docsCleanedUp > 0 && p.ProgressChan != nil {
		p.ProgressChan <- ProgressMessage{DocsCleaned: docsCleanedUp}
	}
	return nil
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
	wg.Add(1)
	session := txns.Database.Session.Copy()
	txns = txns.With(session)
	go func() {
		tStart := time.Now()
		results, err := txns.RemoveAll(bson.M{
			"_id": bson.M{"$in": txnsToDelete},
		})
		logger.Tracef("removing %d txns removed %d", len(txnsToDelete), results.Removed)
		p.stats.TxnsRemoved += int64(results.Removed)
		p.stats.TxnRemoveTime += time.Since(tStart)
		if p.ProgressChan != nil {
			p.ProgressChan <- ProgressMessage{
				TxnsRemoved: results.Removed,
			}
		}
		if err != nil {
			errorCh <- errors.Trace(err)
		}
		session.Close()
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
