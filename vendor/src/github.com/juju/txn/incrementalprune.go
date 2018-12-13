// Copyright 2018 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package txn

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/juju/lru"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const pruneTxnBatchSize = 1000
const pruneMinTxnBatchSize = 10
const pruneMaxTxnBatchSize = 10000
const defaultBatchSleepTime time.Duration = 0
const maxBatchSleepTime = 1 * time.Second
const queryDocBatchSize = 100
const pruneDocCacheSize = 10000
const missingKeyCacheSize = 2000
const strCacheSize = 10000

// IncrementalPruner reads the transaction table incrementally, seeing if it can remove the current set of transactions,
// and then moves on to newer transactions. It only thinks about 1k txns at a time, because that is the batch size that
// can be deleted. Instead, it caches documents that it has seen.
type IncrementalPruner struct {
	maxTime        time.Time
	reverse        bool
	txnBatchSize   int
	batchSleepTime time.Duration
	ProgressChan   chan ProgressMessage
	docCache       docCache
	missingCache   missingKeyCache
	strCache       *lru.StringCache
	strMu          sync.Mutex
	stats          PrunerStats
}

type ProgressMessage struct {
	TxnsRemoved int
	DocsCleaned int
}

// IncrementalPruneArgs specifies the parameters for running incremental cleanup steps.
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

	// TxnBatchSize is how many transactions to process at once.
	TxnBatchSize int

	// TxnBatchSleepTime is how long we should sleep between processing transaction
	// batches, to allow other parts of the system to operate (avoid consuming
	// all resources)
	// The default is to not sleep at all, but this can be configured to reduce
	// load while pruning.
	TxnBatchSleepTime time.Duration

	// TODO(jam): 2018-12-12 Include a github.com/juju/clock.Clock
	// interface so that we can test that sleep is properly handled per
	// batch. Potentially we could also test that we measure performance
	// counters correctly.
}

// PrunerStats collects statistics about how the prune progressed
type PrunerStats struct {
	CacheLookupTime    time.Duration
	DocReadTime        time.Duration
	DocLookupTime      time.Duration
	DocCleanupTime     time.Duration
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
	StrCacheHits       int64
	StrCacheMisses     int64
}

func (ps PrunerStats) String() string {
	durationType := reflect.TypeOf(time.Second)
	v := reflect.ValueOf(ps)
	t := v.Type()
	longestAttrLength := 1
	longestValueLength := 1
	longestTimeLength := 1
	values := make([]string, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		attr := t.Field(i)
		if len(attr.Name) > longestAttrLength {
			longestAttrLength = len(attr.Name)
		}
		val := v.Field(i)
		var valStr string
		if val.Type() == durationType {
			valStr = fmt.Sprintf("%.3f", val.Interface().(time.Duration).Round(time.Millisecond).Seconds())
			if len(valStr) > longestTimeLength {
				longestTimeLength = len(valStr)
			}
		} else {
			valStr = fmt.Sprint(val.Interface())
			if len(valStr) > longestValueLength {
				longestValueLength = len(valStr)
			}
		}
		values[i] = valStr
	}
	resultStrs := []string{
		"PrunerStats(",
	}
	for i := 0; i < t.NumField(); i++ {
		fieldWidth := longestValueLength
		field := t.Field(i)
		if field.Type == durationType {
			fieldWidth = longestTimeLength
		}
		next := fmt.Sprintf("  %*s: %*s", longestAttrLength, field.Name, fieldWidth, values[i])
		resultStrs = append(resultStrs, next)
	}
	resultStrs = append(resultStrs, ")")
	return strings.Join(resultStrs, "\n")
}

// CombineStats aggregates two stats into a single value
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
		StrCacheHits:       a.StrCacheHits + b.StrCacheHits,
		StrCacheMisses:     a.StrCacheMisses + b.StrCacheMisses,
	}
}

func NewIncrementalPruner(args IncrementalPruneArgs) *IncrementalPruner {
	if args.TxnBatchSize == 0 {
		args.TxnBatchSize = pruneTxnBatchSize
	}
	if args.TxnBatchSize < pruneMinTxnBatchSize {
		args.TxnBatchSize = pruneMinTxnBatchSize
	}
	if args.TxnBatchSize > pruneMaxTxnBatchSize {
		args.TxnBatchSize = pruneMaxTxnBatchSize
	}
	if args.TxnBatchSleepTime < 0 {
		args.TxnBatchSleepTime = defaultBatchSleepTime
	}
	if args.TxnBatchSleepTime > maxBatchSleepTime {
		args.TxnBatchSleepTime = maxBatchSleepTime
	}
	return &IncrementalPruner{
		maxTime:        args.MaxTime,
		reverse:        args.ReverseOrder,
		txnBatchSize:   args.TxnBatchSize,
		batchSleepTime: args.TxnBatchSleepTime,
		ProgressChan:   args.ProgressChannel,
		docCache:       docCache{cache: lru.New(pruneDocCacheSize)},
		missingCache:   missingKeyCache{cache: lru.New(missingKeyCacheSize)},
		strCache:       lru.NewStringCache(strCacheSize),
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
		if !done && p.batchSleepTime != 0 {
			time.Sleep(p.batchSleepTime)
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
	p.stats.StrCacheHits = hits.Hit
	p.stats.StrCacheMisses = hits.Miss
	if firstErr == nil {
		firstErr = p.cleanupStash(txnsStash)
	}
	logger.Debugf("%s", p.stats)
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
	query.Batch(p.txnBatchSize)
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
	// TODO(jam):  2018-12-12 Do we need to worry about the txn-remove/txn-insert
	//  attributes?
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
	txns := make([]txnDoc, 0, p.txnBatchSize)
	// We expect a doc in each txn
	docsToCheck := make(docKeySet, p.txnBatchSize)
	txnsBeingCleaned := make(map[bson.ObjectId]struct{})
	for count := 0; count < p.txnBatchSize; count++ {
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

func (p *IncrementalPruner) cleanupDoc(
	collection string,
	doc docWithQueue,
	txnsBeingCleaned map[bson.ObjectId]struct{},
	foundDocs docMap,
	db *mgo.Database,
	txnsStash *mgo.Collection,
) (bool, error) {
	tokensToPull, newQueue, newTxnIds := p.findTxnsToPull(doc, txnsBeingCleaned)
	if len(tokensToPull) == 0 {
		// Nothing to do for this doc
		p.stats.DocsAlreadyClean++
		return false, nil
	}
	coll := db.C(collection)
	p.stats.DocTokensCleaned += int64(len(tokensToPull))
	p.stats.DocQueuesCleaned++
	pull := bson.M{"$pullAll": bson.M{"txn-queue": tokensToPull}}
	err := coll.UpdateId(doc.Id, pull)
	if err != nil {
		if err != mgo.ErrNotFound {
			return false, errors.Trace(err)
		}
		// Look in txns.stash. One option here is to just delete the document if there are no more
		// references in the queue.
		err := txnsStash.UpdateId(stashDocKey{
			Collection: collection,
			Id:         doc.Id,
		}, pull)
		if err != nil {
			if err == mgo.ErrNotFound {
				logger.Warningf("trying to cleanup doc %v, could not be found in collection %q nor stash",
					doc.Id, collection)
			} else {
				return false, errors.Trace(err)
			}
		}
	}
	dKey := docKey{
		Collection: collection,
		DocId:      doc.Id,
	}
	doc.Queue = newQueue
	doc.txns = newTxnIds
	// Update the known Queue of the document, since we cleaned it.
	foundDocs[dKey] = doc
	p.docCache.Add(dKey, doc)
	return true, nil
}

func (p *IncrementalPruner) cleanupDocs(
	foundDocs docMap,
	txns []txnDoc,
	txnsBeingCleaned map[bson.ObjectId]struct{},
	db *mgo.Database,
	txnsStash *mgo.Collection,
) error {
	defer checkTime(&p.stats.DocCleanupTime)()
	docsCleanedUp := 0
	for _, txn := range txns {
		missingDocKeys := make([]docKey, 0)
		for _, docKey := range txn.Ops {
			if p.missingCache.IsMissing(docKey) {
				// Document known to be missing
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
					// Note: (jam 2018-12-06) This is a special case. Metrics are
					// *known* to violate the transaction guarantees. They are
					// created and updated with the transaction logic, but are
					// removed in bulk without transaction logic.
					logger.Tracef("ignoring missing metrics doc: %v", docKey)
				} else {
					missingDocKeys = append(missingDocKeys, docKey)
				}
				continue
			}
			updated, err := p.cleanupDoc(docKey.Collection, doc, txnsBeingCleaned, foundDocs, db, txnsStash)
			if err != nil {
				return errors.Trace(err)
			}
			if updated {
				docsCleanedUp++
			}
		}
		if len(missingDocKeys) > 0 {
			// This might be corruption, or might be an issue, but humans probably can't do anything about it anyway
			logger.Infof("transaction %q referenced documents that could not be found: %v",
				txn.Id.Hex(), missingDocKeys)
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

func (p *IncrementalPruner) removeTxns(txnsToDelete []bson.ObjectId, txns *mgo.Collection, errorCh chan error, wg *sync.WaitGroup) {
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
