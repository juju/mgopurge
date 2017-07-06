// Copyright 2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.
package main

import (
	"fmt"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/mgo.v2/txn"
)

type parsedToken struct {
	token string
	txnId bson.ObjectId
}

// txnDoc is a document that has interesting transactions we want to investigate
type txnDoc struct {
	Id       interface{}   `bson:"_id"`
	TxnQueue []interface{} `bson:"txn-queue"`
	queue    []parsedToken `bson:"-"` // always omitted from bson
}

const (
	tpreparing int = 1 // One or more documents not prepared
	tprepared  int = 2 // Prepared but not yet ready to run
)

type rawTransaction struct {
	Id    bson.ObjectId `bson:"_id"`
	State int           `bson:"s"`
	Ops   []txn.Op      `bson:"o"`
	Nonce string        `bson:"n,omitempty"`
}

var txnFields = bson.D{{"_id", 1}, {"s", 1}, {"o", 1}, {"n", 1}}

func findDocsWithLongQueues(coll *mgo.Collection, queueSize int) ([]txnDoc, error) {
	var docs []txnDoc
	queueEntry := fmt.Sprintf("txn-queue.%d", queueSize-1)
	query := coll.Find(bson.M{queueEntry: bson.M{"$exists": 1}}).Select(bson.M{"txn-queue": 1, "_id": 1})
	err := query.All(&docs)
	if err != nil {
		return nil, err
	}
	return docs, nil
}

func tokenToIdNonce(token interface{}) (bson.ObjectId, string, bool) {
	tokenStr, ok := token.(string)
	if !ok {
		return "", "", false
	}
	if !validToken.MatchString(tokenStr) {
		return "", "", false
	}
	// take the first 24 hex chars as the object ID, and the last 8 chars as the nonce
	return bson.ObjectIdHex(tokenStr[:24]), tokenStr[25:], true
}

// defaultTxnBatchSize is how many transactions we will Remove() at a time
const defaultTxnBatchSize = 5000

// LongTxnTrimmer handles processing transaction queues that have grown unmanageable
// to be handled by the normal Resume logic.
type LongTxnTrimmer struct {
	txns  *mgo.Collection
	timer *simpleTimer

	docCache map[docKey]*txnDoc

	txnsToProcess []bson.ObjectId

	longTxnSize int

	txnBatchSize      int
	txnsRemovedCount  int
	txnsNotPrepared   int
	txnsNotAllDocs    int
	docCleanupCount   int
	tokensPulledCount int
}

// loadTxns ensures the transactions are in the txnCache
func (ltt *LongTxnTrimmer) loadTxns(ids []bson.ObjectId) (map[bson.ObjectId]*rawTransaction, error) {
	remaining := ids
	rawTxns := make(map[bson.ObjectId]*rawTransaction, len(ids))
	// We load them into a slab so that we don't fragment memory with lots
	// of small 'txn' allocations.
	txns := make([]rawTransaction, 0, len(ids))
	for len(remaining) > 0 {
		batch := remaining
		if len(batch) > ltt.txnBatchSize {
			batch = batch[:ltt.txnBatchSize]
		}
		remaining = remaining[len(batch):]
		query := ltt.txns.Find(bson.M{"_id": bson.M{"$in": batch}}).Select(txnFields)
		query.Batch(1000)
		iter := query.Iter()
		var txn rawTransaction
		for iter.Next(&txn) {
			txns = append(txns, txn)
			rawTxns[txn.Id] = &txns[len(txns)-1]
		}
		if err := iter.Close(); err != nil {
			return nil, err
		}
	}
	return rawTxns, nil
}

func (ltt *LongTxnTrimmer) checkProgress() {
	if ltt.timer != nil && ltt.timer.isAfter() {
		logger.Debugf("removed %d txns from %s, pulled %d tokens (preserved %d+%d transactions)",
			ltt.txnsRemovedCount, ltt.txns.Name, ltt.tokensPulledCount,
			ltt.txnsNotPrepared, ltt.txnsNotAllDocs)
	}
}

func (ltt *LongTxnTrimmer) removeTransactions(txnsToRemove []bson.ObjectId) error {
	for len(txnsToRemove) > 0 {
		batch := txnsToRemove
		if len(batch) > ltt.txnBatchSize {
			batch = batch[:ltt.txnBatchSize]
		}
		txnsToRemove = txnsToRemove[len(batch):]
		info, err := ltt.txns.RemoveAll(bson.M{"_id": bson.M{"$in": batch}})
		if err != nil {
			return err
		}
		ltt.txnsRemovedCount += info.Removed
		ltt.checkProgress()
	}
	return nil
}

// findDocsToProcess goes through all the collections and looks for documents
// with transaction queues that are too long. It populates the internal document
// cache and queue of docs to process.
func (ltt *LongTxnTrimmer) findDocsToProcess(collNames []string) error {
	seenTxnIds := make(map[bson.ObjectId]struct{})
	for _, collName := range collNames {
		coll := ltt.txns.Database.C(collName)
		docs, err := findDocsWithLongQueues(coll, ltt.longTxnSize)
		if err != nil {
			return err
		}
		for i := range docs {
			doc := &docs[i]
			key := docKey{
				Id: doc.Id,
				C:  coll.Name,
			}
			ltt.docCache[key] = doc
			doc.queue = make([]parsedToken, 0, len(doc.TxnQueue))
			for _, token := range doc.TxnQueue {
				txnId, valid := tokenToId(token)
				if !valid {
					// Shouldn't happen, at a minimum PurgeMissing should have removed it
					logger.Warningf("%q document %q has invalid token: %v",
						collName, doc.Id, token)
					continue
				}
				doc.queue = append(doc.queue, parsedToken{
					token: token.(string), // wouldn't be valid if it wasn't a string
					txnId: txnId,
				})
				if _, ok := seenTxnIds[txnId]; ok {
					continue
				}
				seenTxnIds[txnId] = struct{}{}
				ltt.txnsToProcess = append(ltt.txnsToProcess, txnId)
			}
			// Now we've converted everything to queue, we can drop the other data
			logger.Infof("%q document %v has %d transactions",
				coll.Name, doc.Id, len(doc.queue))
			doc.TxnQueue = nil
		}
	}
	return nil
}

type txnBatchTrimmer struct {
	txnIds        []bson.ObjectId
	docCache      map[docKey]*txnDoc
	docsToCleanup map[docKey]*txnDoc
	txns          map[bson.ObjectId]*rawTransaction
	txnsToRemove  []bson.ObjectId

	omitNotPreparedCount int
	omitNotAllDocsCount  int

	txnRemover   func([]bson.ObjectId) error
	tokenRemover func(docKey, []interface{}) error
}

// checkTransactionsFindDocs ensures that all of the transactions listed are of
// the type that we want to prune, and all are affecting documents where each
// document also has a transaction queue that needs to be pruned.
// After this function is run, transactions that don't qualify will be removed
// from tb.txns, and the object IDs to remove will be in tb.txnsToRemove.
// Documents involved in those transactions will also be listed in tb.docsToCleanup
func (tb *txnBatchTrimmer) checkTransactionsFindDocs() {
	for _, txnId := range tb.txnIds {
		txn, ok := tb.txns[txnId]
		if !ok {
			logger.Debugf("failed to load transaction: %v", txnId)
			continue
		}
		if txn.State != tpreparing && txn.State != tprepared {
			// This transaction should not be pruned
			// logger.Tracef("txn %v not in state prepared/preparing: %d", txnId, txn.State)
			tb.omitNotPreparedCount++
			delete(tb.txns, txnId)
			continue
		}
		foundAllDocs := true
		for _, op := range txn.Ops {
			key := docKey{
				C:  op.C,
				Id: op.Id,
			}
			if _, ok := tb.docsToCleanup[key]; ok {
				// Already queued for cleanup
				continue
			}
			doc, ok := tb.docCache[key]
			if !ok {
				// logger.Tracef("txn %v refers to unread doc: %q %v", txnId, key.C, key.Id)
				foundAllDocs = false
				break
			}
			tb.docsToCleanup[key] = doc
		}
		if !foundAllDocs {
			// We won't purge this transaction if one of the docs
			// involved doesn't have a long transaction queues
			tb.omitNotAllDocsCount++
			delete(tb.txns, txnId)
			continue
		}
		tb.txnsToRemove = append(tb.txnsToRemove, txnId)
	}
}

// processDocs works through all of the docsToCleanup that were found in
// checkTransactionsFindDocs, it removes all of the tokens that refer to transactions
// that we are removing, and updates the in-memory cache so the doc objects no
// longer refer to those tokens.
func (tb *txnBatchTrimmer) processDocs() error {
	for key, doc := range tb.docsToCleanup {
		tokensToPull := make([]interface{}, 0, len(tb.txnsToRemove))
		remainingQueue := make([]parsedToken, 0, len(doc.queue))
		for _, tokenInfo := range doc.queue {
			if _, ok := tb.txns[tokenInfo.txnId]; ok {
				// we ignore nonce, as we will pull all
				// references to a given txn id, even if
				// the nonce doesn't match
				tokensToPull = append(tokensToPull, tokenInfo.token)
			} else {
				remainingQueue = append(remainingQueue, tokenInfo)
			}
		}
		if err := tb.tokenRemover(key, tokensToPull); err != nil {
			return err
		}
		doc.queue = remainingQueue
	}
	return nil
}

func (tb *txnBatchTrimmer) Process() error {
	tb.checkTransactionsFindDocs()
	// Now we remove this set of transactions, and then purge their tokens from the docs
	if err := tb.txnRemover(tb.txnsToRemove); err != nil {
		return err
	}
	if err := tb.processDocs(); err != nil {
		return err
	}
	return nil
}

func (ltt *LongTxnTrimmer) pullTokens(key docKey, tokens []interface{}) error {
	ltt.tokensPulledCount += len(tokens)
	// default mgopurge includes TRACE logging
	// logger.Tracef("removing %d tokens from %q %v", len(tokens), key.C, key.Id)
	if err := pullTokens(ltt.txns.Database.C(key.C), key.Id, tokens); err != nil {
		return err
	}
	ltt.checkProgress()
	return nil
}

func (ltt *LongTxnTrimmer) processQueue() error {
	logger.Infof("found %d transactions that might be trimmed", len(ltt.txnsToProcess))
	for len(ltt.txnsToProcess) > 0 {
		// We take batches of transactions to process from the end of the stack
		// We walk from the back so that we should be trimming values from
		// the end of long lists, rather than having to resize the lists
		batch := ltt.txnsToProcess
		if len(batch) > ltt.txnBatchSize {
			batch = batch[len(batch)-ltt.txnBatchSize:]
		}
		ltt.txnsToProcess = ltt.txnsToProcess[:len(ltt.txnsToProcess)-len(batch)]
		txns, err := ltt.loadTxns(batch)
		if err != nil {
			return err
		}
		trimmer := txnBatchTrimmer{
			txnIds:        batch,
			txns:          txns,
			docsToCleanup: make(map[docKey]*txnDoc),
			txnsToRemove:  make([]bson.ObjectId, 0, len(batch)),

			docCache:     ltt.docCache,
			txnRemover:   ltt.removeTransactions,
			tokenRemover: ltt.pullTokens,
		}
		if err := trimmer.Process(); err != nil {
			return err
		}
		ltt.txnsNotPrepared += trimmer.omitNotPreparedCount
		ltt.txnsNotAllDocs += trimmer.omitNotAllDocsCount
	}
	return nil
}

func (ltt *LongTxnTrimmer) Trim(collNames []string) error {
	tStart := time.Now()
	ltt.docCache = make(map[docKey]*txnDoc)
	if err := ltt.findDocsToProcess(collNames); err != nil {
		return err
	}
	if err := ltt.processQueue(); err != nil {
		return err
	}
	ltt.docCleanupCount = len(ltt.docCache)
	logger.Infof("trimmed %d docs from %d tokens, removing %d transactions in %v",
		ltt.docCleanupCount, ltt.tokensPulledCount, ltt.txnsRemovedCount, time.Since(tStart))
	return nil
}

func TrimLongTransactionQueues(txns *mgo.Collection, maxQueueLength int, collNames ...string) error {
	trimmer := &LongTxnTrimmer{
		timer:        newSimpleTimer(15 * time.Second),
		txns:         txns,
		longTxnSize:  maxQueueLength,
		txnBatchSize: defaultTxnBatchSize,
	}
	return trimmer.Trim(collNames)
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
