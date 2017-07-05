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

// txnDoc is a document that has interesting transactions we want to investigate
type txnDoc struct {
	Id       interface{}   "_id"
	TxnQueue []interface{} "txn-queue"
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

// txnBatchSize is how many transactions we will Remove() at a time
const txnBatchSize = 5000

type LongTxnTrimmer struct {
	txns  *mgo.Collection
	timer *simpleTimer

	docCache map[docKey]*txnDoc
	txnCache map[bson.ObjectId]*rawTransaction

	docQueue []docKey

	longTxnSize int

	txnsRemovedCount  int
	docCleanupCount   int
	tokensPulledCount int
}

func (ltt *LongTxnTrimmer) findTxnsToRemove(doc *txnDoc, collName string) ([]bson.ObjectId, []interface{}, error) {
	txnsToRemove := make([]bson.ObjectId, 0, len(doc.TxnQueue))
	tokensToPull := make([]interface{}, 0, len(doc.TxnQueue))
	for _, token := range doc.TxnQueue {
		txnId, nonce, valid := tokenToIdNonce(token)
		if !valid {
			// Shouldn't happen, at a minimum PurgeMissing should have removed it
			logger.Warningf("%q document %q has invalid token: %v",
				collName, doc.Id, token)
			continue
		}
		var txn rawTransaction
		if err := ltt.txns.FindId(txnId).One(&txn); err != nil {
			// Shouldn't happen (again PurgeMissing)
			logger.Warningf("%q document %q has missing transaction: %v",
				collName, doc.Id, token)
		}
		if txn.State != tpreparing && txn.State != tprepared {
			// We only prune out prepared/preparing txns
			continue
		}
		// We are extra conservative about the type of transactions we will
		// prune. For now, we will only prune transactions that only involve this document.
		if len(txn.Ops) != 1 {
			continue
		}
		op := txn.Ops[0]
		if op.C != collName || op.Id != doc.Id {
			continue
		}
		if nonce == txn.Nonce {
			// This transaction involves exactly this document,
			// queue it for removal
			txnsToRemove = append(txnsToRemove, txnId)
		} else {
			logger.Debugf("token %v nonce %q %q", token, nonce, txn.Nonce)
		}
		// This token is for a transaction that should be removed, we
		// can remove the token even if the nonce isn't exactly the same,
		// because the one with the right nonce should also be present
		tokensToPull = append(tokensToPull, token)
	}
	return txnsToRemove, tokensToPull, nil
}

func (ltt *LongTxnTrimmer) checkProgress() {
	if ltt.timer != nil && ltt.timer.isAfter() {
		logger.Debugf("removed %d txns from %s, pulled %d tokens",
			ltt.txnsRemovedCount, ltt.txns.Name, ltt.tokensPulledCount)
	}
}

func (ltt *LongTxnTrimmer) removeTransactions(txnsToRemove []bson.ObjectId) error {
	logger.Debugf("removing %d transactions", len(txnsToRemove))
	for len(txnsToRemove) > 0 {
		batch := txnsToRemove
		if len(batch) > txnBatchSize {
			batch = batch[:txnBatchSize]
		}
		txnsToRemove = txnsToRemove[len(batch):]
		logger.Debugf("removing transactions: %v", batch)
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
	for _, collName := range collNames {
		coll := ltt.txns.Database.C(collName)
		docs, err := findDocsWithLongQueues(coll, ltt.longTxnSize)
		if err != nil {
			return err
		}
		for _, doc := range docs {
			key := docKey{
				Id: doc.Id,
				C:  coll.Name,
			}
			ltt.docCache[key] = &doc
			ltt.docQueue = append(ltt.docQueue, key)
			logger.Infof("%q document %v has %d transactions",
				coll.Name, doc.Id, len(doc.TxnQueue))
		}
	}
	return nil
}

func (ltt *LongTxnTrimmer) processQueue() error {
	for _, docKey := range ltt.docQueue {
		doc, ok := ltt.docCache[docKey]
		if !ok {
			return fmt.Errorf("doc %q %v in queue but not cached", docKey.C, docKey.Id)
		}
		txnIds, tokens, err := ltt.findTxnsToRemove(doc, docKey.C)
		if err != nil {
			return err
		}
		logger.Debugf("%q %v found %d txns, %d tokens to remove", docKey.C, docKey.Id, len(txnIds), len(tokens))
		if err := ltt.removeTransactions(txnIds); err != nil {
			return err
		}

		if err := pullTokens(ltt.txns.Database.C(docKey.C), docKey.Id, tokens); err != nil {
			return err
		}
		ltt.docCleanupCount++
		ltt.tokensPulledCount += len(tokens)
	}
	return nil
}

func (ltt *LongTxnTrimmer) Trim(collNames []string) error {
	tStart := time.Now()
	ltt.docCache = make(map[docKey]*txnDoc)
	ltt.txnCache = make(map[bson.ObjectId]*rawTransaction)
	if err := ltt.findDocsToProcess(collNames); err != nil {
		return err
	}
	if err := ltt.processQueue(); err != nil {
		return err
	}
	logger.Infof("cleaned up %d docs from %d tokens, removing %d transactions in %v",
		ltt.docCleanupCount, ltt.tokensPulledCount, ltt.txnsRemovedCount, time.Since(tStart))
	return nil
}

func TrimLongTransactionQueues(txns *mgo.Collection, maxQueueLength int, collNames ...string) error {
	trimmer := &LongTxnTrimmer{
		timer:       newSimpleTimer(15 * time.Second),
		txns:        txns,
		longTxnSize: maxQueueLength,
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
