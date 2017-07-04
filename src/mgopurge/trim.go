// Copyright 2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.
package main

import (
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

func iterDocsWithLongQueues(coll *mgo.Collection) (*mgo.Iter, error) {
	query := coll.Find(bson.M{longTxnEntry: bson.M{"exists": 1}}).Select(bson.M{"txn-queue": 1, "_id": 1})
	return query.Iter(), nil
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
	return bson.ObjectIdHex(tokenStr[:24]), tokenStr[26:], true
}

// txnBatchSize is how many transactions we will Remove() at a time
const txnBatchSize = 5000

// longTxnEntry existing means we have a txn-queue with at least 10,000 items
const longTxnEntry = "txn-queue.10000"

func findTxnsToRemove(doc txnDoc, coll, txns *mgo.Collection) ([]bson.ObjectId, []interface{}, error) {
	txnsToRemove := make([]bson.ObjectId, 0, len(doc.TxnQueue))
	tokensToPull := make([]interface{}, 0, len(doc.TxnQueue))
	for _, token := range doc.TxnQueue {
		txnId, nonce, valid := tokenToIdNonce(token)
		if !valid {
			// Shouldn't happen, at a minimum PurgeMissing should have removed it
			logger.Warningf("%q document %q has invalid token: %v",
				coll.Name, doc.Id, token)
			continue
		}
		var txn rawTransaction
		if err := txns.FindId(txnId).One(&txn); err != nil {
			// Shouldn't happen (again PurgeMissing)
			logger.Warningf("%q document %q has missing transaction: %v",
				coll.Name, doc.Id, token)
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
		if op.C != coll.Name || op.Id != doc.Id {
			continue
		}
		if nonce == txn.Nonce {
			// This transaction involves exactly this document,
			// queue it for removal
			txnsToRemove = append(txnsToRemove, txnId)
		}
		// This token is for a transaction that should be removed, we
		// can remove the token even if the nonce isn't exactly the same,
		// because the one with the right nonce should also be present
		tokensToPull = append(tokensToPull, token)
	}
	return txnsToRemove, tokensToPull, nil
}

func removeTransactions(txns *mgo.Collection, txnsToRemove []bson.ObjectId, timer *simpleTimer, removedCount int) (int, error) {
	for len(txnsToRemove) > 0 {
		batch := txnsToRemove
		if len(batch) > txnBatchSize {
			batch = batch[:txnBatchSize]
		}
		txnsToRemove = txnsToRemove[len(batch):]
		info, err := txns.RemoveAll(bson.M{"_id": bson.M{"$in": batch}})
		if err != nil {
			return removedCount, err
		}
		removedCount += info.Removed
		if timer.isAfter() {
			logger.Debugf("removed %d txns from %s", txns.Name)
		}
	}
	return removedCount, nil
}

func TrimLongTransactionQueues(txns *mgo.Collection, collNames ...string) error {
	txnsRemovedCount := 0
	tokensPulledCount := 0
	docCleanupCount := 0
	tStart := time.Now()
	timer := newSimpleTimer(15 * time.Second)
	for _, collName := range collNames {
		coll := txns.Database.C(collName)
		iter, err := iterDocsWithLongQueues(coll)
		if err != nil {
			return err
		}
		var doc txnDoc
		for iter.Next(&doc) {
			docCleanupCount++
			logger.Infof("%q document %q has %d transactions",
				coll.Name, doc.Id, len(doc.TxnQueue))
			txnsToRemove, tokensToPull, err := findTxnsToRemove(doc, coll, txns)
			if err != nil {
				return err
			}
			// We remove the transactions from the txn collection before we remove them
			// from the document, because PurgeMissing will handle removing them from
			// the document if we fail midway. But anything in 'prepared'+ state is
			// presumed to still be referenced in the document.
			txnsRemovedCount, err = removeTransactions(txns, txnsToRemove, timer, txnsRemovedCount)
			if err != nil {
				return err
			}
			if err := pullTokens(coll, doc.Id, tokensToPull); err != nil {
				return err
			}
			tokensPulledCount += len(tokensToPull)
		}
		if err := iter.Close(); err != nil {
			return err
		}
	}
	logger.Infof("cleaned up %d docs from %d tokens, removing %d transactions in %v",
		docCleanupCount, tokensPulledCount, txnsRemovedCount, time.Since(tStart))
	return nil
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
