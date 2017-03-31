// Copyright 2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package main

import (
	"fmt"
	"strings"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type TxnCompleted interface {
	IsCompleted(txnId bson.ObjectId) bool
}

type txnOracle struct {
	Completed      map[bson.ObjectId]bool
	TxnsCollection *mgo.Collection
	HitCount       uint64
	QueryCount     uint64
	CompleteCount  uint64
	IncompleteCount uint64
}

type txnStatus struct {
	Status int `bson:"s"`
}
func (oracle *txnOracle) IsCompleted(txnId bson.ObjectId) bool {
	oracle.QueryCount++
	complete, found := oracle.Completed[txnId]
	if found {
		oracle.HitCount++
		return complete
	}
	var st txnStatus
	q := oracle.TxnsCollection.Find(bson.M{"_id": txnId}).Select(bson.M{"_id": 0, "s": 1})
	err := q.One(&st)
	complete = false
	if err == nil {
		// Not found is treated as not-complete for our purposes
		complete = (st.Status == 5 || st.Status == 6)
	}
	oracle.Completed[txnId] = complete
	if complete {
		oracle.CompleteCount++
	} else {
		oracle.IncompleteCount++
	}
	return complete
}

func (oracle *txnOracle) Describe() string {
	hitRate := 0.0
	if oracle.QueryCount > 0 {
		hitRate = (float64(oracle.HitCount) * 100.0 / float64(oracle.QueryCount))
	}
	return fmt.Sprintf("Oracle HitCount %d, QueryCount %d, HitRate %.1f%%, Completed: %d, Incomplete: %d",
		oracle.HitCount, oracle.QueryCount, hitRate,
		oracle.CompleteCount, oracle.IncompleteCount)
}

func NewTXNOracle(coll *mgo.Collection) *txnOracle {
	return &txnOracle{
		Completed:      make(map[bson.ObjectId]bool),
		TxnsCollection: coll,
	}
}

// CollectionsToPruneQueues finds collection names that we should process for
// txn-queue fields.
func CollectionsToPruneQueues(db *mgo.Database, txnsName string) ([]string, error) {
	collNames, err := db.CollectionNames()
	if err != nil {
		return nil, fmt.Errorf("reading collection names: %v", err)
	}
	outNames := make([]string, 0, len(collNames))
	for _, name := range collNames {
		include := true
		switch {
		case name == txnsName+".stash":
			include = true
		case name == txnsName, strings.HasPrefix(name, txnsName+"."):
			// Don't look in things other than txns.stash that are related to txns
			include = false
		case strings.HasPrefix(name, "system."):
			// Don't look in system collections.
			include = false
		default:
			// Everything else needs to be considered.
			include = true
		}
		if include {
			outNames = append(outNames, name)
		}
	}
	return outNames, nil
}

// PruneAllCollectionQueues iterates through all known databases and looks for
// transactions that should be considered completed.
func PruneAllCollectionQueues(db *mgo.Database, txns *mgo.Collection) error {
	names, err := CollectionsToPruneQueues(db, txns.Name)
	if err != nil {
		return err
	}
	logger.Infof("%d collections to prune queues", len(names))
	oracle := NewTXNOracle(txns)
	for _, name := range names {
		logger.Infof("pruning queues in collection %q", name)
		coll := db.C(name)
		err := FixCollectionTxnQueue(coll, oracle)
		logger.Infof("%s", oracle.Describe())
		if err != nil {
			return err
		}
	}
	return nil
}

// FixCollectionTxnQueue removes txn-queue field errors for machine
// documents which refer to completed transactions. This addresses a
// problem with the machine address updater in historical juju
// versions which resulted in runaway txn-queue fields.
func FixCollectionTxnQueue(coll *mgo.Collection, oracle TxnCompleted) error {

	type TDoc struct {
		Id       interface{}   "_id"
		TxnQueue []interface{} "txn-queue"
	}

	query := coll.Find(nil).Select(bson.M{"_id": 1, "txn-queue": 1})
	query.Batch(100)
	iter := query.Iter()
	var doc TDoc
	docCount := 0
	totalPulled := 0
	for iter.Next(&doc) {
		var pullTokens []interface{}
		for _, txnToken := range doc.TxnQueue {
			if txnId, ok := tokenToId(txnToken); ok {
				if oracle.IsCompleted(txnId) {
					pullTokens = append(pullTokens, txnToken)
				}
			}
		}
		if len(pullTokens) > 0 {
			/// err := coll.UpdateId(doc.Id, bson.M{"$pullAll": bson.M{"txn-queue": pullTokens}})
			/// if err != nil {
			/// 	return err
			/// }
			// We don't log everything because most objects will have at least 1 token
			if len(pullTokens) > 1 {
				logger.Debugf("%s %s: removed %d txn-queue entries", coll.Name, doc.Id, len(pullTokens))
			}
			totalPulled += len(pullTokens)
		}
		docCount++
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf(" iteration error: %v", err)
	}
	logger.Infof("Finished checking %d %s documents, pulled %d entries", docCount, coll.Name, totalPulled)
	return nil
}
