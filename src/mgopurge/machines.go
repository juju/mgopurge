package main

import (
	"fmt"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// FixMachinesTxnQueue removes txn-queue field errors for machine
// documents which refer to completed transactions. This addresses a
// problem with the machine address updater in historical juju
// versions which resulted in runaway txn-queue fields.
func FixMachinesTxnQueue(machines, tc *mgo.Collection) error {
	completed := make(map[bson.ObjectId]bool)
	txnCompleted := func(txnId bson.ObjectId) bool {
		complete, found := completed[txnId]
		if found {
			return complete
		}
		q := tc.Find(bson.M{
			"_id": txnId,
			"s":   bson.M{"$in": []int{5, 6}},
		})
		complete = q.One(nil) == nil
		completed[txnId] = complete
		return complete
	}

	type TDoc struct {
		Id       interface{}   "_id"
		TxnQueue []interface{} "txn-queue"
	}

	iter := machines.Find(nil).Select(bson.M{"_id": 1, "txn-queue": 1}).Iter()
	var doc TDoc
	machineCount := 0
	for iter.Next(&doc) {
		var pullTokens []interface{}
		for _, txnToken := range doc.TxnQueue {
			if txnId, ok := tokenToId(txnToken); ok {
				if txnCompleted(txnId) {
					pullTokens = append(pullTokens, txnToken)
				}
			}
		}
		if len(pullTokens) > 0 {
			err := machines.UpdateId(doc.Id, bson.M{"$pullAll": bson.M{"txn-queue": pullTokens}})
			if err != nil {
				return err
			}
			logger.Debugf("machine %s: removed %d txn-queue entries", doc.Id, len(pullTokens))
		}
		machineCount++
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("machines iteration error: %v", err)
	}
	logger.Infof("Finished checking %d machine documents", machineCount)
	return nil
}
