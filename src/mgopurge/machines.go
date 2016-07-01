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
		fixCount := 0
		for _, txnToken := range doc.TxnQueue {
			if txnId, ok := tokenToId(txnToken); ok {
				if txnCompleted(txnId) {
					if err := pullTxn(machines, doc.Id, txnId); err != nil {
						return err
					}
					fixCount++
				}
			}
		}
		if fixCount > 0 {
			fmt.Println("%s: removed %d completed txn-queue entries\n", doc.Id, fixCount)
		}
		machineCount++
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("machines iteration error: %v", err)
	}
	fmt.Println("Finished checking %d machine documents\n", machineCount)
	return nil
}
