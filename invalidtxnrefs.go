// Copyright 2021 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package main

import (
	"github.com/juju/mgo/v2"
	"github.com/juju/mgo/v2/bson"
)
// InvalidTxnReferenceCleaner looks for transactions that reference documents that cannot be found,
// as well as documents that do not have the transaction in their queue.
// These cannot be resumed cleanly, so we just remove them.
type InvalidTxnReferenceCleaner struct {
	txns *mgo.Collection
	db *mgo.Database

	// acceptablyMissingCollections lists the collection names that we 'accept' if they are
	// missing. Normally the only collection that things should 'disappear' is "metrics".
	// Because they are deleted by a batch process. However,
	//we make this field serviceable by allowing additional collections to be supplied.
	acceptablyMissingCollections []string
}


func (invalidtxn *InvalidTxnReferenceCleaner) lookupDoc(coll string, id interface{}) (*txnDoc, error) {
	var doc txnDoc
	collection := invalidtxn.db.C(coll)
	err := collection.FindId(id).One(&doc)
	if err != nil {
		return nil, err
	}
}
func (invalidtxn *InvalidTxnReferenceCleaner) Run() {
	iter := invalidtxn.iterStagedTransactions()
	var txn rawTransaction
	for iter.Next(&txn) {
		for _, op := range txn.Ops {
			// TODO: eventually we could try to do this in some sort of batch lookup,
			//  but for now that is premature optimization
			doc, err := invalidtxn.lookupDoc(op.C, op.Id)
		}
	}
}

func (invalidtxn *InvalidTxnReferenceCleaner) iterStagedTransactions() *mgo.Iter{
	// Look for transactions that are in either stage 1 "Pending" or stage 2 "Prepared"
	query := invalidtxn.txns.Find(
		bson.M{"s": bson.M{"$in": []int{1, 2}}},
	).Select(txnFields)
	return query.Iter()
}

func CleanupInvalidTxnReferences(txns *mgo.Collection) error {

	cleaner := InvalidTxnReferenceCleaner{
		txns: txns,
		db: txns.Database,
		acceptablyMissingCollections: []string{},
	}
}
