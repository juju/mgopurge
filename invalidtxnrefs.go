// Copyright 2021 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package main

import (
	"github.com/juju/errors"
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


func (invalidtxn *InvalidTxnReferenceCleaner) lookupDoc(coll string, id interface{}) (txnDoc, error) {
	var doc txnDoc
	collection := invalidtxn.db.C(coll)
	err := collection.FindId(id).One(&doc)
	if err != nil {
		return doc, err
	}
	return doc, nil
}

func (invalidtxn *InvalidTxnReferenceCleaner) Run() error{
	iter := invalidtxn.iterStagedTransactions()
	var txn rawTransaction
	for iter.Next(&txn) {
		token := txn.Id.Hex() + "_" + txn.Nonce
		is_valid := true
		for _, op := range txn.Ops {
			// TODO: jam 2021-08-09 eventually we could try to do this in some sort of batch
			//  lookup, but for now that is premature optimization
			doc, err := invalidtxn.lookupDoc(op.C, op.Id)
			if err != nil {
				if err != mgo.ErrNotFound {
					return errors.Trace(err)
				}
				logger.Infof("Transaction %q references a missing document %q %v",
					token, op.C, op.Id)
				is_valid = false
				break
			}
			found := false
			for _, tt := range doc.TxnQueue {
				if tt == token {
					found = true
				}
			}
			if !found {
				logger.Infof("Transaction %q references a document %q %v, " +
					"but the txn is not found in transaction queue",
					token, op.C, op.Id)
				is_valid = false
				break
			}
		}
		if !is_valid {
			// The best way to take a 'staged' transaction, and convert it to
			// something safe is to remove the nonce and declare it "preparing"
			// again
			invalidtxn.txns.UpdateId(txn.Id, bson.M{"$set": bson.M{"s": tpreparing, "n": ""}})
		}
	}
	return nil
}

func (invalidtxn *InvalidTxnReferenceCleaner) iterStagedTransactions() *mgo.Iter{
	// TODO: jam 2021-08-09 Should we consider transactions in state 1 "preparing"?
	//  I don't think we need to, because the should immediately go to failed once we try
	//  to touch a document that doesn't exist

	// Look for transactions that are in stage 2 "prepared"
	query := invalidtxn.txns.Find(
		bson.M{"s": bson.M{"$in": []int{tprepared}}},
	).Select(txnFields)
	return query.Iter()
}

func CleanupInvalidTxnReferences(txns *mgo.Collection) error {

	cleaner := InvalidTxnReferenceCleaner{
		txns: txns,
		db: txns.Database,
		acceptablyMissingCollections: []string{},
	}
	return cleaner.Run()
}
