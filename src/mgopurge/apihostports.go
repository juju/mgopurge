// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package main

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const apiHostPortsKey = "apiHostPorts"

type tdoc struct {
	Id       interface{}   "_id"
	TxnQueue []interface{} "txn-queue"
}

// FixApiHostPorts removes the transactions on the apiHostPorts
// documents if there's more than one in the txn-queue. This is the
// result of a bad interaction between Juju and mgo/txn that has been
// observed in the field where 1000's of txns in the "preparing" or
// "prepared" state are queued up on the document.
func FixApiHostPorts(db *mgo.Database, txns *mgo.Collection) error {
	coll, doc, err := getApiHostPortsDoc(db)
	if err != nil {
		return err
	}

	if len(doc.TxnQueue) > 1 {
		logger.Infof("%d transactions to be cleared from apiHostPorts doc", len(doc.TxnQueue))
		err := coll.UpdateId(apiHostPortsKey, bson.M{
			"$set": bson.M{"txn-queue": []string{}},
		})
		if err != nil {
			return err
		}

		var txnIdsToRemove []bson.ObjectId
		for _, txnToken := range doc.TxnQueue {
			if txnId, ok := tokenToId(txnToken); ok {
				txnIdsToRemove = append(txnIdsToRemove, txnId)
			}
		}

		if len(txnIdsToRemove) > 0 {
			_, err := txns.RemoveAll(bson.M{"_id": bson.M{"$in": txnIdsToRemove}})
			if err != nil {
				return err
			}
		}

		logger.Infof("apiHostPorts transactions cleared")
	} else {
		logger.Infof("apiHostPorts doc looks ok - not repairing")
	}

	return nil
}

func getApiHostPortsDoc(db *mgo.Database) (*mgo.Collection, *tdoc, error) {
	// Location of the document is different across Juju versions.
	var doc tdoc
	coll := db.C("stateServers")
	err := coll.FindId(apiHostPortsKey).One(&doc)
	if err == mgo.ErrNotFound {
		coll = db.C("controllers")
		err = coll.FindId(apiHostPortsKey).One(&doc)
	}
	if err != nil {
		return nil, nil, err
	}
	return coll, &doc, nil
}
