// Copyright 2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package main

import (
	"fmt"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/mgo.v2/txn"
)

const tprepared = 2

type txnDoc struct {
	Id     bson.ObjectId `bson:"_id"`
	State  int           `bson:"s"`
	Info   interface{}   `bson:"i,omitempty"`
	Ops    []txn.Op      `bson:"o"`
	Nonce  string        `bson:"n,omitempty"`
	Revnos []int64       `bson:"r,omitempty"`
}

func repairStash(db *mgo.Database, txns *mgo.Collection) error {
	stashes := db.C(txnsStashC)
	iter := txns.Find(bson.M{"s": tprepared}).Iter()

	var txn txnDoc
	for iter.Next(&txn) {
		for _, op := range txn.Ops {
			if op.Insert != nil {
				token := fmt.Sprintf("%s_%s", txn.Id.Hex(), txn.Nonce)
				err := ensureCorrectStash(stashes, token, op)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func ensureCorrectStash(sc *mgo.Collection, token string, op txn.Op) error {
	key := docKey{C: op.C, Id: op.Id}
	err := sc.UpdateId(key, bson.M{"$addToSet": bson.M{"txn-queue": token}})
	// It's ok for there to not be a stash document.
	if err != nil && err != mgo.ErrNotFound {
		return err
	}
	return nil
}
