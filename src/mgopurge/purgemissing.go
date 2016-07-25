// This file contains a modified version of a function from mgo
// (https://github.com/go-mgo/mgo)
// Copyright (c) 2010-2013 - Gustavo Niemeyer <gustavo@niemeyer.net>
// mgo's license is can be found at https://raw.githubusercontent.com/go-mgo/mgo/v2/LICENSE

package main

import (
	"fmt"
	"regexp"
	"sort"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// PurgeMissing removes from collections any state that refers to transaction
// documents that for whatever reason have been lost from the system (removed
// by accident or lost in a hard crash, for example).
//
// This method should very rarely be needed, if at all, and should never be
// used during the normal operation of an application. Its purpose is to put
// a system that has seen unavoidable corruption back in a working state.
//
// NOTE! - This is a fork of the upstream PurgeMissing which correctly
// handles invalid txn tokens.
func PurgeMissing(tc, sc *mgo.Collection, collections ...string) error {

	type TDoc struct {
		Id       interface{}   "_id"
		TxnQueue []interface{} "txn-queue"
	}

	found := make(map[bson.ObjectId]bool)
	txnExists := func(txnId bson.ObjectId) bool {
		if found[txnId] {
			return true
		}
		if tc.FindId(txnId).One(nil) == nil {
			found[txnId] = true
			return true
		}
		return false
	}

	sort.Strings(collections)
	for _, collection := range collections {
		c := tc.Database.C(collection)
		iter := c.Find(nil).Select(bson.M{"_id": 1, "txn-queue": 1}).Iter()
		var tdoc TDoc
		for iter.Next(&tdoc) {
			for _, txnToken := range tdoc.TxnQueue {
				if txnId, ok := tokenToId(txnToken); ok {
					if !txnExists(txnId) {
						logger.Debugf("purging from document %s/%v the missing transaction id %s\n",
							collection, tdoc.Id, txnId)
						if err := pullTxn(c, tdoc.Id, txnId); err != nil {
							return err
						}
					}
				} else {
					logger.Debugf("purging from document %s/%v the invalid transaction token %#v\n",
						collection, tdoc.Id, txnToken)
					if err := pullToken(c, tdoc.Id, txnToken); err != nil {
						return err
					}
				}
			}
		}
		if err := iter.Close(); err != nil {
			return fmt.Errorf("transaction queue iteration error for %s: %v", collection, err)
		}
	}

	type StashTDoc struct {
		Id       docKey        "_id"
		TxnQueue []interface{} "txn-queue"
	}

	iter := sc.Find(nil).Select(bson.M{"_id": 1, "txn-queue": 1}).Iter()
	var stdoc StashTDoc
	for iter.Next(&stdoc) {
		for _, txnToken := range stdoc.TxnQueue {
			if txnId, ok := tokenToId(txnToken); ok {
				if !txnExists(txnId) {
					logger.Debugf("purging from stash document %s/%v the missing transaction id %s\n",
						stdoc.Id.C, stdoc.Id.Id, txnId)
					if err := pullTxn(sc, stdoc.Id, txnId); err != nil {
						return err
					}
				}
			} else {
				logger.Debugf("purging from stash document %s/%v the invalid transaction token %#v\n",
					stdoc.Id.C, stdoc.Id.Id, txnToken)
				if err := pullToken(sc, stdoc.Id, txnToken); err != nil {
					return err
				}
			}
		}
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("transaction stash iteration error: %v", err)
	}

	return nil
}

type docKey struct {
	C  string
	Id interface{}
}

var validToken = regexp.MustCompile(`[a-f0-9]{24}_[a-f0-9]{8}`)

func tokenToId(token interface{}) (bson.ObjectId, bool) {
	tokenStr, ok := token.(string)
	if !ok {
		return "", false
	}
	if !validToken.MatchString(tokenStr) {
		return "", false
	}
	return bson.ObjectIdHex(tokenStr[:24]), true
}

func pullToken(collection *mgo.Collection, docId interface{}, token interface{}) error {
	type M bson.M
	err := collection.UpdateId(docId, M{"$pullAll": M{"txn-queue": []interface{}{token}}})
	if err != nil {
		return fmt.Errorf("error purging invalid token %#v: %v", token, err)
	}
	return nil
}

func pullTxn(collection *mgo.Collection, docId interface{}, txnId bson.ObjectId) error {
	type M bson.M
	err := collection.UpdateId(docId, M{"$pull": M{"txn-queue": M{"$regex": "^" + txnId.Hex() + "_*"}}})
	if err != nil {
		return fmt.Errorf("error purging missing transaction %s: %v", txnId.Hex(), err)
	}
	return nil
}
