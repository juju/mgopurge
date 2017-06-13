// This file contains a modified version of a function from mgo
// (https://github.com/go-mgo/mgo)
// Copyright (c) 2010-2013 - Gustavo Niemeyer <gustavo@niemeyer.net>
// mgo's license is can be found at https://raw.githubusercontent.com/go-mgo/mgo/v2/LICENSE

package main

import (
	"fmt"
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
func PurgeMissing(tc, sc *mgo.Collection, collNames ...string) error {
	found := make(map[bson.ObjectId]bool)
	txnTokenOk := func(txnToken interface{}) bool {
		txnId, ok := tokenToId(txnToken)
		if !ok {
			return false
		}
		if found[txnId] {
			return true
		}
		if tc.FindId(txnId).One(nil) == nil {
			found[txnId] = true
			return true
		}
		return false
	}

	sort.Strings(collNames)
	for _, collName := range collNames {
		c := tc.Database.C(collName)
		iter := c.Find(nil).Select(bson.M{"_id": 1, "txn-queue": 1}).Iter()
		var doc tdoc
		for iter.Next(&doc) {
			var badTokens []interface{}
			for _, txnToken := range doc.TxnQueue {
				if !txnTokenOk(txnToken) {
					badTokens = append(badTokens, txnToken)
				}
			}
			if len(badTokens) > 0 {
				logger.Debugf("document %s/%v: purging %d orphaned or invalid txn tokens",
					collName, doc.Id, len(badTokens))
				if err := pullTokens(c, doc.Id, badTokens); err != nil {
					return err
				}
			}
		}
		if err := iter.Close(); err != nil {
			return fmt.Errorf("transaction queue iteration error for %s: %v", collName, err)
		}
	}

	iter := sc.Find(nil).Select(bson.M{"_id": 1, "txn-queue": 1}).Iter()
	var stdoc stashTDoc
	for iter.Next(&stdoc) {
		var badTokens []interface{}
		for _, txnToken := range stdoc.TxnQueue {
			if !txnTokenOk(txnToken) {
				badTokens = append(badTokens, txnToken)
			}
		}
		if len(badTokens) > 0 {
			logger.Debugf("stash document %s/%v: purging %d orphaned or invalid txn tokens",
				stdoc.Id.C, stdoc.Id.Id, len(badTokens))
			if err := pullTokens(sc, stdoc.Id, badTokens); err != nil {
				return err
			}
		}
	}

	if err := iter.Close(); err != nil {
		return fmt.Errorf("transaction stash iteration error: %v", err)
	}

	return nil
}

func pullTokens(collection *mgo.Collection, docId interface{}, tokens []interface{}) error {
	type M bson.M
	err := collection.UpdateId(docId, M{"$pullAll": M{"txn-queue": tokens}})
	if err != nil {
		return fmt.Errorf("error purging invalid tokens from %s/%v: %v", collection.Name, docId, err)
	}
	return nil
}
