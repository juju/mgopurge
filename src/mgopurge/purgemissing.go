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
func PurgeMissing(tc, sc *mgo.Collection, collNames ...string) error {

	type TDoc struct {
		Id       interface{}   "_id"
		TxnQueue []interface{} "txn-queue"
	}

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
		var tdoc TDoc
		for iter.Next(&tdoc) {
			var badTokens []interface{}
			for _, txnToken := range tdoc.TxnQueue {
				if !txnTokenOk(txnToken) {
					badTokens = append(badTokens, txnToken)
				}
			}
			if len(badTokens) > 0 {
				logger.Debugf("document %s/%v: purging %d orphaned or invalid txn tokens",
					collName, tdoc.Id, len(badTokens))
				if err := pullTokens(c, tdoc.Id, badTokens); err != nil {
					return err
				}
			}
		}
		if err := iter.Close(); err != nil {
			return fmt.Errorf("transaction queue iteration error for %s: %v", collName, err)
		}
	}

	type StashTDoc struct {
		Id       docKey        "_id"
		TxnQueue []interface{} "txn-queue"
	}

	iter := sc.Find(nil).Select(bson.M{"_id": 1, "txn-queue": 1}).Iter()
	var stdoc StashTDoc
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

func pullTokens(collection *mgo.Collection, docId interface{}, tokens []interface{}) error {
	type M bson.M
	err := collection.UpdateId(docId, M{"$pullAll": M{"txn-queue": tokens}})
	if err != nil {
		return fmt.Errorf("error purging invalid tokens from %s/%v: %v", collection.Name, docId, err)
	}
	return nil
}
