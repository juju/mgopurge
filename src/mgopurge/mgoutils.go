// Copyright 2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package main

import (
	"regexp"

	"gopkg.in/mgo.v2/bson"
)

type tdoc struct {
	Id       interface{}   "_id"
	TxnQueue []interface{} "txn-queue"
}

type docKey struct {
	C  string
	Id interface{}
}

type stashTDoc struct {
	Id       docKey        "_id"
	TxnQueue []interface{} "txn-queue"
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
