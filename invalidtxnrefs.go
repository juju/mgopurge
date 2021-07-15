// Copyright 2021 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package main

import (
	"github.com/juju/mgo/v2"
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

func CleanupInvalidTxnReferences(txns *mgo.Collection) error {

}
