// Copyright 2016 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package txn

import (
	"time"

	"gopkg.in/mgo.v2"
)

type TxnRunner txnRunner

// Specify the function that creates the txnRunner for testing.
func SetRunnerFunc(r Runner, f func() TxnRunner) {
	inner := r.(*transactionRunner)
	inner.newRunner = func() txnRunner {
		return f()
	}
}

var CheckMongoSupportsOut = checkMongoSupportsOut

// NewDBOracleNoOut is only used for testing. It forces the DBOracle to not ask
// mongo to populate the working set in the aggregation pipeline, which is our
// compatibility code for older mongo versions.
func NewDBOracleNoOut(txns *mgo.Collection, thresholdTime time.Time) (*DBOracle, func(), error) {
	oracle := &DBOracle{
		db:            txns.Database,
		txns:          txns,
		usingMongoOut: false,
		thresholdTime: thresholdTime,
	}
	cleanup, err := oracle.prepare()
	return oracle, cleanup, err
}
