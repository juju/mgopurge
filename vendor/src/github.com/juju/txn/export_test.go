// Copyright 2016 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package txn

type TxnRunner txnRunner

// Specify the function that creates the txnRunner for testing.
func SetRunnerFunc(r Runner, f func() TxnRunner) {
	inner := r.(*transactionRunner)
	inner.newRunner = func() txnRunner {
		return f()
	}
}
