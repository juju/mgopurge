// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package main

import (
	"gopkg.in/mgo.v2"
)

// FixMachinesTxnQueue removes txn-queue field errors for machine
// documents which refer to completed transactions. This addresses a
// problem with the machine address updater in historical juju
// versions which resulted in runaway txn-queue fields.
func FixMachinesTxnQueue(machines, tc *mgo.Collection) error {
	oracle := NewTXNOracle(tc)
	err := FixCollectionTxnQueue(machines, oracle)
	logger.Infof("%s", oracle.Describe())
	return err
}
