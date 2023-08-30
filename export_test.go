// Copyright 2016 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package txn

import (
	"time"

	"github.com/juju/mgo/v3"
)

type TxnRunner txnRunner

// Specify the function that creates the txnRunner for testing.
func SetRunnerFunc(r Runner, f func() TxnRunner) {
	inner := r.(*transactionRunner)
	inner.newRunner = func() txnRunner {
		return f()
	}
}

// Specify the transaction timeout for some tests.
func SetTxnTimeout(r Runner, t time.Duration) {
	inner := r.(*transactionRunner)
	inner.txnTimeout = t
}

var CheckMongoSupportsOut = checkMongoSupportsOut

// NewDBOracleNoOut is only used for testing. It forces the DBOracle to not ask
// mongo to populate the working set in the aggregation pipeline, which is our
// compatibility code for older mongo versions.
func NewDBOracleNoOut(txns *mgo.Collection, thresholdTime time.Time, maxTxns int) (*DBOracle, func(), error) {
	oracle := &DBOracle{
		db:            txns.Database,
		txns:          txns,
		maxTxns:       maxTxns,
		usingMongoOut: false,
		thresholdTime: thresholdTime,
	}
	cleanup, err := oracle.prepare()
	return oracle, cleanup, err
}
