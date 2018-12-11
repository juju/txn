// Copyright 2018 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package txn

import (
	//"github.com/juju/loggo"
	"github.com/juju/testing"
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/mgo.v2/txn"
)

var _ = gc.Suite(&IncrementalPruneSuite{})

type IncrementalPruneSuite struct {
	TxnSuite
}

func (*IncrementalPruneSuite) TestSimplePrune(c *gc.C) {
	c.Check(1, gc.Equals, 1)

}

type TxnSuite struct {
	testing.IsolationSuite
	testing.MgoSuite
	db     *mgo.Database
	txns   *mgo.Collection
	runner *txn.Runner
}

func (s *TxnSuite) SetUpSuite(c *gc.C) {
	s.IsolationSuite.SetUpSuite(c)
	s.MgoSuite.SetUpSuite(c)
}

func (s *TxnSuite) TearDownSuite(c *gc.C) {
	// Make sure we've removed any Chaos
	txn.SetChaos(txn.Chaos{})
	s.MgoSuite.TearDownSuite(c)
	s.IsolationSuite.TearDownSuite(c)
}

func (s *TxnSuite) SetUpTest(c *gc.C) {
	s.IsolationSuite.SetUpTest(c)
	s.MgoSuite.SetUpTest(c)
	txn.SetChaos(txn.Chaos{})

	s.db = s.Session.DB("mgo-test")
	s.txns = s.db.C("txns")
	s.runner = txn.NewRunner(s.txns)
}

func (s *TxnSuite) TearDownTest(c *gc.C) {
	// Make sure we've removed any Chaos
	txn.SetChaos(txn.Chaos{})
	s.MgoSuite.TearDownTest(c)
	s.IsolationSuite.TearDownTest(c)
}

func (s *TxnSuite) runTxn(c *gc.C, ops ...txn.Op) bson.ObjectId {
	txnId := bson.NewObjectId()
	err := s.runner.Run(ops, txnId, nil)
	c.Assert(err, jc.ErrorIsNil)
	return txnId
}
