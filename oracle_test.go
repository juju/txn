// Copyright 2017 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package txn_test

import (
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/mgo.v2/txn"

	jujutxn "github.com/juju/txn"
)

// OracleSuite will be run against all oracle implementations.
type OracleSuite struct {
	TxnSuite
	OracleFunc func(*mgo.Collection) (jujutxn.Oracle, func(), error)
}

func (s *OracleSuite) txnToToken(c *gc.C, id bson.ObjectId) string {
	var noncer struct {
		Nonce string `bson:"n"`
	}

	err := s.txns.FindId(id).Select(bson.M{"n": 1}).One(&noncer)
	c.Assert(err, jc.ErrorIsNil)
	return id.Hex() + "_" + noncer.Nonce
}

func (s *OracleSuite) TestKnownAndUnknownTxns(c *gc.C) {
	completedTxnId := s.runTxn(c, txn.Op{
		C:      "coll",
		Id:     0,
		Insert: bson.M{},
	})
	pendingTxnId := s.runInterruptedTxn(c, txn.Op{
		C:      "coll",
		Id:     0,
		Update: bson.M{},
	})
	oracle, cleanup, err := s.OracleFunc(s.txns)
	defer cleanup()
	c.Assert(oracle, gc.NotNil)
	c.Assert(err, jc.ErrorIsNil)
	// One is the real one, one is a flusher that raced and failed
	completedToken1 := s.txnToToken(c, completedTxnId)
	completedToken2 := completedTxnId.Hex() + "_56780123"
	pendingToken := s.txnToToken(c, pendingTxnId)
	unknownToken := "0123456789abcdef78901234_deadbeef"
	tokens := []string{completedToken1, completedToken2, pendingToken, unknownToken}
	completed, err := oracle.CompletedTokens(tokens)
	c.Assert(err, jc.ErrorIsNil)
	c.Check(completed, jc.DeepEquals, map[string]bool{
		completedToken1: true,
		completedToken2: true,
	})
}

func (s *OracleSuite) TestRemovedTxns(c *gc.C) {
	txnId1 := s.runTxn(c, txn.Op{
		C:      "coll",
		Id:     0,
		Insert: bson.M{},
	})
	txnId2 := s.runTxn(c, txn.Op{
		C:      "coll",
		Id:     1,
		Insert: bson.M{},
	})
	oracle, cleanup, err := s.OracleFunc(s.txns)
	defer cleanup()
	c.Assert(oracle, gc.NotNil)
	c.Assert(err, jc.ErrorIsNil)
	token1 := s.txnToToken(c, txnId1)
	token2 := s.txnToToken(c, txnId2)
	completed, err := oracle.CompletedTokens([]string{token1, token2})
	c.Assert(err, jc.ErrorIsNil)
	c.Check(completed, jc.DeepEquals, map[string]bool{
		token1: true,
		token2: true,
	})
	err = oracle.RemoveTxns([]bson.ObjectId{txnId1})
	c.Assert(err, jc.ErrorIsNil)
	completed, err = oracle.CompletedTokens([]string{token1, token2})
	c.Assert(err, jc.ErrorIsNil)
	c.Check(completed, jc.DeepEquals, map[string]bool{
		token2: true,
	})
}

func (s *OracleSuite) TestIterTxns(c *gc.C) {
	txnId1 := s.runTxn(c, txn.Op{
		C:      "coll",
		Id:     0,
		Insert: bson.M{},
	})
	txnId2 := s.runTxn(c, txn.Op{
		C:      "coll",
		Id:     1,
		Insert: bson.M{},
	})
	txnId3 := s.runTxn(c, txn.Op{
		C:      "coll",
		Id:     2,
		Insert: bson.M{},
	})
	oracle, cleanup, err := s.OracleFunc(s.txns)
	defer cleanup()
	c.Assert(oracle, gc.NotNil)
	c.Assert(err, jc.ErrorIsNil)
	c.Check(oracle.Count(), gc.Equals, 3)
	oracle.RemoveTxns([]bson.ObjectId{txnId2})
	c.Check(oracle.Count(), gc.Equals, 2)
	all := make([]bson.ObjectId, 0)
	iter, err := oracle.IterTxns()
	c.Assert(err, jc.ErrorIsNil)
	var txnId bson.ObjectId
	for txnId, err = iter.Next(); err == nil; txnId, err = iter.Next() {
		all = append(all, txnId)
	}
	c.Assert(err, gc.Equals, jujutxn.EOF)
	// Do we care about the order here?
	c.Check(all, jc.DeepEquals, []bson.ObjectId{txnId1, txnId3})
}

func dbOracleFunc(c *mgo.Collection) (jujutxn.Oracle, func(), error) {
	return jujutxn.NewDBOracle(c)
}

// DBOracleSuite causes the test suite to run against the DBOracle implementation
type DBOracleSuite struct {
	OracleSuite
}

var _ = gc.Suite(&DBOracleSuite{
	OracleSuite: OracleSuite{
		OracleFunc: dbOracleFunc,
	},
})

func (s *DBOracleSuite) TestConfirmOutSupported(c *gc.C) {
	tmpname := "coll.temp"
	coll := s.db.C("coll")
	pipe := coll.Pipe([]bson.M{{"$match": bson.M{}}, {"$out": tmpname}})
	err := pipe.All(&bson.D{})
	if jujutxn.CheckMongoSupportsOut(s.db) {
		c.Assert(err, jc.ErrorIsNil)
	} else {
		c.Check(err, gc.ErrorMatches, ".*Unrecognized pipeline stage name: '\\$out'")
	}
	s.db.C(tmpname).DropCollection()
}

type DBCompatOracleSuite struct {
	OracleSuite
}

func dbNoOutOracleFunc(c *mgo.Collection) (jujutxn.Oracle, func(), error) {
	return jujutxn.NewDBOracleNoOut(c)
}

var _ = gc.Suite(&DBCompatOracleSuite{
	OracleSuite: OracleSuite{
		OracleFunc: dbNoOutOracleFunc,
	},
})

func memOracleFunc(c *mgo.Collection) (jujutxn.Oracle, func(), error) {
	return jujutxn.NewMemOracle(c)
}

type MemOracleSuite struct {
	OracleSuite
}

var _ = gc.Suite(&MemOracleSuite{
	OracleSuite: OracleSuite{
		OracleFunc: memOracleFunc,
	},
})
