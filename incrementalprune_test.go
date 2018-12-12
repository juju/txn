// Copyright 2018 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package txn

import (
	"time"

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
	testing.IsolatedMgoSuite
	db     *mgo.Database
	txns   *mgo.Collection
	runner *txn.Runner
}

func (s *TxnSuite) SetUpTest(c *gc.C) {
	s.IsolatedMgoSuite.SetUpTest(c)
	txn.SetChaos(txn.Chaos{})

	s.db = s.Session.DB("mgo-test")
	s.txns = s.db.C("txns")
	s.runner = txn.NewRunner(s.txns)
}

func (s *TxnSuite) TearDownTest(c *gc.C) {
	// Make sure we've removed any Chaos
	txn.SetChaos(txn.Chaos{})
	s.IsolatedMgoSuite.TearDownTest(c)
}

func (s *TxnSuite) runTxn(c *gc.C, ops ...txn.Op) bson.ObjectId {
	txnId := bson.NewObjectId()
	err := s.runner.Run(ops, txnId, nil)
	c.Assert(err, jc.ErrorIsNil)
	return txnId
}

type PrunerStatsSuite struct {
	testing.IsolationSuite
}

var _ = gc.Suite(&PrunerStatsSuite{})

func (*PrunerStatsSuite) TestBaseString(c *gc.C) {
	v1 := PrunerStats{}
	c.Check(v1.String(), gc.Equals, `
PrunerStats(
     CacheLookupTime: 0.000
       DocLookupTime: 0.000
      DocCleanupTime: 0.000
         DocReadTime: 0.000
     StashLookupTime: 0.000
     StashRemoveTime: 0.000
         TxnReadTime: 0.000
       TxnRemoveTime: 0.000
        DocCacheHits: 0
      DocCacheMisses: 0
  DocMissingCacheHit: 0
         DocsMissing: 0
   CollectionQueries: 0
            DocReads: 0
     DocStillMissing: 0
        StashQueries: 0
       StashDocReads: 0
    StashDocsRemoved: 0
    DocQueuesCleaned: 0
    DocTokensCleaned: 0
    DocsAlreadyClean: 0
         TxnsRemoved: 0
      TxnsNotRemoved: 0
        ObjCacheHits: 0
      ObjCacheMisses: 0
)`[1:])
}

func (*PrunerStatsSuite) TestAlignedTimes(c *gc.C) {
	v1 := PrunerStats{
		CacheLookupTime: time.Duration(12345 * time.Millisecond),
		DocReadTime:     time.Duration(23456789 * time.Microsecond),
		StashLookupTime: time.Duration(200 * time.Millisecond),
	}
	c.Check(v1.String(), gc.Equals, `
PrunerStats(
     CacheLookupTime: 12.345
       DocLookupTime:  0.000
      DocCleanupTime:  0.000
         DocReadTime: 23.457
     StashLookupTime:  0.200
     StashRemoveTime:  0.000
         TxnReadTime:  0.000
       TxnRemoveTime:  0.000
        DocCacheHits: 0
      DocCacheMisses: 0
  DocMissingCacheHit: 0
         DocsMissing: 0
   CollectionQueries: 0
            DocReads: 0
     DocStillMissing: 0
        StashQueries: 0
       StashDocReads: 0
    StashDocsRemoved: 0
    DocQueuesCleaned: 0
    DocTokensCleaned: 0
    DocsAlreadyClean: 0
         TxnsRemoved: 0
      TxnsNotRemoved: 0
        ObjCacheHits: 0
      ObjCacheMisses: 0
)`[1:])
}

func (*PrunerStatsSuite) TestAlignedValues(c *gc.C) {
	v1 := PrunerStats{
		StashDocsRemoved: 1000,
		StashDocReads:    12345,
	}
	c.Check(v1.String(), gc.Equals, `
PrunerStats(
     CacheLookupTime: 0.000
       DocLookupTime: 0.000
      DocCleanupTime: 0.000
         DocReadTime: 0.000
     StashLookupTime: 0.000
     StashRemoveTime: 0.000
         TxnReadTime: 0.000
       TxnRemoveTime: 0.000
        DocCacheHits:     0
      DocCacheMisses:     0
  DocMissingCacheHit:     0
         DocsMissing:     0
   CollectionQueries:     0
            DocReads:     0
     DocStillMissing:     0
        StashQueries:     0
       StashDocReads: 12345
    StashDocsRemoved:  1000
    DocQueuesCleaned:     0
    DocTokensCleaned:     0
    DocsAlreadyClean:     0
         TxnsRemoved:     0
      TxnsNotRemoved:     0
        ObjCacheHits:     0
      ObjCacheMisses:     0
)`[1:])
}
