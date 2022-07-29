// Copyright 2014 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package txn_test

import (
	"errors"
	"fmt"
	"time"

	"github.com/juju/clock/testclock"
	"github.com/juju/mgo/v3"
	"github.com/juju/mgo/v3/bson"
	mgotesting "github.com/juju/mgo/v3/testing"
	"github.com/juju/mgo/v3/txn"
	"github.com/juju/testing"
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"

	jujutxn "github.com/juju/txn/v3"
	txntesting "github.com/juju/txn/v3/testing"
)

var _ = gc.Suite(&txnSuite{})

type txnSuite struct {
	testing.IsolationSuite
	mgotesting.MgoSuite
	collection  *mgo.Collection
	txnRunner   jujutxn.Runner
	supportsSST bool
	backoffs    []time.Duration
}

func (s *txnSuite) SetUpSuite(c *gc.C) {
	s.IsolationSuite.SetUpSuite(c)
	s.MgoSuite.SetUpSuite(c)
}

func (s *txnSuite) TearDownSuite(c *gc.C) {
	s.MgoSuite.TearDownSuite(c)
	s.IsolationSuite.TearDownSuite(c)
}

func (s *txnSuite) SetUpTest(c *gc.C) {
	s.IsolationSuite.SetUpTest(c)
	s.MgoSuite.SetUpTest(c)
	db := s.Session.DB("juju")
	s.collection = db.C("test")
	s.collection.Create(&mgo.CollectionInfo{})
	txnsLog := db.C("txns.log")
	txnsLog.Create(&mgo.CollectionInfo{})
	s.backoffs = nil
	s.txnRunner = jujutxn.NewRunner(jujutxn.RunnerParams{
		Database:               db,
		ChangeLogName:          "txns.log",
		ServerSideTransactions: false,
		PauseFunc: func(dur time.Duration) {
			s.backoffs = append(s.backoffs, dur)
		},
	})
	s.supportsSST = false
}

func (s *txnSuite) TearDownTest(c *gc.C) {
	s.MgoSuite.TearDownTest(c)
	s.IsolationSuite.TearDownTest(c)
}

var _ = gc.Suite(&sstxnSuite{})

type sstxnSuite struct {
	txnSuite
	origReplicaSet bool
}

func (s *sstxnSuite) SetUpSuite(c *gc.C) {
	// Check to see if we can even use server-side transactions,
	// check first so we don't restart the server just to say it
	// doesn't support server-side transactions.
	if mgotesting.MgoServer.Addr() != "" {
		// The existing server is running, check its version
		s.CheckSSTXNSupported(c)
	}
	// Make sure MgoServer is set up with replicaset enabled
	s.origReplicaSet = mgotesting.MgoServer.EnableReplicaSet
	if !s.origReplicaSet {
		mgotesting.MgoServer.EnableReplicaSet = true
		c.Logf("restarting Mongo with replicaset enabled")
		mgotesting.MgoServer.Restart()
		s.CheckSSTXNSupported(c)
	}
	s.txnSuite.SetUpSuite(c)
}

func (s *sstxnSuite) CheckSSTXNSupported(c *gc.C) {
	info := mgotesting.MgoServer.DialInfo()
	session, err := mgo.DialWithInfo(info)
	c.Assert(err, gc.IsNil)
	defer session.Close()
	db := session.DB(info.Database)
	if !jujutxn.SupportsServerSideTransactions(db) {
		c.Skip(fmt.Sprintf("mongo version doesn't support server-side-transactions"))
	}
}

func (s *sstxnSuite) TearDownSuite(c *gc.C) {
	s.MgoSuite.TearDownSuite(c)
	s.IsolationSuite.TearDownSuite(c)
	if s.origReplicaSet != mgotesting.MgoServer.EnableReplicaSet {
		mgotesting.MgoServer.EnableReplicaSet = s.origReplicaSet
		mgotesting.MgoServer.Restart()
	}
}

func (s *sstxnSuite) SetUpTest(c *gc.C) {
	s.txnSuite.SetUpTest(c)
	s.txnRunner = jujutxn.NewRunner(jujutxn.RunnerParams{
		Database:               s.collection.Database,
		ChangeLogName:          "txns.log",
		ServerSideTransactions: true,
		PauseFunc: func(dur time.Duration) {
			s.backoffs = append(s.backoffs, dur)
		},
	})
	s.supportsSST = true
}

func (s *sstxnSuite) TestNoChangeLog(c *gc.C) {
	s.txnRunner = jujutxn.NewRunner(jujutxn.RunnerParams{
		Database:               s.collection.Database,
		ChangeLogName:          "-",
		ServerSideTransactions: true,
		PauseFunc: func(dur time.Duration) {
			s.backoffs = append(s.backoffs, dur)
		},
	})

	before, err := s.collection.Database.C("txns.log").Count()
	c.Assert(err, gc.IsNil)

	doc := simpleDoc{"1", "Foo"}
	ops := []txn.Op{{
		C:      s.collection.Name,
		Id:     doc.Id,
		Assert: txn.DocMissing,
		Insert: doc,
	}}
	err = s.txnRunner.RunTransaction(&jujutxn.Transaction{
		Ops:     ops,
		Attempt: 0,
	})
	c.Assert(err, gc.IsNil)
	var found simpleDoc
	err = s.collection.FindId("1").One(&found)
	c.Assert(err, gc.IsNil)
	c.Assert(found, gc.DeepEquals, doc)

	after, err := s.collection.Database.C("txns.log").Count()
	c.Assert(err, gc.IsNil)
	c.Assert(after, gc.Equals, before, gc.Commentf("txns.log was altered"))
}

type simpleDoc struct {
	Id   string `bson:"_id"`
	Name string
}

func (s *txnSuite) TestRunTransaction(c *gc.C) {
	doc := simpleDoc{"1", "Foo"}
	ops := []txn.Op{{
		C:      s.collection.Name,
		Id:     doc.Id,
		Assert: txn.DocMissing,
		Insert: doc,
	}}
	err := s.txnRunner.RunTransaction(&jujutxn.Transaction{
		Ops:     ops,
		Attempt: 0,
	})
	c.Assert(err, gc.IsNil)
	var found simpleDoc
	err = s.collection.FindId("1").One(&found)
	c.Assert(err, gc.IsNil)
	c.Assert(found, gc.DeepEquals, doc)
}

func (s *txnSuite) TestRun(c *gc.C) {
	doc := simpleDoc{"1", "Foo"}
	maxAttempt := 0
	buildTxn := func(attempt int) ([]txn.Op, error) {
		maxAttempt = attempt
		ops := []txn.Op{{
			C:      s.collection.Name,
			Id:     doc.Id,
			Assert: txn.DocMissing,
			Insert: doc,
		}}
		return ops, nil
	}
	err := s.txnRunner.Run(buildTxn)
	c.Assert(err, gc.IsNil)
	var found simpleDoc
	err = s.collection.FindId("1").One(&found)
	c.Assert(err, gc.IsNil)
	c.Assert(maxAttempt, gc.Equals, 0)
	c.Assert(found, gc.DeepEquals, doc)
}

func (s *txnSuite) setDocName(c *gc.C, id, name string) {
	ops := []txn.Op{{
		C:      s.collection.Name,
		Id:     id,
		Assert: txn.DocExists,
		Update: bson.D{{"$set", bson.D{{"name", name}}}},
	}}
	err := s.txnRunner.RunTransaction(&jujutxn.Transaction{
		Ops:     ops,
		Attempt: 0,
	})
	c.Assert(err, gc.IsNil)
}

func (s *txnSuite) insertDoc(c *gc.C, id, name string) {
	doc := simpleDoc{id, name}
	ops := []txn.Op{{
		C:      s.collection.Name,
		Id:     id,
		Assert: txn.DocMissing,
		Insert: doc,
	}}
	err := s.txnRunner.RunTransaction(&jujutxn.Transaction{
		Ops:     ops,
		Attempt: 0,
	})
	c.Assert(err, gc.IsNil)
}

func (s *txnSuite) TestBeforeHooks(c *gc.C) {
	s.insertDoc(c, "1", "Simple")
	changeFuncs := []func(){
		func() { s.setDocName(c, "1", "FooBar") },
		func() { s.setDocName(c, "1", "Foo") },
	}
	defer txntesting.SetBeforeHooks(c, s.txnRunner, changeFuncs...).Check()
	maxAttempt := 0
	buildTxn := func(attempt int) ([]txn.Op, error) {
		maxAttempt = attempt
		ops := []txn.Op{{
			C:      s.collection.Name,
			Id:     "1",
			Assert: bson.D{{"name", "Foo"}},
			Update: bson.D{{"$set", bson.D{{"name", "Bar"}}}},
		}}
		return ops, nil
	}
	err := s.txnRunner.Run(buildTxn)
	c.Assert(err, gc.IsNil)
	var found simpleDoc
	err = s.collection.FindId("1").One(&found)
	c.Assert(err, gc.IsNil)
	c.Assert(maxAttempt, gc.Equals, 1)
	doc := simpleDoc{"1", "Bar"}
	c.Assert(found, gc.DeepEquals, doc)
}

func (s *sstxnSuite) TestStartedHooks(c *gc.C) {
	s.insertDoc(c, "1", "Simple")

	secondSession := s.collection.Database.Session.Copy()
	defer secondSession.Close()
	secondRunner := jujutxn.NewRunner(jujutxn.RunnerParams{
		Database:               secondSession.DB("juju"),
		ChangeLogName:          "txns.log",
		ServerSideTransactions: true,
		PauseFunc: func(dur time.Duration) {
			s.backoffs = append(s.backoffs, dur)
		},
	})

	setDocName := func(id, name string) {
		ops := []txn.Op{{
			C:      s.collection.Name,
			Id:     id,
			Assert: txn.DocExists,
			Update: bson.D{{"$set", bson.D{{"name", name}}}},
		}}
		err := secondRunner.RunTransaction(&jujutxn.Transaction{
			Ops:     ops,
			Attempt: 0,
		})
		c.Assert(err, gc.IsNil)
	}

	hooks := []jujutxn.TestHook{
		{Started: func() { setDocName("1", "FooBar") }},
		{Started: func() { setDocName("1", "Foo") }},
	}
	defer txntesting.SetTestHooks(c, s.txnRunner, hooks...).Check()
	maxAttempt := 0
	buildTxn := func(attempt int) ([]txn.Op, error) {
		maxAttempt = attempt
		ops := []txn.Op{{
			C:      s.collection.Name,
			Id:     "1",
			Assert: bson.D{{"name", "Foo"}},
			Update: bson.D{{"$set", bson.D{{"name", "Bar"}}}},
		}}
		return ops, nil
	}
	err := s.txnRunner.Run(buildTxn)
	c.Assert(err, gc.IsNil)
	var found simpleDoc
	err = s.collection.FindId("1").One(&found)
	c.Assert(err, gc.IsNil)
	c.Assert(maxAttempt, gc.Equals, 1)
	doc := simpleDoc{"1", "Bar"}
	c.Assert(found, gc.DeepEquals, doc)
}

func (s *sstxnSuite) TestAssertedHooks(c *gc.C) {
	s.insertDoc(c, "1", "Foo")

	secondSession := s.collection.Database.Session.Copy()
	defer secondSession.Close()
	secondRunner := jujutxn.NewRunner(jujutxn.RunnerParams{
		Database:               secondSession.DB("juju"),
		ChangeLogName:          "txns.log",
		ServerSideTransactions: true,
		PauseFunc: func(dur time.Duration) {
			s.backoffs = append(s.backoffs, dur)
		},
	})

	setDocName := func(id, name string) {
		ops := []txn.Op{{
			C:      s.collection.Name,
			Id:     id,
			Assert: txn.DocExists,
			Update: bson.D{{"$set", bson.D{{"name", name}}}},
		}}
		err := secondRunner.RunTransaction(&jujutxn.Transaction{
			Ops:     ops,
			Attempt: 0,
		})
		c.Assert(err, gc.IsNil)
	}

	asserted := false
	hooks := []jujutxn.TestHook{
		{Asserted: func() {
			asserted = true
			setDocName("1", "FooBar")
		}},
		{Started: func() { setDocName("1", "Foo") }},
	}
	defer txntesting.SetTestHooks(c, s.txnRunner, hooks...).Check()
	maxAttempt := 0
	buildTxn := func(attempt int) ([]txn.Op, error) {
		maxAttempt = attempt
		ops := []txn.Op{{
			C:      s.collection.Name,
			Id:     "1",
			Assert: bson.D{{"name", "Foo"}},
			Update: bson.D{{"$set", bson.D{{"name", "Bar"}}}},
		}}
		return ops, nil
	}
	err := s.txnRunner.Run(buildTxn)
	c.Assert(err, gc.IsNil)
	c.Assert(asserted, jc.IsTrue)
	var found simpleDoc
	err = s.collection.FindId("1").One(&found)
	c.Assert(err, gc.IsNil)
	c.Assert(maxAttempt, gc.Equals, 1)
	doc := simpleDoc{"1", "Bar"}
	c.Assert(found, gc.DeepEquals, doc)
}

func (s *txnSuite) TestAfterHooks(c *gc.C) {
	changeFuncs := []func(){
		func() { s.insertDoc(c, "1", "Foo") },
	}
	defer txntesting.SetAfterHooks(c, s.txnRunner, changeFuncs...).Check()
	maxAttempt := 0
	buildTxn := func(attempt int) ([]txn.Op, error) {
		maxAttempt = attempt
		ops := []txn.Op{{
			C:      s.collection.Name,
			Id:     "1",
			Assert: bson.D{{"name", "Foo"}},
			Update: bson.D{{"$set", bson.D{{"name", "Bar"}}}},
		}}
		return ops, nil
	}
	err := s.txnRunner.Run(buildTxn)
	c.Assert(err, gc.IsNil)
	var found simpleDoc
	err = s.collection.FindId("1").One(&found)
	c.Assert(err, gc.IsNil)
	c.Assert(maxAttempt, gc.Equals, 1)
	doc := simpleDoc{"1", "Bar"}
	c.Assert(found, gc.DeepEquals, doc)
}

func (s *txnSuite) TestRetryHooks(c *gc.C) {
	s.insertDoc(c, "1", "Foo")
	defer txntesting.SetRetryHooks(c, s.txnRunner, func() {
		s.setDocName(c, "1", "Bar")
	}, func() {
		s.setDocName(c, "1", "Foo")
	}).Check()

	maxAttempt := 0
	buildTxn := func(attempt int) ([]txn.Op, error) {
		maxAttempt = attempt
		ops := []txn.Op{{
			C:      s.collection.Name,
			Id:     "1",
			Assert: bson.D{{"name", "Foo"}},
			Update: bson.D{{"$set", bson.D{{"name", "FooBar"}}}},
		}}
		return ops, nil
	}
	err := s.txnRunner.Run(buildTxn)
	c.Assert(err, gc.IsNil)
	c.Assert(maxAttempt, gc.Equals, 2)
	var found simpleDoc
	err = s.collection.FindId("1").One(&found)
	c.Assert(err, gc.IsNil)
	doc := simpleDoc{"1", "FooBar"}
	c.Assert(found, gc.DeepEquals, doc)
}

func (s *txnSuite) TestExcessiveContention(c *gc.C) {
	maxAttempt := 0
	// This keeps failing because the Assert is wrong.
	buildTxn := func(attempt int) ([]txn.Op, error) {
		maxAttempt = attempt
		ops := []txn.Op{{
			C:      s.collection.Name,
			Id:     "1",
			Assert: bson.D{{"name", "Foo"}},
			Update: bson.D{{"$set", bson.D{{"name", "Bar"}}}},
		}}
		return ops, nil
	}
	err := s.txnRunner.Run(buildTxn)
	c.Assert(err, gc.Equals, jujutxn.ErrExcessiveContention)
	if s.supportsSST {
		c.Assert(maxAttempt, gc.Equals, 49)
	} else {
		c.Assert(maxAttempt, gc.Equals, 2)
	}
}

func (s *txnSuite) TestPause(c *gc.C) {
	buildTxn := func(attempt int) ([]txn.Op, error) {
		ops := []txn.Op{{
			C:      s.collection.Name,
			Id:     "1",
			Assert: bson.D{{"name", "Foo"}},
			Update: bson.D{{"$set", bson.D{{"name", "Bar"}}}},
		}}
		return ops, nil
	}
	err := s.txnRunner.Run(buildTxn)
	c.Assert(err, gc.Equals, jujutxn.ErrExcessiveContention)
	if s.supportsSST {
		c.Assert(s.backoffs, gc.HasLen, 49)
		c.Assert(s.backoffs[48], jc.DurationLessThan, 50*time.Millisecond)
		for i := 0; i < len(s.backoffs); i++ {
			c.Assert(s.backoffs[i], jc.GreaterThan, 0)
			if i > 0 {
				c.Assert(s.backoffs[i-1], jc.DurationLessThan, s.backoffs[i])
			}
		}
	} else {
		c.Assert(s.backoffs, gc.HasLen, 0)
	}
}

func (s *txnSuite) TestSetFailIfTransactionNoTxn(c *gc.C) {
	checker := txntesting.SetFailIfTransaction(c, s.txnRunner)
	// Check should succeed if there is no transaction
	checker.Check()
}

func (s *txnSuite) TestSetFailIfTransaction(c *gc.C) {
	checker := txntesting.SetFailIfTransaction(c, s.txnRunner)
	buildTxn := func(attempt int) ([]txn.Op, error) {
		ops := []txn.Op{{
			C:      s.collection.Name,
			Id:     "1",
			Assert: txn.DocMissing,
			Insert: bson.D{{"name", "Bar"}},
		}}
		return ops, nil
	}
	err := s.txnRunner.Run(buildTxn)
	c.Check(err, gc.IsNil)
	// The test should currently be flagged failing. We use Assert because
	// we have to abort the test now for it to fail the test suite.
	c.Assert(c.Failed(), gc.Equals, true)
	// Reset the failed state
	c.Succeed()
	// checker.Check() should also fail the test suite
	checker.Check()
	failed := c.Failed()
	c.Succeed()
	c.Assert(failed, gc.Equals, true)
}

func (s *txnSuite) TestNothingToDo(c *gc.C) {
	maxAttempt := 0
	buildTxn := func(attempt int) ([]txn.Op, error) {
		maxAttempt = attempt
		return nil, jujutxn.ErrNoOperations
	}
	err := s.txnRunner.Run(buildTxn)
	c.Assert(err, gc.Equals, nil)
	c.Assert(maxAttempt, gc.Equals, 0)
}

func (s *txnSuite) TestErrorReturned(c *gc.C) {
	maxAttempt := 0
	realErr := errors.New("this is my error")
	buildTxn := func(attempt int) ([]txn.Op, error) {
		maxAttempt = attempt
		return nil, realErr
	}
	err := s.txnRunner.Run(buildTxn)
	c.Assert(err, gc.Equals, realErr)
	c.Assert(maxAttempt, gc.Equals, 0)
}

func (s *txnSuite) TestNoOperationsNoErrors(c *gc.C) {
	maxAttempt := 0
	buildTxn := func(attempt int) ([]txn.Op, error) {
		maxAttempt = attempt
		return []txn.Op{}, nil
	}
	err := s.txnRunner.Run(buildTxn)
	c.Assert(err, gc.IsNil)
	c.Assert(maxAttempt, gc.Equals, 0)
}

func (s *txnSuite) TestTransientFailure(c *gc.C) {
	s.insertDoc(c, "1", "Foo")
	maxAttempt := 0
	buildTxn := func(attempt int) ([]txn.Op, error) {
		maxAttempt = attempt
		if attempt == 0 {
			return nil, jujutxn.ErrTransientFailure
		}
		ops := []txn.Op{{
			C:      s.collection.Name,
			Id:     "1",
			Assert: bson.D{{"name", "Foo"}},
			Update: bson.D{{"$set", bson.D{{"name", "Bar"}}}},
		}}
		return ops, nil
	}
	err := s.txnRunner.Run(buildTxn)
	c.Assert(err, gc.Equals, nil)
	c.Assert(maxAttempt, gc.Equals, 1)
	doc := simpleDoc{"1", "Bar"}
	var found simpleDoc
	err = s.collection.FindId("1").One(&found)
	c.Assert(found, gc.DeepEquals, doc)
}

func (s *txnSuite) TestRunFailureIntermittentUnexpectedMessage(c *gc.C) {
	runner := jujutxn.NewRunner(jujutxn.RunnerParams{})
	fake := &fakeRunner{errors: []error{errors.New("unexpected message")}}
	jujutxn.SetRunnerFunc(runner, fake.new)
	tries := 0
	// Doesn't matter what this returns as long as it isn't an error.
	buildTxn := func(attempt int) ([]txn.Op, error) {
		tries++
		// return 1 op that happens to do nothing
		return []txn.Op{{}}, nil
	}
	err := runner.Run(buildTxn)
	c.Check(err, gc.Equals, nil)
	c.Check(tries, gc.Equals, 2)
}

func (s *txnSuite) TestRunFailureAlwaysUnexpectedMessage(c *gc.C) {
	runner := jujutxn.NewRunner(jujutxn.RunnerParams{})
	fake := &fakeRunner{errors: []error{
		errors.New("unexpected message"),
		errors.New("unexpected message"),
		errors.New("unexpected message"),
		errors.New("unexpected message"),
	}}
	jujutxn.SetRunnerFunc(runner, fake.new)
	tries := 0
	// Doesn't matter what this returns as long as it isn't an error.
	buildTxn := func(attempt int) ([]txn.Op, error) {
		tries++
		// return 1 op that happens to do nothing
		return []txn.Op{{}}, nil
	}
	err := runner.Run(buildTxn)
	c.Check(err, gc.ErrorMatches, "unexpected message")
	c.Check(tries, gc.Equals, 3)
}

func (s *txnSuite) TestRunFailureIOTimeout(c *gc.C) {
	runner := jujutxn.NewRunner(jujutxn.RunnerParams{})
	fake := &fakeRunner{errors: []error{errors.New("i/o timeout")}}
	jujutxn.SetRunnerFunc(runner, fake.new)
	tries := 0
	// Doesn't matter what this returns as long as it isn't an error.
	buildTxn := func(attempt int) ([]txn.Op, error) {
		tries++
		// return 1 op that happens to do nothing
		return []txn.Op{{}}, nil
	}
	err := runner.Run(buildTxn)
	c.Check(err, gc.Equals, nil)
	c.Check(tries, gc.Equals, 2)
}

func (s *txnSuite) TestRunFailureAlwaysIOTimeout(c *gc.C) {
	runner := jujutxn.NewRunner(jujutxn.RunnerParams{})
	fake := &fakeRunner{errors: []error{
		errors.New("i/o timeout"),
		errors.New("i/o timeout"),
		errors.New("i/o timeout"),
		errors.New("i/o timeout"),
	}}
	jujutxn.SetRunnerFunc(runner, fake.new)
	tries := 0
	// Doesn't matter what this returns as long as it isn't an error.
	buildTxn := func(attempt int) ([]txn.Op, error) {
		tries++
		// return 1 op that happens to do nothing
		return []txn.Op{{}}, nil
	}
	err := runner.Run(buildTxn)
	c.Check(err, gc.ErrorMatches, "i/o timeout")
	c.Check(tries, gc.Equals, 3)
}

func (s *txnSuite) TestRunTransactionObserver(c *gc.C) {
	var calls []jujutxn.Transaction
	clock := testclock.NewClock(time.Now())
	runner := jujutxn.NewRunner(jujutxn.RunnerParams{
		RunTransactionObserver: func(txn jujutxn.Transaction) {
			calls = append(calls, txn)
		},
		Clock: clock,
	})
	fake := &fakeRunner{
		errors: []error{
			txn.ErrAborted,
			nil,
		},
		durations: []time.Duration{
			time.Second,
			100 * time.Millisecond,
		},
		clock: clock,
	}
	jujutxn.SetRunnerFunc(runner, fake.new)
	ops := []txn.Op{{
		C:      "testColl",
		Id:     "testId",
		Assert: bson.D{{"attr", "value"}},
		Update: bson.M{"$set": bson.M{"attr": "newvalue"}},
	}}
	buildTxn := func(attempt int) ([]txn.Op, error) {
		return ops, nil
	}
	err := runner.Run(buildTxn)
	c.Check(err, gc.IsNil)
	c.Check(calls, gc.HasLen, 2)
	c.Check(calls[0].Ops, gc.DeepEquals, ops)
	c.Check(calls[0].Error, gc.Equals, txn.ErrAborted)
	c.Check(calls[0].Duration, gc.Equals, time.Second)
	c.Check(calls[0].Attempt, gc.Equals, 0)
	c.Check(calls[1].Ops, gc.DeepEquals, ops)
	c.Check(calls[1].Error, gc.IsNil)
	c.Check(calls[1].Duration, gc.Equals, 100*time.Millisecond)
	c.Check(calls[1].Attempt, gc.Equals, 1)
}

type fakeRunner struct {
	jujutxn.TxnRunner
	errors    []error
	durations []time.Duration
	clock     *testclock.Clock
}

// Since a new transaction runner is created each time the code
// is retried, we want to have a single source of errors, so make the
// fake a factory that returns itself.
func (f *fakeRunner) new() jujutxn.TxnRunner {
	return f
}

func (f *fakeRunner) Run([]txn.Op, bson.ObjectId, interface{}) error {
	if len(f.durations) > 0 && f.clock != nil {
		f.clock.Advance(f.durations[0])
		f.durations = f.durations[1:]
	}
	if len(f.errors) == 0 {
		return nil
	}
	err := f.errors[0]
	f.errors = f.errors[1:]
	return err
}
