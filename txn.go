// Copyright 2014 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

// Package txn provides a Runner, which applies operations as part
// of a transaction onto any number of collections within a database.
// The execution of the operations is delegated to a mgo/txn/Runner.
// The purpose of the Runner is to execute the operations multiple
// times in there is a TxnAborted error, in the expectation that subsequent
// attempts will be successful.
// Also included is a mechanism whereby tests can use SetTestHooks to induce
// arbitrary state mutations before and after particular transactions.

package txn

import (
	stderrors "errors"
	"math/rand"
	"strings"
	"time"

	"github.com/juju/clock"
	"github.com/juju/loggo"
	"github.com/juju/mgo/v3"
	"github.com/juju/mgo/v3/bson"
	"github.com/juju/mgo/v3/sstxn"
	"github.com/juju/mgo/v3/txn"
)

var logger = loggo.GetLogger("juju.txn")

const (
	// defaultClientTxnRetries is the default number of times a transaction will be retried
	// when there is an invariant assertion failure (for client side transactions).
	defaultClientTxnRetries = 3

	// defaultServerTxnRetries is the default number of times a transaction will be retried
	// when there is an invariant assertion failure (for server side transactions).
	defaultServerTxnRetries = 50

	// defaultRetryBackoff is the default interval used to pause between
	// unsuccessful transaction operations.
	defaultRetryBackoff = 1 * time.Millisecond

	// defaultRetryFuzzPercent is the default fudge factor used when pausing between
	// unsuccessful transaction operations.
	defaultRetryFuzzPercent = 20

	// defaultTxnCollectionName is the default name of the collection used
	// to initialise the underlying mgo transaction runner.
	defaultTxnCollectionName = "txns"

	// defaultChangeLogName is the default mgo transaction runner change log.
	defaultChangeLogName = "txns.log"
)

var (
	// ErrExcessiveContention is used to signal that even after retrying, the transaction operations
	// could not be successfully applied due to database contention.
	ErrExcessiveContention = stderrors.New("state changing too quickly; try again soon")

	// ErrNoOperations is returned by TransactionSource implementations to signal that
	// no transaction operations are available to run.
	ErrNoOperations = stderrors.New("no transaction operations are available")

	// ErrTransientFailure is returned by TransactionSource implementations to signal that
	// the transaction list could not be built but the caller should retry.
	ErrTransientFailure = stderrors.New("transient failure")
)

// TransactionSource defines a function that can return transaction operations to run.
type TransactionSource func(attempt int) ([]txn.Op, error)

// PruneOptions controls when we will trigger a database prune.
type PruneOptions struct {

	// PruneFactor will trigger a prune when the current count of
	// transactions in the database is greater than old*PruneFactor
	PruneFactor float32

	// MinNewTransactions will skip a prune even if pruneFactor is true
	// if there are less than MinNewTransactions that might be cleaned up.
	MinNewTransactions int

	// MaxNewTransactions will force a prune if it sees more than
	// MaxNewTransactions since the last run.
	MaxNewTransactions int

	// MaxTime sets a threshold for 'completed' transactions. Transactions
	// will be considered completed only if they are both older than
	// MaxTime and have a status of Completed or Aborted. Passing the
	// zero Time will cause us to only filter on the Status field.
	MaxTime time.Time

	// MaxBatchTransactions is the most transactions that we will prune in a single pass.
	// It is possible to pass 0 to prune all transactions in a pass. Note
	// that MaybePruneTransactions will always process all transactions, it
	// is just whether we do so in multiple passes, or whether it is done
	// all at once.
	MaxBatchTransactions int

	// MaxBatches is the maximum number of passes we will attempt. 0 or
	// negative values are treated as do a single pass.
	MaxBatches int

	// SmallBatchTransactionCount is the number of transactions to read at a time.
	// A value of 1000 seems to be a good balance between how much time we spend
	// processing, and how many documents we evaluate at one time. (a value of
	// 100 empirically processes slower, and a value of 10,000 wasn't any faster)
	SmallBatchTransactionCount int

	// BatchTransactionSleepTime is an amount of time that we will sleep between
	// processing batches of transactions. This allows us to avoid excess load
	// on the system while pruning.
	BatchTransactionSleepTime time.Duration
}

// Runner instances applies operations to collections in a database.
type Runner interface {
	// RunTransaction applies the specified transaction operations to a database.
	RunTransaction(*Transaction) error

	// Run calls the nominated function to get the transaction operations to apply to a database.
	// If there is a failure due to a txn.ErrAborted error, the attempt is retried up to nrRetries times.
	Run(transactions TransactionSource) error

	// ResumeTransactions resumes all pending transactions.
	ResumeTransactions() error

	// MaybePruneTransactions removes data for completed transactions
	// from mgo/txn's transaction collection. It is intended to be
	// called periodically.
	//
	// Pruning is an I/O heavy activity so it will only be undertaken
	// if:
	//
	//   txn_count >= pruneFactor * txn_count_at_last_prune
	//
	MaybePruneTransactions(pruneOpts PruneOptions) error
}

type txnRunner interface {
	Run([]txn.Op, bson.ObjectId, interface{}) error
	ChangeLog(*mgo.Collection)
	ResumeAll() error
}

// Clock is a simplified form of juju/clock.Clock, since we don't need all the methods
// and this allows us to be compatible with both juju/clock.Clock and juju/utils/clock.Clock
type Clock interface {
	// Now returns the current clock time.
	Now() time.Time
}

type transactionRunner struct {
	db                        *mgo.Database
	transactionCollectionName string
	changeLogName             string
	testHooks                 chan ([]TestHook)
	runTransactionObserver    func(Transaction)
	clock                     Clock

	serverSideTransactions bool
	nrRetries              int
	retryBackoff           time.Duration
	retryFuzzPercent       int
	pauseFunc              func(duration time.Duration)

	newRunner func() txnRunner
}

var _ Runner = (*transactionRunner)(nil)

// Transaction is a struct that is passed to RunTransactionObserver whenever a
// transaction is run.
type Transaction struct {
	// Ops is the operations that were performed
	Ops []txn.Op
	// Error is the error returned from running the operation, might be nil
	Error error
	// Duration is length of time it took to run the operation
	Duration time.Duration
	// Attempt is the current attempt to apply the operation.
	Attempt int
}

// RunnerParams are used to construct a new transaction runner.
// Only the Database value is mandatory, defaults will be used for
// the other attributes if not specified.
type RunnerParams struct {
	// Database is the mgo database for which the transaction runner will be used.
	Database *mgo.Database

	// TransactionCollectionName is the name of the collection
	// used to initialise the underlying mgo transaction runner,
	// defaults to "txns" if unspecified.
	TransactionCollectionName string

	// ChangeLogName is the mgo transaction runner change log,
	// defaults to "txns.log" if unspecified.
	ChangeLogName string

	// RunTransactionObserver, if non-nil, will be called when
	// a Run or RunTransaction call has completed. It will be
	// passed the txn.Ops and the error result.
	RunTransactionObserver func(Transaction)

	// Clock is an optional clock to use. If Clock is nil, clock.WallClock will
	// be used.
	Clock Clock

	// ServerSideTransactions indicates that if SSTXNs are available, use them.
	// Note that we will check if they are supported server side, and fall
	// back to client-side transactions if they are not supported.
	ServerSideTransactions bool

	// MaxRetryAttempts is the number of times a transaction will be retried
	// when there is an invariant assertion failure.
	MaxRetryAttempts int

	// RetryBackoff is the interval used to pause between
	// unsuccessful transaction operations.
	RetryBackoff time.Duration

	// RetryFuzzPercent is the fudge factor used when pausing between
	// unsuccessful transaction operations.
	RetryFuzzPercent int

	// PauseFunc, if non-nil, overrides the default function to sleep
	// for the specified duration.
	PauseFunc func(duration time.Duration)
}

// NewRunner returns a Runner which runs transactions for the database specified in params.
// Collection names used to manage the transactions and change log may also be specified in
// params, but if not, default values will be used.
func NewRunner(params RunnerParams) Runner {
	sstxn := params.ServerSideTransactions
	if sstxn {
		sstxn = SupportsServerSideTransactions(params.Database)
		if !sstxn {
			logger.Warningf("server-side transactions requested, but database does not support them")
		}
	}
	txnRunner := &transactionRunner{
		db:                        params.Database,
		transactionCollectionName: params.TransactionCollectionName,
		changeLogName:             params.ChangeLogName,
		runTransactionObserver:    params.RunTransactionObserver,
		clock:                     params.Clock,
		serverSideTransactions:    sstxn,
		nrRetries:                 params.MaxRetryAttempts,
		retryBackoff:              params.RetryBackoff,
		retryFuzzPercent:          params.RetryFuzzPercent,
		pauseFunc:                 params.PauseFunc,
	}
	if txnRunner.transactionCollectionName == "" {
		txnRunner.transactionCollectionName = defaultTxnCollectionName
	}
	if txnRunner.changeLogName == "-" {
		txnRunner.changeLogName = ""
	} else if txnRunner.changeLogName == "" {
		txnRunner.changeLogName = defaultChangeLogName
	}
	if txnRunner.nrRetries == 0 {
		txnRunner.nrRetries = defaultClientTxnRetries
		if txnRunner.serverSideTransactions {
			txnRunner.nrRetries = defaultServerTxnRetries
		}
	}
	if txnRunner.retryBackoff == 0 {
		txnRunner.retryBackoff = defaultRetryBackoff
	}
	if txnRunner.retryFuzzPercent == 0 {
		txnRunner.retryFuzzPercent = defaultRetryFuzzPercent
	}
	if txnRunner.pauseFunc == nil {
		txnRunner.pauseFunc = txnRunner.pause
	}
	txnRunner.testHooks = make(chan ([]TestHook), 1)
	txnRunner.testHooks <- nil
	txnRunner.newRunner = txnRunner.newRunnerImpl
	if txnRunner.clock == nil {
		// We allow callers to pass in a nil clock because it is only used if
		// they also specify a RunTransactionObserver.
		txnRunner.clock = clock.WallClock
	}
	return txnRunner
}

func (tr *transactionRunner) newRunnerImpl() txnRunner {
	db := tr.db
	var runner txnRunner
	if tr.serverSideTransactions {
		runner = sstxn.NewRunner(db, logger)
	} else {
		runner = txn.NewRunner(db.C(tr.transactionCollectionName))
	}
	if tr.changeLogName != "" {
		runner.ChangeLog(db.C(tr.changeLogName))
	}
	return runner
}

// Run is defined on Runner.
func (tr *transactionRunner) Run(transactions TransactionSource) error {
	var lastErr error
	for i := 0; i < tr.nrRetries; i++ {
		// If we are retrying, give other txns a chance to have a go.
		if i > 0 && tr.serverSideTransactions {
			tr.backoff(i)
		}
		ops, err := transactions(i)
		if err == ErrTransientFailure {
			continue
		}
		if err == ErrNoOperations {
			return nil
		}
		if err != nil {
			return err
		}
		if len(ops) == 0 {
			// Treat this the same as ErrNoOperations but don't suppress other errors.
			return nil
		}
		if err = tr.RunTransaction(&Transaction{
			Ops:     ops,
			Attempt: i,
		}); err == nil {
			return nil
		} else if err != txn.ErrAborted && !mgo.IsRetryable(err) && !mgo.IsSnapshotError(err) {
			// Mongo very occasionally returns an intermittent
			// "unexpected message" error. Retry those.
			// Also mongo sometimes gets very busy and we get an
			// i/o timeout. We retry those too.
			// However if this is the last time, return that error
			// rather than the excessive contention error.
			msg := err.Error()
			retryErr := strings.HasSuffix(msg, "unexpected message") ||
				strings.HasSuffix(msg, "i/o timeout")
			if !retryErr || i == (tr.nrRetries-1) {
				return err
			}
		}
		lastErr = err
	}
	if lastErr == txn.ErrAborted {
		return ErrExcessiveContention
	}
	return lastErr
}

func (tr *transactionRunner) backoff(attempt int) {
	// Backoff a little longer each failed attempt and throw in
	// a bit of fuzz for good measure.
	dur := tr.retryBackoff * time.Duration(attempt)

	// fuzzFactor is between -1.0 and 1.0 * fuzzPercent
	fuzzFactor := 2.0 * (0.5 - rand.Float32()) * float32(tr.retryFuzzPercent) / 100.0

	// Include a random amount of time as well.
	dur += time.Duration(fuzzFactor * float32(tr.retryBackoff))

	tr.pauseFunc(dur)
}

func (tr *transactionRunner) pause(dur time.Duration) {
	time.Sleep(dur)
}

// RunTransaction is defined on Runner.
func (tr *transactionRunner) RunTransaction(transaction *Transaction) error {
	testHooks := <-tr.testHooks
	tr.testHooks <- nil
	if len(testHooks) > 0 {
		// Note that this code should only ever be triggered
		// during tests. If we see the log messages below
		// in a production run, something is wrong.
		defer func() {
			if testHooks[0].After != nil {
				logger.Infof("transaction 'after' hook start")
				testHooks[0].After()
				logger.Infof("transaction 'after' hook end")
			}
			if <-tr.testHooks != nil {
				panic("concurrent use of transaction hooks")
			}
			tr.testHooks <- testHooks[1:]
		}()
		if testHooks[0].Before != nil {
			logger.Infof("transaction 'before' hook start")
			testHooks[0].Before()
			logger.Infof("transaction 'before' hook end")
		}
	}
	start := tr.clock.Now()
	runner := tr.newRunner()
	err := runner.Run(transaction.Ops, "", nil)
	if tr.runTransactionObserver != nil {
		transaction.Error = err
		transaction.Duration = tr.clock.Now().Sub(start)
		tr.runTransactionObserver(*transaction)
	}
	return err
}

// ResumeTransactions is defined on Runner.
func (tr *transactionRunner) ResumeTransactions() error {
	runner := tr.newRunner()
	return runner.ResumeAll()
}

// MaybePruneTransactions is defined on Runner.
func (tr *transactionRunner) MaybePruneTransactions(pruneOpts PruneOptions) error {
	return maybePrune(tr.db, tr.transactionCollectionName, pruneOpts)
}

// TestHook holds a pair of functions to be called before and after a
// mgo/txn transaction is run.
// Exported only for testing.
type TestHook struct {
	Before func()
	After  func()
}

// TestHooks returns the test hooks for a transaction runner.
// Exported only for testing.
func TestHooks(runner Runner) chan ([]TestHook) {
	return runner.(*transactionRunner).testHooks
}

// SupportsServerSideTransactions lets you know if the given database can support
// server-side transactions.
func SupportsServerSideTransactions(db *mgo.Database) bool {
	info, err := db.Session.BuildInfo()
	if err != nil {
		return false
	}
	if len(info.VersionArray) < 1 || info.VersionArray[0] < 4 {
		return false
	}
	return true
}
