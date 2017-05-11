// Copyright 2015 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package txn

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	// Transaction states copied from mgo/txn.
	taborted = 5 // Pre-conditions failed, nothing done
	tapplied = 6 // All changes applied

	// maxBatchDocs defines the maximum MongoDB batch size (in number of documents).
	maxBatchDocs = 1616

	// defaultPruneFactor will be used if users don't request a pruneFactor
	defaultPruneFactor = 2.0

	// defaultMinNewTransactions will avoid pruning if there are only a
	// small number of documents to prune. This is set because if a
	// database can get to 0 txns, then any pruneFactor will always say
	// that we should prune.
	defaultMinNewTransactions = 100

	// defaultMaxNewTransactions will trigger a prune if we see more than
	// this many new transactions, even if pruneFactor hasn't been satisfied
	defaultMaxNewTransactions = 100000

	// maxBulkOps defines the maximum number of operations in a bulk
	// operation.
	maxBulkOps = 1000

	// logInterval defines often to report progress during long
	// operations.
	logInterval = 15 * time.Second

	// maxIterCount is the number of times we will pass over the data to
	// make sure all documents are cleaned up. (removing from a
	// collection you are iterating can cause you to miss entries).
	// The loop should exit early if it finds nothing to do anyway, so
	// this only affects the number of times we will evaluate documents
	// we aren't removing.
	maxIterCount = 5

	// maxMemoryTokens caps our in-memory cache. When it is full, we will
	// apply our current list of items to process, and then flag the loop
	// to run again. At 100k the maximum memory was around 200MB.
	maxMemoryTokens = 50000

	// queueBatchSize is the number of documents we will load before
	// evaluating their transaction queues. This was found to be
	// reasonably optimal when querying mongo.
	queueBatchSize = 200
)

type pruneStats struct {
	Id              bson.ObjectId `bson:"_id"`
	Started         time.Time     `bson:"started"`
	Completed       time.Time     `bson:"completed"`
	TxnsBefore      int           `bson:"txns-before"`
	TxnsAfter       int           `bson:"txns-after"`
	StashDocsBefore int           `bson:"stash-docs-before"`
	StashDocsAfter  int           `bson:"stash-docs-after"`
}

func validatePruneOptions(pruneOptions *PruneOptions) {
	if pruneOptions.PruneFactor == 0 {
		pruneOptions.PruneFactor = defaultPruneFactor
	}
	if pruneOptions.MinNewTransactions == 0 {
		pruneOptions.MinNewTransactions = defaultMinNewTransactions
	}
	if pruneOptions.MaxNewTransactions == 0 {
		pruneOptions.MaxNewTransactions = defaultMaxNewTransactions
	}
}

func shouldPrune(oldCount, newCount int, pruneOptions PruneOptions) (bool, string) {
	if oldCount < 0 {
		return true, "no pruning run found"
	}
	difference := newCount - oldCount
	if difference < pruneOptions.MinNewTransactions {
		return false, "not enough new transactions"
	}
	if difference > pruneOptions.MaxNewTransactions {
		return true, "too many new transactions"
	}
	factored := float32(oldCount) * pruneOptions.PruneFactor
	if float32(newCount) >= factored {
		return true, "transactions have grown significantly"
	}
	return false, "transactions have not grown significantly"
}

func maybePrune(db *mgo.Database, txnsName string, pruneOpts PruneOptions) error {
	validatePruneOptions(&pruneOpts)
	txnsPrune := db.C(txnsPruneC(txnsName))
	txns := db.C(txnsName)
	txnsStashName := txnsName + ".stash"
	txnsStash := db.C(txnsStashName)

	txnsCount, err := txns.Count()
	if err != nil {
		return fmt.Errorf("failed to retrieve starting txns count: %v", err)
	}
	lastTxnsCount, err := getPruneLastTxnsCount(txnsPrune)
	if err != nil {
		return fmt.Errorf("failed to retrieve pruning stats: %v", err)
	}

	required, rationale := shouldPrune(lastTxnsCount, txnsCount, pruneOpts)

	if !required {
		logger.Infof("txns after last prune: %d, txns now: %d, not pruning: %s",
			lastTxnsCount, txnsCount, rationale)
		return nil
	}
	logger.Infof("txns after last prune: %d, txns now: %d, pruning: %s",
		lastTxnsCount, txnsCount, rationale)
	started := time.Now()

	stashDocsBefore, err := txnsStash.Count()
	if err != nil {
		return fmt.Errorf("failed to retrieve starting %q count: %v", txnsStashName, err)
	}

	stats, err := Prune(PruneArgs{
		Txns:      txns,
		TxnsCount: txnsCount,
	})
	completed := time.Now()

	txnsCountAfter, err := txns.Count()
	if err != nil {
		return fmt.Errorf("failed to retrieve final txns count: %v", err)
	}
	stashDocsAfter, err := txnsStash.Count()
	if err != nil {
		return fmt.Errorf("failed to retrieve final %q count: %v", txnsStashName, err)
	}
	logger.Infof("txn pruning complete after %v. txns now: %d, inspected %d collections, %d docs (%d cleaned)\n   removed %d stash docs and %d txn docs",
		completed.Sub(started),
		txnsCountAfter,
		stats.CollectionsInspected,
		stats.DocsInspected,
		stats.DocsCleaned,
		stats.StashDocumentsRemoved,
		stats.TransactionsRemoved,
	)
	return recordPruneStats(txnsPrune, started, completed, txnsCount,
		txnsCountAfter, stashDocsBefore, stashDocsAfter)
}

// PruneArgs specifies the parameters required by Prune.
type PruneArgs struct {
	Txns      *mgo.Collection
	TxnsCount int
}

func (args *PruneArgs) validate() error {
	if args.Txns == nil {
		return errors.New("nil Txns not valid")
	}
	return nil
}

// PruneStats gives some numbers as to what work was done as part of
// Prune.
type PruneStats struct {

	// CollectionsInspected is the total number of collections we looked at for documents
	CollectionsInspected int

	// DocsInspected is how many documents we loaded to evaluate their txn queues
	DocsInspected int

	// DocsCleaned is how many documents we Updated to remove entries from their txn queue.
	DocsCleaned int

	// StashDocumentsRemoved is how many total documents we remove from txns.stash
	StashDocumentsRemoved int

	// StashDocumentsRemoved is how many documents we remove from txns
	TransactionsRemoved int
}

// Prune removed references to completed transactions from the
// txn-queue of all documents and then removes the data for
// transaction which are no longer referenced.
func Prune(args PruneArgs) (PruneStats, error) {
	var stats PruneStats

	if err := args.validate(); err != nil {
		return stats, err
	}

	db := args.Txns.Database

	if args.TxnsCount <= 0 {
		txnsCount, err := args.Txns.Count()
		if err != nil {
			return stats, err
		}
		args.TxnsCount = txnsCount
	}

	oracle, cleanup, err := getOracle(args, maxMemoryTokens)
	defer cleanup()
	if err != nil {
		return stats, err
	}
	txnsStashName := args.Txns.Name + ".stash"
	txnsStash := db.C(txnsStashName)

	if err := cleanupStash(oracle, txnsStash, &stats); err != nil {
		return stats, err
	}

	if err := cleanupAllCollections(db, oracle, args.Txns.Name, &stats); err != nil {
		return stats, err
	}

	if err := removeTxns(oracle, args.Txns, &stats); err != nil {
		return stats, err
	}

	return stats, nil
}

func getOracle(args PruneArgs, maxMemoryTxns int) (Oracle, func(), error) {
	// If we don't have very many transactions, just use the in-memory version
	if args.TxnsCount < maxMemoryTxns {
		return NewMemOracle(args.Txns)
	}
	return NewDBOracle(args.Txns)
}

// getPruneLastTxnsCount will return how many documents were in 'txns' the
// last time we pruned. It will return -1 if it cannot find a reliable value
// (no value available, or corrupted document.)
func getPruneLastTxnsCount(txnsPrune *mgo.Collection) (int, error) {
	// Retrieve the doc which points to the latest stats entry.
	var ptrDoc bson.M
	err := txnsPrune.FindId("last").One(&ptrDoc)
	if err == mgo.ErrNotFound {
		return -1, nil
	} else if err != nil {
		return -1, fmt.Errorf("failed to load pruning stats pointer: %v", err)
	}

	// Get the stats.
	var doc pruneStats
	err = txnsPrune.FindId(ptrDoc["id"]).One(&doc)
	if err == mgo.ErrNotFound {
		// Pointer was broken. Recover by returning 0 which will force
		// pruning.
		logger.Warningf("pruning stats pointer was broken - will recover")
		return -1, nil
	} else if err != nil {
		return -1, fmt.Errorf("failed to load pruning stats: %v", err)
	}
	return doc.TxnsAfter, nil
}

func recordPruneStats(
	txnsPrune *mgo.Collection,
	started, completed time.Time,
	txnsBefore, txnsAfter,
	stashBefore, stashAfter int,
) error {
	id := bson.NewObjectId()
	err := txnsPrune.Insert(pruneStats{
		Id:              id,
		Started:         started,
		Completed:       completed,
		TxnsBefore:      txnsBefore,
		TxnsAfter:       txnsAfter,
		StashDocsBefore: stashBefore,
		StashDocsAfter:  stashAfter,
	})
	if err != nil {
		return fmt.Errorf("failed to write prune stats: %v", err)
	}

	// Set pointer to latest stats document.
	_, err = txnsPrune.UpsertId("last", bson.M{"$set": bson.M{"id": id}})
	if err != nil {
		return fmt.Errorf("failed to write prune stats pointer: %v", err)
	}
	return nil
}

func txnsPruneC(txnsName string) string {
	return txnsName + ".prune"
}

// removeTxns removes applied and aborted transactions - as tracked by
// the supplied Oracle - from the txns collection.
func removeTxns(oracle Oracle, txns *mgo.Collection, stats *PruneStats) error {
	count := oracle.Count()
	logger.Debugf("%d completed txns found", count)

	t := newSimpleTimer(logInterval)
	var remover Remover
	if checkMongoSupportsOut(txns.Database) {
		remover = newBulkRemover(txns)
	} else {
		remover = newBatchRemover(txns)
	}
	iter, err := oracle.IterTxns()
	if err != nil {
		return err
	}
	var loopErr error
	var txnId bson.ObjectId
	for txnId, loopErr = iter.Next(); loopErr == nil; txnId, loopErr = iter.Next() {
		if err := remover.Remove(txnId); err != nil {
			return fmt.Errorf("removing txns: %v", err)
		}
		if t.isAfter() {
			logger.Debugf("%d completed txns pruned so far", remover.Removed())
		}
	}
	if err := remover.Flush(); err != nil {
		return fmt.Errorf("removing txns: %v", err)
	}
	if loopErr != EOF {
		return loopErr
	}
	if stats != nil {
		stats.TransactionsRemoved += remover.Removed()
	}
	logger.Debugf("pruning completed: removed %d txns", remover.Removed())
	return nil
}

// txnCollections takes the list of all collections in a database and
// filters them to just the ones that may have txn references.
func txnCollections(inNames []string, txnsName string) []string {
	// hasTxnReferences returns true if a collection may have
	// references to txns.
	hasTxnReferences := func(name string) bool {
		switch {
		case name == txnsName+".stash":
			return true // Need to look in the stash.
		case name == txnsName, strings.HasPrefix(name, txnsName+"."):
			// The txns collection and its children shouldn't be considered.
			return false
		case name == "statuseshistory":
			// statuseshistory is a special case that doesn't use txn and does get fairly big, so skip it
			return false
		case strings.HasPrefix(name, "system."):
			// Don't look in system collections.
			return false
		default:
			// Everything else needs to be considered.
			return true
		}
	}

	outNames := make([]string, 0, len(inNames))
	for _, name := range inNames {
		if hasTxnReferences(name) {
			outNames = append(outNames, name)
		}
	}
	return outNames
}

// cleanupAllCollections iterates all collections that might have transaction queues and checks them to see if
func cleanupAllCollections(db *mgo.Database, oracle Oracle, txnsName string, stats *PruneStats) error {
	collNames, err := db.CollectionNames()
	if err != nil {
		return fmt.Errorf("reading collection names: %v", err)
	}
	collNames = txnCollections(collNames, txnsName)
	logger.Debugf("%d collections with txns to cleanup", len(collNames))
	for _, name := range collNames {
		cleaner := NewCollectionCleaner(CollectionConfig{
			Oracle: oracle,
			Source: db.C(name),
		})
		if err := cleaner.Cleanup(); err != nil {
			return err
		}
		if stats != nil {
			stats.CollectionsInspected += 1
			stats.DocsInspected += cleaner.stats.DocCount
			stats.DocsCleaned += cleaner.stats.UpdatedDocCount
		}
	}
	return nil
}

func txnTokenToId(token string) bson.ObjectId {
	// mgo/txn transaction tokens are the 24 character txn id
	// followed by "_<nonce>"
	return bson.ObjectIdHex(token[:24])
}

func newBatchRemover(coll *mgo.Collection) *batchRemover {
	return &batchRemover{
		coll: coll,
	}
}

type Remover interface {
	Remove(id interface{}) error
	Flush() error
	Removed() int
}

type batchRemover struct {
	coll    *mgo.Collection
	queue   []interface{}
	removed int
}

var _ Remover = (*batchRemover)(nil)

func (r *batchRemover) Remove(id interface{}) error {
	r.queue = append(r.queue, id)
	if len(r.queue) >= maxBulkOps {
		return r.Flush()
	}
	return nil
}

func (r *batchRemover) Flush() error {
	if len(r.queue) < 1 {
		return nil // Nothing to do
	}
	filter := bson.M{"_id": bson.M{"$in": r.queue}}
	switch result, err := r.coll.RemoveAll(filter); err {
	case nil, mgo.ErrNotFound:
		// It's OK for txns to no longer exist. Another process
		// may have concurrently pruned them.
		r.removed += result.Removed
		r.queue = r.queue[:0]
		return nil
	default:
		return err
	}
}

func (r *batchRemover) Removed() int {
	return r.removed
}

func newBulkRemover(coll *mgo.Collection) *bulkRemover {
	r := &bulkRemover{coll: coll}
	r.newChunk()
	return r
}

type bulkRemover struct {
	coll      *mgo.Collection
	chunk     *mgo.Bulk
	chunkSize int
	removed   int
}

var _ Remover = (*bulkRemover)(nil)

func (r *bulkRemover) newChunk() {
	r.chunk = r.coll.Bulk()
	r.chunk.Unordered()
	r.chunkSize = 0
}

func (r *bulkRemover) Remove(id interface{}) error {
	r.chunk.Remove(bson.D{{"_id", id}})
	r.chunkSize++
	if r.chunkSize >= maxBulkOps {
		return r.Flush()
	}
	return nil
}

func (r *bulkRemover) Flush() error {
	if r.chunkSize < 1 {
		return nil // Nothing to do
	}
	switch result, err := r.chunk.Run(); err {
	case nil, mgo.ErrNotFound:
		// It's OK for txns to no longer exist. Another process
		// may have concurrently pruned them.
		r.removed += result.Matched
		r.newChunk()
		return nil
	default:
		return err
	}
}

func (r *bulkRemover) Removed() int {
	return r.removed
}

func newSimpleTimer(interval time.Duration) *simpleTimer {
	return &simpleTimer{
		interval: interval,
		next:     time.Now().Add(interval),
	}
}

type simpleTimer struct {
	interval time.Duration
	next     time.Time
}

func (t *simpleTimer) isAfter() bool {
	now := time.Now()
	if now.After(t.next) {
		t.next = now.Add(t.interval)
		return true
	}
	return false
}
