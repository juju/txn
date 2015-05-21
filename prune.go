// Copyright 2015 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package txn

import (
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/mgo.v2/txn"
)

// Transaction states copied From mgo/txn.
const (
	taborted = 5 // Pre-conditions failed, nothing done
	tapplied = 6 // All changes applied
)

func isPruningRequired(txnsPrune, txns *mgo.Collection, pruneFactor float32) (bool, error) {
	txnsCount, err := txns.Count()
	if err != nil {
		return false, err
	}

	lastTxnsCount, err := getPruneTxnsCount(txnsPrune)
	if err != nil {
		return false, err
	}

	required := lastTxnsCount == 0 || float32(txnsCount) >= float32(lastTxnsCount)*pruneFactor

	logger.Infof("txns after last prune: %d, txns now = %d, pruning required: %v", lastTxnsCount, txnsCount, required)
	return required, nil
}

type pruneStats struct {
	Id        string    `bson:"_id"`
	Completed time.Time `bson:"completed"`
	TxnsCount int       `bson:"txns-count"`
}

func getPruneTxnsCount(txnsPrune *mgo.Collection) (int, error) {
	var doc pruneStats
	err := txnsPrune.FindId("last").One(&doc)
	if err == mgo.ErrNotFound {
		return 0, nil
	} else if err != nil {
		return -1, err
	}
	return doc.TxnsCount, nil
}

func writePruneTxnsCount(txnsPrune, txns *mgo.Collection) error {
	txnsCount, err := txns.Count()
	if err != nil {
		return err
	}
	logger.Infof("txn pruning complete. txns now = %d", txnsCount)

	doc := pruneStats{
		Id:        "last",
		Completed: time.Now(),
		TxnsCount: txnsCount,
	}
	_, err := txnsPrune.UpsertId("last", doc)
	return err
}

func txnsPruneC(txnsName string) string {
	return txnsName + ".prune"
}

// pruneTxns removes applied and aborted entries from the txns
// collection that are no longer referenced by any document.
//
// Warning: this is a fairly heavyweight activity and therefore should
// be done infrequently.
//
// TODO(mjs) - this knows way too much about mgo/txn's internals and
// with a bit of luck something like this will one day be part of
// mgo/txn.
func pruneTxns(db *mgo.Database, txns *mgo.Collection) error {
	present := struct{}{}

	// Load the ids of all completed txns and all collections
	// referred to by those txns.
	//
	// This set could potentially contain many entries, however even
	// 500,000 entries requires only ~44MB of memory. Given that the
	// memory hit is short-lived this is probably acceptable.
	txnIds := make(map[bson.ObjectId]struct{})
	collNames := make(map[string]struct{})

	var txnDoc struct {
		Id  bson.ObjectId `bson:"_id"`
		Ops []txn.Op      `bson:"o"`
	}

	completed := bson.M{
		"s": bson.M{"$in": []int{taborted, tapplied}},
	}
	iter := txns.Find(completed).Select(bson.M{"_id": 1, "o": 1}).Iter()
	for iter.Next(&txnDoc) {
		txnIds[txnDoc.Id] = present
		for _, op := range txnDoc.Ops {
			collNames[op.C] = present
		}
	}
	if err := iter.Close(); err != nil {
		return err
	}

	// Transactions may also be referenced in the stash.
	collNames["txns.stash"] = present

	// Now remove the txn ids referenced by all documents in all
	// txn using collections from the set of known txn ids.
	//
	// Working the other way - starting with the set of txns
	// referenced by documents and then removing any not in that set
	// from the txns collection - is unsafe as it will result in the
	// removal of transactions run while pruning executes.
	//
	for collName := range collNames {
		coll := db.C(collName)
		var tDoc struct {
			Queue []string `bson:"txn-queue"`
		}
		iter := coll.Find(nil).Select(bson.M{"txn-queue": 1}).Iter()
		for iter.Next(&tDoc) {
			for _, token := range tDoc.Queue {
				delete(txnIds, txnTokenToId(token))
			}
		}
		if err := iter.Close(); err != nil {
			return err
		}
	}

	// Remove the unreferenced transactions.
	err := bulkRemoveTxns(txns, txnIds)
	return err
}

func txnTokenToId(token string) bson.ObjectId {
	// mgo/txn transaction tokens are the 24 character txn id
	// followed by "_<nonce>"
	return bson.ObjectIdHex(token[:24])
}

// bulkRemoveTxns removes transaction documents in chunks. It should
// be significantly more efficient than removing one document per
// remove query while also not trigger query document size limits.
func bulkRemoveTxns(txns *mgo.Collection, txnIds map[bson.ObjectId]struct{}) error {
	removeTxns := func(ids []bson.ObjectId) error {
		_, err := txns.RemoveAll(bson.M{"_id": bson.M{"$in": ids}})
		switch err {
		case nil, mgo.ErrNotFound:
			// It's OK for txns to no longer exist. Another process
			// may have concurrently pruned them.
			return nil
		default:
			return err
		}
	}

	const chunkMax = 1024
	chunk := make([]bson.ObjectId, 0, chunkMax)
	for txnId := range txnIds {
		chunk = append(chunk, txnId)
		if len(chunk) == chunkMax {
			if err := removeTxns(chunk); err != nil {
				return err
			}
			chunk = chunk[:0] // Avoid reallocation.
		}
	}
	if len(chunk) > 0 {
		if err := removeTxns(chunk); err != nil {
			return err
		}
	}

	return nil
}
