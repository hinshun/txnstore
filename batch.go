package txnstore

import (
	"runtime"
)

const (
	MaxChangesPerTransaction = 200
)

type Batch interface {
	Update(cb func(Txn) error) error
}

type batch struct {
	txn   *txn
	store *store
	err   error
}

func (b *batch) Update(callback func(Txn) error) error {
	if b.err != nil {
		return b.err
	}

	err := callback(b.txn)
	if err != nil {
		return err
	}

	if len(b.txn.changelist) >= MaxChangesPerTransaction {
		err := b.commit()
		if err != nil {
			return err
		}

		// Yield the update lock
		b.store.updateLock.Unlock()
		runtime.Gosched()
		b.store.updateLock.Lock()

		b.newTxn()
	}

	return nil
}

func (b *batch) newTxn() {
	dbTxn := b.store.db.Txn(true)
	b.txn = newTxn(dbTxn)
}

func (b *batch) commit() (err error) {
	defer func() {
		if err != nil {
			b.err = err
			b.txn.dbTxn.Abort()
		}
	}()

	b.txn.dbTxn.Commit()
	for _, change := range b.txn.changelist {
		b.store.queue.Publish(change)
	}
	if len(b.txn.changelist) != 0 {
		b.store.queue.Publish(EventCommit{Changelist: b.txn.changelist})
	}

	return nil
}
