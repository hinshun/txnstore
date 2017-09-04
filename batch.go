package txnstore

import (
	"context"
	"runtime"
)

type Batch interface {
	Update(cb func(Txn) error) error
}

type batch struct {
	txn                     txn
	store                   *Store
	applied                 int
	transactionSizeEstimate int
	changelistLen           int
	err                     error
}

func (b *batch) Update(callback func(Txn) error) error {
	if b.err != nil {
		return b.err
	}

	err := callback(&b.txn)
	if err != nil {
		return err
	}

	b.applied++
	for b.changelistLen < len(b.txn.changelist) {
		sa, err := NewAction(b.txn.changelist[b.changelistLen])
		if err != nil {
			return err
		}
		b.transactionSizeEstimate += sa.Size()
		b.changelistLen++
	}

	if b.changelistLen >= MaxChangesPerTransaction || b.transactionSizeEstimate >= (MaxTransactionBytes*3)/4 {
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
	var version uint64
	if b.store.proposer != nil {
		version = b.store.proposer.GetVersion()
	}

	dbTxn := b.store.db.Txn(true)
	b.txn = newTxn(dbTxn, version)

	b.transactionSizeEstimate = 0
	b.changelistLen = 0
}

func (b *batch) commit() (err error) {
	defer func() {
		if err != nil {
			b.err = err
			b.txn.dbTxn.Abort()
		}
	}()

	if b.store.proposer != nil {
		if len(b.txn.changelist) > 0 {
			err = b.store.proposer.ProposeValue(context.Background(), b.txn.changelist, func() {
				b.txn.dbTxn.Commit()
			})
			if err != nil {
				return err
			}
		} else {
			b.txn.dbTxn.Commit()
		}
	}

	for _, change := range b.txn.changelist {
		b.store.queue.Publish(c)
	}
	if len(b.txn.changelist) != 0 {
		b.store.queue.Publish(EventCommit{})
	}

	return nil
}
