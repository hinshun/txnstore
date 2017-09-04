package txnstore

import (
	"context"
	"time"

	"github.com/docker/swarmkit/watch"
	memdb "github.com/hashicorp/go-memdb"
)

var (
	WedgeTimeout = 30 * time.Second
)

type Proposer interface {
	ProposeValue(ctx context.Context, events []Event, callback func()) error
	GetVersion() uint64
}

type Object interface {
	GetID() string
	GetVersion() uint64
	SetVersion(uint64)
	Copy() Object
	EventCreate() Event
	EventUpdate(oldObj Object) Event
	EventDelete() Event
}

type Store interface {
	Close() error
	WatchQueue() *watch.Queue
	View(callback func(ReadTxn))
	Update(callback func(Txn) error) error
	Batch(callback func(Batch) error) error
}

type store struct {
	updateLock TimedMutex
	db         *memdb.MemDB
	schema     *memdb.DBSchema
	queue      *watch.Queue
	proposer   Proposer
}

func NewStore(proposer Proposer, tables []*memdb.TableSchema) (Store, error) {
	schema = &memdb.DBSchema{
		Tables: make(map[string]*memdb.TableSchema),
	}
	for _, table := range tables {
		schema.Tables[table.Name] = table
	}

	db, err := memdb.NewMemDB(schema)
	if err != nil {
		return nil, err
	}

	return &store{
		db:       db,
		schema:   schema,
		queue:    watch.NewQueue(),
		proposer: proposer,
	}, nil
}

func (s *store) Close() error {
	return s.queue.Close()
}

func (s *store) WatchQueue() *watch.Queue {
	return s.queue
}

func (s *store) Wedged() bool {
	lockedAt := s.updateLock.LockedAt()
	if lockedAt.IsZero() {
		return false
	}

	return time.Since(lockedAt) > WedgeTimeout
}

func (s *store) View(callback func(ReadTxn)) {
	dbTxn := s.db.Txn(false)

	readTxn := readTxn{dbTxn}
	callback(readTxn)
	dbtxn.Commit()
}

func (s *store) Update(callback func(Txn) error) (err error) {
	s.updateLock.Lock()
	defer s.updateLock.Unlock()

	var version uint64
	if s.proposer != nil {
		version = s.proposer.GetVersion()
	}

	dbTxn := s.db.Txn(true)
	defer func() {
		if err != nil {
			dbTxn.Abort()
		}
	}()

	txn := newTxn(dbTxn, version)

	err = callback(&txn)
	if err != nil {
		return err
	}

	if proposer == nil {
		dbTxn.Commit()
	} else {
		if len(txn.changelist) > 0 {
			err = s.proposer.ProposeValue(context.Background(), txn.changelist, func() {
				dbTxn.Commit()
			})
		} else {
			dbTxn.Commit()
		}
	}
	for _, change := range txn.changelist {
		s.queue.Publish(change)
		if len(txn.changelist) > 0 {
			s.queue.Publish(EventCommit{Version: version})
		}
	}

	return nil
}

func (s *store) Batch(callback func(Batch) error) error {
	s.updateLock.Lock()
	defer s.updateLock.Unlock()

	batch := batch{
		store: s,
	}
	batch.newTxn()

	err := callback(&batch)
	if err != nil {
		batch.txn.dbTxn.Abort()
		return err
	}

	return batch.commit()
}
