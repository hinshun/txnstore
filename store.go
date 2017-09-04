package txnstore

import (
	"time"

	"github.com/docker/swarmkit/watch"
	memdb "github.com/hashicorp/go-memdb"
)

const (
	IndexID = "id"
)

var (
	WedgeTimeout = 30 * time.Second
)

type Object interface {
	GetID() string
	GetVersion() uint64
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
	db         *memdb.MemDB
	schema     *memdb.DBSchema
	queue      *watch.Queue
	updateLock TimedMutex
}

func NewStore(tables []*memdb.TableSchema) (Store, error) {
	schema := &memdb.DBSchema{
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
		db:     db,
		schema: schema,
		queue:  watch.NewQueue(),
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
	callback(&readTxn{dbTxn})
}

func (s *store) Update(callback func(Txn) error) (err error) {
	s.updateLock.Lock()
	defer s.updateLock.Unlock()

	dbTxn := s.db.Txn(true)
	defer func() {
		if err != nil {
			dbTxn.Abort()
		}
	}()

	txn := newTxn(dbTxn)
	err = callback(txn)
	if err != nil {
		return err
	}

	dbTxn.Commit()
	for _, change := range txn.changelist {
		s.queue.Publish(change)
	}
	if len(txn.changelist) > 0 {
		s.queue.Publish(EventCommit{Changelist: txn.changelist})
	}

	return nil
}

func (s *store) Batch(callback func(Batch) error) error {
	s.updateLock.Lock()
	defer s.updateLock.Unlock()

	batch := batch{store: s}
	batch.newTxn()

	err := callback(&batch)
	if err != nil {
		batch.txn.dbTxn.Abort()
		return err
	}

	return batch.commit()
}
