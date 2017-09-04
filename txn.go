package txnstore

import (
	"errors"

	memdb "github.com/hashicorp/go-memdb"
)

var (
	ErrExist = errors.New("object already exists")

	ErrNotExist = errors.New("object does not exist")

	ErrSequenceConflict = errors.New("update out of sequence")
)

type Txn interface {
	ReadTxn
	Create(table string, obj Object) error
	Update(table string, obj Object) error
	Delete(table, id string) error
}

type txn struct {
	readTxn
	changelist []Event
}

func newTxn(dbTxn *memdb.Txn) *txn {
	return &txn{
		readTxn: readTxn{dbTxn},
	}
}

func (t *txn) Create(table string, obj Object) error {
	if t.Lookup(table, IndexID, obj.GetID()) != nil {
		return ErrExist
	}

	copy := obj.Copy()
	err := t.dbTxn.Insert(table, copy)
	if err != nil {
		return err
	}

	t.changelist = append(t.changelist, copy.EventCreate())
	return nil
}

func (t *txn) Update(table string, obj Object) error {
	oldObj := t.Lookup(table, IndexID, obj.GetID())
	if oldObj == nil {
		return ErrNotExist
	}

	if oldObj.GetVersion() != obj.GetVersion() {
		return ErrSequenceConflict
	}

	copy := obj.Copy()
	err := t.dbTxn.Insert(table, copy)
	if err != nil {
		return err
	}

	t.changelist = append(t.changelist, copy.EventUpdate(oldObj))
	return nil
}

func (t *txn) Delete(table, id string) error {
	obj := t.Lookup(table, IndexID, id)
	if obj == nil {
		return ErrNotExist
	}

	err := t.dbTxn.Delete(table, obj)
	if err != nil {
		return err
	}

	t.changelist = append(t.changelist, obj.EventDelete())
	return nil
}
