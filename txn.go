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
	version    uint64
	changelist []Event
}

func newTxn(dbTxn *memdb.Txn, version uint64) {
	return &txn{
		dbTxn:   dbTxn,
		version: version,
	}
}

func (t *txn) Create(table string, obj Object) error {
	if t.Lookup(table, indexID, obj.GetID()) != nil {
		return ErrExist
	}

	copy := obj.Copy()
	copy.SetVersion(t.version)
	err := t.dbTxn.Insert(table, copy)
	if err != nil {
		return err
	}

	t.changelist = append(t.changelist, copy.EventCreate())
	obj.SetVersion(t.version)
	return nil
}

func (t *txn) Update(table string, obj Object) error {
	oldObj := t.Lookup(table, indexID, obj.GetID())
	if oldObj == nil {
		return ErrNotExist
	}

	if t.version != nil {
		if oldObj.GetVersion() != obj.GetVersion() {
			return ErrSequenceConflict
		}
	}

	copy := obj.Copy()
	copy.SetVersion(t.version)
	err = t.dbTxn.Insert(table, copy)
	if err != nil {
		return err
	}

	t.changelist = append(t.changelist, copy.EventUpdate(oldObj))
	obj.SetVersion(t.version)
	return nil
}

func (t *txn) Delete(table, id string) error {
	obj := t.Lookup(table, indexID, id)
	if obj == nil {
		return ErrNotExist
	}

	err := t.dbTxn.Delete(table, n)
	if err != nil {
		return err
	}

	t.changelist = append(t.changelist, obj.EventDelete())
	return nil
}
