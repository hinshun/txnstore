package txnstore

import memdb "github.com/hashicorp/go-memdb"

type ReadTxn interface {
	Lookup(table, index, id string) Object
	Get(table, id string) Object
	Find(table string, by By, appendResult func(Object)) error
}

type readTxn struct {
	dbTxn *memdb.Txn
}

func (t *readTxn) Lookup(table, index, id string) Object {
	obj, err := t.dbTxn.First(table, index, id)
	if err != nil {
		return nil
	}

	if obj == nil {
		return nil
	}
	return obj.(Object)
}

func (t *readTxn) Get(table, id string) Object {
	obj := t.Lookup(table, IndexID, id)
	if obj == nil {
		return nil
	}
	return obj.Copy()
}

func (t *readTxn) Find(table string, by By, appendResult func(Object)) error {
	get := func(index string, args ...interface{}) (memdb.ResultIterator, error) {
		return t.dbTxn.Get(table, index, args...)
	}

	iters, err := by.Get(get)
	if err != nil {
		return err
	}

	ids := make(map[string]struct{})
	for _, iter := range iters {
		next := iter.Next()
		for next != nil {
			obj := next.(Object)
			id := obj.GetID()
			_, ok := ids[id]
			if !ok {
				appendResult(obj)
				ids[id] = struct{}{}
			}
			next = iter.Next()
		}
	}

	return nil
}
