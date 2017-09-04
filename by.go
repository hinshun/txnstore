package txnstore

import memdb "github.com/hashicorp/go-memdb"

type By interface {
	Get(get GetFunc) ([]memdb.ResultIterator, error)
}

type GetFunc func(index string, args ...interface{}) (memdb.ResultIterator, error)

func All() By {
	return &all{}
}

type all struct{}

func (a *all) Get(get GetFunc) ([]memdb.ResultIterator, error) {
	iter, err := get(IndexID)
	if err != nil {
		return nil, err
	}
	return []memdb.ResultIterator{iter}, nil
}

func Or(bys ...By) By {
	return &or{bys}
}

type or struct {
	bys []By
}

func (o *or) Get(get GetFunc) ([]memdb.ResultIterator, error) {
	var orIters []memdb.ResultIterator
	for _, by := range o.bys {
		iters, err := by.Get(get)
		if err != nil {
			return nil, err
		}
		orIters = append(orIters, iters...)
	}
	return orIters, nil
}

func ByID(id string) By {
	return &byID{id}
}

type byID struct {
	id string
}

func (b *byID) Get(get GetFunc) ([]memdb.ResultIterator, error) {
	iter, err := get(IndexID, b.id)
	if err != nil {
		return nil, err
	}
	return []memdb.ResultIterator{iter}, nil
}
