package txnstore

import memdb "github.com/hashicorp/go-memdb"

type By interface {
	Get(get GetFunc) ([]memdb.ResultIterator, error)
	GetArgs() []interface{}
}

type GetFunc func(index string, args ...interface{}) (memdb.ResultIterator, error)

func Or(bys ...By) By {
	return &or{bys}
}

type or struct {
	bys []By
}

func (o *Or) Get(get GetFunc) ([]memdb.ResultIterator, error) {
	var iters []memdb.ResultIterator
	for _, by := range o.bys {
		iter, err := by.Get(get)
		if err != nil {
			return nil, err
		}
		iters = append(iters, iter)
	}
	return iters, nil
}

func ByID(id string) By {
	return &byID{id}
}

type byID struct {
	id string
}

func (b *byID) Get(get GetFunc) ([]memdb.ResultIterator, error) {
	iter, err := get(indexID, b.id)
	if err != nil {
		return nil, err
	}
	return []memdbResultIterator{iter}, nil
}
