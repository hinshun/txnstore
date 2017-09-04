package txnstore

import (
	events "github.com/docker/go-events"
	"github.com/docker/swarmkit/watch"
)

type Event interface {
	Matches(events.Event) bool
}

type EventCommit struct {
	Changelist []Event
}

func (e EventCommit) Matches(watchEvent events.Event) bool {
	_, ok := watchEvent.(EventCommit)
	return ok
}

func Watch(queue *watch.Queue, specifiers ...Event) (watch chan events.Event, cancel func()) {
	if len(specifiers) == 0 {
		return queue.Watch()
	}
	return queue.CallbackWatch(Matcher(specifiers...))
}

func ViewAndWatch(store Store, callback func(ReadTxn) error, specifiers ...Event) (watch chan events.Event, cancel func(), err error) {
	err = store.Update(func(txn Txn) error {
		err := callback(txn)
		if err != nil {
			return err
		}
		watch, cancel = Watch(store.WatchQueue(), specifiers...)
		return nil
	})
	if watch != nil && err != nil {
		cancel()
		cancel = nil
		watch = nil
	}
	return
}

func Matcher(specifiers ...Event) events.MatcherFunc {
	return events.MatcherFunc(func(event events.Event) bool {
		for _, s := range specifiers {
			if s.Matches(event) {
				return true
			}
		}
		return false
	})
}
