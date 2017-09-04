package txnstore

import (
	"sync"
	"sync/atomic"
	"time"
)

type TimedMutex struct {
	sync.Mutex
	lockedAt atomic.Value
}

func (m *TimedMutex) Lock() {
	tm.Mutex.Lock()
	tm.lockedAt.Store(time.Now())
}

func (m *TimedMutex) Unlock() {
	tm.Mutex.Unlock()
	tm.lockedAt.Store(time.Time{})
}

func (m *TimedMutex) LockedAt() time.Time {
	lockedTimestamp := tm.lockedAt.Load()
	if lockedTimestamp == nil {
		return time.Time{}
	}
	return lockedTimestamp.(time.Time)
}
