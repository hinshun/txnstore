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
	m.Mutex.Lock()
	m.lockedAt.Store(time.Now())
}

func (m *TimedMutex) Unlock() {
	m.Mutex.Unlock()
	m.lockedAt.Store(time.Time{})
}

func (m *TimedMutex) LockedAt() time.Time {
	lockedTimestamp := m.lockedAt.Load()
	if lockedTimestamp == nil {
		return time.Time{}
	}
	return lockedTimestamp.(time.Time)
}
