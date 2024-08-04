package memq

import (
	"context"
	"sync/atomic"
)

// Task -
type Task interface {
	Key() string
}

const (
	stateInit     = 0
	stateCanceled = 1 << 0
	stateLocked   = 1 << 1
)

type qwait[T Task, R any] struct {
	c context.Context
	t T
	r R
	e error

	state int32
	done  chan struct{}
}

func (w *qwait[T, R]) tryCancel() bool {
	return atomic.CompareAndSwapInt32(&w.state, stateInit, stateCanceled)

}

func (w *qwait[T, R]) tryLock() bool {
	return atomic.CompareAndSwapInt32(&w.state, stateInit, stateLocked)
}

func (w *qwait[T, R]) setR(r R) {
	w.r = r
	close(w.done)
}

func (w *qwait[T, R]) setE(e error) {
	w.e = e
	close(w.done)
}

func (w *qwait[T, R]) reset() {
	*w = qwait[T, R]{}
}

func (w *qwait[T, R]) init(c context.Context, t T) {
	*w = qwait[T, R]{
		c:     c,
		t:     t,
		state: stateInit,
		done:  make(chan struct{}),
	}
}
