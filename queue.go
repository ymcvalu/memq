package memq

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrClosed = errors.New("queue closed")
)

// New -
func New[T Task, R any](bs int) Queue[T, R] {
	ctx, cancel := context.WithCancel(context.Background())

	return &queue[T, R]{
		ctx:    ctx,
		cancel: cancel,

		bs: bs,
		wq: make(chan *qwait[T, R], bs),

		wc: sync.Pool{
			New: func() any {
				return new(qwait[T, R])
			},
		},
	}
}

// Queue -
type Queue[T Task, R any] interface {
	Push(ctx context.Context, t T) (Wait[T, R], error)
	PWait(ctx context.Context, t T) (R, error)
	Consume(h Handler[T, R])
	Close()
}

// Handler -
type Handler[T Task, R any] func(ctx context.Context, t T) (R, error)

// Wait -
type Wait[T Task, R any] func() (R, error)

type queue[T Task, R any] struct {
	ctx    context.Context
	cancel func()

	bs int
	wq chan *qwait[T, R]

	wc sync.Pool
}

// Push -
func (q *queue[T, R]) Push(ctx context.Context, t T) (Wait[T, R], error) {
	w := q.wc.Get().(*qwait[T, R])
	w.init(ctx, t)

	select {
	case <-q.ctx.Done():
		return nil, ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	case q.wq <- w:
	}

	return func() (R, error) {
		select {
		case <-q.ctx.Done():
			if w.tryCancel() {
				var r R
				return r, ErrClosed
			}
			<-w.done
		case <-ctx.Done():
			if w.tryCancel() {
				var r R
				return r, ctx.Err()
			}
			<-w.done
		case <-w.done:
		}

		r, e := w.r, w.e
		w.reset()
		q.wc.Put(w)

		return r, e
	}, nil
}

// PWait push and wait
func (q *queue[T, R]) PWait(ctx context.Context, t T) (R, error) {
	w, err := q.Push(ctx, t)
	if err != nil {
		var r R
		return r, err
	}

	return w()
}

// Consume -
func (q *queue[T, R]) Consume(h Handler[T, R]) {
	for {
		select {
		case <-q.ctx.Done():
			return

		case w, ok := <-q.wq:
			if !ok {
				return
			}

			q.execute(w, h)
		}
	}
}

func (q *queue[T, R]) execute(w *qwait[T, R], h Handler[T, R]) {
	// task has canceled, ignore it
	if !w.tryLock() {
		return
	}

	select {
	case <-q.ctx.Done():
		w.setE(ErrClosed)
		return
	case <-w.c.Done():
		w.setE(w.c.Err())
		return
	default:
	}

	// TODO handle panic

	r, e := h(w.c, w.t)
	if e != nil {
		w.setE(e)
		return
	}

	w.setR(r)
}

// Close -
func (q *queue[T, R]) Close() {
	q.cancel()
}
