package memq

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

type strTask string

func (t strTask) Key() string {
	return string(t)
}

func TestQueue(t *testing.T) {
	q := New[strTask, int](10240)

	pwg := sync.WaitGroup{}
	pctx, pcancel := context.WithCancel(context.Background())
	{
		pgn := 1000
		tickDur := time.Millisecond * 500
		pwg.Add(pgn)
		for i := 0; i < pgn; i++ {
			pgi := i + 1
			go func() {
				defer pwg.Done()

				tick := time.NewTicker(tickDur)
				defer tick.Stop()

				select {
				case <-tick.C:
					task := strTask(fmt.Sprintf("%d-%d", pgi, time.Now().UnixMilli()))
					ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)

					w, err := q.Push(ctx, task)
					if err != nil {
						t.Error(err, task)
					} else {
						_, err := w()
						if err != nil {
							t.Error(err, task)
						}
					}
					cancel()
				case <-pctx.Done():
					return
				}

			}()

		}
	}

	cwg := sync.WaitGroup{}
	{
		cgn := 64
		cwg.Add(cgn)
		for i := 0; i < cgn; i++ {
			cgi := i + 1
			go func() {
				defer cwg.Done()

				cnt := 0

				r := rand.New(rand.NewSource(time.Now().UnixNano()))
				q.Consume(func(ctx context.Context, t strTask) (int, error) {
					// mock handle time cost: 20~40ms
					time.Sleep(time.Millisecond * time.Duration(20+r.Intn(21)))

					cnt++
					return cgi*100_000_000_000 + cnt, nil
				})
			}()
		}
	}

	time.Sleep(time.Second * 10)
	pcancel()
	pwg.Wait()
	q.Close()
	cwg.Wait()
}
