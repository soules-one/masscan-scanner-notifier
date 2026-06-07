package common
import (
	"context"
	"log"
	"sync"
	"time"
)
func Broadcast[T any](val T, ctx context.Context, outs []chan T, timeout time.Duration) {
	var wg sync.WaitGroup
	wg.Add(len(outs))
	for i, out := range outs {
		go func(idx int, ch chan T) {
			defer wg.Done()
			timer := time.NewTimer(timeout)
			defer timer.Stop()
			select {
			case ch <- val:
			case <-ctx.Done():
				return
			case <-timer.C:
				log.Printf("timeout on %v channel broadcast", idx)
				return
			}
		}(i, out)
	}
	wg.Wait()
}
func Tee[T any](ctx context.Context, in <-chan T, nOut int, timeout time.Duration) []<-chan T {
	outs := make([]chan T, nOut)
	retOuts := make([]<-chan T, nOut)
	for i := range nOut {
		outs[i] = make(chan T, 128)
		retOuts[i] = outs[i]
	}
	go func() {
		defer func() {
			for i := range nOut {
				close(outs[i])
			}
		}()
		for {
			var val T
			var ok bool
			select {
			case <-ctx.Done():
				return
			case val, ok = <-in:
				if !ok {
					return
				}
			}
			Broadcast(val, ctx, outs, timeout)
		}
	}()
	return retOuts
}
