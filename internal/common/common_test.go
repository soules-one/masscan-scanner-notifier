package common
import (
	"context"
	"sync"
	"testing"
	"time"
)
func TestBroadcast_Success(t *testing.T) {
	ctx := context.Background()
	ch1 := make(chan int, 1)
	ch2 := make(chan int, 1)
	outs := []chan int{ch1, ch2}
	Broadcast(42, ctx, outs, 50*time.Millisecond)
	select {
	case val := <-ch1:
		if val != 42 {
			t.Errorf("expected 42, got %v from ch1", val)
		}
	default:
		t.Error("ch1 did not receive value")
	}
	select {
	case val := <-ch2:
		if val != 42 {
			t.Errorf("expected 42, got %v from ch2", val)
		}
	default:
		t.Error("ch2 did not receive value")
	}
}
func TestBroadcast_Timeout(t *testing.T) {
	ctx := context.Background()
	ch1 := make(chan int)
	outs := []chan int{ch1}
	start := time.Now()
	Broadcast(42, ctx, outs, 10*time.Millisecond)
	duration := time.Since(start)
	if duration < 10*time.Millisecond {
		t.Errorf("broadcast returned too fast: %v", duration)
	}
}
func TestBroadcast_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ch1 := make(chan int)
	outs := []chan int{ch1}
	cancel()
	start := time.Now()
	Broadcast(42, ctx, outs, 1*time.Second)
	duration := time.Since(start)
	if duration > 100*time.Millisecond {
		t.Errorf("broadcast did not respect context cancel in time: %v", duration)
	}
}
func TestTee_Success(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in := make(chan int, 2)
	outs := Tee(ctx, in, 3, 50*time.Millisecond)
	if len(outs) != 3 {
		t.Fatalf("expected 3 output channels, got %d", len(outs))
	}
	in <- 100
	in <- 200
	close(in)
	for i, ch := range outs {
		var vals []int
		var wg sync.WaitGroup
		wg.Add(1)
		go func(c <-chan int, idx int) {
			defer wg.Done()
			for v := range c {
				vals = append(vals, v)
			}
		}(ch, i)
		wg.Wait()
		if len(vals) != 2 {
			t.Fatalf("channel %d: expected 2 values, got %d", i, len(vals))
		}
		if vals[0] != 100 || vals[1] != 200 {
			t.Errorf("channel %d: expected [100, 200], got %v", i, vals)
		}
	}
}
func TestTee_Cancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan int, 1)
	outs := Tee(ctx, in, 2, 50*time.Millisecond)
	cancel()
	time.Sleep(10 * time.Millisecond)
	for i, ch := range outs {
		select {
		case _, ok := <-ch:
			if ok {
				t.Errorf("channel %d should have been closed but received a value", i)
			}
		case <-time.After(100 * time.Millisecond):
			t.Errorf("channel %d was not closed after context cancel", i)
		}
	}
}
