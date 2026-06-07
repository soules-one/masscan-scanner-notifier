package app
import (
	"context"
	"testing"
	"time"
)
func TestScheduler_InvalidInterval(t *testing.T) {
	out := make(chan ScanRequest, 1)
	s := NewScheduler(out)
	job := ScheduleJob{
		Name:     "Invalid",
		Interval: -1,
	}
	s.StartJob(context.Background(), job)
	select {
	case <-out:
		t.Error("Job with invalid interval should not be enqueued")
	case <-time.After(50 * time.Millisecond):
	}
}
func TestScheduler_EnqueueFull(t *testing.T) {
	out := make(chan ScanRequest, 1)
	s := NewScheduler(out)
	job := ScheduleJob{Name: "Full"}
	out <- ScanRequest{} 
	s.enqueue(job) 
}
