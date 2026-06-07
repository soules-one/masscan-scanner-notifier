package app
import (
	"context"
	"fmt"
	"log"
	"time"
)
type ScheduleJob struct {
	Name     string
	Target   string
	Ports    string
	Rate     int
	Interval time.Duration
	Timeout  time.Duration
	Meta     map[string]string
}
type Scheduler struct {
	out chan<- ScanRequest
}
func NewScheduler(out chan<- ScanRequest) *Scheduler {
	return &Scheduler{
		out: out,
	}
}
func (s *Scheduler) StartJob(ctx context.Context, job ScheduleJob) {
	if job.Interval <= 0 {
		log.Printf("skip schedule job=%s: invalid interval=%s", job.Name, job.Interval)
		return
	}
	go func() {
		s.enqueue(job)
		ticker := time.NewTicker(job.Interval)
		defer ticker.Stop()
		log.Printf("scheduler started: job=%s target=%s interval=%s timeout=%s",
			job.Name, job.Target, job.Interval, job.Timeout)
		for {
			select {
			case <-ctx.Done():
				log.Printf("scheduler stopped: job=%s", job.Name)
				return
			case <-ticker.C:
				s.enqueue(job)
			}
		}
	}()
}
func (s *Scheduler) enqueue(job ScheduleJob) {
	req := ScanRequest{
		ID:          fmt.Sprintf("%s-%s", job.Name, time.Now().Format("20060102T150405")),
		Target:      job.Target,
		Ports:       job.Ports,
		Rate:        job.Rate,
		Source:      "schedule",
		RequestedAt: time.Now(),
		Timeout:     job.Timeout,
		Meta:        job.Meta,
	}
	select {
	case s.out <- req:
		log.Printf("scan enqueued: job=%s target=%s", job.Name, job.Target)
	default:
		log.Printf("scan queue full, dropping job=%s", job.Name)
	}
}
