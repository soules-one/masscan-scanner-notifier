package app
import (
	"context"
	"log"
	"sync"
	"time"
	"masscan-monitor/internal/scanner"
)
type masscanRunner interface {
	Run(ctx context.Context, target string) ([]scanner.MasscanRecord, error)
}
type Service struct {
	masscanBin string
	maxActive  int
	in  <-chan ScanRequest
	out chan<- RawScanBatch
	newMasscan func(binary string, rate int, ports string) masscanRunner
	sem        chan struct{}
	wg         sync.WaitGroup
}
func NewService(
	masscanBin string,
	maxActive int,
	in <-chan ScanRequest,
	out chan<- RawScanBatch,
) *Service {
	if maxActive <= 0 {
		maxActive = 1
	}
	return &Service{
		masscanBin: masscanBin,
		maxActive:  maxActive,
		in:         in,
		out:        out,
		newMasscan: func(binary string, rate int, ports string) masscanRunner {
			return scanner.NewMasscan(binary, rate, ports)
		},
		sem: make(chan struct{}, maxActive),
	}
}
func (s *Service) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			s.wg.Wait()
			return
		case req, ok := <-s.in:
			if !ok {
				s.wg.Wait()
				return
			}
			select {
			case s.sem <- struct{}{}:
			case <-ctx.Done():
				s.wg.Wait()
				return
			}
			s.wg.Add(1)
			go func(req ScanRequest) {
				defer s.wg.Done()
				defer func() { <-s.sem }()
				s.execute(ctx, req)
			}(req)
		}
	}
}
func (s *Service) execute(parent context.Context, req ScanRequest) {
	timeout := req.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	started := time.Now()
	m := s.newMasscan(s.masscanBin, req.Rate, req.Ports)
	records, err := m.Run(ctx, req.Target)
	s.publish(ctx, RawScanBatch{
		Request:   req,
		StartedAt: started,
		EndedAt:   time.Now(),
		Records:   records,
		Err:       err,
	})
}
func (s *Service) publish(ctx context.Context, batch RawScanBatch) {
	select {
	case s.out <- batch:
	case <-ctx.Done():
		log.Printf("publish canceled: id=%s target=%s", batch.Request.ID, batch.Request.Target)
	}
}
