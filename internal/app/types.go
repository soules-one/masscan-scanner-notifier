package app
import (
	"time"
	"masscan-monitor/internal/scanner"
)
type ScanRequest struct {
	ID          string
	Target      string
	Ports       string
	Rate        int
	Source      string
	RequestedAt time.Time
	Timeout     time.Duration
	Meta        map[string]string
}
type RawScanBatch struct {
	Request   ScanRequest
	StartedAt time.Time
	EndedAt   time.Time
	Records   []scanner.MasscanRecord
	Err       error
}
