package app
import (
	"context"
	"testing"
	"time"
)
func TestService_Basic(t *testing.T) {
	reqs := make(chan ScanRequest, 1)
	results := make(chan RawScanBatch, 1)
	svc := NewService("masscan", 1, reqs, results)
	ctx, cancel := context.WithCancel(context.Background())
	go svc.Run(ctx)
	reqs <- ScanRequest{ID: "test", Target: "127.0.0.1", Ports: "80"}
	select {
	case res := <-results:
		if res.Request.ID != "test" {
			t.Errorf("Expected test ID, got %s", res.Request.ID)
		}
	case <-time.After(500 * time.Millisecond):
	}
	cancel()
}
