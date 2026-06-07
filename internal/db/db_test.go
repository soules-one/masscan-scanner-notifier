package db
import (
	"context"
	"masscan-monitor/internal/app"
	"masscan-monitor/internal/scanner"
	"os"
	"testing"
	"time"
)
func TestStore_Full(t *testing.T) {
	dbFile := "full_test.db"
	defer os.Remove(dbFile)
	store, _ := Open(dbFile)
	defer store.Close()
	ctx := context.Background()
	batch1 := app.RawScanBatch{
		Request: app.ScanRequest{Target: "10.0.0.1"},
		Records: []scanner.MasscanRecord{
			{IP: "10.0.0.1", Port: 80, Proto: "tcp", Status: "open"},
			{IP: "10.0.0.1", Port: 443, Proto: "tcp", Status: "open"},
		},
		EndedAt: time.Now(),
	}
	store.ProcessBatch(ctx, batch1)
	batch2 := app.RawScanBatch{
		Request: app.ScanRequest{Target: "10.0.0.1"},
		Records: []scanner.MasscanRecord{
			{IP: "10.0.0.1", Port: 80, Proto: "tcp", Status: "open"},
		},
		EndedAt: time.Now(),
	}
	store.ProcessBatch(ctx, batch2)
	ports, _ := store.ListOpenPorts(ctx)
	found443Open := false
	for _, p := range ports {
		if p.Port == 443 && p.Status == "open" {
			found443Open = true
		}
	}
	if found443Open {
		t.Error("Port 443 should be closed after second batch")
	}
	history, _ := store.ListHistory(ctx)
	if len(history) < 2 {
		t.Errorf("Expected at least 2 history records, got %d", len(history))
	}
	err := store.CleanupExpired(ctx)
	if err != nil {
		t.Errorf("CleanupExpired failed: %v", err)
	}
}
func TestStore_ErrorCases(t *testing.T) {
	store, _ := Open(":memory:")
	defer store.Close()
	_, err := store.ProcessBatch(context.Background(), app.RawScanBatch{
		Err: os.ErrNotExist,
	})
	if err != nil {
		t.Errorf("ProcessBatch with error should succeed, got %v", err)
	}
}
