package vk
import (
	"masscan-monitor/internal/db"
	"strings"
	"testing"
)
func TestBuildMessage_BannerChange(t *testing.T) {
	event := db.PortEvent{
		Type:      db.EventTypeBannerChanged,
		IP:        "8.8.8.8",
		Port:      53,
		Proto:     "udp",
		OldBanner: "old",
		NewBanner: "new",
	}
	msg := buildMessage(event)
	if !strings.Contains(msg, "Изменился баннер") {
		t.Error("Expected banner change warning")
	}
	if !strings.Contains(msg, "old") || !strings.Contains(msg, "new") {
		t.Error("Message should contain both old and new banners")
	}
}
func TestBuildMessage_Basic(t *testing.T) {
	event := db.PortEvent{
		Type: db.EventTypeNewPort,
		IP:   "1.2.3.4",
		Port: 22,
	}
	msg := buildMessage(event)
	if !strings.Contains(msg, "Обнаружен новый порт") {
		t.Error("Expected new port notification")
	}
}
