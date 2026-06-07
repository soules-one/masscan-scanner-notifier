package vk
import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"masscan-monitor/internal/db"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)
type VKResponse struct {
	Response []VKMessageStatus `json:"response"`
	Error    *VKAPIError       `json:"error"`
}
type VKMessageStatus struct {
	PeerID    int `json:"peer_id"`
	MessageID int `json:"message_id"`
	Error *VKItemError `json:"error"`
}
type VKItemError struct {
	Code        int    `json:"code"`
	Description string `json:"description"`
}
type VKAPIError struct {
	ErrorCode int    `json:"error_code"`
	ErrorMsg  string `json:"error_msg"`
}
var (
	VKToken   string
	VKGroupID string
	VKApiVer  = "5.131"
)
func StartVKNotifier(ctx context.Context, eventsChan <-chan []db.PortEvent, userIDs []int) {
	if VKToken == "" {
		log.Fatal("[VK] VKToken is not set")
	}
	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	ticker := time.NewTicker(350 * time.Millisecond)
	log.Println("[VK] Notifier started...")
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case events, ok := <-eventsChan:
				if !ok {
					return
				}
				for _, record := range events {
					message := buildMessage(record)
					go sendInChunks(ctx, client, userIDs, message, ticker)
				}
			}
		}
	}()
}
func buildMessage(record db.PortEvent) string {
	var sb strings.Builder
	if record.Type == db.EventTypeBannerChanged {
		sb.WriteString("⚠️ Изменился баннер сервиса!\n\n")
	} else {
		sb.WriteString("🔔 Обнаружен новый порт!\n\n")
	}
	sb.WriteString(fmt.Sprintf("🌐 IP: %s\n", record.IP))
	sb.WriteString(fmt.Sprintf("🔌 Порт: %d (%s)\n", record.Port, record.Proto))
	sb.WriteString(fmt.Sprintf("📊 Статус: %s\n", record.Status))
	if record.Type == db.EventTypeBannerChanged {
		sb.WriteString(fmt.Sprintf("📜 Старый баннер: %s\n", record.OldBanner))
		sb.WriteString(fmt.Sprintf("📜 Новый баннер: %s\n", record.NewBanner))
	} else if record.NewBanner != "" {
		sb.WriteString(fmt.Sprintf("📝 Баннер: %s\n", record.NewBanner))
	}
	sb.WriteString(fmt.Sprintf("📅 Время: %s", record.DiscoveredAt.Format("2006-01-02 15:04:05")))
	return sb.String()
}
func sendInChunks(ctx context.Context, client *http.Client, userIDs []int, message string, ticker *time.Ticker) {
	const chunkSize = 50
	totalUsers := len(userIDs)
	for i := 0; i < totalUsers; i += chunkSize {
		end := i + chunkSize
		if end > totalUsers {
			end = totalUsers
		}
		chunk := userIDs[i:end]
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sendChunk(ctx, client, chunk, message)
		}
	}
}
func sendChunk(ctx context.Context, client *http.Client, chunk []int, message string) {
	var idStrings []string
	for _, id := range chunk {
		idStrings = append(idStrings, strconv.Itoa(id))
	}
	userIDsParam := strings.Join(idStrings, ",")
	formData := url.Values{}
	formData.Set("user_ids", userIDsParam)
	formData.Set("message", message)
	formData.Set("random_id", strconv.FormatInt(time.Now().UnixNano(), 10))
	formData.Set("access_token", VKToken)
	formData.Set("v", VKApiVer)
	if VKGroupID != "" {
		formData.Set("group_id", VKGroupID)
	}
	req, err := http.NewRequestWithContext(ctx, "POST", "https://api.vk.com/method/messages.send", strings.NewReader(formData.Encode()))
	if err != nil {
		log.Printf("[VK ERROR] Failed to create request: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[VK ERROR] Network error: %v", err)
		return
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("[VK ERROR] Failed to read response: %v", err)
		return
	}
	var vkResp VKResponse
	if err := json.Unmarshal(body, &vkResp); err != nil {
		log.Printf("[VK ERROR] Failed to parse JSON: %v. Body: %s", err, string(body))
		return
	}
	if vkResp.Error != nil {
		log.Printf("[VK API ERROR] Code %d: %s", vkResp.Error.ErrorCode, vkResp.Error.ErrorMsg)
		return
	}
	for _, status := range vkResp.Response {
		if status.Error != nil {
			log.Printf("[VK WARNING] Failed to deliver to %d: Code %d, %s", status.PeerID, status.Error.Code, status.Error.Description)
		} else {
			log.Printf("[VK SUCCESS] Delivered to %d (MessageID: %d)", status.PeerID, status.MessageID)
		}
	}
}
