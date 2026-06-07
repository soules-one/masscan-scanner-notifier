package dashboard
import (
	"crypto/sha256"
	"encoding/hex"
	"masscan-monitor/internal/db"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
)
func TestDashboard_Login(t *testing.T) {
	dbFile := "dash_login_test.db"
	defer os.Remove(dbFile)
	store, _ := db.Open(dbFile)
	defer store.Close()
	password := "123456"
	hash := sha256.Sum256([]byte(password))
	accessHash := hex.EncodeToString(hash[:])
	dash := NewDashboard(store, accessHash, 8081)
	form := url.Values{}
	form.Add("password", password)
	req := httptest.NewRequest("POST", "/login", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	dash.handleLogin(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %v", rr.Code)
	}
	form = url.Values{}
	form.Add("password", "wrong")
	req = httptest.NewRequest("POST", "/login", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr = httptest.NewRecorder()
	dash.handleLogin(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %v", rr.Code)
	}
}
func TestDashboard_AuthMiddleware(t *testing.T) {
	dash := NewDashboard(nil, "", 8081)
	handler := dash.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest("GET", "/stats", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %v", rr.Code)
	}
	req = httptest.NewRequest("GET", "/stats", nil)
	req.AddCookie(&http.Cookie{Name: "session", Value: "authenticated"})
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %v", rr.Code)
	}
}
func TestDashboard_DataHandlers(t *testing.T) {
	dbFile := "dash_data_test.db"
	defer os.Remove(dbFile)
	store, _ := db.Open(dbFile)
	defer store.Close()
	dash := NewDashboard(store, "", 8081)
	handlers := []struct {
		name string
		h    http.HandlerFunc
	}{
		{"stats", dash.handleStats},
		{"recent", dash.handleRecent},
		{"ports", dash.handlePorts},
	}
	for _, tc := range handlers {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/"+tc.name, nil)
			rr := httptest.NewRecorder()
			tc.h(rr, req)
			if rr.Code != http.StatusOK {
				t.Errorf("%s handler failed: %v", tc.name, rr.Code)
			}
		})
	}
}
