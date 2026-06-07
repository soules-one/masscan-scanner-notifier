package dashboard
import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"html/template"
	"masscan-monitor/internal/db"
	"net/http"
	"strings"
	"sync"
	"time"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)
type Dashboard struct {
	store      *db.Store
	accessHash string
	port       int
	clients map[chan string]struct{}
	mu      sync.Mutex
}
func NewDashboard(store *db.Store, accessHash string, port int) *Dashboard {
	return &Dashboard{
		store:      store,
		accessHash: accessHash,
		port:       port,
		clients:    make(map[chan string]struct{}),
	}
}
func (d *Dashboard) Start(ctx context.Context, eventsIn <-chan []db.PortEvent) error {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Get("/", d.handleIndex)
	r.Post("/login", d.handleLogin)
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("OK"))
	})
	r.Group(func(r chi.Router) {
		r.Use(d.authMiddleware)
		r.Get("/stats", d.handleStats)
		r.Get("/recent", d.handleRecent)
		r.Get("/ports", d.handlePorts)
		r.Get("/events", d.handleSSE)
	})
	server := &http.Server{
		Addr:    fmt.Sprintf("0.0.0.0:%d", d.port),
		Handler: r,
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case events, ok := <-eventsIn:
				if !ok {
					return
				}
				d.broadcastEvents(events)
			}
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	fmt.Printf("[DASHBOARD] Starting on http://0.0.0.0:%d\n", d.port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}
func (d *Dashboard) broadcastEvents(events []db.PortEvent) {
	tmpl := template.Must(template.New("recent_item").Parse(recentHTML))
	_, recent, _, _ := d.store.GetDashboardStats(context.Background())
	var buf bytes.Buffer
	tmpl.Execute(&buf, recent)
	html := buf.String()
	data := strings.ReplaceAll(html, "\n", "")
	d.mu.Lock()
	defer d.mu.Unlock()
	for client := range d.clients {
		select {
		case client <- data:
		default:
		}
	}
}
func (d *Dashboard) handleSSE(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	messageChan := make(chan string, 10)
	d.mu.Lock()
	d.clients[messageChan] = struct{}{}
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		delete(d.clients, messageChan)
		d.mu.Unlock()
	}()
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case msg := <-messageChan:
			fmt.Fprintf(w, "event: recent-update\ndata: %s\n\n", msg)
			flusher.Flush()
		}
	}
}
func (d *Dashboard) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie("session")
		if err != nil || cookie.Value != "authenticated" {
			w.Header().Set("HX-Redirect", "/")
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
func (d *Dashboard) handleIndex(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie("session")
	authenticated := err == nil && cookie.Value == "authenticated"
	tmpl := template.Must(template.New("index").Parse(indexHTML))
	tmpl.Execute(w, map[string]interface{}{
		"Authenticated": authenticated,
	})
}
func (d *Dashboard) handleLogin(w http.ResponseWriter, r *http.Request) {
	pass := r.FormValue("password")
	hash := sha256.Sum256([]byte(pass))
	if hex.EncodeToString(hash[:]) == d.accessHash {
		http.SetCookie(w, &http.Cookie{
			Name:  "session",
			Value: "authenticated",
			Path:  "/",
		})
		w.Header().Set("HX-Redirect", "/")
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte("Invalid password"))
	}
}
func (d *Dashboard) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, _, _, err := d.store.GetDashboardStats(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	tmpl := template.Must(template.New("stats").Parse(statsHTML))
	tmpl.Execute(w, stats)
}
func (d *Dashboard) handleRecent(w http.ResponseWriter, r *http.Request) {
	_, recent, _, err := d.store.GetDashboardStats(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	tmpl := template.Must(template.New("recent").Parse(recentHTML))
	tmpl.Execute(w, recent)
}
func (d *Dashboard) handlePorts(w http.ResponseWriter, r *http.Request) {
	_, _, ports, err := d.store.GetDashboardStats(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	tmpl := template.Must(template.New("ports").Parse(portsHTML))
	tmpl.Execute(w, ports)
}
const indexHTML = `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Masscan Monitor Dashboard</title>
    <link rel="stylesheet" href="https://unpkg.com/purecss@2.0.6/build/pure-min.css">
    <script src="https://unpkg.com/htmx.org@1.9.10"></script>
    <script src="https://unpkg.com/htmx.org/dist/ext/sse.js"></script>
    <style>
        body { background-color: #f4f7f6; color: #333; font-family: sans-serif; }
        .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
        .header { text-align: center; margin-bottom: 40px; }
        .card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 20px; }
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; }
        .stat-item { text-align: center; }
        .stat-value { font-size: 2em; font-weight: bold; color: #2c3e50; }
        .stat-label { color: #7f8c8d; }
        table { width: 100%; }
        .status-open { color: green; font-weight: bold; }
        .status-closed { color: red; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🛡️ Masscan Monitor</h1>
        </div>
        {{if .Authenticated}}
            <div class="stats-grid">
                <div class="card" hx-get="/stats" hx-trigger="load, every 5s">
                    Loading stats...
                </div>
            </div>
            <div class="card" hx-ext="sse" sse-connect="/events">
                <h3>Recent Activity (Events)</h3>
                <div id="recent-activity" hx-get="/recent" hx-trigger="load" sse-swap="recent-update">
                    Loading recent events...
                </div>
            </div>
            <div class="card">
                <h3>Open Ports Summary</h3>
                <div hx-get="/ports" hx-trigger="load, every 10s">
                    Loading ports...
                </div>
            </div>
        {{else}}
            <div class="card" style="max-width: 400px; margin: 0 auto;">
                <h3>Login</h3>
                <form class="pure-form pure-form-stacked" hx-post="/login">
                    <label for="password">Access Code</label>
                    <input type="password" name="password" id="password" class="pure-input-1">
                    <button type="submit" class="pure-button pure-button-primary pure-input-1">Enter</button>
                </form>
            </div>
        {{end}}
    </div>
</body>
</html>
`
const statsHTML = `
<div class="stat-item">
    <div class="stat-value">{{.TotalScans}}</div>
    <div class="stat-label">Total Scans</div>
</div>
<div class="stat-item">
    <div class="stat-value">{{.OpenPortsCount}}</div>
    <div class="stat-label">Currently Open Ports</div>
</div>
`
const recentHTML = `
<table class="pure-table pure-table-horizontal">
    <thead>
        <tr>
            <th>Time</th>
            <th>Event</th>
            <th>IP</th>
            <th>Port</th>
            <th>Banner Info</th>
        </tr>
    </thead>
    <tbody>
        {{range .}}
        <tr>
            <td>{{.DiscoveredAt.Format "2006-01-02 15:04:05"}}</td>
            <td>
                {{if eq .Type "NEW_PORT"}}<span class="status-open">NEW</span>
                {{else}}<span style="color: #e67e22; font-weight: bold;">CHANGE</span>{{end}}
            </td>
            <td>{{.IP}}</td>
            <td>{{.Port}}/{{.Proto}}</td>
            <td>
                {{if eq .Type "BANNER_CHANGED"}}
                    <div style="font-size: 0.8em; color: #7f8c8d;">Old: {{.OldBanner}}</div>
                    <div style="font-size: 0.8em; color: #2c3e50;">New: {{.NewBanner}}</div>
                {{else}}
                    <div style="font-size: 0.8em;">{{.NewBanner}}</div>
                {{end}}
            </td>
        </tr>
        {{else}}
        <tr><td colspan="5">No events yet</td></tr>
        {{end}}
    </tbody>
</table>
`
const portsHTML = `
<table class="pure-table pure-table-horizontal">
    <thead>
        <tr>
            <th>IP</th>
            <th>Port</th>
            <th>Last Seen</th>
            <th>Banner</th>
        </tr>
    </thead>
    <tbody>
        {{range .}}
        <tr>
            <td>{{.IP}}</td>
            <td>{{.Port}}/{{.Proto}}</td>
            <td>{{.LastSeenAt.Format "2006-01-02 15:04:05"}}</td>
            <td><small>{{.Banner}}</small></td>
        </tr>
        {{else}}
        <tr><td colspan="4">No open ports found</td></tr>
        {{end}}
    </tbody>
</table>
`
