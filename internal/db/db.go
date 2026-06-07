package db
import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"masscan-monitor/internal/app"
	"time"
	_ "github.com/mattn/go-sqlite3"
)
const schemaSQL = `
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
`
const tablesSQL = `
CREATE TABLE IF NOT EXISTS scan_batches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_json TEXT NOT NULL,
    started_at   DATETIME NOT NULL,
    ended_at     DATETIME NOT NULL,
    error_text   TEXT
);
CREATE TABLE IF NOT EXISTS port_history (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    ip                 TEXT NOT NULL,
    port               INTEGER NOT NULL,
    proto              TEXT NOT NULL,
    status             TEXT NOT NULL,
    banner             TEXT,
    first_seen_at      DATETIME NOT NULL,
    last_seen_at       DATETIME NOT NULL,
    last_scan_batch_id INTEGER NOT NULL,
    FOREIGN KEY (last_scan_batch_id) REFERENCES scan_batches(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_port_history_unique
ON port_history(ip, port, proto);
CREATE INDEX IF NOT EXISTS idx_port_history_last_seen
ON port_history(last_seen_at DESC);
CREATE TABLE IF NOT EXISTS event_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type  TEXT NOT NULL,
    ip          TEXT NOT NULL,
    port        INTEGER NOT NULL,
    proto       TEXT NOT NULL,
    old_banner  TEXT,
    new_banner  TEXT,
    timestamp   DATETIME NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_event_log_timestamp ON event_log(timestamp DESC);
`
type Store struct {
	db *sql.DB
}
type PortRecord struct {
	ID              int64     `json:"id"`
	IP              string    `json:"ip"`
	Port            int       `json:"port"`
	Proto           string    `json:"proto"`
	Status          string    `json:"status"`
	Banner          string    `json:"banner,omitempty"`
	FirstSeenAt     time.Time `json:"first_seen_at"`
	LastSeenAt      time.Time `json:"last_seen_at"`
	LastScanBatchID int64     `json:"last_scan_batch_id"`
}
type EventType string
const (
	EventTypeNewPort       EventType = "NEW_PORT"
	EventTypeBannerChanged EventType = "BANNER_CHANGED"
)
type PortEvent struct {
	Type         EventType `json:"type"`
	IP           string    `json:"ip"`
	Port         int       `json:"port"`
	Proto        string    `json:"proto"`
	Status       string    `json:"status"`
	OldBanner    string    `json:"old_banner,omitempty"`
	NewBanner    string    `json:"new_banner,omitempty"`
	DiscoveredAt time.Time `json:"discovered_at"`
}
type NewPortRecord = PortEvent
func Open(path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("empty sqlite path")
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	if _, err := db.Exec(schemaSQL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}
	if _, err := db.Exec(tablesSQL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("create tables: %w", err)
	}
	migrations := []struct {
		table  string
		column string
		typeDef string
	}{
		{"port_history", "banner", "TEXT"},
	}
	for _, m := range migrations {
		var exists int
		query := fmt.Sprintf("SELECT count(*) FROM pragma_table_info('%s') WHERE name='%s'", m.table, m.column)
		err := db.QueryRow(query).Scan(&exists)
		if err != nil {
			log.Printf("[DB WARNING] Failed to check column %s.%s: %v", m.table, m.column, err)
			continue
		}
		if exists == 0 {
			log.Printf("[DB MIGRATION] Adding missing column %s to table %s", m.column, m.table)
			alterQuery := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", m.table, m.column, m.typeDef)
			if _, err := db.Exec(alterQuery); err != nil {
				log.Printf("[DB ERROR] Failed to add column %s.%s: %v", m.table, m.column, err)
			}
		}
	}
	return &Store{db: db}, nil
}
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}
func (s *Store) ProcessBatch(ctx context.Context, batch app.RawScanBatch) ([]PortEvent, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("store is not initialized")
	}
	if batch.StartedAt.IsZero() {
		batch.StartedAt = time.Now().UTC()
	}
	if batch.EndedAt.IsZero() {
		batch.EndedAt = batch.StartedAt
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()
	reqJSON, err := json.Marshal(batch.Request)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	var errText *string
	if batch.Err != nil {
		msg := batch.Err.Error()
		errText = &msg
	}
	res, err := tx.ExecContext(ctx, `
        INSERT INTO scan_batches (request_json, started_at, ended_at, error_text)
        VALUES (?, ?, ?, ?)
    `, string(reqJSON), toSQLiteTime(batch.StartedAt), toSQLiteTime(batch.EndedAt), errText)
	if err != nil {
		return nil, fmt.Errorf("insert scan batch: %w", err)
	}
	batchID, err := res.LastInsertId()
	if err != nil {
		return nil, fmt.Errorf("last insert id: %w", err)
	}
	var events []PortEvent
	processedIPs := make(map[string]struct{})
	for _, rec := range batch.Records {
		if rec.IP == "" {
			continue
		}
		processedIPs[rec.IP] = struct{}{}
		if rec.Port <= 0 || rec.Proto == "" {
			continue
		}
		var oldStatus, oldBanner string
		err = tx.QueryRowContext(ctx, `
                SELECT status, COALESCE(banner, '')
                FROM port_history
                WHERE ip = ? AND port = ? AND proto = ?
                LIMIT 1
            `, rec.IP, rec.Port, rec.Proto).Scan(&oldStatus, &oldBanner)
		isNew := false
		bannerChanged := false
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				isNew = true
			} else {
				return nil, fmt.Errorf("check existing record: %w", err)
			}
		} else {
			if rec.Banner != "" && oldBanner != "" && rec.Banner != oldBanner {
				bannerChanged = true
			}
		}
		_, err = tx.ExecContext(ctx, `
                INSERT INTO port_history (
                    ip, port, proto, status, banner, first_seen_at, last_seen_at, last_scan_batch_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(ip, port, proto)
                DO UPDATE SET
                    status = excluded.status,
                    banner = CASE WHEN excluded.banner != '' THEN excluded.banner ELSE port_history.banner END,
                    last_seen_at = excluded.last_seen_at,
                    last_scan_batch_id = excluded.last_scan_batch_id
            `,
			rec.IP, rec.Port, rec.Proto, rec.Status, rec.Banner,
			toSQLiteTime(batch.EndedAt), toSQLiteTime(batch.EndedAt), batchID,
		)
		if err != nil {
			return nil, fmt.Errorf("upsert port history: %w", err)
		}
		if isNew {
			ev := PortEvent{
				Type:         EventTypeNewPort,
				IP:           rec.IP,
				Port:         rec.Port,
				Proto:        rec.Proto,
				Status:       rec.Status,
				NewBanner:    rec.Banner,
				DiscoveredAt: batch.EndedAt.UTC(),
			}
			events = append(events, ev)
			_, _ = tx.ExecContext(ctx, `
                INSERT INTO event_log (event_type, ip, port, proto, new_banner, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            `, ev.Type, ev.IP, ev.Port, ev.Proto, ev.NewBanner, toSQLiteTime(ev.DiscoveredAt))
		} else if bannerChanged {
			ev := PortEvent{
				Type:         EventTypeBannerChanged,
				IP:           rec.IP,
				Port:         rec.Port,
				Proto:        rec.Proto,
				Status:       rec.Status,
				OldBanner:    oldBanner,
				NewBanner:    rec.Banner,
				DiscoveredAt: batch.EndedAt.UTC(),
			}
			events = append(events, ev)
			_, _ = tx.ExecContext(ctx, `
                INSERT INTO event_log (event_type, ip, port, proto, old_banner, new_banner, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            `, ev.Type, ev.IP, ev.Port, ev.Proto, ev.OldBanner, ev.NewBanner, toSQLiteTime(ev.DiscoveredAt))
		}
	}
	if len(processedIPs) > 0 {
		stmt, err := tx.PrepareContext(ctx, `
            UPDATE port_history
            SET status = 'closed',
                last_seen_at = ?,
                last_scan_batch_id = ?
            WHERE ip = ? AND last_scan_batch_id != ? AND status != 'closed'
        `)
		if err != nil {
			return nil, fmt.Errorf("prepare closure stmt: %w", err)
		}
		defer stmt.Close()
		endTimeStr := toSQLiteTime(batch.EndedAt)
		for ip := range processedIPs {
			_, err := stmt.ExecContext(ctx, endTimeStr, batchID, ip, batchID)
			if err != nil {
				return nil, fmt.Errorf("close missing ports for ip %s: %w", ip, err)
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `
        DELETE FROM port_history
        WHERE last_seen_at < datetime('now', '-365 day')
    `); err != nil {
		return nil, fmt.Errorf("cleanup expired after batch: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}
	return events, nil
}
type Stats struct {
	TotalScans     int `json:"total_scans"`
	OpenPortsCount int `json:"open_ports_count"`
}
func (s *Store) GetDashboardStats(ctx context.Context) (Stats, []PortEvent, []PortRecord, error) {
	var stats Stats
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM scan_batches").Scan(&stats.TotalScans)
	if err != nil {
		return stats, nil, nil, err
	}
	err = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM port_history WHERE status = 'open'").Scan(&stats.OpenPortsCount)
	if err != nil {
		return stats, nil, nil, err
	}
	rows, err := s.db.QueryContext(ctx, `
        SELECT event_type, ip, port, proto, COALESCE(old_banner, ''), COALESCE(new_banner, ''), timestamp 
        FROM event_log 
        ORDER BY timestamp DESC LIMIT 10
    `)
	if err != nil {
		return stats, nil, nil, err
	}
	defer rows.Close()
	var recentEvents []PortEvent
	for rows.Next() {
		var e PortEvent
		var ts string
		if err := rows.Scan(&e.Type, &e.IP, &e.Port, &e.Proto, &e.OldBanner, &e.NewBanner, &ts); err != nil {
			return stats, nil, nil, err
		}
		e.DiscoveredAt, _ = parseSQLiteTime(ts)
		recentEvents = append(recentEvents, e)
	}
	openPorts, err := s.ListOpenPorts(ctx)
	if err != nil {
		return stats, recentEvents, nil, err
	}
	return stats, recentEvents, openPorts, nil
}
func (s *Store) ListOpenPorts(ctx context.Context) ([]PortRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
        SELECT id, ip, port, proto, status, COALESCE(banner, ''), first_seen_at, last_seen_at, last_scan_batch_id
        FROM port_history
        WHERE status = 'open'
        ORDER BY last_seen_at DESC, ip ASC, port ASC
    `)
	if err != nil {
		return nil, fmt.Errorf("query open ports: %w", err)
	}
	defer rows.Close()
	var out []PortRecord
	for rows.Next() {
		var r PortRecord
		var firstSeen, lastSeen string
		if err := rows.Scan(&r.ID, &r.IP, &r.Port, &r.Proto, &r.Status, &r.Banner, &firstSeen, &lastSeen, &r.LastScanBatchID); err != nil {
			return nil, err
		}
		r.FirstSeenAt, _ = parseSQLiteTime(firstSeen)
		r.LastSeenAt, _ = parseSQLiteTime(lastSeen)
		out = append(out, r)
	}
	return out, nil
}
func (s *Store) ListHistory(ctx context.Context) ([]PortRecord, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("store is not initialized")
	}
	rows, err := s.db.QueryContext(ctx, `
        SELECT id, ip, port, proto, status, COALESCE(banner, ''), first_seen_at, last_seen_at, last_scan_batch_id
        FROM port_history
        ORDER BY last_seen_at DESC, ip ASC, port ASC
    `)
	if err != nil {
		return nil, fmt.Errorf("query history: %w", err)
	}
	defer rows.Close()
	var out []PortRecord
	for rows.Next() {
		var r PortRecord
		var firstSeen string
		var lastSeen string
		if err := rows.Scan(
			&r.ID,
			&r.IP,
			&r.Port,
			&r.Proto,
			&r.Status,
			&r.Banner,
			&firstSeen,
			&lastSeen,
			&r.LastScanBatchID,
		); err != nil {
			return nil, fmt.Errorf("scan history row: %w", err)
		}
		r.FirstSeenAt, _ = parseSQLiteTime(firstSeen)
		r.LastSeenAt, _ = parseSQLiteTime(lastSeen)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return out, nil
}
func (s *Store) CleanupExpired(ctx context.Context) error {
	if s == nil || s.db == nil {
		return errors.New("store is not initialized")
	}
	_, err := s.db.ExecContext(ctx, `
        DELETE FROM port_history
        WHERE last_seen_at < datetime('now', '-365 day')
	`)
	if err != nil {
		return fmt.Errorf("cleanup expired: %w", err)
	}
	return nil
}
func toSQLiteTime(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05")
}
func parseSQLiteTime(v string) (time.Time, error) {
	layouts := []string{
		"2006-01-02 15:04:05",
		time.RFC3339,
		time.RFC3339Nano,
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, v); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported sqlite datetime format: %q", v)
}
