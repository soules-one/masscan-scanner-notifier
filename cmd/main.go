package main
import (
	"bufio"
	"context"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
	"masscan-monitor/internal/app"
	"masscan-monitor/internal/common"
	"masscan-monitor/internal/dashboard"
	"masscan-monitor/internal/db"
	"masscan-monitor/internal/vk"
)
func main() {
	loadEnv("config.env")
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	dbPath := getEnv("DB_PATH", "./scanner.db")
	absPath, _ := filepath.Abs(dbPath)
	sdb, err := db.Open(absPath)
	if err != nil {
		log.Printf("[DB ERROR] Failed to open DB at %s: %v", absPath, err)
		log.Printf("[DB] Trying fallback to /tmp/scanner.db")
		absPath = "/tmp/scanner.db"
		sdb, err = db.Open(absPath)
		if err != nil {
			log.Fatalf("[DB CRITICAL] Failed to open fallback DB at %s: %v", absPath, err)
		}
	}
	log.Printf("[DB] Initialized at: %s", absPath)
	defer sdb.Close()
	requests := make(chan app.ScanRequest, 128)
	rawResults := make(chan app.RawScanBatch, 128)
	eventsIn := make(chan []db.PortEvent, 128)
	masscanBin := getEnv("MASSCAN_BINARY", "/usr/bin/masscan")
	svc := app.NewService(masscanBin, 2, requests, rawResults)
	go svc.Run(ctx)
	numNotifiers := 0
	vkEnabled := getEnv("VK_ENABLED", "false") == "true"
	if vkEnabled {
		numNotifiers++
	}
	totalConsumers := numNotifiers + 1
	broadOuts := common.Tee(ctx, eventsIn, totalConsumers, 10*time.Second)
	idx := 0
	if vkEnabled {
		vk.VKToken = getEnv("VK_TOKEN", "")
		vk.VKGroupID = getEnv("VK_GROUP_ID", "")
		userIDsRaw := getEnv("VK_USER_IDS", "")
		var userIDs []int
		for _, idStr := range strings.Split(userIDsRaw, ",") {
			if id, err := strconv.Atoi(strings.TrimSpace(idStr)); err == nil {
				userIDs = append(userIDs, id)
			}
		}
		vk.StartVKNotifier(ctx, broadOuts[idx], userIDs)
		idx++
	}
	portStr := getEnv("SERVER_PORT", "8081")
	dashPort, err := strconv.Atoi(portStr)
	if err != nil {
		log.Printf("Invalid SERVER_PORT %q, using 8081: %v", portStr, err)
		dashPort = 8081
	}
	accessHash := getEnv("DASHBOARD_ACCESS_HASH", "")
	dash := dashboard.NewDashboard(sdb, accessHash, dashPort)
	go func() {
		log.Printf("[DASHBOARD] Initializing on port %d...", dashPort)
		if err := dash.Start(ctx, broadOuts[idx]); err != nil {
			log.Printf("Dashboard error: %v", err)
		}
	}()
	scheduler := app.NewScheduler(requests)
	if getEnv("SCHEDULE_ENABLED", "false") == "true" {
		targetsRaw := getEnv("SCAN_TARGETS", "")
		globalPorts := getEnv("MASSCAN_PORTS", "80,443")
		globalRate, _ := strconv.Atoi(getEnv("MASSCAN_RATE", "1000"))
		targetsRaw = strings.ReplaceAll(targetsRaw, "\n", ",")
		var rawJobs []string
		var current strings.Builder
		inBrackets := false
		for _, r := range targetsRaw {
			if r == '[' {
				inBrackets = true
			} else if r == ']' {
				inBrackets = false
			}
			if (r == ',' || r == ';') && !inBrackets {
				if current.Len() > 0 {
					rawJobs = append(rawJobs, strings.TrimSpace(current.String()))
					current.Reset()
				}
			} else {
				current.WriteRune(r)
			}
		}
		if current.Len() > 0 {
			rawJobs = append(rawJobs, strings.TrimSpace(current.String()))
		}
		for _, rawJob := range rawJobs {
			if rawJob == "" {
				continue
			}
			re := regexp.MustCompile(`^([^:]+):([^:\[]+)(?::\[(.*?)\])?(?::([^:]+))?(?::([^:]+))?$`)
			m := re.FindStringSubmatch(rawJob)
			if m == nil {
				log.Printf("Failed to parse scan target: %s", rawJob)
				continue
			}
			name := m[1]
			target := m[2]
			targetPorts := m[3]
			intervalStr := m[4]
			timeoutStr := m[5]
			if targetPorts == "" {
				targetPorts = globalPorts
			}
			interval := 1 * time.Hour
			if intervalStr != "" {
				if d, err := time.ParseDuration(intervalStr); err == nil {
					interval = d
				}
			}
			timeout := 2 * time.Hour
			if timeoutStr != "" {
				if d, err := time.ParseDuration(timeoutStr); err == nil {
					timeout = d
				}
			}
			scheduler.StartJob(ctx, app.ScheduleJob{
				Name:     name,
				Target:   target,
				Ports:    targetPorts,
				Rate:     globalRate,
				Interval: interval,
				Timeout:  timeout,
			})
			log.Printf("Scheduled job %s for %s with ports %s", name, target, targetPorts)
		}
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case batch := <-rawResults:
				events, err := sdb.ProcessBatch(ctx, batch)
				if err != nil {
					log.Printf("Error processing batch: %v", err)
					continue
				}
				if len(events) > 0 {
					for _, ev := range events {
						prefix := "[NEW]"
						if ev.Type == db.EventTypeBannerChanged {
							prefix = "[CHANGE]"
						}
						log.Printf("%s %s:%d (%s) - Banner: %s",
							prefix, ev.IP, ev.Port, ev.Proto, ev.NewBanner)
					}
					eventsIn <- events
				}
				log.Printf("Scan completed for %s: %d events triggered", batch.Request.Target, len(events))
			}
		}
	}()
	log.Println("Masscan Monitor is running. Press Ctrl+C to stop.")
	<-ctx.Done()
	log.Println("Shutting down...")
	time.Sleep(500 * time.Millisecond)
}
func loadEnv(path string) {
	file, err := os.Open(path)
	if err != nil {
		log.Printf("Warning: could not open config file %s: %v", path, err)
		return
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			key := parts[0]
			val := parts[1]
			if key == "SCAN_TARGETS" {
				if existing := os.Getenv(key); existing != "" {
					os.Setenv(key, existing+","+val)
					continue
				}
			}
			os.Setenv(key, val)
		}
	}
}
func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
