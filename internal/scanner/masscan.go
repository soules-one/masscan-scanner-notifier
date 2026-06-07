package scanner
import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)
type MasscanRecord struct {
	IP     string `json:"ip"`
	Port   int    `json:"port"`
	Proto  string `json:"proto"`
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
	Banner string `json:"banner,omitempty"`
}
type masscanRawLine struct {
	IP    string `json:"ip"`
	Ports []struct {
		Port    int    `json:"port"`
		Proto   string `json:"proto"`
		Status string `json:"status"`
		Reason string `json:"reason,omitempty"`
		Service struct {
			Banner string `json:"banner,omitempty"`
		} `json:"service,omitempty"`
	} `json:"ports"`
}
type Masscan struct {
	binary string
	rate   int
	ports  string
}
func NewMasscan(binary string, rate int, ports string) *Masscan {
	return &Masscan{binary: binary, rate: rate, ports: ports}
}
func (m *Masscan) Run(ctx context.Context, target string) ([]MasscanRecord, error) {
	tmp, err := os.CreateTemp("", "masscan-*.json")
	if err != nil {
		return nil, err
	}
	tmp.Close()
	defer os.Remove(tmp.Name())
	args := []string{
		target,
		"-p", m.ports,
		"--rate", fmt.Sprintf("%d", m.rate),
		"--banners",
		"--output-format", "json",
		"--output-filename", tmp.Name(),
		"--wait", "3",
	}
	cmd := exec.CommandContext(ctx, m.binary, args...)
	if err := cmd.Run(); err != nil && !strings.Contains(err.Error(), "exit status 1") {
		return nil, err
	}
	f, _ := os.Open(tmp.Name())
	defer f.Close()
	return parseMasscanLines(f)
}
func parseMasscanLines(r io.Reader) ([]MasscanRecord, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	aggregated := make(map[string]MasscanRecord)
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(strings.TrimRight(line, ","))
		if line == "" || line == "[" || line == "]" {
			continue
		}
		var raw masscanRawLine
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			continue
		}
		for _, p := range raw.Ports {
			key := fmt.Sprintf("%s:%d", raw.IP, p.Port)
			record, exists := aggregated[key]
			if !exists {
				record = MasscanRecord{
					IP:     raw.IP,
					Port:   p.Port,
					Proto:  p.Proto,
					Status: p.Status,
					Reason: p.Reason,
				}
			}
			if p.Service.Banner != "" {
				record.Banner = p.Service.Banner
			}
			if p.Status != "" {
				record.Status = p.Status
			}
			aggregated[key] = record
		}
	}
	result := make([]MasscanRecord, 0, len(aggregated))
	for _, rec := range aggregated {
		result = append(result, rec)
	}
	return result, nil
}
