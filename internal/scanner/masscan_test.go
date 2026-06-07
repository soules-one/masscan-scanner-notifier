package scanner
import (
	"strings"
	"testing"
)
func TestParseMasscanLines_Complex(t *testing.T) {
	input := `
[
{ "ip": "1.1.1.1", "ports": [ {"port": 80, "proto": "tcp", "status": "open"} ] },
{ "ip": "1.1.1.1", "ports": [ {"port": 80, "proto": "tcp", "service": {"banner": "nginx"}} ] },
{ "ip": "1.1.1.1", "ports": [ {"port": 443, "proto": "tcp", "status": "open"} ] },
{ "ip": "2.2.2.2", "ports": [ {"port": 22, "proto": "tcp", "status": "open"} ] }
]
`
	records, _ := parseMasscanLines(strings.NewReader(input))
	if len(records) != 3 {
		t.Errorf("Expected 3 aggregated records, got %d", len(records))
	}
	for _, r := range records {
		if r.IP == "1.1.1.1" && r.Port == 80 && r.Banner != "nginx" {
			t.Error("Failed to aggregate banner for 1.1.1.1:80")
		}
	}
}
func TestParseMasscanLines_Invalid(t *testing.T) {
	input := `
[
{ "invalid": "json" },
,
{ "ip": "1.1.1.1", "ports": [ {"port": 80} ] }
]
`
	records, _ := parseMasscanLines(strings.NewReader(input))
	if len(records) != 1 {
		t.Errorf("Expected 1 valid record, got %d", len(records))
	}
}
