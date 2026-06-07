package main
import (
	"os"
	"regexp"
	"testing"
)
func TestMain_TargetParsing(t *testing.T) {
	re := regexp.MustCompile(`^([^:]+):([^:\[]+)(?::\[(.*?)\])?(?::([^:]+))?(?::([^:]+))?$`)
	cases := []struct {
		input  string
		name   string
		target string
		ports  string
	}{
		{"Local:192.168.1.0/24:[80,443]:1h:2h", "Local", "192.168.1.0/24", "80,443"},
		{"Simple:10.0.0.1", "Simple", "10.0.0.1", ""},
		{"NoPorts:172.16.0.1:[]:10m", "NoPorts", "172.16.0.1", ""},
	}
	for _, c := range cases {
		m := re.FindStringSubmatch(c.input)
		if m == nil {
			t.Errorf("Failed to parse %s", c.input)
			continue
		}
		if m[1] != c.name {
			t.Errorf("Expected name %s, got %s", c.name, m[1])
		}
		if m[2] != c.target {
			t.Errorf("Expected target %s, got %s", c.target, m[2])
		}
		if m[3] != c.ports {
			t.Errorf("Expected ports %s, got %s", c.ports, m[3])
		}
	}
}
func TestLoadEnv_Complex(t *testing.T) {
	os.Setenv("SCAN_TARGETS", "existing")
	defer os.Unsetenv("SCAN_TARGETS")
	tmpFile := "test_complex.env"
	os.WriteFile(tmpFile, []byte("SCAN_TARGETS=new\nIGNORE=me"), 0644)
	defer os.Remove(tmpFile)
	loadEnv(tmpFile)
	if os.Getenv("SCAN_TARGETS") != "existing,new" {
		t.Errorf("Expected existing,new, got %s", os.Getenv("SCAN_TARGETS"))
	}
}
