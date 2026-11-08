package logging

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestNewJSONFiltersByLevel(t *testing.T) {
	var buf bytes.Buffer
	logger, err := New(&buf, "info", "json")
	if err != nil {
		t.Fatal(err)
	}
	logger.Debug("hidden")
	logger.Info("shown", "key", "value")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("got %d log lines, want 1: %q", len(lines), buf.String())
	}
	var rec map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("log line is not JSON: %v", err)
	}
	if rec["msg"] != "shown" || rec["key"] != "value" {
		t.Fatalf("unexpected record: %v", rec)
	}
}

func TestNewTextFormat(t *testing.T) {
	var buf bytes.Buffer
	logger, err := New(&buf, "DEBUG", "text")
	if err != nil {
		t.Fatal(err)
	}
	logger.Debug("hello")
	if !strings.Contains(buf.String(), "msg=hello") {
		t.Fatalf("unexpected text output: %q", buf.String())
	}
}

func TestNewRejectsInvalidInput(t *testing.T) {
	if _, err := New(&bytes.Buffer{}, "loud", "json"); err == nil {
		t.Error("expected error for invalid level")
	}
	if _, err := New(&bytes.Buffer{}, "info", "xml"); err == nil {
		t.Error("expected error for invalid format")
	}
}
