package docker

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func fakeEngine(status int, body string, err error) *Engine {
	return &Engine{
		baseURL: "http://docker",
		http: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if err != nil {
				return nil, err
			}
			if r.URL.Path != "/_ping" || r.Method != http.MethodGet {
				return &http.Response{StatusCode: http.StatusNotFound, Body: io.NopCloser(strings.NewReader(""))}, nil
			}
			return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body))}, nil
		})},
	}
}

func TestPing(t *testing.T) {
	tests := []struct {
		name    string
		engine  *Engine
		wantErr bool
	}{
		{"ok", fakeEngine(200, "OK", nil), false},
		{"ok with newline", fakeEngine(200, "OK\n", nil), false},
		{"server error", fakeEngine(500, "boom", nil), true},
		{"unexpected body", fakeEngine(200, "nope", nil), true},
		{"transport error", fakeEngine(0, "", errors.New("connection refused")), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.engine.Ping(context.Background())
			if (err != nil) != tt.wantErr {
				t.Fatalf("Ping error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPingHonorsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	e := &Engine{baseURL: "http://docker", http: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return nil, r.Context().Err()
	})}}
	if err := e.Ping(ctx); err == nil {
		t.Fatal("expected error for canceled context")
	}
}

func TestNewEngine(t *testing.T) {
	valid := []string{"unix:///var/run/docker.sock", "tcp://127.0.0.1:2375", "tcp://localhost:2375"}
	invalid := []string{"", "unix://", "tcp://10.0.0.5:2375", "tcp://docker.example.com:2375", "http://127.0.0.1:2375", "npipe:////./pipe/docker"}
	for _, h := range valid {
		e, err := NewEngine(h)
		if err != nil {
			t.Errorf("NewEngine(%q) error = %v", h, err)
			continue
		}
		_ = e.Close()
	}
	for _, h := range invalid {
		if _, err := NewEngine(h); err == nil {
			t.Errorf("NewEngine(%q) succeeded, want error", h)
		}
	}
}
