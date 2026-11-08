package store

import (
	"context"
	"testing"
)

func TestMemoryConformance(t *testing.T) {
	runStoreConformance(t, NewMemory())
}

func TestMemoryDoesNotAliasLabels(t *testing.T) {
	m := NewMemory()
	labels := map[string]string{"a": "1"}
	if _, err := m.UpsertNode(context.Background(), Node{ID: "n", Labels: labels}); err != nil {
		t.Fatal(err)
	}
	labels["a"] = "mutated"
	got, err := m.GetNode(context.Background(), "n")
	if err != nil {
		t.Fatal(err)
	}
	if got.Labels["a"] != "1" {
		t.Fatalf("store aliased caller's map: %v", got.Labels)
	}
}

func TestValidName(t *testing.T) {
	valid := []string{"a", "node-1", "web.api_2", "A1"}
	invalid := []string{"", "-a", ".a", "a b", "a/b", "a;drop", "ünï"}
	for _, s := range valid {
		if !ValidName(s) {
			t.Errorf("ValidName(%q) = false, want true", s)
		}
	}
	for _, s := range invalid {
		if ValidName(s) {
			t.Errorf("ValidName(%q) = true, want false", s)
		}
	}
	long := make([]byte, 64)
	for i := range long {
		long[i] = 'a'
	}
	if ValidName(string(long)) {
		t.Error("64-character name should be rejected")
	}
}
