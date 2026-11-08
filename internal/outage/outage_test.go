package outage

import (
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var t0 = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

func TestSinceIsTheLatestMark(t *testing.T) {
	tr := New()
	if !tr.Since().IsZero() {
		t.Fatalf("a store that never failed: %v", tr.Since())
	}
	tr.Mark(t0.Add(5 * time.Second))
	tr.Mark(t0) // an older mark, arriving late, changes nothing
	if got := tr.Since(); !got.Equal(t0.Add(5 * time.Second)) {
		t.Fatalf("Since = %v", got)
	}
	tr.Mark(t0.Add(9 * time.Second))
	if got := tr.Since(); !got.Equal(t0.Add(9 * time.Second)) {
		t.Fatalf("Since = %v", got)
	}
}

// A nil tracker is no tracker: loops without one (tests, a single-purpose tool)
// need no check of their own.
func TestANilTrackerIsSafe(t *testing.T) {
	var tr *Tracker
	tr.Mark(t0)
	if !tr.Since().IsZero() {
		t.Fatal("a nil tracker remembers")
	}
}

func TestMarksFromManyLoopsKeepTheLatest(t *testing.T) {
	tr := New()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tr.Mark(t0.Add(time.Duration(i) * time.Second))
			_ = tr.Since()
		}()
	}
	wg.Wait()
	if got := tr.Since(); !got.Equal(t0.Add(49 * time.Second)) {
		t.Fatalf("Since = %v, want the latest of 50 concurrent marks", got)
	}
}

// Everything that judges nodes shares this package, so it must not depend on any
// of them.
func TestOutageImportsOnlyTheStandardLibrary(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		checked++
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, spec := range parsed.Imports {
			path, _ := strconv.Unquote(spec.Path.Value)
			if first, _, _ := strings.Cut(path, "/"); strings.Contains(first, ".") {
				t.Errorf("%s imports %s", file, path)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no source files were checked")
	}
}
