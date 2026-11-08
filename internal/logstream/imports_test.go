package logstream

import (
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// The package moves log lines between the agents and the readers and knows
// nothing of gRPC, Docker or HTTP, which is what lets both the manager and the
// agent use it: it imports the standard library only, and the transports plug in
// through the Conn interfaces.
func TestLogstreamImportsOnlyTheStandardLibrary(t *testing.T) {
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
			t.Fatalf("parse %s: %v", file, err)
		}
		for _, spec := range parsed.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if first, _, _ := strings.Cut(path, "/"); strings.Contains(first, ".") {
				t.Errorf("%s imports %s: this package may import only the standard library", file, path)
			}
		}
	}
	if checked < 3 {
		t.Fatalf("checked only %d source files; the test is not looking at the package", checked)
	}
}
