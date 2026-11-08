package metrics

import (
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Every layer reports through this package, so it must not depend on any of
// them: it imports the standard library only, and the state of the cluster is
// handed to it by collectors the callers register.
func TestMetricsImportsOnlyTheStandardLibrary(t *testing.T) {
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
	if checked < 2 {
		t.Fatalf("checked only %d source files; the test is not looking at the package", checked)
	}
}
