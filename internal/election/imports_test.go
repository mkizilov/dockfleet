package election

import (
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// The election decides who leads and knows nothing of what leading means: etcd,
// the database and the loops are all handed to it. Every layer that acts under an
// epoch imports it (the loops, the protocol, the API), so it must import none of
// them, nor anything but the standard library.
func TestElectionImportsOnlyTheStandardLibrary(t *testing.T) {
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
	if checked == 0 {
		t.Fatal("no source files were checked; the test is not looking at the package")
	}
}
