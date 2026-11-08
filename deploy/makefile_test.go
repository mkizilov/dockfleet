package deploy

import (
	"regexp"
	"strings"
	"testing"
)

// make starts a recipe line that has no shell syntax in it by running the
// program directly, and looks the program up on the PATH make was started with,
// not on the PATH the Makefile exports. A tool the Makefile installs into
// .tools/bin therefore has to be named by its path, or the recipe works only on
// a machine whose shell already had .tools/bin on its PATH, which is exactly the
// machine nobody needs the Makefile for. (A clean machine found this: `make
// proto` stopped with "buf: No such file or directory".)
func TestRecipesNameInstalledToolsByPath(t *testing.T) {
	installed := map[string]bool{
		"buf": true, "protoc-gen-go": true, "protoc-gen-go-grpc": true,
		"govulncheck": true, "actionlint": true, "gitleaks": true,
		"shellcheck": true, "hadolint": true, "check-jsonschema": true,
	}
	recipe := regexp.MustCompile(`^\t[@+-]*\s*(\S+)`)
	seen := 0
	for n, line := range strings.Split(read(t, "Makefile"), "\n") {
		m := recipe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		seen++
		if installed[m[1]] {
			t.Errorf("Makefile line %d runs %q by bare name; use $(TOOLS_BIN)/%s (or $(VENV)/bin/%s)", n+1, m[1], m[1], m[1])
		}
	}
	if seen < 30 {
		t.Fatalf("looked at only %d recipe lines; the test is not reading the Makefile", seen)
	}
}
