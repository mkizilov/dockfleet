package deploy

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The project is published under the MIT license. These tests keep the license
// text, every statement about the license and what the container images carry
// in agreement, so that none of them can drift on its own.

// mitText is the license text as the SPDX license list gives it, with the
// copyright line the project uses.
const mitText = `MIT License

Copyright (c) 2026 Mykyta Kizilov

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.`

func squash(s string) string { return strings.Join(strings.Fields(s), " ") }

func TestLicenseIsMITWithTheOwnersCopyright(t *testing.T) {
	text := read(t, "LICENSE")
	if squash(text) != squash(mitText) {
		t.Fatal("LICENSE is not the MIT license text with the copyright line `Copyright (c) 2026 Mykyta Kizilov`")
	}
	if !strings.HasPrefix(text, "MIT License\n\nCopyright (c) 2026 Mykyta Kizilov\n\n") || !strings.HasSuffix(text, "SOFTWARE.\n") {
		t.Error("LICENSE should start with the title and the copyright line and end with a newline")
	}
	if strings.Contains(text, "\r") {
		t.Error("LICENSE contains carriage returns")
	}
}

func TestEveryStatementAboutTheLicenseAgrees(t *testing.T) {
	readme := read(t, "README.md")
	i := strings.Index(readme, "\n## License\n")
	if i < 0 {
		t.Fatal("README.md has no License section")
	}
	section := readme[i:]
	if next := strings.Index(section[len("\n## License\n"):], "\n## "); next >= 0 {
		section = section[:len("\n## License\n")+next]
	}
	for _, want := range []string{"MIT", "(LICENSE)"} {
		if !strings.Contains(section, want) {
			t.Errorf("the README's License section does not contain %q:\n%s", want, section)
		}
	}
	if strings.Contains(section, "THIRD_PARTY_NOTICES.md") {
		t.Error("the README's License section should describe only the project license")
	}
	// Statements from before the license existed, whatever the quoting around the file name.
	stale := regexp.MustCompile(`(?i)no license (has been chosen|file)|no ` + "`?LICENSE`?" + ` file|not yet licensed|license (file )?is added outside|license is scheduled|has not (chosen|picked) a license`)
	for _, f := range []string{"README.md", "ROADMAP.md", "docs/verification.md", "docs/limitations.md", "docs/deployment.md", "docs/architecture.md", "AGENTS.md"} {
		if m := stale.FindString(read(t, f)); m != "" {
			t.Errorf("%s still says %q", f, m)
		}
	}

	// The image labels name the same license.
	docker := read(t, "Dockerfile")
	labels := regexp.MustCompile(`org\.opencontainers\.image\.licenses="([^"]+)"`).FindAllStringSubmatch(docker, -1)
	if len(labels) != 2 {
		t.Fatalf("the Dockerfile sets %d license labels, want one per image", len(labels))
	}
	for _, l := range labels {
		if l[1] != "MIT" {
			t.Errorf("an image is labeled with the license %q, want MIT", l[1])
		}
	}

	// Nothing the project owns claims another license or reserves all rights.
	// Generated and third-party text is exempt: it is not the project's to relicense.
	forbidden := regexp.MustCompile(`(?i)all rights reserved|GNU (Affero |Lesser )?General Public License|Licensed under the Apache License|SPDX-License-Identifier: *(GPL|AGPL|LGPL|Apache|BSD|MPL)`)
	walk(t, func(rel, text string) {
		if rel == "THIRD_PARTY_NOTICES.md" || rel == "scripts/notices/main.go" || rel == "scripts/notices/notices_test.go" || rel == "deploy/license_test.go" {
			return
		}
		if m := forbidden.FindString(text); m != "" {
			t.Errorf("%s says %q, but the project is under the MIT license", rel, m)
		}
	})
}

// contextCopies lists the sources of the COPY instructions that read from the
// build context (not from another stage).
func contextCopies(dockerfile string) []string {
	var sources []string
	for _, line := range strings.Split(dockerfile, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 || fields[0] != "COPY" {
			continue
		}
		args := fields[1:]
		fromStage := false
		var paths []string
		for _, a := range args {
			if strings.HasPrefix(a, "--from=") {
				fromStage = true
			} else if !strings.HasPrefix(a, "--") {
				paths = append(paths, a)
			}
		}
		if !fromStage && len(paths) >= 2 {
			sources = append(sources, paths[:len(paths)-1]...)
		}
	}
	return sources
}

// An image build copies from a context that .dockerignore has emptied of
// everything not named. A COPY of something that is ignored fails only when the
// image is built, which nothing here can do, so the two files are checked against
// each other.
func TestEveryFileTheDockerfileCopiesIsLetThroughByDockerignore(t *testing.T) {
	docker, ignore := read(t, "Dockerfile"), read(t, ".dockerignore")
	var exceptions []string
	for _, line := range strings.Split(ignore, "\n") {
		if strings.HasPrefix(line, "!") {
			exceptions = append(exceptions, strings.TrimPrefix(line, "!"))
		}
	}
	sources := contextCopies(docker)
	if len(sources) < 6 {
		t.Fatalf("found only %d COPY sources in the Dockerfile: %v", len(sources), sources)
	}
	for _, src := range sources {
		ok := false
		for _, e := range exceptions {
			switch {
			case e == src, strings.HasPrefix(e, src+"/"):
				ok = true
			case strings.HasSuffix(e, "/**") && strings.HasPrefix(src, strings.TrimSuffix(e, "**")):
				ok = true
			}
		}
		if !ok {
			t.Errorf("the Dockerfile copies %q but .dockerignore does not let it into the build context", src)
		}
	}

	// Both images carry the licenses.
	if got := strings.Count(docker, "COPY LICENSE THIRD_PARTY_NOTICES.md /licenses/"); got != 2 {
		t.Errorf("%d images copy the license and the notices into /licenses/, want both", got)
	}
	for _, f := range []string{"LICENSE", "THIRD_PARTY_NOTICES.md"} {
		if _, err := os.Stat(filepath.Join("..", f)); err != nil {
			t.Errorf("%s is copied into the images but does not exist", f)
		}
	}
}

// skippedDirs hold local tooling, caches and build output, never project text.
var skippedDirs = map[string]bool{".git": true, ".tools": true, ".venv": true, ".agent-local": true, ".opencode": true, ".cortexkit": true, "bin": true, "node_modules": true}

// walk calls f with the slash-separated path and the text of every project-owned
// text file: no local tooling, no generated protobuf code, no binaries.
func walk(t *testing.T, f func(rel, text string)) {
	t.Helper()
	root := ".."
	count := 0
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			if skippedDirs[d.Name()] || rel == "internal/protocol/dockfleet" {
				return filepath.SkipDir
			}
			return nil
		}
		switch filepath.Ext(rel) {
		case ".go", ".md", ".yml", ".yaml", ".sh", ".service", ".proto", ".sql", ".html", ".css", ".example", ".mod", ".env", "":
		default:
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.ContainsRune(string(raw[:min(len(raw), 512)]), 0) {
			return nil // a binary
		}
		count++
		f(rel, string(raw))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count < 150 {
		t.Fatalf("looked at only %d files; the walk is not seeing the repository", count)
	}
}
