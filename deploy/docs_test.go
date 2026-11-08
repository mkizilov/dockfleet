package deploy

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"unicode"
)

// The documents refer to each other and to source files in many places. A link
// that no longer resolves, or an anchor whose heading was renamed, is a document
// that is wrong without anyone noticing; this finds them.

var (
	codeFence  = regexp.MustCompile("(?m)^```.*$")
	inlineCode = regexp.MustCompile("`[^`\n]*`")
	mdLink     = regexp.MustCompile(`\]\(([^)\s]+)\)`)
	mdHeading  = regexp.MustCompile(`(?m)^#{1,6} +(.+?) *#*$`)
	linkInHead = regexp.MustCompile(`\[([^\]]*)\]\([^)]*\)`)
)

// withoutCode removes fenced blocks and inline code, where square brackets and
// parentheses are not links.
func withoutCode(text string) string {
	var out strings.Builder
	in := false
	for _, line := range strings.Split(text, "\n") {
		if codeFence.MatchString(line) {
			in = !in
			continue
		}
		if !in {
			out.WriteString(inlineCode.ReplaceAllString(line, "") + "\n")
		}
	}
	return out.String()
}

// slug is how GitHub turns a heading into an anchor.
func slug(heading string) string {
	h := strings.ToLower(linkInHead.ReplaceAllString(heading, "$1"))
	var b strings.Builder
	for _, r := range h {
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '-':
			b.WriteRune(r)
		case r == ' ':
			b.WriteRune('-')
		}
	}
	return b.String()
}

// anchors lists the anchors a Markdown file defines, with GitHub's numbering of
// repeated headings.
func anchors(text string) map[string]bool {
	out := map[string]bool{}
	seen := map[string]int{}
	for _, m := range mdHeading.FindAllStringSubmatch(withoutCode(text), -1) {
		base := slug(strings.ReplaceAll(m[1], "`", ""))
		a := base
		if n := seen[base]; n > 0 {
			a = base + "-" + string(rune('0'+n))
			if n >= 10 {
				a = base + "-n"
			}
		}
		seen[base]++
		out[a] = true
	}
	return out
}

func markdownFiles(t *testing.T) []string {
	t.Helper()
	files := []string{"README.md", "ROADMAP.md", "AGENTS.md", "deploy/examples/README.md"}
	docs, err := filepath.Glob(filepath.Join("..", "docs", "*.md"))
	if err != nil || len(docs) < 15 {
		t.Fatalf("found %d documents under docs/ (%v); the test is not looking at them", len(docs), err)
	}
	for _, d := range docs {
		files = append(files, "docs/"+filepath.Base(d))
	}
	return files
}

func TestDocumentLinksResolve(t *testing.T) {
	checked := 0
	for _, rel := range markdownFiles(t) {
		text := read(t, rel)
		dir := filepath.Dir(rel)
		for _, m := range mdLink.FindAllStringSubmatch(withoutCode(text), -1) {
			target := m[1]
			if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") || strings.HasPrefix(target, "mailto:") {
				continue
			}
			checked++
			path, anchor, _ := strings.Cut(target, "#")
			file := rel
			if path != "" {
				file = filepath.ToSlash(filepath.Clean(filepath.Join(dir, path)))
			}
			info, err := os.Stat(filepath.Join("..", file))
			if err != nil {
				t.Errorf("%s links to %s, which does not exist", rel, target)
				continue
			}
			if anchor == "" || info.IsDir() || !strings.HasSuffix(file, ".md") {
				continue
			}
			if !anchors(read(t, file))[anchor] {
				t.Errorf("%s links to %s, but %s has no heading that makes the anchor #%s", rel, target, file, anchor)
			}
		}
	}
	if checked < 100 {
		t.Fatalf("checked only %d links; the test is not reading the documents", checked)
	}
}

// The status matrix and the README must agree about what is documented where.
func TestReadmeReferencesTheCurrentDocuments(t *testing.T) {
	readme := read(t, "README.md")
	docs, _ := filepath.Glob(filepath.Join("..", "docs", "*.md"))
	for _, d := range docs {
		name := filepath.Base(d)
		if !strings.Contains(readme, "docs/"+name) {
			t.Errorf("README.md never links docs/%s; every document should be reachable from it", name)
		}
	}
	if !strings.Contains(readme, "ROADMAP.md") {
		t.Error("README.md does not link ROADMAP.md")
	}
}
