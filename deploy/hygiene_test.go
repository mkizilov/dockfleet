package deploy

import (
	"regexp"
	"strings"
	"testing"
)

// Checks for what must not be in a repository that is published: a person's
// local paths, wording that credits how the project was made, and ignore rules
// that would hide a real secret. They read the files rather than git, so they
// run wherever the tests do.

// The patterns are written so that this file does not match them itself.
var (
	personalData = regexp.MustCompile(`/Use[r]s/|mk-run[e]|/private/va[r]/|C:\\Use[r]s`)
	attribution  = regexp.MustCompile(`(?i)\b(AI|human|machine)[- ]+(assisted|generated|written|authored|powered)\b|\b(written|authored|generated|created|made|built|developed|produced) (by|with) (an? )?(AI|LLM|large language model|chatbot|Claude|ChatGPT|GPT|Copilot)\b|\bas an AI\b|co-authored-by|\bvibe[- ]cod`)
)

func TestNoPersonalLocalDataIsPublished(t *testing.T) {
	walk(t, func(rel, text string) {
		if m := personalData.FindString(text); m != "" {
			t.Errorf("%s contains %q, a path or name from the machine this was built on", rel, m)
		}
	})
}

func TestNoCreationAttributionIsPublished(t *testing.T) {
	walk(t, func(rel, text string) {
		if rel == "deploy/hygiene_test.go" {
			return
		}
		if m := attribution.FindString(text); m != "" {
			t.Errorf("%s contains %q: project text describes what the software does, not how it was written", rel, m)
		}
	})
}

// .gitleaksignore may name only findings, each by its full fingerprint. A
// pattern, a path or a rule would silence real secrets in future commits too.
func TestSecretScanIgnoresOnlyNamedFindings(t *testing.T) {
	fingerprint := regexp.MustCompile(`^[0-9a-f]{40}:[^:*?\s]+:[a-z0-9-]+:[0-9]+$`)
	entries := 0
	for i, line := range strings.Split(read(t, ".gitleaksignore"), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		entries++
		if !fingerprint.MatchString(line) {
			t.Errorf(".gitleaksignore line %d is not a full commit:file:rule:line fingerprint: %q", i+1, line)
		}
	}
	if entries == 0 || entries > 10 {
		t.Errorf(".gitleaksignore has %d entries; it should list the few known false positives of the history and nothing else", entries)
	}
}

// The README's examples must not paste a token, even a development one: they
// read it from the environment, as the secret scanner expects.
func TestExamplesDoNotPasteTokens(t *testing.T) {
	paste := regexp.MustCompile(`Bearer [A-Za-z0-9_-]{12,}`)
	walk(t, func(rel, text string) {
		if !strings.HasSuffix(rel, ".md") {
			return
		}
		for _, m := range paste.FindAllString(text, -1) {
			t.Errorf("%s pastes a bearer token (%q); use $DOCKFLEET_API_TOKEN or $TOKEN", rel, m)
		}
	})
}
