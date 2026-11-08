package deploy

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// ROADMAP.md is a plan other people will schedule work from, so its arithmetic
// and its ordering are checked: a total that does not add up, a step that depends
// on a later one, or a new feature ranked above a security gap would mislead
// whoever trusts it.

type roadmapStep struct {
	n     int
	title string
	body  string
}

var (
	anySection  = regexp.MustCompile(`(?m)^## .*$`)
	stepHeading = regexp.MustCompile(`(?m)^## (\d+)\. (.+)$`)
	stepRef     = regexp.MustCompile(`(?i)\bsteps? (\d+)`)
)

// roadmapSteps splits the document into its numbered steps; the text of a step
// ends at the next "## " heading of any kind.
func roadmapSteps(t *testing.T) ([]roadmapStep, string) {
	t.Helper()
	text := read(t, "ROADMAP.md")
	all := anySection.FindAllStringIndex(text, -1)
	var steps []roadmapStep
	for i, loc := range all {
		heading := text[loc[0]:loc[1]]
		m := stepHeading.FindStringSubmatch(heading)
		if m == nil {
			continue
		}
		end := len(text)
		if i+1 < len(all) {
			end = all[i+1][0]
		}
		n, _ := strconv.Atoi(m[1])
		steps = append(steps, roadmapStep{n: n, title: m[2], body: text[loc[1]:end]})
	}
	return steps, text
}

func field(body, label string) (string, bool) {
	m := regexp.MustCompile(`(?m)^\*\*` + regexp.QuoteMeta(label) + `:\*\* ?(.*)$`).FindStringSubmatch(body)
	if m == nil {
		return "", false
	}
	return strings.TrimSpace(m[1]), true
}

// rank orders the categories: everything that fixes or finishes what exists comes
// before anything new.
func rank(category string) int {
	if category == "new feature" {
		return 2
	}
	return 1
}

func TestRoadmapIsComplete(t *testing.T) {
	steps, _ := roadmapSteps(t)
	if len(steps) < 12 {
		t.Fatalf("the roadmap has %d numbered steps, want at least 12", len(steps))
	}
	categories := map[string]bool{"incomplete requirement": true, "verification gap": true, "correctness": true, "security": true, "new feature": true}
	lastRank := 0
	for i, s := range steps {
		if s.n != i+1 {
			t.Errorf("heading %d is numbered %d; steps must be numbered 1, 2, 3, ... in order", i+1, s.n)
		}
		if len(strings.TrimSpace(s.title)) < 10 {
			t.Errorf("step %d has no real title: %q", s.n, s.title)
		}
		for _, label := range []string{"Category", "Scope", "Rationale", "Dependencies", "Estimated effort"} {
			v, ok := field(s.body, label)
			if !ok || v == "" {
				t.Errorf("step %d has no %q", s.n, label)
			}
			if (label == "Scope" || label == "Rationale") && len(v) < 150 {
				t.Errorf("step %d: %s is %d characters; say what it covers and why", s.n, label, len(v))
			}
		}

		category, _ := field(s.body, "Category")
		if !categories[category] {
			t.Errorf("step %d: category %q is not one of the five allowed", s.n, category)
		}
		if r := rank(category); r < lastRank {
			t.Errorf("step %d (%s) comes after a new feature: unfinished requirements and gaps in correctness, security and verification come first", s.n, category)
		} else {
			lastRank = r
		}

		deps, _ := field(s.body, "Dependencies")
		for _, m := range stepRef.FindAllStringSubmatch(deps, -1) {
			d, _ := strconv.Atoi(m[1])
			if d < 1 || d >= s.n {
				t.Errorf("step %d depends on step %d: a step may depend only on a step with a lower number", s.n, d)
			}
		}
		if !strings.HasPrefix(strings.ToLower(deps), "none") && !stepRef.MatchString(deps) {
			t.Errorf("step %d: dependencies %q name no step and do not say none", s.n, deps)
		}

		idx := strings.Index(s.body, "**Acceptance criteria:**")
		if idx < 0 {
			t.Errorf("step %d has no acceptance criteria", s.n)
			continue
		}
		bullets := 0
		for _, line := range strings.Split(s.body[idx:], "\n") {
			if strings.HasPrefix(line, "- ") && len(line) > 40 {
				bullets++
			}
		}
		if bullets < 3 {
			t.Errorf("step %d lists %d acceptance criteria, want at least 3 that can be checked", s.n, bullets)
		}

		if got := len(regexp.MustCompile(`(?m)^\*\*Estimated effort:\*\* \d+ hours$`).FindAllString(s.body, -1)); got != 1 {
			t.Errorf("step %d: the effort must be stated once as %q", s.n, "**Estimated effort:** N hours")
		}
	}
}

func effortOf(s roadmapStep) int {
	v, _ := field(s.body, "Estimated effort")
	n, _ := strconv.Atoi(strings.TrimSuffix(v, " hours"))
	return n
}

func TestRoadmapTotalsAddUp(t *testing.T) {
	steps, text := roadmapSteps(t)
	sum := 0
	byCategory := map[string]int{}
	features := 0
	for _, s := range steps {
		h := effortOf(s)
		if h < 1 || h > 80 {
			t.Errorf("step %d: %d hours; a step over two working weeks should be split", s.n, h)
		}
		sum += h
		c, _ := field(s.body, "Category")
		byCategory[c] += h
		if c == "new feature" {
			features += h
		}
	}

	totals := regexp.MustCompile(`(?m)^\*\*Total estimate:\*\* (\d+) hours$`).FindAllStringSubmatch(text, -1)
	if len(totals) != 1 {
		t.Fatalf("the roadmap must state its total once as %q, found %d", "**Total estimate:** N hours", len(totals))
	}
	if stated, _ := strconv.Atoi(totals[0][1]); stated != sum {
		t.Errorf("the roadmap states %d hours in total, its steps add up to %d", stated, sum)
	}

	// The breakdown paragraph quotes each category's sum; check the numbers it quotes.
	breakdown := regexp.MustCompile(`(?m)^By category: .*$`).FindString(text)
	if breakdown == "" {
		t.Fatal(`the total is followed by no "By category:" breakdown`)
	}
	for category, hours := range byCategory {
		if !strings.Contains(breakdown, fmt.Sprintf(" %d hours", hours)) {
			t.Errorf("the breakdown does not quote %d hours for %s: %s", hours, category, breakdown)
		}
	}
	if want := fmt.Sprintf("come to %d hours", sum-features); !strings.Contains(breakdown, want) {
		t.Errorf("the breakdown should say the steps before the new features %q: %s", want, breakdown)
	}
}

func TestRoadmapStatesItsAssumptions(t *testing.T) {
	_, text := roadmapSteps(t)
	i := strings.Index(text, "\n## Assumptions\n")
	if i < 0 {
		t.Fatal("the roadmap has no Assumptions section")
	}
	bullets := 0
	for _, line := range strings.Split(text[i:], "\n") {
		if strings.HasPrefix(line, "- ") {
			bullets++
		}
	}
	if bullets < 4 {
		t.Errorf("the assumptions section has %d bullets, want at least 4", bullets)
	}
	for _, want := range []string{"## Total estimate", "docs/verification.md", "docs/security-audit.md"} {
		if !strings.Contains(text, want) {
			t.Errorf("the roadmap never mentions %q", want)
		}
	}
}
