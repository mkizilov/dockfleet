// Command notices writes THIRD_PARTY_NOTICES.md: the licenses of the third-party
// software that is linked into DockFleet's released binaries.
//
// The binaries and the container images built from this repository contain code
// from other projects, and the licenses of that code (BSD, MIT, Apache 2.0)
// require their notices to travel with the binaries. The list is not written by
// hand: it is read from the build (`go list -deps` for the three commands, on
// each released platform), the license texts are copied verbatim from the
// module sources, and a license that is not on the reviewed list below stops the
// run, so that a new dependency is looked at before it is shipped.
//
//	go run ./scripts/notices          check that the file is current (exit 1 if not)
//	go run ./scripts/notices -write   regenerate it
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// The commands that are shipped, and the platforms they are released for.
var (
	commands  = []string{"./cmd/manager", "./cmd/agent", "./cmd/pki"}
	platforms = [][2]string{{"linux", "amd64"}, {"linux", "arm64"}, {"darwin", "arm64"}}
)

// allowed are the licenses that have been reviewed for redistribution in a
// binary under the MIT license. Anything else must be looked at by a person
// before this list is extended.
var allowed = map[string]bool{"MIT": true, "BSD-2-Clause": true, "BSD-3-Clause": true, "Apache-2.0": true, "ISC": true}

const outputFile = "THIRD_PARTY_NOTICES.md"

type file struct{ name, text string }

type module struct {
	path, version string
	licenses      []file // LICENSE, COPYING
	notices       []file // NOTICE, PATENTS
	spdx          string
}

var (
	licenseName = regexp.MustCompile(`(?i)^(licen[sc]e|copying)([.-].*)?$`)
	noticeName  = regexp.MustCompile(`(?i)^(notice|patents)([.-].*)?$`)
)

func main() {
	write := flag.Bool("write", false, "write "+outputFile+" instead of checking it")
	flag.Parse()
	root, err := findRoot()
	if err != nil {
		fatal(err)
	}
	got, err := generate(root)
	if err != nil {
		fatal(err)
	}
	path := filepath.Join(root, outputFile)
	if *write {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			fatal(err)
		}
		fmt.Println("wrote", outputFile)
		return
	}
	have, err := os.ReadFile(path)
	if err != nil || string(have) != got {
		fatal(fmt.Errorf("%s is not current: run `go run ./scripts/notices -write` and commit it", outputFile))
	}
	fmt.Println(outputFile, "is current")
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "notices:", err)
	os.Exit(1)
}

func findRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		if parent := filepath.Dir(dir); parent != dir {
			dir = parent
			continue
		}
		return "", errors.New("go.mod not found")
	}
}

// generate returns the contents of THIRD_PARTY_NOTICES.md.
func generate(root string) (string, error) {
	mods, err := collect(root)
	if err != nil {
		return "", err
	}
	std, err := goStandardLibrary(root)
	if err != nil {
		return "", err
	}

	var problems []string
	for _, m := range mods {
		if len(m.licenses) == 0 {
			problems = append(problems, m.path+" has no license file")
		} else if !allowed[m.spdx] {
			problems = append(problems, fmt.Sprintf("%s is licensed %s, which has not been reviewed", m.path, m.spdx))
		}
	}
	if len(problems) > 0 {
		return "", errors.New(strings.Join(problems, "; "))
	}

	// Identical texts are written once, with the modules that use them.
	type text struct {
		spdx, body string
		users      []string
	}
	byHash := map[string]*text{}
	var texts []*text
	add := func(spdx, body, user string) {
		sum := sha256.Sum256([]byte(body))
		key := hex.EncodeToString(sum[:])
		if t, ok := byHash[key]; ok {
			t.users = append(t.users, user)
			return
		}
		t := &text{spdx: spdx, body: body, users: []string{user}}
		byHash[key] = t
		texts = append(texts, t)
	}
	add(std.spdx, std.licenses[0].text, "the Go standard library")
	for _, m := range mods {
		for _, l := range m.licenses {
			add(m.spdx, l.text, m.path)
		}
	}
	sort.Slice(texts, func(i, j int) bool {
		if texts[i].spdx != texts[j].spdx {
			return texts[i].spdx < texts[j].spdx
		}
		return texts[i].users[0] < texts[j].users[0]
	})

	var b strings.Builder
	b.WriteString(`# Third-party notices

DockFleet is licensed under the MIT License (see [LICENSE](LICENSE)). The binaries and container images built from this repository also contain the third-party software listed below, under the licenses shown. The license texts are copied verbatim from the module sources.

Only software linked into the released binaries (` + "`dockfleet-manager`, `dockfleet-agent` and `dockfleet-pki`" + ` for linux/amd64, linux/arm64 and darwin/arm64) is listed. Software used only to build or test DockFleet is not redistributed and is not listed. This file is generated from the build by ` + "`go run ./scripts/notices -write`" + `, and a test fails when it is out of date.

## Components

| Component | Version | License |
| --- | --- | --- |
`)
	fmt.Fprintf(&b, "| the Go standard library | the release named in `go.mod` | %s |\n", std.spdx)
	for _, m := range mods {
		fmt.Fprintf(&b, "| `%s` | %s | %s |\n", m.path, m.version, m.spdx)
	}

	b.WriteString("\n## License texts\n")
	total, seen := map[string]int{}, map[string]int{}
	for _, t := range texts {
		total[t.spdx]++
	}
	for _, t := range texts {
		seen[t.spdx]++
		fmt.Fprintf(&b, "\n### %s", t.spdx)
		if total[t.spdx] > 1 {
			fmt.Fprintf(&b, " (text %d)", seen[t.spdx])
		}
		fmt.Fprintf(&b, "\n\nUsed by: %s\n\n~~~~text\n%s\n~~~~\n", strings.Join(quote(t.users), ", "), strings.TrimRight(t.body, "\n"))
	}

	var notices []string
	for _, f := range std.notices {
		notices = append(notices, fmt.Sprintf("\n### the Go standard library: %s\n\n~~~~text\n%s\n~~~~\n", f.name, strings.TrimRight(f.text, "\n")))
	}
	for _, m := range mods {
		for _, f := range m.notices {
			notices = append(notices, fmt.Sprintf("\n### `%s`: %s\n\n~~~~text\n%s\n~~~~\n", m.path, f.name, strings.TrimRight(f.text, "\n")))
		}
	}
	if len(notices) > 0 {
		b.WriteString("\n## Notices\n\nSome components come with a NOTICE or PATENTS file, which is reproduced here.\n")
		for _, n := range notices {
			b.WriteString(n)
		}
	}
	return b.String(), nil
}

func quote(ss []string) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = "`" + s + "`"
		if strings.HasPrefix(s, "the ") {
			out[i] = s
		}
	}
	return out
}

// collect lists the modules that are linked into the shipped binaries, with
// their license files.
func collect(root string) ([]module, error) {
	found := map[string]*module{}
	dirs := map[string]string{}
	for _, p := range platforms {
		args := append([]string{"list", "-deps", "-f", "{{if .Module}}{{if not .Module.Main}}{{.Module.Path}}\t{{.Module.Version}}\t{{.Module.Dir}}{{end}}{{end}}"}, commands...)
		cmd := exec.Command("go", args...)
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "GOOS="+p[0], "GOARCH="+p[1], "CGO_ENABLED=0")
		out, err := cmd.Output()
		if err != nil {
			var ee *exec.ExitError
			if errors.As(err, &ee) {
				return nil, fmt.Errorf("go list for %s/%s: %v: %s", p[0], p[1], err, ee.Stderr)
			}
			return nil, err
		}
		for _, line := range strings.Split(string(out), "\n") {
			f := strings.Split(strings.TrimSpace(line), "\t")
			if len(f) != 3 {
				continue
			}
			if f[2] == "" {
				return nil, fmt.Errorf("module %s %s is not in the module cache; run `go mod download`", f[0], f[1])
			}
			found[f[0]] = &module{path: f[0], version: f[1]}
			dirs[f[0]] = f[2]
		}
	}
	var mods []module
	for path, m := range found {
		lic, notice, err := readLicenseFiles(dirs[path])
		if err != nil {
			return nil, err
		}
		m.licenses, m.notices = lic, notice
		if len(lic) > 0 {
			m.spdx = classify(lic[0].text)
		}
		mods = append(mods, *m)
	}
	sort.Slice(mods, func(i, j int) bool { return mods[i].path < mods[j].path })
	return mods, nil
}

func goStandardLibrary(root string) (module, error) {
	cmd := exec.Command("go", "env", "GOROOT")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		return module{}, err
	}
	lic, notice, err := readLicenseFiles(strings.TrimSpace(string(out)))
	if err != nil {
		return module{}, err
	}
	if len(lic) == 0 {
		return module{}, errors.New("the Go distribution has no LICENSE file")
	}
	return module{path: "std", licenses: lic, notices: notice, spdx: classify(lic[0].text)}, nil
}

// readLicenseFiles reads the license and notice files in the root of dir.
func readLicenseFiles(dir string) (licenses, notices []file, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		var into *[]file
		switch name := e.Name(); {
		case licenseName.MatchString(name):
			into = &licenses
		case noticeName.MatchString(name):
			into = &notices
		default:
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, nil, err
		}
		*into = append(*into, file{name: e.Name(), text: strings.ReplaceAll(string(raw), "\r\n", "\n")})
	}
	return licenses, notices, nil
}

// classify names the license of a license file from its wording. It only has to
// be right enough to compare against the reviewed list; the full text is
// published beside it either way.
func classify(text string) string {
	t := strings.Join(strings.Fields(strings.ToLower(text)), " ")
	switch {
	case strings.Contains(t, "apache license") && strings.Contains(t, "version 2.0"):
		return "Apache-2.0"
	case strings.Contains(t, "permission is hereby granted, free of charge"):
		return "MIT"
	case strings.Contains(t, "redistribution and use in source and binary forms"):
		if strings.Contains(t, "neither the name") {
			return "BSD-3-Clause"
		}
		return "BSD-2-Clause"
	case strings.Contains(t, "permission to use, copy, modify, and/or distribute this software for any purpose with or without fee"):
		return "ISC"
	case strings.Contains(t, "mozilla public license"):
		return "MPL-2.0"
	case strings.Contains(t, "gnu general public license"), strings.Contains(t, "gnu lesser general public license"), strings.Contains(t, "gnu affero"):
		return "GPL-family"
	}
	return "UNRECOGNIZED"
}
