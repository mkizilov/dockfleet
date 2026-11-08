package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// THIRD_PARTY_NOTICES.md has to describe the build it ships with. Upgrading or
// adding a dependency without regenerating it would put binaries into the world
// without the notices their licenses require, so this fails until it is
// regenerated.
func TestNoticesFileIsCurrent(t *testing.T) {
	root, err := findRoot()
	if err != nil {
		t.Fatal(err)
	}
	want, err := generate(root)
	if err != nil {
		t.Fatal(err)
	}
	have, err := os.ReadFile(filepath.Join(root, outputFile))
	if err != nil {
		t.Fatal(err)
	}
	if string(have) != want {
		t.Fatalf("%s is out of date: run `make notices` (that is, `go run ./scripts/notices -write`) and commit it", outputFile)
	}

	// What the file claims is also what it should contain.
	for _, must := range []string{"MIT License", "Apache License", "Redistribution and use in source and binary forms", "github.com/jackc/pgx/v5", "google.golang.org/grpc", "the Go standard library"} {
		if !strings.Contains(have2s(have), must) {
			t.Errorf("%s does not contain %q", outputFile, must)
		}
	}
}

func have2s(b []byte) string { return string(b) }

func TestClassifyRecognizesTheLicensesWeAllowAndRefusesTheRest(t *testing.T) {
	tests := []struct {
		name, text, want string
		allowed          bool
	}{
		{"MIT", "MIT License\n\nPermission is hereby granted, free of charge, to any person obtaining a copy", "MIT", true},
		{"Apache 2.0", "Apache License\nVersion 2.0, January 2004", "Apache-2.0", true},
		{"BSD 3", "Redistribution and use in source and binary forms, with or without modification, are permitted provided that ... Neither the name of the copyright holder", "BSD-3-Clause", true},
		{"BSD 2", "Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following two conditions are met", "BSD-2-Clause", true},
		{"ISC", "Permission to use, copy, modify, and/or distribute this software for any purpose with or without fee is hereby granted", "ISC", true},
		{"MPL", "Mozilla Public License Version 2.0", "MPL-2.0", false},
		{"GPL", "GNU GENERAL PUBLIC LICENSE Version 3", "GPL-family", false},
		{"LGPL", "GNU Lesser General Public License", "GPL-family", false},
		{"AGPL", "GNU Affero General Public License", "GPL-family", false},
		{"something else", "Do whatever you like, but do not blame me.", "UNRECOGNIZED", false},
		{"empty", "", "UNRECOGNIZED", false},
	}
	for _, tt := range tests {
		got := classify(tt.text)
		if got != tt.want || allowed[got] != tt.allowed {
			t.Errorf("%s: classified %q (allowed %v), want %q (allowed %v)", tt.name, got, allowed[got], tt.want, tt.allowed)
		}
	}
}

func TestLicenseFilesAreFoundByName(t *testing.T) {
	dir := t.TempDir()
	for name, content := range map[string]string{
		"LICENSE":      "MIT text",
		"LICENSE.txt":  "second",
		"COPYING":      "third",
		"NOTICE":       "a notice",
		"PATENTS":      "a grant",
		"README.md":    "not a license",
		"license_test": "also ignored: no separator",
		"licensing.go": "ignored",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content+"\r\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(dir, "LICENSE-dir"), 0o700); err != nil {
		t.Fatal(err)
	}
	lic, notices, err := readLicenseFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(lic) != 3 || len(notices) != 2 {
		t.Fatalf("found %d license files and %d notice files: %+v %+v", len(lic), len(notices), lic, notices)
	}
	for _, f := range append(lic, notices...) {
		if strings.Contains(f.text, "\r") {
			t.Errorf("%s keeps carriage returns", f.name)
		}
	}
}
