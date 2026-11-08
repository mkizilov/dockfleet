package store

import (
	"strings"
	"testing"
	"testing/fstest"
)

// These checks need no database: they validate the migration files themselves.

func TestEmbeddedMigrationsAreWellFormed(t *testing.T) {
	migrations, err := loadMigrations(migrationFS, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	if len(migrations) < 7 {
		t.Fatalf("only %d migrations found; the embedded directory looks incomplete", len(migrations))
	}
	for i, m := range migrations {
		if m.number != i+1 {
			t.Errorf("migration %s is number %d at position %d", m.name, m.number, i+1)
		}
		if strings.TrimSpace(m.sql) == "" {
			t.Errorf("migration %s is empty", m.name)
		}
		if len(m.checksum) != 64 {
			t.Errorf("migration %s has checksum %q", m.name, m.checksum)
		}
	}
}

// Released migrations must never change: editing one makes the checksum of
// every database that applied it stop matching. This pins the ones that have
// been released; add new migrations, never edit these.
func TestReleasedMigrationsAreUnchanged(t *testing.T) {
	migrations, err := loadMigrations(migrationFS, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	released := map[string]string{
		"001_init.sql":            "CREATE TABLE nodes (",
		"002_workload_status.sql": "ADD COLUMN status_message",
		"003_enrollments.sql":     "CREATE TABLE enrollments (",
	}
	for _, m := range migrations {
		if marker, ok := released[m.name]; ok && !strings.Contains(m.sql, marker) {
			t.Errorf("released migration %s no longer contains %q: schema changes must be new migrations", m.name, marker)
		}
		delete(released, m.name)
	}
	for name := range released {
		t.Errorf("released migration %s is missing", name)
	}
}

func TestLoadMigrationsRejectsBadDirectories(t *testing.T) {
	file := func(sql string) *fstest.MapFile { return &fstest.MapFile{Data: []byte(sql)} }
	tests := []struct {
		name string
		fsys fstest.MapFS
	}{
		{"gap", fstest.MapFS{"m/001_a.sql": file("SELECT 1"), "m/003_c.sql": file("SELECT 1")}},
		{"starts at two", fstest.MapFS{"m/002_b.sql": file("SELECT 1")}},
		{"duplicate number", fstest.MapFS{"m/001_a.sql": file("SELECT 1"), "m/001_b.sql": file("SELECT 1")}},
		{"bad file name", fstest.MapFS{"m/001_a.sql": file("SELECT 1"), "m/readme.md": file("hi")}},
		{"upper case name", fstest.MapFS{"m/001_Init.sql": file("SELECT 1")}},
		{"two digit number", fstest.MapFS{"m/01_init.sql": file("SELECT 1")}},
		{"missing directory", fstest.MapFS{}},
	}
	for _, tt := range tests {
		if _, err := loadMigrations(tt.fsys, "m"); err == nil {
			t.Errorf("%s: loadMigrations succeeded, want an error", tt.name)
		}
	}

	ok := fstest.MapFS{"m/001_a.sql": file("SELECT 1"), "m/002_b.sql": file("SELECT 2"), "m/sub": &fstest.MapFile{Mode: 1 << 31}}
	got, err := loadMigrations(ok, "m")
	if err != nil || len(got) != 2 || got[0].name != "001_a.sql" || got[1].number != 2 {
		t.Fatalf("loadMigrations = %+v, %v", got, err)
	}
	if got[0].checksum == got[1].checksum {
		t.Error("different files must have different checksums")
	}
}
