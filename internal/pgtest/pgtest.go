// Package pgtest creates throwaway PostgreSQL databases for integration tests.
//
// It depends on nothing else in this module, so any package's tests can use
// it. It is imported by tests only; no binary links it.
package pgtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
)

// EnvURL is the variable naming the PostgreSQL server tests may use.
const EnvURL = "DOCKFLEET_TEST_DATABASE_URL"

// NewDatabase creates an empty database named dockfleet_test_<random> on the
// server named by DOCKFLEET_TEST_DATABASE_URL and returns its URL. The database
// is dropped when the test ends. The test is skipped when the variable is not
// set.
//
// The URL names a server and a role that may CREATE DATABASE (PostgreSQL 13 or
// newer, for DROP DATABASE ... WITH (FORCE)). The database in that URL is only
// used to connect: it is never written to, so tests never touch development
// data and never share state with each other.
func NewDatabase(t testing.TB) string {
	t.Helper()
	admin := os.Getenv(EnvURL)
	if admin == "" {
		t.Skip(EnvURL + " not set")
	}
	parsed, err := url.Parse(admin)
	if err != nil || parsed.Scheme == "" {
		t.Fatalf("%s must be a postgres:// URL: %v", EnvURL, err)
	}
	suffix := make([]byte, 8)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatal(err)
	}
	name := "dockfleet_test_" + hex.EncodeToString(suffix)
	quoted := pgx.Identifier{name}.Sanitize()

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Fatalf("connect to the PostgreSQL server: %v", err)
	}
	if _, err := conn.Exec(ctx, `CREATE DATABASE `+quoted); err != nil {
		_ = conn.Close(ctx)
		t.Fatalf("create the isolated database: %v", err)
	}
	t.Cleanup(func() {
		defer conn.Close(ctx)
		if _, err := conn.Exec(ctx, `DROP DATABASE IF EXISTS `+quoted+` WITH (FORCE)`); err != nil {
			t.Errorf("drop %s: %v", name, err)
		}
	})

	parsed.Path = "/" + name
	return parsed.String()
}
