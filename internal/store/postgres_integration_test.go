//go:build integration

package store

import (
	"context"
	"os"
	"testing"
)

func TestPostgresConformance(t *testing.T) {
	url := os.Getenv("DOCKFLEET_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("DOCKFLEET_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	p, err := OpenPostgres(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	if err := p.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if err := p.Migrate(ctx); err != nil {
		t.Fatalf("Migrate must be idempotent: %v", err)
	}
	if _, err := p.pool.Exec(ctx, `TRUNCATE workloads, nodes`); err != nil {
		t.Fatal(err)
	}
	runStoreConformance(t, p)
}
