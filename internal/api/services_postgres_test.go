//go:build integration

package api

import (
	"context"
	"net/http"
	"reflect"
	"testing"

	"github.com/dockfleet/dockfleet/internal/pgtest"
	"github.com/dockfleet/dockfleet/internal/store"
)

// These tests need a PostgreSQL server: set DOCKFLEET_TEST_DATABASE_URL (see
// pgtest). Each test gets its own throwaway database.

func openMigrated(t *testing.T, dbURL string) *store.Postgres {
	t.Helper()
	ctx := context.Background()
	p, err := store.OpenPostgres(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	if err := p.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return p
}

// The same suite that runs against the in-memory store, through the same HTTP
// interface, on PostgreSQL.
func TestServiceAPIWithPostgres(t *testing.T) {
	runServiceAPITests(t, func(t *testing.T) store.Store {
		return openMigrated(t, pgtest.NewDatabase(t))
	})
}

// Service definitions are the manager's configuration: a restart must not lose
// them, and the new process must carry on from the stored versions.
func TestServicesSurviveAManagerRestart(t *testing.T) {
	dbURL := pgtest.NewDatabase(t)
	first := newServiceEnv(t, openMigrated(t, dbURL))

	api := first.create(fullService)
	first.request(http.MethodPut, "/v1/services/api", `{"replicas":5,"spec":`+specJSON(api.Spec)+`}`)
	first.create(minimalService)
	gone := first.create(`{"name":"old","replicas":1,"spec":{"image":"x","cpu_millis":1,"memory_bytes":134217728}}`)
	first.request(http.MethodDelete, "/v1/services/old", "")
	before := first.get("api")
	first.st.Close()

	// A new manager process on the same database.
	second := newServiceEnv(t, openMigrated(t, dbURL))
	after := second.get("api")
	if !reflect.DeepEqual(after, before) || after.Replicas != 5 || after.Version != 2 {
		t.Fatalf("after a restart:\n%+v\nbefore:\n%+v", after, before)
	}
	if !reflect.DeepEqual(after.Spec, api.Spec) {
		t.Fatalf("the spec changed across a restart:\n%+v\nwant\n%+v", after.Spec, api.Spec)
	}
	var list struct {
		Services []serviceView `json:"services"`
	}
	decode(t, second.request(http.MethodGet, "/v1/services?include_deleted=true", ""), &list)
	if len(list.Services) != 3 || list.Services[0].Name != "api" || list.Services[1].Name != "old" || list.Services[1].ID != gone.ID || list.Services[1].DeletedAt == nil || list.Services[2].Name != "web" {
		t.Fatalf("services after a restart = %+v", list.Services)
	}

	// The new process continues the history rather than starting over.
	rec := second.request(http.MethodPut, "/v1/services/api", `{"replicas":5,"spec":`+specJSON(withImage(api.Spec, "registry.example.com:5000/team/api:v2"))+`}`, "If-Match", `"2"`)
	var updated serviceView
	decode(t, rec, &updated)
	if rec.Code != http.StatusOK || updated.Revision != 2 || updated.Version != 3 {
		t.Fatalf("update after a restart = %d %+v", rec.Code, updated)
	}
	events, err := second.st.ListEvents(context.Background(), store.EventFilter{ResourceType: "service", ResourceID: api.ID})
	if err != nil || len(events) != 3 {
		t.Fatalf("events for the service = %+v, %v; want created, scaled and deployed", events, err)
	}
}

// Two managers on one database: concurrent changes to one service resolve
// through its version, and each sees the other's work.
func TestTwoManagersEditingOneService(t *testing.T) {
	dbURL := pgtest.NewDatabase(t)
	a := newServiceEnv(t, openMigrated(t, dbURL))
	b := newServiceEnv(t, openMigrated(t, dbURL))

	a.create(minimalService)
	if v := b.get("web"); v.Replicas != 2 || v.Version != 1 {
		t.Fatalf("the second manager sees %+v", v)
	}

	// Both read version 1. The first to write wins; the other is told to re-read.
	if rec := a.request(http.MethodPut, "/v1/services/web", serviceBodyWith("nginx:1.27", "3"), "If-Match", `"1"`); rec.Code != http.StatusOK {
		t.Fatalf("first writer = %d: %s", rec.Code, rec.Body.String())
	}
	if rec := b.request(http.MethodPut, "/v1/services/web", serviceBodyWith("nginx:1.27", "9"), "If-Match", `"1"`); rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("second writer = %d, want 412: %s", rec.Code, rec.Body.String())
	}
	if v := b.get("web"); v.Replicas != 3 || v.Version != 2 {
		t.Fatalf("service = %+v", v)
	}
	if rec := b.request(http.MethodPut, "/v1/services/web", serviceBodyWith("nginx:1.27", "9"), "If-Match", `"2"`); rec.Code != http.StatusOK {
		t.Fatalf("second writer after re-reading = %d: %s", rec.Code, rec.Body.String())
	}
	if v := a.get("web"); v.Replicas != 9 || v.Version != 3 {
		t.Fatalf("the first manager sees %+v", v)
	}

	// A name is taken once, whichever manager gets there first.
	if rec := b.request(http.MethodPost, "/v1/services", minimalService); rec.Code != http.StatusConflict {
		t.Fatalf("duplicate on the other manager = %d, want 409", rec.Code)
	}
}
