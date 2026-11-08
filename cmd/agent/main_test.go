package main

import (
	"go/build"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/dockfleet/dockfleet/internal/testnet"
)

const fakeEngine = "github.com/dockfleet/dockfleet/internal/dockerfake"

// The end-to-end tests build the agent with -tags e2efake, which keeps its
// containers in memory. That must be impossible to do by accident: a build
// without the tag has no trace of it, and the tagged file is the only one that
// has.
func TestTheReleaseAgentHasNoFakeContainerEngine(t *testing.T) {
	release, err := build.Default.ImportDir(".", 0)
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(release.Imports, fakeEngine) {
		t.Fatalf("the agent built without tags imports %s", fakeEngine)
	}
	if slices.Contains(release.GoFiles, "engine_e2efake.go") || !slices.Contains(release.GoFiles, "engine_docker.go") {
		t.Fatalf("the agent built without tags compiles %v", release.GoFiles)
	}
	if !slices.Contains(release.IgnoredGoFiles, "engine_e2efake.go") {
		t.Fatalf("engine_e2efake.go is not excluded from the default build: %v", release.IgnoredGoFiles)
	}

	ctx := build.Default
	ctx.BuildTags = []string{"e2efake"}
	tagged, err := ctx.ImportDir(".", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(tagged.Imports, fakeEngine) || slices.Contains(tagged.GoFiles, "engine_docker.go") {
		t.Fatalf("the e2efake build is not the in-memory engine alone: imports it %v, files %v", slices.Contains(tagged.Imports, fakeEngine), tagged.GoFiles)
	}
}

// `dockfleet-agent healthcheck` is the image's HEALTHCHECK: it asks the agent's
// own /readyz, on the address the agent was configured with.
func TestHealthcheckSubcommandAsksTheAgentItself(t *testing.T) {
	status := http.StatusOK
	srv := testnet.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/readyz" || r.Method != http.MethodGet {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"ready":false,"reason":"no manager leads"}`))
	}), false)
	defer srv.Close()
	t.Setenv("DOCKFLEET_HEALTH_ADDR", strings.TrimPrefix(srv.URL, "http://"))

	if err := probeHealth(); err != nil {
		t.Fatalf("a ready agent: %v", err)
	}
	status = http.StatusServiceUnavailable
	if err := probeHealth(); err == nil || !strings.Contains(err.Error(), "no manager leads") {
		t.Fatalf("an unready agent: %v; the reason it gave should reach whoever reads the health log", err)
	}
	t.Setenv("DOCKFLEET_HEALTH_ADDR", "off")
	if err := probeHealth(); err == nil {
		t.Fatal("a probe of an endpoint that is turned off succeeded")
	}
	t.Setenv("DOCKFLEET_HEALTH_ADDR", "127.0.0.1:1") // nothing listens
	if err := probeHealth(); err == nil {
		t.Fatal("a probe of a dead port succeeded")
	}
}
