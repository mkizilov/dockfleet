package docker

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const modulePath = "github.com/dockfleet/dockfleet"

// importChains walks the non-test imports of the package in dir, following
// imports inside this module, and returns every import path reached with the
// chain of packages that led to it.
func importChains(t *testing.T, root, dir string) map[string][]string {
	t.Helper()
	chains := map[string][]string{}
	var visit func(dir string, chain []string)
	visited := map[string]bool{}
	visit = func(dir string, chain []string) {
		if visited[dir] {
			return
		}
		visited[dir] = true
		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, file := range files {
			if strings.HasSuffix(file, "_test.go") {
				continue
			}
			parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.ImportsOnly)
			if err != nil {
				t.Fatalf("parse %s: %v", file, err)
			}
			for _, spec := range parsed.Imports {
				path, err := strconv.Unquote(spec.Path.Value)
				if err != nil {
					t.Fatal(err)
				}
				here := append(append([]string{}, chain...), filepath.ToSlash(dir))
				if _, seen := chains[path]; !seen {
					chains[path] = here
				}
				if rel, ok := strings.CutPrefix(path, modulePath+"/"); ok {
					visit(filepath.Join(root, filepath.FromSlash(rel)), here)
				}
			}
		}
	}
	visit(dir, nil)
	return chains
}

func isDockerPackage(path string) bool {
	return path == modulePath+"/internal/docker" ||
		strings.HasPrefix(path, "github.com/docker/") ||
		strings.HasPrefix(path, "github.com/moby/")
}

// The manager must never need a Docker socket: nothing it is built from may
// import the Docker adapter or any Docker SDK package. Only the agent
// talks to its local Docker Engine.
func TestManagerSideNeverImportsDocker(t *testing.T) {
	root := filepath.Join("..", "..")
	for _, entry := range []string{"cmd/manager", "internal/manager", "internal/api", "internal/registry", "internal/store", "internal/scheduler", "internal/placement", "internal/reconcile", "internal/rollout", "internal/logstream", "internal/metrics", "internal/dashboard", "internal/election", "internal/etcd", "internal/outage", "internal/healthcheck", "internal/protocol"} {
		for path, chain := range importChains(t, root, filepath.Join(root, filepath.FromSlash(entry))) {
			if isDockerPackage(path) {
				t.Errorf("%s imports %s (via %s): the manager side must not depend on Docker", entry, path, strings.Join(chain, " -> "))
			}
		}
	}
}

// Guards the test above against passing vacuously: the agent really does reach the adapter.
func TestAgentReachesDockerThroughTheAdapterOnly(t *testing.T) {
	root := filepath.Join("..", "..")
	chains := importChains(t, root, filepath.Join(root, "cmd", "agent"))
	if _, ok := chains[modulePath+"/internal/docker"]; !ok {
		t.Fatal("cmd/agent should import internal/docker; the boundary test is not looking at the right packages")
	}
	for path, chain := range chains {
		if strings.HasPrefix(path, "github.com/docker/") && !strings.HasSuffix(chain[len(chain)-1], "internal/docker") {
			t.Errorf("%s is imported outside internal/docker (via %s)", path, strings.Join(chain, " -> "))
		}
	}
}

func TestOnlySDKFileUsesTheDockerSDK(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") || file == "sdk.go" {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, spec := range parsed.Imports {
			if strings.Contains(spec.Path.Value, "github.com/docker/") {
				t.Errorf("%s imports %s; keep Docker SDK use inside sdk.go", file, spec.Path.Value)
			}
		}
	}
}

// The Docker socket is root on the machine it is on. No deployment file hands it
// to a service: not a Compose file, not an environment file. The only files under
// deploy/ that may name it are the ones that belong to the agent itself, which is
// the one process that must talk to the engine: its systemd unit (the agent runs
// as its own user, in the docker group, in a sandbox) and its example settings
// (DOCKFLEET_DOCKER_HOST). Test sources are not deployment files.
func TestDeploymentFilesNeverMountTheDockerSocket(t *testing.T) {
	root := filepath.Join("..", "..", "deploy")
	agentOwned := map[string]bool{
		filepath.Join("examples", "systemd", "dockfleet-agent.service"): true,
		filepath.Join("examples", "worker.env.example"):                 true,
		filepath.Join("examples", "README.md"):                          true, // explains why there is no Compose file for the agent
	}
	scanned := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		if d.IsDir() || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		scanned++
		if strings.Contains(string(data), "docker.sock") && !agentOwned[rel] {
			t.Errorf("deploy/%s mentions docker.sock; no deployment file may hand the Docker socket to a service", rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if scanned < 8 {
		t.Fatalf("scanned only %d deployment files; the test is not looking at deploy/", scanned)
	}
	// The files that may name it really are the agent's.
	manager, err := os.ReadFile(filepath.Join(root, "examples", "compose.manager.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(manager), "docker.sock") {
		t.Error("the manager's Compose file names the Docker socket")
	}
}
