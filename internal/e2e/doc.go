// Package e2e holds the end-to-end tests of DockFleet. They are compiled only
// with the `e2e` build tag (`make test-e2e`) because they build the real
// binaries and start real PostgreSQL and etcd servers.
//
// What they run is what a deployment runs: the manager and agent binaries as
// separate processes, configured only through their environment and credential
// files, talking over mutual TLS with certificates made by dockfleet-pki, with
// state in PostgreSQL and (for several managers) leadership in etcd. The one
// difference is the agent's container engine: the agent is built with
// `-tags e2efake`, which keeps its containers in memory, because these tests run
// where there is no Docker daemon to use. Everything else about the agent,
// including its configuration, health endpoint, TLS and shutdown, is the
// release code. Tests that need a real daemon are the `dockerintegration` ones.
//
// The package has no non-test code.
package e2e
