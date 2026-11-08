# Deployment examples

Everything here is an example to copy and edit, never to run as it is. The guide is
[docs/deployment.md](../../docs/deployment.md); how credentials are made and moved is
[docs/credentials.md](../../docs/credentials.md).

| File | What it is |
| --- | --- |
| `manager.env.example`, `worker.env.example` | Every setting a manager and a worker usually needs, with the defaults and the reasons. No secrets: credentials are files. |
| `compose.manager.yml` | A manager as a hardened container (read-only filesystem, no capabilities, secrets as files). |
| `systemd/dockfleet-manager.service` | A manager as a sandboxed service with systemd credentials. |
| `systemd/dockfleet-agent.service` | An agent as a sandboxed service: the way to run a worker. There is deliberately no Compose file for the agent (see below). |

`deploy/artifacts_test.go` checks that the examples stay valid: the environment files load
through the real configuration code, every variable they set exists, and the hardening the
Compose files and units promise has not been removed.

## Why there is no Compose file for the agent

The agent needs the node's Docker Engine socket, and the socket is root on the node. No file in `deploy/` is allowed to hand it to a container (`TestDeploymentFilesNeverMountTheDockerSocket`), the agent's own systemd unit being the one exception: it is the agent process itself, running as its own user in the `docker` group with the rest of the sandbox closed. The `agent` target of the `Dockerfile` exists for those who package the agent their own way; if you run it in a container, join the socket's group with `--group-add` instead of running as root, prefer a rootless engine's socket, and read [the audit](../../docs/security-audit.md#container-isolation) first.
