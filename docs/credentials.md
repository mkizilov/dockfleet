# Provisioning credentials

DockFleet has five kinds of secret. This document says what each is, how to make it, how to hand it to the process without leaving it where it can leak, and how to replace it. The certificates' layout and the node identity rules are in [mtls.md](mtls.md); here is the operational side.

| Secret | Protects | Lives on | Made with |
| --- | --- | --- | --- |
| **CA private key** (`ca.key`) | every certificate: whoever has it can mint any node | the CA host only, offline | `dockfleet-pki init-ca` |
| **Manager key** (`manager.key`) | the manager's identity to agents and administrators | that manager | `dockfleet-pki issue-manager` |
| **Agent key** (`agent.key`) | one worker's identity | **that worker only**; it is never copied | `dockfleet-pki new-csr` on the worker |
| **Admin API token** | the whole administrative API and the dashboard | managers; administrators' tooling | a random string |
| **Database URL** (with password) | the entire state of the fleet | managers | your database |
| *(high availability)* **etcd client key** | who may lead | managers | your etcd's CA |
| *(development only)* agent token | plaintext agents on loopback | dev machines | `config/dev.env` (a labelled throwaway) |

## The two rules

1. **A credential is a file, not an environment variable.** Every credential setting has a `_FILE` form: `DOCKFLEET_API_TOKEN_FILE`, `DOCKFLEET_DATABASE_URL_FILE`, `DOCKFLEET_AGENT_TOKEN_FILE`. The process reads the file once at startup, trims surrounding whitespace, refuses an empty file, a directory, a relative path or one over 8 KiB, and refuses to start if both the variable and its `_FILE` are set. Errors name the file and never what is in it. A value in the environment is visible to `docker inspect`, to `ps e`, to `/proc/<pid>/environ` and to anything that dumps the environment on a crash; a file that only the process can read is not. The end-to-end tests start every process with files only and check that no log of any process contains the admin token or the database password.
2. **A private key never travels.** The worker makes its own key; the CA host sees only the signing request.

## Generating the shared secrets

```sh
umask 077
head -c 32 /dev/urandom | base64 | tr -d '\n=' > api-token          # 43 characters; the minimum accepted is 16
```

Use a different value in every environment, and never the labelled throwaway values in `config/dev.env`. There is one admin token (no per-user accounts), so treat it like a root password: give it to the few tools that need it, and rotate it when someone who knew it leaves.

For the database, create a role with a generated password, put the complete URL (with `sslmode=verify-full`) in a file, and nowhere else.

## The CA and the manager's certificate

On the CA host:

```sh
umask 077
dockfleet-pki init-ca -dir pki                                           # pki/ca.pem, pki/ca.key (0600)
dockfleet-pki issue-manager -dir pki -dns manager-1.internal -ip 10.0.0.11   # pki/manager.pem, pki/manager.key
```

Keep `ca.key` offline once the certificates you need are issued (copy it to removable media; delete it from the working machine). The development CA that `dockfleet-pki` makes has no CRL or OCSP, no HSM and no audit trail: for production use your organization's CA with the same layout (one URI SAN `spiffe://dockfleet/node/<id>` on agent certificates; client-auth for agents, server-auth for the manager). `init-ca` never overwrites a CA, and no command prints key material.

Move `manager.pem`, `manager.key` and `ca.pem` to the manager with a channel you trust (`scp` over SSH, or your secret store). Never email or chat a key, put one in an image, or commit one: `make check-secrets` fails CI if a PEM private key or a `*.key` file is tracked, and `.dockerignore` keeps keys and `.pem`/`.env` files out of a build context.

## A worker certificate: the key stays on the worker

```sh
# On the worker, as the user the agent runs as:
dockfleet-pki new-csr -node-id worker-1 -out /etc/dockfleet/tls          # agent.key (0600) and agent.csr

# Copy agent.csr (public) to the CA host and sign it there:
dockfleet-pki sign-agent -dir pki -node-id worker-1 -csr agent.csr -days 90

# Copy pki/agents/worker-1/agent.pem and pki/ca.pem back to /etc/dockfleet/tls/ on the worker.
# Then enroll the node (an administrator, once):
curl --cacert pki/ca.pem -H "Authorization: Bearer $TOKEN" -d '{"node_id":"worker-1"}' https://manager-1.internal:21280/v1/enrollments
```

`sign-agent` takes the identity from `-node-id`, not from the request, and refuses a request that names another node. A node is admitted only if all three hold: its certificate chains to the CA the manager trusts, it names that very node ID, and an administrator enrolled the ID. One certificate per node: a certificate reused on two machines makes them indistinguishable.

The agent and the manager **refuse a private key that others can read** (`chmod 600`; group access is tolerated so a service group can share one) and an agent refuses a certificate for a different node ID than it is configured with.

## Handing secrets to the process

| Where it runs | How | Notes |
| --- | --- | --- |
| systemd (250 or newer) | `LoadCredential=api-token:/etc/dockfleet/secrets/api-token` and `Environment=DOCKFLEET_API_TOKEN_FILE=%d/api-token` | systemd copies the file into a private, memory-backed directory only the service can read; it never enters the environment. [`dockfleet-manager.service`](../deploy/examples/systemd/dockfleet-manager.service). The TLS key is read directly from `/etc/dockfleet/tls` so renewals are noticed. |
| Docker / Compose | `secrets:` with `file:`; the setting points at `/run/secrets/<name>` | [`compose.manager.yml`](../deploy/examples/compose.manager.yml); the secret files on the host are mode 0400 |
| anything else | a file readable only by the service user, `DOCKFLEET_..._FILE` naming it | |

Keep `/etc/dockfleet/secrets` and the TLS directory at `0750` or tighter, owned by `root` and readable by the service's group only.

## Rotation

| What | How | Interruption |
| --- | --- | --- |
| **Agent or manager certificate** | issue a new one over the old files (`sign-agent -force`, `issue-manager -force`) | none: the files are re-read when they change, and a new connection uses the new certificate. A broken renewal keeps the previous certificate and logs the error. Renew well before expiry: the processes log warnings as it approaches, and `dockfleet-pki inspect` shows dates. Default validity is 90 days. |
| **CA** | add the new CA to the bundle on managers and workers (`ca.pem` may hold several), reissue every certificate from the new CA, then remove the old CA from the bundles | none, if done in that order; the client CA bundle is re-read too |
| **Admin API token** | write the new value, restart the managers one at a time | requests with the old token fail from the restart on; dashboard sessions (signed with the token) end |
| **Database password** | change it in PostgreSQL to accept both if you can, update the file, restart managers one at a time | with several managers none; with one, seconds. Agents ride out a manager restart (workloads keep running) |
| **etcd client certificate** | as the manager's: replace the files | none |
| **A lost worker key / stolen laptop** | revoke the enrollment at once: `DELETE /v1/enrollments/{id}`; the node's session ends at its next heartbeat and its replicas are moved when it is given up on | there is no certificate revocation list; enrollment is the revocation. Re-enroll only after reissuing the node's certificate with a new key. |

## What never to do

- Do not set a credential in `docker run -e`, a Compose `environment:` entry, a systemd `Environment=` (except the `_FILE` path) or a CI variable that prints.
- Do not bake `DOCKFLEET_*TOKEN*`, `*PASSWORD*` or a URL with a password into an image: a test fails the Dockerfile if an `ENV` or `ARG` looks like one.
- Do not use `--insecure`-style shortcuts: there are none. Plaintext exists only for loopback development with an agent token, a non-loopback address without TLS is refused at startup, an agent dialing a remote manager without TLS is refused, and a TLS failure is never retried in plaintext.
- Do not share `ca.key` with the manager. A compromised manager must not be able to mint nodes.
- Do not reuse the development credentials in `config/dev.env` or `.dev-pki/` anywhere that is not your own workstation.

## Checking what you have done

```sh
dockfleet-pki inspect pki/manager.pem /etc/dockfleet/tls/agent.pem     # names, node ID, expiry, fingerprint
stat -c '%a %U:%G %n' /etc/dockfleet/secrets/* /etc/dockfleet/tls/*.key     # 400/600, not world-readable
curl -s --cacert pki/ca.pem https://manager-1.internal:21280/v1/nodes       # 401: the token is required
```

The end-to-end tests (`internal/e2e/security_test.go`) exercise the refusals: every API route without the token, a token in the query string, plain HTTP and TLS 1.2 to the TLS ports, a client without a certificate, with a certificate from another CA, a plaintext client on the agents' port, an agent with another node's certificate, an agent that does not trust the manager's CA, a world-readable key, an unenrolled node, and credentials in logs.
