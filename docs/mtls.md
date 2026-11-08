# Mutual TLS, node identity and enrollment

Agents and the manager authenticate each other with certificates. A node is only admitted when three things hold:

1. **Trusted certificate.** Its client certificate chains to the CA the manager trusts (`DOCKFLEET_TLS_CLIENT_CA_FILE`), is inside its validity window, and is valid for client authentication.
2. **Bound identity.** The certificate carries the node's identity, and every request names that same node ID.
3. **Explicit enrollment.** An administrator enrolled the node ID through the admin API.

The agent in turn verifies the manager's certificate against its own CA bundle and the expected host name. If TLS fails, there is no retry in plaintext.

## Rules at a glance

| Situation | Behavior |
| --------- | -------- |
| Manager listener on a non-loopback address without TLS | Refused at configuration time and again in `Manager.Serve` |
| TLS configured | The agent listener requires and verifies client certificates (TLS 1.3 only). There is no TLS mode without client certificates, and `DOCKFLEET_AGENT_TOKEN` is ignored |
| Agent dialing a non-loopback manager without TLS | Refused (`ErrInsecureRemote`, and the configuration is rejected at startup) |
| Agent with TLS configured against a plaintext manager | Handshake fails; the agent never downgrades |
| Plaintext (no TLS) | Loopback only, authenticated by the shared agent token, intended for local development. The caller can claim any enrolled node ID |
| Admin API | HTTPS when TLS is configured (server certificate only); always requires the admin bearer token |

## Identity

A node certificate carries one URI SAN: `spiffe://dockfleet/node/<node-id>`. The node ID must match `^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$`. Certificates with no such URI, or with more than one, are rejected. The common name is informational only.

The manager derives the caller's identity from the verified client certificate, never from the request. `RegisterNode`, `Heartbeat` and `DeregisterNode` fail with `PERMISSION_DENIED` / `IDENTITY_MISMATCH` when the `node_id` differs from it. An agent whose configured `DOCKFLEET_NODE_ID` differs from its certificate refuses to start.

The agent's container-log stream (`Logs`, [logs.md](logs.md)) is bound the same way: the certificate is verified when the stream opens, and the `node_id` in its first message must equal the identity in it, so an agent cannot receive the log requests of another node. The stream also needs the node's current session and an active enrollment, which the manager keeps checking while it is open.

## Enrollment

Enrollment is the administrator's statement "this node ID may join". It does not depend on certificates and is checked on every registration and every heartbeat, so revoking it takes effect at the node's next heartbeat.

```sh
TOKEN=...   # DOCKFLEET_API_TOKEN
curl -s --cacert ca.pem -H "Authorization: Bearer $TOKEN" \
  -d '{"node_id":"worker-1","description":"rack 4"}' https://manager.example.com:21280/v1/enrollments

curl -s --cacert ca.pem -H "Authorization: Bearer $TOKEN" https://manager.example.com:21280/v1/enrollments
curl -s --cacert ca.pem -H "Authorization: Bearer $TOKEN" -X DELETE https://manager.example.com:21280/v1/enrollments/worker-1
```

- `POST` returns `201`, or `409` while an active enrollment exists. Enrolling a revoked node ID again works.
- `DELETE` revokes (idempotent, `404` for an unknown ID). The node's session ends at its next heartbeat and it stops refreshing its liveness, so it leaves scheduling after `DOCKFLEET_NODE_TTL`.
- A valid certificate for a node that is not enrolled is rejected with `PERMISSION_DENIED` / `NOT_ENROLLED`, nothing is stored for it, and the agent keeps retrying (logging an error each time) so enrolling it, or re-enrolling after a revocation, needs no restart.

## Development credentials

`dockfleet-pki` (`cmd/pki`, built into `bin/dockfleet-pki`) manages a CA and its certificates. Everything is written under `.dev-pki/` by default, which is git-ignored, with owner-only file permissions (private keys `0600`, directories `0700`). No command prints key material, and existing files are never overwritten unless `-force` is given (and `init-ca` has no `-force`, so a CA key cannot be replaced by accident). `make check-secrets` fails if a PEM private key or a `*.key` file is tracked by git.

```sh
make dev-pki                 # CA + manager certificate + certificate for dev-node-1
make run-manager-mtls        # HTTPS admin API, mutual TLS for agents (needs `make compose-up`; add DOCKFLEET_STORE=memory to skip PostgreSQL)
make dev-enroll SCHEME=https # enroll dev-node-1
make run-agent-mtls
```

Or run the commands yourself:

```sh
dockfleet-pki init-ca -dir pki
dockfleet-pki issue-manager -dir pki -dns manager.example.com -ip 10.0.0.5
dockfleet-pki issue-agent -dir pki -node-id worker-1        # key and certificate in one step (development)
dockfleet-pki inspect pki/manager.pem pki/agents/worker-1/agent.pem
```

`issue-manager` without `-dns` / `-ip` produces a certificate for `localhost`, `127.0.0.1` and `::1`. For a real manager name every host name or address agents will dial; agents verify it (`DOCKFLEET_TLS_SERVER_NAME` overrides the expected name).

## Distributing credentials to worker machines

(For production, [credentials.md](credentials.md) covers the whole life of every secret: making them, handing them to the process as files, rotating them, and what to do when one is lost. [deployment.md](deployment.md) puts it in order.)

What each machine needs, and what must never leave the CA host:

| File | Where | Secret? |
| ---- | ----- | ------- |
| `ca.key` | CA host only. Never copy it to the manager or workers | **yes** |
| `ca.pem` | manager (as `DOCKFLEET_TLS_CLIENT_CA_FILE`) and every worker (as `DOCKFLEET_TLS_CA_FILE`) | no |
| `manager.pem`, `manager.key` | manager only | key: **yes** |
| `agent.pem`, `agent.key` | the one worker they are issued for | key: **yes** |

**Recommended: the signing-request flow, so a worker's private key never travels.**

```sh
# 1. On the worker (as the user that runs the agent):
dockfleet-pki new-csr -node-id worker-1 -out /etc/dockfleet     # writes agent.key (0600) and agent.csr
# 2. Copy agent.csr to the CA host (it is public) and sign it there:
dockfleet-pki sign-agent -dir pki -node-id worker-1 -csr agent.csr -days 90
# 3. Copy pki/agents/worker-1/agent.pem and ca.pem back to the worker:
scp pki/agents/worker-1/{agent.pem,ca.pem} worker-1:/etc/dockfleet/
# 4. Enroll the node (once), then start the agent.
```

`sign-agent` ignores the names inside the request: the identity comes from `-node-id`, and the request must name the same node, so a mix-up is caught.

Worker configuration (`/etc/dockfleet/agent.env`, readable by the agent user only):

```
DOCKFLEET_MANAGER_ADDR=manager.example.com:21281
DOCKFLEET_NODE_ID=worker-1
DOCKFLEET_TLS_CA_FILE=/etc/dockfleet/ca.pem
DOCKFLEET_TLS_CERT_FILE=/etc/dockfleet/agent.pem
DOCKFLEET_TLS_KEY_FILE=/etc/dockfleet/agent.key
```

Manager configuration (non-loopback addresses are allowed because TLS is on):

```
DOCKFLEET_API_ADDR=0.0.0.0:21280
DOCKFLEET_GRPC_ADDR=0.0.0.0:21281
DOCKFLEET_TLS_CERT_FILE=/etc/dockfleet/manager.pem
DOCKFLEET_TLS_KEY_FILE=/etc/dockfleet/manager.key
DOCKFLEET_TLS_CLIENT_CA_FILE=/etc/dockfleet/ca.pem
```

Guidelines for administrators:

- Move files with a channel you trust (`scp`/`rsync` over SSH, a configuration-management secret store). Never email or chat private keys, put them in images, or commit them.
- Keep `ca.key` offline or on a dedicated host. The development CA created by `dockfleet-pki` is a convenience, not a production PKI: it has no CRL/OCSP, no HSM and no audit trail. Use your organization's CA for production and keep the same certificate layout (URI SAN identity, client/server extended key usage).
- Private keys must not be accessible to all users: the manager and agent refuse a key with world permissions (`chmod 600`). Group access is tolerated so a service group can share a key.
- Use one certificate per node. Reusing a certificate on two machines makes them indistinguishable.
- Check what you issued with `dockfleet-pki inspect` (it prints the node ID, expiry and SHA-256 fingerprint).

## Expiration and renewal

- Expired and not-yet-valid certificates are rejected in the TLS handshake. The manager also re-checks the validity window on every call, so a connection opened with a valid certificate cannot outlive it (`UNAUTHENTICATED` / `CERTIFICATE_INVALID`). The agent treats that as temporary and keeps running. A stream such as the log stream is checked when it opens; it ends with its connection, which the manager closes after at most 10 minutes.
- The agent refuses to start with an expired certificate or one for another node. Both processes check their own certificate at startup and every hour afterwards: a warning once it is within 30 days of expiry, an error once it has expired.
- Certificates and the CA bundle are re-read when the files change, so renewal needs no restart: replace the files (atomically, e.g. `dockfleet-pki sign-agent ... -force`, which writes through a temporary file and rename), and the next connection uses them. The manager closes agent connections after at most 10 minutes (plus jitter), so even a quiet connection moves to the new certificate soon. If a new file is unreadable or does not match its key, the process logs one error and keeps the previous certificate.
- Renew agent certificates by running `new-csr`/`sign-agent` again (with `-force`) well before expiry; the default validity is 90 days.
- A CA change (adding a new CA to the bundle) is picked up by the manager for new connections. Agents load their trust bundle at startup, so restart them after changing the CA that signs the manager certificate.

## Revoking access

There is no CRL. To stop a compromised node: revoke its enrollment (immediate effect at the next heartbeat; its log stream, which is checked every few seconds, ends too, and its containers' logs can no longer be read through the manager) and stop issuing certificates for its ID. Because enrollment is by node ID, a leaked key for an ID that stays enrolled can still be used until the certificate expires; to close that gap, revoke the enrollment and then enroll a replacement under a new node ID (and new certificate). If the CA key leaks, replace the CA and every certificate.

## Troubleshooting

| Symptom (agent log) | Cause |
| ------------------- | ----- |
| `agent certificate ... has expired` at startup | Renew the certificate |
| `agent certificate ... is for node "a" but DOCKFLEET_NODE_ID is "b"` | Fix the node ID or issue the right certificate |
| `registration failed ... remote error: tls: bad certificate` | The manager does not trust the agent certificate: wrong CA, expired, or not a client certificate |
| `tls: failed to verify certificate: x509: certificate is valid for ...` | The manager certificate lacks the name the agent dialed; reissue with `-dns`/`-ip` or set `DOCKFLEET_TLS_SERVER_NAME` |
| `certificate signed by unknown authority` | The agent's `DOCKFLEET_TLS_CA_FILE` is not the CA that signed the manager certificate |
| `this node is not enrolled; an administrator must enroll it` | Enroll the node ID (or re-enroll after a revocation) |
| `manager rejected registration ... IDENTITY_MISMATCH` | The certificate is for another node ID |
| `private key ... is accessible by all users` | `chmod 600` the key file |
