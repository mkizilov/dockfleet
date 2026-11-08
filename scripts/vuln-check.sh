#!/usr/bin/env bash
# Runs govulncheck and fails on any vulnerability that is not in the accepted
# list below. govulncheck reports only vulnerabilities in code the binaries call.
#
# Accepted, with the reason (see docs/security-audit.md, "Dependencies and toolchain"):
#   GO-2026-4887, GO-2026-4883  github.com/docker/docker v27.5.1: bugs in the Docker
#   daemon's authorization-plugin and plugin-privilege code. DockFleet imports only the
#   client packages and never runs daemon code, and the module has no fixed version.
#
# usage: scripts/vuln-check.sh [govulncheck arguments, default ./...]
set -uo pipefail

accepted=" GO-2026-4887 GO-2026-4883 "

out=$(govulncheck "${@:-./...}" 2>&1)
status=$?
printf '%s\n' "${out}" | tail -n 12

case "${status}" in
  0) echo "govulncheck: no vulnerabilities in code the binaries call"; exit 0 ;;
  3) ;;  # vulnerabilities found: judged below
  *) echo "govulncheck failed to run (exit ${status})" >&2; exit "${status}" ;;
esac

unexpected=""
for id in $(printf '%s\n' "${out}" | sed -n 's/^Vulnerability #[0-9]*: \(GO-[0-9]*-[0-9]*\).*/\1/p'); do
  case "${accepted}" in
    *" ${id} "*) ;;
    *) unexpected="${unexpected} ${id}" ;;
  esac
done
if [ -n "${unexpected}" ]; then
  echo "govulncheck: vulnerabilities that are not accepted:${unexpected}" >&2
  exit 1
fi
if ! printf '%s\n' "${out}" | grep -q '^Vulnerability #'; then
  echo "govulncheck exited 3 but reported no vulnerability this script can read" >&2
  exit 1
fi
echo "govulncheck: only the accepted vulnerabilities remain:${accepted}"
