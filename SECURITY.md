# Security Policy

## Reporting a vulnerability

Report vulnerabilities privately through the repository's GitHub Security
Advisories page. Do not open a public issue for an unpatched vulnerability.
Include affected versions, reproduction steps, impact, and any suggested
mitigation. Maintainers will acknowledge the report, coordinate a fix and
release, and credit the reporter when requested.

## Supported version

qcg has no public release yet. Security fixes apply to the current development
source; no released version is supported.

## Scope reminders

qcg executes generator contracts selected from an installed catalog or passed
explicitly as a local path. Approved commands, containers, network access,
secrets, and side effects remain security-sensitive. Review package provenance
and the permission summary before installation or execution, and do not use
`--yes` for an unreviewed package. URL installs currently require out-of-band
checksum or signature verification.
