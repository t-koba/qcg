# Local OpenAPI core fork

`qcg-openapi-core-1.34.20.tgz` is the private, MIT-licensed
`@qcg/openapi-core@1.34.20` fork, **not** an upstream Redocly release.
`openapi-core-upstream.json` records the exact upstream registry URL, version,
repository and SHA-512 integrity from the original frontend lock.
The tarball embeds that record as `QCG-UPSTREAM.json` and the human-readable
`openapi-core.patch` as `QCG-PATCH.diff`. Upstream files are otherwise retained,
including its MIT package metadata; the upstream tarball does not contain a
standalone LICENSE file. No missing upstream copyright text is invented here.

## Why a local fork

The sole locked minimatch consumer was Redocly 1.34.20, pinned to minimatch 5.1.9.
The fork pins minimatch 11.0.3 and adapts its named CommonJS export in
`lib/utils.js`, with the matching named import in `src/utils.ts`.
It retains upstream version 1.34.20 so openapi-typescript's existing semver range
can reuse it, but declares its own name and private status. Its Node engine is
`20 || >=22`, matching the stricter brace-expansion dependency (CI uses Node 22).

The root devDependency installs the local file under the compatibility key
`@redocly/openapi-core`. npm reuses that node for openapi-typescript; there are
no overrides, postinstall mutations, registry aliases or falsified upstream
integrity/name fields. The lock records `name: "@qcg/openapi-core"` and the
local file's own integrity. `npm ls minimatch --all` shows the local alias and
only minimatch 11.0.3.

The new locked closure preserves the supplied registry tarball integrity and
license for each package:

| Package | Version | License |
| --- | --- | --- |
| minimatch | 11.0.3 | BlueOak-1.0.0 |
| @isaacs/nonrecursive-glob-match | 1.0.3 | BlueOak-1.0.0 |
| brace-expansion | 5.0.12 | MIT |
| balanced-match | 4.0.4 | MIT |

## Offline reproduction and validation

With Python 3, `patch`, and an already available upstream tarball (the builder
never downloads), from `frontend/generator`:

```sh
python3 vendor/build-openapi-core.py /path/to/upstream.tgz /tmp/rebuilt.tgz
cmp vendor/qcg-openapi-core-1.34.20.tgz /tmp/rebuilt.tgz
npm ci --offline --ignore-scripts --no-audit --no-fund
npm ls minimatch --all
npm run check:vendor
CARGO_NET_OFFLINE=true npm_config_offline=true npm run check:api
npm sbom --offline --sbom-format=cyclonedx > /tmp/frontend-sbom.json
```

The builder rejects a mismatched upstream SRI, applies the patch without fuzz,
and emits a canonical tar/gzip (sorted entries, fixed timestamps and ownership).
The artifact SRI is:

```text
sha512-gXKlAEx2ldvGJpPLhhi6s28CZx7eBMP2GvKXSdczHfqr8jfeSDGVkD+3lH0iavMvy8j32bErJeK+diymmfdF/A==
```

`check:vendor` checks file integrity, alias reuse by openapi-typescript, truthful
installed identity, embedded provenance/patch, the exact closure SRIs/licenses,
and ten URL/header cases through Redocly's real `readFileFromUrl` with a local
customFetch stub (no network). `check:api` compares generated OpenAPI and
TypeScript byte-for-byte with the checked-in canonical copies. In npm 10's
CycloneDX output, the component display name is the compatibility install key,
but its purl is `pkg:npm/%40qcg/openapi-core@1.34.20`, with the local artifact's
SHA-512 hash and MIT license.

## Audit policy and remaining verification

The CI command remains `npm audit --audit-level=moderate`; no advisory is
suppressed. In particular, deepmerge and its existing low advisory are not
remediated or hidden by this change; its lock entry is unchanged.

**Do not treat `npm audit --offline` as security evidence.** npm 10 skips its
advisory request in offline mode and prints “found 0 vulnerabilities” without
checking even the known low deepmerge advisory. An authorized online CI audit
is still required to establish the moderate-threshold result. This fork also
changes the package name reported to audit, so the embedded upstream provenance
and patch must remain available for upstream vulnerability review; a clean audit
is not proof that all upstream code is free of advisories.
