# Historical API migration fixtures

These test-only fixtures are byte-for-byte copies from commit `5b96bea`
(`4b8d56a^`), immediately before the Hey API migration:

- `legacy-api-types.ts`: `clients/ts/types.ts`, produced by the legacy
  openapi-typescript pipeline.
- `legacy-openapi.json`: `docs/openapi.json`, the corresponding pre-migration
  input, 181,413 bytes, SHA-256
  `fd2b4ec2ffeb8194e645ae4a0b0257268ccf6fd266e32d7c60ca8dceed9ce9bc`.

They are not a second generated SDK. Do not regenerate or update either fixture
when refreshing the current client or changing the live contract.

`test:api` compares every legacy public export against fresh Hey API output
from **only the frozen OpenAPI input**. Its synthetic mutation also starts from
that input and anchors unchanged types in the frozen declarations. The legacy
file has no `components.securitySchemes` member; the test separately describes
the complete added security-scheme map rather than attributing it to the legacy
generator or excluding it from component comparison.

Current-contract validation remains independent: `check:api` checks generated
API types against live `docs/openapi.json`, and `scripts/check-sdk.sh` checks
SDK freshness against that live contract and strict TypeScript compilation.
`test:api` also preserves unsupported-OpenAPI rejection checks based on the live
spec and compiles the current SDK and frontend API client. Normal generation
continues to use the live spec, never these historical fixtures.
