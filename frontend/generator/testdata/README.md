# Legacy API declaration fixture

`legacy-api-types.ts` is a byte-for-byte copy of `clients/ts/types.ts` from
commit `5b96bea` (`4b8d56a^`), immediately before the Hey API migration. It was
produced by the legacy openapi-typescript pipeline. It is test-only, not a
second generated SDK: do not regenerate it with Hey API or update it when
refreshing the current client.

`test:api` compares every legacy public export against fresh Hey API output and
anchors the synthetic mutation's unchanged types in this fixture. The legacy
file has no `components.securitySchemes` member; the test separately describes
the complete added security-scheme map rather than attributing it to the legacy
generator or excluding it from component comparison.
