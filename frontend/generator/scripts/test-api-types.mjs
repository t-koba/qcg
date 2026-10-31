// Bidirectional structural parity, not a text snapshot: every legacy public
// export is checked, including the populated operationId mutation. No network.
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, readFileSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { openapiTypes } from './openapi-types.mjs';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../../..');
const spec = JSON.parse(readFileSync(resolve(root, 'docs/openapi.json')));
const directory = mkdtempSync(resolve(tmpdir(), 'qcg-api-parity-'));
try {
  writeFileSync(resolve(directory, 'baseline.ts'), await openapiTypes(spec));
  writeFileSync(resolve(directory, 'canonical.ts'), readFileSync(resolve(root, 'clients/ts/types.ts')));
  const mutated = structuredClone(spec);
  const op = mutated.paths['/healthz'].get;
  op.operationId = 'probeHealth';
  op.parameters = [{ in: 'query', name: 'mode', required: true, schema: { type: 'string', enum: ['brief', 'full'] } }];
  mutated.components.schemas.MigrationProbe = {
    type: 'object', properties: { label: { type: 'string', default: 'ready' }, free: true },
  };
  op.responses['200'].content['application/json'].schema = { $ref: '#/components/schemas/MigrationProbe' };
  writeFileSync(resolve(directory, 'mutated.ts'), await openapiTypes(mutated));
  writeFileSync(resolve(directory, 'parity.ts'), `
import type * as A from './canonical';
import type * as B from './baseline';
import type * as M from './mutated';
type Assert<T extends true> = T;
type Bi<A, B> = [A] extends [B] ? [B] extends [A] ? true : false : false;
type Baseline = [Assert<Bi<A.paths, B.paths>>,
  Assert<Bi<Omit<A.components, 'securitySchemes'>, Omit<B.components, 'securitySchemes'>>>,
  Assert<Bi<A.webhooks, B.webhooks>>, Assert<Bi<A.$defs, B.$defs>>, Assert<Bi<A.operations, B.operations>>,
  Assert<Bi<A.components['securitySchemes']['bearerAuth'], Bearer>>];
type Probe = { label: string; free?: unknown };
type Bearer = { type: 'http'; scheme: string };
type Old = A.paths['/healthz']['get'];
type Operation = Omit<Old, 'parameters' | 'responses'> & {
  parameters: { query: { mode: 'brief' | 'full' }; header?: never; path?: never; cookie?: never };
  responses: Omit<Old['responses'], 200> & {
    200: Omit<Old['responses'][200], 'content'> & { content: { 'application/json': Probe } };
  };
};
type Paths = Omit<A.paths, '/healthz'> & { '/healthz': Omit<A.paths['/healthz'], 'get'> & { get: Operation } };
type Components = Omit<A.components, 'schemas' | 'securitySchemes'> & {
  schemas: A.components['schemas'] & { MigrationProbe: Probe };
  securitySchemes: { bearerAuth: Bearer };
};
type Mutation = [Assert<Bi<Paths, M.paths>>, Assert<Bi<Components, M.components>>,
  Assert<Bi<A.webhooks, M.webhooks>>, Assert<Bi<A.$defs, M.$defs>>, Assert<Bi<{probeHealth: Operation}, M.operations>>];
`);
  execFileSync(process.execPath, [resolve(root, 'frontend/generator/node_modules/typescript/bin/tsc'),
    '--noEmit', '--strict', '--target', 'ES2022', '--module', 'ESNext', '--moduleResolution', 'bundler', '--lib', 'ES2022,DOM',
    resolve(directory, 'parity.ts'), ...['client.ts', 'types.ts'].map((p) => resolve(root, 'clients/ts', p)),
    resolve(root, 'frontend/generator/src/api/client.ts')], { stdio: 'inherit' });
  // Unsupported content must fail before Hey API can approximate or discard it.
  for (const [name, change] of [
    ['webhooks', (s) => { s.webhooks = { event: {} }; }],
    ['$defs', (s) => { s.$defs = { Event: { type: 'string' } }; }],
    ['components.pathItems', (s) => { s.components.pathItems = { Item: {} }; }],
    ['unsupported security scheme', (s) => { s.components.securitySchemes.oauth = { type: 'oauth2', flows: {} }; }],
    ['allOf', (s) => { s.components.schemas.Probe = { allOf: [{ type: 'string' }] }; }],
    ['remote ref', (s) => { s.components.schemas.Probe = { $ref: 'https://example.invalid/schema.json' }; }],
    ['duplicate operationId', (s) => { s.paths['/healthz'].get.operationId = s.paths['/metrics'].get.operationId = 'duplicate'; }],
  ]) {
    const unsupported = structuredClone(spec);
    change(unsupported);
    await assert.rejects(() => openapiTypes(unsupported), /Unsupported OpenAPI/, name);
  }
} finally { rmSync(directory, { recursive: true, force: true }); }
console.log('All five exports: baseline and synthetic mutation parity; strict SDK/API-client compilation; explicit rejection passed');
