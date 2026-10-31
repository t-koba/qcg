// Hey API owns schema translation. This adapter retains qcg's public indexed
// OpenAPI types and its defaulted-property requiredness; it is not an SDK runtime.
import { createClient } from '@hey-api/openapi-ts';
import ts from 'typescript';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const methods = ['get', 'put', 'post', 'delete', 'options', 'head', 'patch', 'trace'];
const sections = ['schemas', 'responses', 'securitySchemes', 'parameters', 'requestBodies', 'headers', 'pathItems'];
const quote = JSON.stringify;
const object = (entries) => `{ ${entries.join('; ')} }`;
const field = (name, type, optional = false) => `${quote(name)}${optional ? '?' : ''}: ${type}`;
const entries = (value = {}) => Object.entries(value);
function fail(where, message) { throw new Error(`Unsupported OpenAPI at ${where}: ${message}`); }
function keys(value, allowed, where) {
  for (const key of Object.keys(value)) if (!allowed.includes(key)) fail(where, key);
}

export async function openapiTypes(spec) {
  if (!/^3\.1\./.test(spec.openapi)) fail('openapi', 'expected 3.1');
  keys(spec, ['openapi', 'info', 'paths', 'components', 'security', 'servers', 'tags', 'externalDocs', 'webhooks', '$defs'], 'root');
  for (const key of ['webhooks', '$defs']) if (entries(spec[key]).length) fail(key, 'nonempty section');
  keys(spec.components ?? {}, sections, 'components');
  for (const section of ['parameters', 'requestBodies', 'headers', 'pathItems'])
    if (entries(spec.components?.[section]).length) fail(`components.${section}`, 'nonempty section');
  const models = new Map(entries(spec.components?.schemas).map(([name], i) => [name, `QcgModel${i}`]));
  const schemas = {};
  const ref = (value, section) => {
    const match = /^#\/components\/([^/]+)\/([^/]+)$/.exec(value);
    if (!match || match[1] !== section) fail(value, `expected local components/${section} reference`);
    const name = match[2].replaceAll('~1', '/').replaceAll('~0', '~');
    if (!Object.hasOwn(spec.components?.[section] ?? {}, name)) fail(value, 'unresolved reference');
    return { name, type: `components[${quote(section)}][${quote(name)}]` };
  };
  function normalize(schema, where) {
    // Hey API 0.97.1 drops boolean-true object properties; {} preserves unknown.
    if (schema === true) return {};
    if (schema === false) fail(where, 'boolean-false schema');
    if (!schema || typeof schema !== 'object' || Array.isArray(schema)) fail(where, 'expected schema');
    keys(schema, ['$ref', '$schema', 'title', 'description', 'default', 'examples', 'example', 'deprecated', 'type', 'format', 'minimum', 'maximum', 'pattern', 'minLength', 'maxLength', 'minItems', 'maxItems', 'uniqueItems', 'properties', 'required', 'additionalProperties', 'items', 'oneOf', 'anyOf', 'enum', 'const', 'not'], where);
    if (schema.$ref) {
      keys(schema, ['$ref', 'description', 'title', 'default'], where);
      return { ...schema, $ref: `#/components/schemas/${models.get(ref(schema.$ref, 'schemas').name)}` };
    }
    const copy = { ...schema };
    // The legacy declarations use string for binary, not a browser Blob union.
    if (schema.format === 'binary') delete copy.format;
    // These validation-only constraints were never representable in the public
    // TS types. Accept only the existing FileValue negated-required shape.
    if (schema.not) {
      keys(schema.not, ['required'], `${where}/not`);
      if (!Array.isArray(schema.not.required) || !schema.not.required.every((x) => typeof x === 'string')) fail(where, 'not.required');
    }
    if (schema.type !== undefined && ![].concat(schema.type).every((t) => ['null', 'boolean', 'integer', 'number', 'string', 'object', 'array'].includes(t))) fail(where, 'type');
    for (const key of ['oneOf', 'anyOf']) if (schema[key]) copy[key] = schema[key].map((s, i) => normalize(s, `${where}/${key}/${i}`));
    if (schema.properties) {
      copy.properties = Object.fromEntries(entries(schema.properties).map(([name, s]) => [name, normalize(s, `${where}/properties/${name}`)]));
      copy.required = [...new Set([...(schema.required ?? []), ...entries(schema.properties).filter(([, s]) => s && Object.hasOwn(s, 'default')).map(([name]) => name)])];
    }
    if (schema.items !== undefined) copy.items = normalize(schema.items, `${where}/items`);
    if (schema.type === 'array' && schema.items === undefined) fail(where, 'array without items');
    if (typeof schema.additionalProperties === 'object') copy.additionalProperties = normalize(schema.additionalProperties, `${where}/additionalProperties`);
    return copy;
  }
  function schemaType(schema, where) {
    if (schema?.$ref) { normalize(schema, where); return ref(schema.$ref, 'schemas').type; }
    const name = `QcgInline${Object.keys(schemas).length}`;
    schemas[name] = normalize(schema, where);
    return name;
  }
  for (const [name, schema] of entries(spec.components?.schemas)) schemas[models.get(name)] = normalize(schema, `components/schemas/${name}`);
  function reference(value, section, render, where) {
    if (value.$ref) { keys(value, ['$ref', 'description', 'summary'], where); return ref(value.$ref, section).type; }
    return render(value, where);
  }
  function content(value, where) {
    return object(entries(value).map(([mime, media]) => {
      keys(media, ['schema', 'example', 'examples'], `${where}/${mime}`);
      return field(mime, schemaType(media.schema ?? true, `${where}/${mime}/schema`));
    }));
  }
  function header(value, where) {
    return reference(value, 'headers', (h) => {
      keys(h, ['description', 'required', 'deprecated', 'schema', 'example', 'examples', 'style', 'explode'], where);
      if (!h.schema) fail(where, 'header without schema');
      return schemaType(h.schema, `${where}/schema`);
    }, where);
  }
  function securityScheme(value, where) {
    keys(value, ['type', 'scheme', 'bearerFormat', 'description'], where);
    if (value.type !== 'http' || value.scheme !== 'bearer') fail(where, 'only HTTP bearer security schemes are supported');
    if (value.bearerFormat !== undefined && typeof value.bearerFormat !== 'string') fail(where, 'bearerFormat');
    if (value.description !== undefined && typeof value.description !== 'string') fail(where, 'description');
    return object([
      field('type', quote('http')),
      field('scheme', 'string'),
      ...(value.bearerFormat === undefined ? [] : [field('bearerFormat', 'string', true)]),
      ...(value.description === undefined ? [] : [field('description', 'string', true)]),
    ]);
  }
  function response(value, where) {
    return reference(value, 'responses', (r) => {
      keys(r, ['description', 'headers', 'content'], where);
      return object([
        field('headers', object([...entries(r.headers).map(([name, h]) => field(name, header(h, `${where}/headers/${name}`), !h.required)), '[name: string]: unknown'])),
        field('content', r.content ? content(r.content, `${where}/content`) : 'never', !r.content),
      ]);
    }, where);
  }
  function body(value, where) {
    return reference(value, 'requestBodies', (b) => {
      keys(b, ['description', 'required', 'content'], where);
      if (!b.content) fail(where, 'body without content');
      return object([field('content', content(b.content, `${where}/content`))]);
    }, where);
  }
  function parameter(p, where) {
    keys(p, ['name', 'in', 'required', 'description', 'deprecated', 'schema', 'example', 'examples', 'style', 'explode', 'allowReserved', 'allowEmptyValue'], where);
    if (!['query', 'header', 'path', 'cookie'].includes(p.in) || !p.name || !p.schema) fail(where, 'parameter location/name/schema');
    return schemaType(p.schema, `${where}/schema`);
  }
  function parameters(values = [], where) {
    const groups = Object.fromEntries(['query', 'header', 'path', 'cookie'].map((k) => [k, []]));
    for (const p of values) {
      const type = parameter(p, where);
      if (!groups[p.in]) fail(where, 'parameter location');
      if (groups[p.in].some((item) => item.name === p.name)) fail(where, 'duplicate parameter');
      groups[p.in].push({ ...p, type });
    }
    return object(entries(groups).map(([location, ps]) => field(location,
      ps.length ? object(ps.map((p) => field(p.name, p.type, !p.required && location !== 'path'))) : 'never',
      !ps.some((p) => p.required || location === 'path'))));
  }
  const operations = [];
  function operation(op, where) {
    keys(op, ['operationId', 'summary', 'description', 'tags', 'deprecated', 'security', 'servers', 'externalDocs', 'parameters', 'requestBody', 'responses'], where);
    const type = object([
      field('parameters', parameters(op.parameters, `${where}/parameters`)),
      field('requestBody', op.requestBody ? body(op.requestBody, `${where}/requestBody`) : 'never', !op.requestBody || !op.requestBody.required),
      field('responses', object(entries(op.responses).map(([code, r]) => field(code, response(r, `${where}/responses/${code}`)))))
    ]);
    if (!op.operationId) return type;
    if (operations.some(([id]) => id === op.operationId)) fail(where, 'duplicate operationId');
    operations.push([op.operationId, type]);
    return `operations[${quote(op.operationId)}]`;
  }
  const paths = object(entries(spec.paths).map(([path, item]) => {
    keys(item, [...methods, 'summary', 'description', 'servers', 'parameters'], `paths/${path}`);
    if (item.parameters?.length) fail(path, 'path-level parameters');
    return field(path, object([
      field('parameters', parameters()),
      ...methods.map((m) => field(m, item[m] ? operation(item[m], `${path}/${m}`) : 'never', !item[m]))
    ]));
  }));
  const renderers = { schemas: (_, name) => models.get(name), responses: response, securitySchemes: securityScheme, parameters: parameter, requestBodies: body, headers: header };
  const components = object(sections.map((section) => field(section, entries(spec.components?.[section]).length
    ? object(entries(spec.components[section]).map(([name, value]) => field(name, renderers[section](value, section === 'schemas' ? name : `components/${section}/${name}`)))) : 'never')));
  const directory = mkdtempSync(join(tmpdir(), 'qcg-openapi-'));
  try {
    await createClient({
      input: { openapi: '3.1.0', info: { title: 'qcg types', version: '1' }, paths: {}, components: { schemas } },
      output: directory, plugins: ['@hey-api/typescript'], logs: { level: 'silent' },
    });
    const ast = ts.createSourceFile('types.ts', readFileSync(join(directory, 'types.gen.ts'), 'utf8'), ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
    const aliases = new Map(ast.statements.filter(ts.isTypeAliasDeclaration).map((s) => [s.name.text, s]));
    if (ast.parseDiagnostics.length) fail('Hey API output', 'invalid TypeScript');
    function checkReferences(node) {
      if (ts.isTypeReferenceNode(node) && !Object.hasOwn(schemas, node.typeName.getText(ast)) && !['Array', 'Record'].includes(node.typeName.getText(ast)))
        fail('Hey API output', `unexpected type reference ${node.typeName.getText(ast)}`);
      ts.forEachChild(node, checkReferences);
    }
    const printer = ts.createPrinter({ newLine: ts.NewLineKind.LineFeed });
    const types = Object.keys(schemas).map((name) => {
      const node = aliases.get(name);
      if (!node) fail(name, 'Hey API did not emit schema');
      checkReferences(node.type);
      return printer.printNode(ts.EmitHint.Unspecified, ts.factory.updateTypeAliasDeclaration(node, undefined, node.name, node.typeParameters, node.type), ast);
    });
    const facade = ts.createSourceFile('facade.ts', [
      `export type paths = ${paths};`,
      'export type webhooks = Record<string, never>;',
      `export type components = ${components};`,
      'export type $defs = Record<string, never>;',
      `export type operations = ${operations.length ? object(operations.map(([name, type]) => field(name, type))) : 'Record<string, never>'};`,
    ].join('\n'), ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
    return '// Generated by @hey-api/openapi-ts and the qcg compatibility adapter. Do not edit.\n\n' + types.join('\n\n') + '\n\n' + printer.printFile(facade);
  } finally { rmSync(directory, { recursive: true, force: true }); }
}
