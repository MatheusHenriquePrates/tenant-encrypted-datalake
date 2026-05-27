import Fastify from 'fastify';
import 'dotenv/config';
import { authMiddleware, validateTenantToken, checkRateLimit } from './auth.js';
import { executeQuery, validateSQL } from './query.js';
import { globalCache, QueryCache } from './cache.js';
import { listTenantTables, getDecryptedParquetPath, cleanCache } from './loader.js';
import { getTenantById } from '../config/tenants.js';
import { createChildLogger } from '../utils/logger.js';
import duckdb from 'duckdb';
import type { Row } from '../utils/parquet.js';

const log = createChildLogger('proxy-server');

const PORT = parseInt(process.env['PROXY_PORT'] || '4500', 10);
const HOST = process.env['PROXY_HOST'] || '127.0.0.1';
const startedAt = Date.now();
let totalRequests = 0;
let cacheHits = 0;

const app = Fastify({ logger: false, requestTimeout: 30000 });

// --- Health check (no auth) ---
app.get('/health', async () => ({
  status: 'ok',
  uptime: Math.floor((Date.now() - startedAt) / 1000),
  timestamp: new Date().toISOString(),
  cache: {
    entries: globalCache.size,
    hitRate: totalRequests > 0 ? +(cacheHits / totalRequests * 100).toFixed(1) : 0,
  },
  requests: totalRequests,
}));

// --- POST /query --- inline auth from body { tenantId, token, sql }
app.post('/query', async (request, reply) => {
  const body = request.body as { tenantId?: string; token?: string; sql?: string; params?: unknown[] } | null;

  if (!body?.tenantId || !body?.sql || !body?.token) {
    return reply.code(400).send({ success: false, error: 'Missing tenantId, token, or sql' });
  }

  const tenant = getTenantById(body.tenantId);
  if (!tenant || !tenant.active) {
    return reply.code(404).send({ success: false, error: 'Tenant not found or inactive' });
  }
  if (!validateTenantToken(body.tenantId, body.token)) {
    return reply.code(401).send({ success: false, error: 'Invalid token' });
  }
  if (!checkRateLimit(body.tenantId)) {
    return reply.code(429).send({ success: false, error: 'Rate limit exceeded (100 req/min)' });
  }

  const validation = validateSQL(body.sql);
  if (!validation.valid) {
    return reply.code(403).send({ success: false, error: validation.error });
  }

  totalRequests++;

  const cacheKey = QueryCache.cacheKey(body.tenantId, body.sql, body.params);
  const cached = globalCache.get<Record<string, unknown>>(cacheKey);
  if (cached) {
    cacheHits++;
    return reply.send({ success: true, ...cached, cached: true });
  }

  try {
    const result = await executeQuery(body.tenantId, body.sql);
    const response = {
      data: result.rows,
      columns: result.columns,
      rowCount: result.rowCount,
      executionTime: result.durationMs,
    };
    globalCache.set(cacheKey, response);
    return reply.send({ success: true, ...response, cached: false });
  } catch (err) {
    log.error({ tenantId: body.tenantId, error: (err as Error).message }, 'Query failed');
    return reply.code(500).send({ success: false, error: (err as Error).message });
  }
});

// --- GET /tenants/:id/tables --- (Bearer header auth)
app.get('/tenants/:id/tables', { preHandler: authMiddleware }, async (request, reply) => {
  const { id } = request.params as { id: string };
  const tables = await listTenantTables(id);
  return reply.send({ tenantId: id, tables });
});

// --- GET /tenants/:id/schema/:table --- (Bearer header auth)
app.get('/tenants/:id/schema/:table', { preHandler: authMiddleware }, async (request, reply) => {
  const { id, table } = request.params as { id: string; table: string };

  const parquetPath = await getDecryptedParquetPath(id, table);
  if (!parquetPath) {
    return reply.code(404).send({ error: `Table '${table}' not found` });
  }

  return new Promise<void>((resolve) => {
    const db = new duckdb.Database(':memory:');
    const conn = db.connect();
    conn.all(
      `DESCRIBE SELECT * FROM read_parquet('${parquetPath}')`,
      (err: Error | null, rows: Row[]) => {
        db.close();
        if (err) {
          reply.code(500).send({ error: err.message });
        } else {
          reply.send({
            tenantId: id,
            table,
            columns: rows.map((r) => ({ name: r['column_name'], type: r['column_type'] })),
          });
        }
        resolve();
      }
    );
  });
});

setInterval(cleanCache, 60000);

app.listen({ port: PORT, host: HOST }, (err, address) => {
  if (err) {
    log.error({ error: err.message }, 'Failed to start proxy');
    process.exit(1);
  }
  log.info({ address }, 'DuckDB proxy started');
  console.log(`DuckDB Proxy listening on ${address}`);
});
