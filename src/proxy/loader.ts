import { ListObjectsV2Command, GetObjectCommand } from '@aws-sdk/client-s3';
import { getR2Client, isR2Configured } from '../config/r2.js';
import { loadMasterKey, deriveTenantKey } from '../security/keystore.js';
import { decryptParquetFile } from '../security/encryption.js';
import { loadState } from '../ingestion/state.js';
import { getTenantById } from '../config/tenants.js';
import { createChildLogger } from '../utils/logger.js';
import { writeFileSync, unlinkSync, existsSync, mkdirSync } from 'node:fs';

const log = createChildLogger('loader');

const TMP_DIR = process.env['PROXY_TMP_DIR'] || '/tmp/datalake-proxy';

interface CacheEntry {
  path: string;
  loadedAt: number;
}

const PARQUET_CACHE = new Map<string, CacheEntry>();
const CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes

function ensureTmpDir(): void {
  if (!existsSync(TMP_DIR)) mkdirSync(TMP_DIR, { recursive: true });
}

export async function listTenantTables(tenantId: string): Promise<string[]> {
  // Try ingestion state first (fast, no R2 call)
  const state = loadState();
  const tenantState = state.tenants[tenantId];
  if (tenantState) {
    const tables = new Set<string>();
    for (const key of Object.keys(tenantState.tables)) {
      tables.add(key.split('.').pop()!);
    }
    if (tables.size > 0) return Array.from(tables).sort();
  }

  if (!isR2Configured()) return [];

  const client = getR2Client();
  const tables = new Set<string>();
  try {
    const result = await client.send(new ListObjectsV2Command({
      Bucket: getTenantById(tenantId)?.bucketName || `datalake-${tenantId}`,
      Prefix: 'silver/',
      Delimiter: '/',
    }));
    if (result.CommonPrefixes) {
      for (const prefix of result.CommonPrefixes) {
        const name = prefix.Prefix?.replace('silver/', '').replace(/\/$/, '');
        if (name) tables.add(name);
      }
    }
  } catch (err) {
    log.warn({ tenantId, error: (err as Error).message }, 'Failed to list R2 tables');
  }

  return Array.from(tables).sort();
}

async function findLatestPath(tenantId: string, tableName: string): Promise<string | null> {
  const state = loadState();
  const tenantState = state.tenants[tenantId];
  if (tenantState) {
    for (const [key, ts] of Object.entries(tenantState.tables)) {
      if (key.endsWith(`.${tableName}`) && ts.lastR2Path) return ts.lastR2Path;
    }
  }

  if (!isR2Configured()) return null;
  const client = getR2Client();

  for (const layer of ['silver', 'bronze']) {
    try {
      const result = await client.send(new ListObjectsV2Command({
        Bucket: getTenantById(tenantId)?.bucketName || `datalake-${tenantId}`,
        Prefix: `${layer}/${tableName}/`,
      }));
      if (result.Contents && result.Contents.length > 0) {
        const sorted = result.Contents.sort(
          (a, b) => (b.LastModified?.getTime() || 0) - (a.LastModified?.getTime() || 0)
        );
        return sorted[0]?.Key || null;
      }
    } catch { /* try next layer */ }
  }

  return null;
}

export async function getDecryptedParquetPath(tenantId: string, tableName: string): Promise<string | null> {
  ensureTmpDir();

  const cacheKey = `${tenantId}:${tableName}`;
  const cached = PARQUET_CACHE.get(cacheKey);
  if (cached && (Date.now() - cached.loadedAt) < CACHE_TTL_MS && existsSync(cached.path)) {
    return cached.path;
  }

  const r2Path = await findLatestPath(tenantId, tableName);
  if (!r2Path) return null;

  if (!isR2Configured()) return null;
  const client = getR2Client();

  try {
    const response = await client.send(new GetObjectCommand({
      Bucket: getTenantById(tenantId)?.bucketName || `datalake-${tenantId}`,
      Key: r2Path,
    }));

    const chunks: Uint8Array[] = [];
    const stream = response.Body as AsyncIterable<Uint8Array>;
    for await (const chunk of stream) chunks.push(chunk);
    const encryptedBuffer = Buffer.concat(chunks);

    const masterKeyPath = process.env['MASTER_KEY_PATH'] || './keys/master.key';
    const masterKey = loadMasterKey(masterKeyPath);
    const tenantKey = deriveTenantKey(masterKey, tenantId);
    const decrypted = decryptParquetFile(encryptedBuffer, tenantKey);

    const safeTable = tableName.replace(/[^a-zA-Z0-9_]/g, '_');
    const localPath = `${TMP_DIR}/${tenantId}_${safeTable}_${Date.now()}.parquet`;
    writeFileSync(localPath, decrypted);

    if (cached && existsSync(cached.path)) {
      try { unlinkSync(cached.path); } catch { /* ignore */ }
    }

    PARQUET_CACHE.set(cacheKey, { path: localPath, loadedAt: Date.now() });
    log.info({ tenantId, tableName, r2Path, sizeKB: (decrypted.length / 1024).toFixed(1) }, 'Parquet loaded');
    return localPath;
  } catch (err) {
    log.error({ tenantId, tableName, r2Path, error: (err as Error).message }, 'Failed to load parquet');
    return null;
  }
}

export function cleanCache(): void {
  const now = Date.now();
  for (const [key, entry] of PARQUET_CACHE) {
    if (now - entry.loadedAt > CACHE_TTL_MS) {
      try { if (existsSync(entry.path)) unlinkSync(entry.path); } catch { /* ignore */ }
      PARQUET_CACHE.delete(key);
    }
  }
}
