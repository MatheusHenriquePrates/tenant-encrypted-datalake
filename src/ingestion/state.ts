import { readFileSync, writeFileSync, existsSync, mkdirSync, statSync, unlinkSync } from 'node:fs';
import { dirname } from 'node:path';
import { createChildLogger } from '../utils/logger.js';

const log = createChildLogger('state');

const STATE_PATH = process.env['STATE_PATH'] || './cache/ingestion-state.json';
const LOCKS_DIR = process.env['LOCKS_DIR'] || './cache/locks';

export interface TableState {
  lastIngestedAt: string;
  lastRowCount: number;
  lastR2Path: string;
  timeColumn?: string;
}

export interface TenantState {
  tables: Record<string, TableState>;
  lastRunAt: string;
}

export interface IngestionState {
  tenants: Record<string, TenantState>;
}

function ensureDirs(): void {
  const cacheDir = dirname(STATE_PATH);
  if (!existsSync(cacheDir)) mkdirSync(cacheDir, { recursive: true });
  if (!existsSync(LOCKS_DIR)) mkdirSync(LOCKS_DIR, { recursive: true });
}

export function loadState(): IngestionState {
  ensureDirs();
  if (!existsSync(STATE_PATH)) {
    return { tenants: {} };
  }
  try {
    return JSON.parse(readFileSync(STATE_PATH, 'utf-8'));
  } catch {
    return { tenants: {} };
  }
}

export function saveState(state: IngestionState): void {
  ensureDirs();
  writeFileSync(STATE_PATH, JSON.stringify(state, null, 2));
}

export function getLastIngestion(
  state: IngestionState,
  tenantId: string,
  dbName: string,
  tableName: string
): TableState | undefined {
  const key = `${dbName}.${tableName}`;
  return state.tenants[tenantId]?.tables[key];
}

export function updateTableState(
  state: IngestionState,
  tenantId: string,
  dbName: string,
  tableName: string,
  rowCount: number,
  r2Path: string,
  timeColumn?: string
): void {
  const key = `${dbName}.${tableName}`;
  if (!state.tenants[tenantId]) {
    state.tenants[tenantId] = { tables: {}, lastRunAt: new Date().toISOString() };
  }
  state.tenants[tenantId].tables[key] = {
    lastIngestedAt: new Date().toISOString(),
    lastRowCount: rowCount,
    lastR2Path: r2Path,
    timeColumn,
  };
  state.tenants[tenantId].lastRunAt = new Date().toISOString();
}

const LOCK_MAX_AGE_MS = 25 * 60 * 1000; // 25 minutes

export function acquireLock(tenantId: string): boolean {
  ensureDirs();
  const lockFile = `${LOCKS_DIR}/${tenantId}.lock`;

  if (existsSync(lockFile)) {
    const stat = statSync(lockFile);
    const age = Date.now() - stat.mtimeMs;
    if (age > LOCK_MAX_AGE_MS) {
      log.warn({ tenantId, ageMin: (age / 60000).toFixed(1) }, 'Stale lock detected, removing');
      unlinkSync(lockFile);
    } else {
      log.warn({ tenantId, ageMin: (age / 60000).toFixed(1) }, 'Lock exists, skipping tenant');
      return false;
    }
  }

  writeFileSync(lockFile, JSON.stringify({ pid: process.pid, startedAt: new Date().toISOString() }));
  return true;
}

export function releaseLock(tenantId: string): void {
  const lockFile = `${LOCKS_DIR}/${tenantId}.lock`;
  try {
    if (existsSync(lockFile)) unlinkSync(lockFile);
  } catch (err) {
    log.error({ tenantId, error: (err as Error).message }, 'Failed to release lock');
  }
}
