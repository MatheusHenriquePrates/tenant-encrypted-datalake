import duckdb from 'duckdb';
import { getDecryptedParquetPath } from './loader.js';
import { createChildLogger } from '../utils/logger.js';
import type { Row } from '../utils/parquet.js';

const log = createChildLogger('query');

const FORBIDDEN_KEYWORDS = /\b(DROP|DELETE|INSERT|UPDATE|CREATE|ALTER|TRUNCATE|GRANT|REVOKE|EXEC|EXECUTE)\b/i;

export interface QueryResult {
  columns: string[];
  rows: Row[];
  rowCount: number;
  durationMs: number;
}

export function validateSQL(sql: string): { valid: boolean; error?: string } {
  const trimmed = sql.trim();

  if (!trimmed.toUpperCase().startsWith('SELECT')) {
    return { valid: false, error: 'Only SELECT queries are allowed' };
  }

  if (FORBIDDEN_KEYWORDS.test(trimmed)) {
    return { valid: false, error: 'Query contains forbidden keywords (DDL/DML operations not allowed)' };
  }

  if (trimmed.includes('--') || trimmed.includes('/*')) {
    return { valid: false, error: 'SQL comments are not allowed' };
  }

  const parts = trimmed.split(';').filter((s) => s.trim());
  if (parts.length > 1) {
    return { valid: false, error: 'Multiple SQL statements are not allowed' };
  }

  return { valid: true };
}

export function injectTenantFilter(sql: string, tenantId: string): string {
  const tenantFilter = `tenant_id = '${tenantId.replace(/'/g, "''")}'`;

  const upperSql = sql.toUpperCase();
  const whereIndex = upperSql.indexOf('WHERE');
  const groupByIndex = upperSql.indexOf('GROUP BY');
  const orderByIndex = upperSql.indexOf('ORDER BY');
  const limitIndex = upperSql.indexOf('LIMIT');

  if (whereIndex !== -1) {
    const afterWhere = whereIndex + 5;
    return `${sql.substring(0, afterWhere)} ${tenantFilter} AND${sql.substring(afterWhere)}`;
  }

  let insertAt = sql.length;
  for (const idx of [groupByIndex, orderByIndex, limitIndex]) {
    if (idx !== -1 && idx < insertAt) {
      insertAt = idx;
    }
  }

  return `${sql.substring(0, insertAt)} WHERE ${tenantFilter} ${sql.substring(insertAt)}`;
}

function extractTableNames(sql: string): string[] {
  const tables: string[] = [];
  const fromRegex = /\bFROM\s+"?([a-zA-Z_][a-zA-Z0-9_]*)"?/gi;
  const joinRegex = /\bJOIN\s+"?([a-zA-Z_][a-zA-Z0-9_]*)"?/gi;

  let match;
  while ((match = fromRegex.exec(sql)) !== null) tables.push(match[1]!);
  while ((match = joinRegex.exec(sql)) !== null) tables.push(match[1]!);

  return [...new Set(tables)];
}

/** Convert BigInt values to Number in query results (DuckDB returns BigInt for aggregates). */
function convertBigInts(rows: Row[]): Row[] {
  return rows.map((row) => {
    const converted: Row = {};
    for (const [key, value] of Object.entries(row)) {
      if (typeof value === 'bigint') {
        converted[key] = Number(value);
      } else {
        converted[key] = value;
      }
    }
    return converted;
  });
}

export async function executeQuery(tenantId: string, sql: string): Promise<QueryResult> {
  const startTime = Date.now();

  const validation = validateSQL(sql);
  if (!validation.valid) throw new Error(validation.error);

  const tableNames = extractTableNames(sql);
  if (tableNames.length === 0) {
    throw new Error('Could not determine tables from SQL query');
  }

  log.info({ tenantId, tables: tableNames }, 'Loading tables for query');

  const db = new duckdb.Database(':memory:');
  const conn = db.connect();

  try {
    for (const tableName of tableNames) {
      const parquetPath = await getDecryptedParquetPath(tenantId, tableName);
      if (!parquetPath) {
        throw new Error(`Table '${tableName}' not found for tenant '${tenantId}'`);
      }

      await new Promise<void>((resolve, reject) => {
        conn.run(
          `CREATE TABLE "${tableName}" AS SELECT * FROM read_parquet('${parquetPath}')`,
          (err) => { if (err) reject(err); else resolve(); }
        );
      });
    }

    const filteredSql = injectTenantFilter(sql, tenantId);
    log.debug({ tenantId, originalSql: sql, filteredSql }, 'Executing filtered query');

    return await new Promise<QueryResult>((resolve, reject) => {
      conn.all(filteredSql, (err: Error | null, rows: Row[]) => {
        db.close();
        if (err) {
          reject(new Error(`Query failed: ${err.message}`));
          return;
        }
        const safeRows = convertBigInts(rows);
        const columns = safeRows.length > 0 ? Object.keys(safeRows[0]!) : [];
        resolve({
          columns,
          rows: safeRows,
          rowCount: safeRows.length,
          durationMs: Date.now() - startTime,
        });
      });
    });
  } catch (err) {
    db.close();
    throw err;
  }
}
