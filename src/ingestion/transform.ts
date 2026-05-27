import { createChildLogger } from '../utils/logger.js';
import type { Row } from '../utils/parquet.js';

const log = createChildLogger('transform');

const SENSITIVE_FIELDS = new Set([
  'password', 'password_hash', 'password_digest', 'encrypted_password',
  'secret', 'token', 'api_key', 'access_token', 'refresh_token',
  'private_key', 'credit_card', 'ssn', 'cpf',
]);

export interface AggregatedRow extends Row {
  _aggregation_type: string;
  _period: string;
}

export function transformBronze(rows: Row[], tenantId: string, tableName: string, sourceDb: string): Row[] {
  const now = new Date().toISOString();

  return rows.map((row) => {
    const cleaned: Row = {};
    for (const [key, value] of Object.entries(row)) {
      if (SENSITIVE_FIELDS.has(key.toLowerCase())) {
        cleaned[key] = '[REDACTED]';
        continue;
      }
      if (value instanceof Date) {
        cleaned[key] = value.toISOString();
      } else if (value === undefined) {
        cleaned[key] = null;
      } else {
        cleaned[key] = value;
      }
    }

    return {
      ...cleaned,
      tenant_id: tenantId,
      _ingested_at: now,
      _source_table: tableName,
      _source_db: sourceDb,
    };
  });
}

export function transformSilver(bronzeRows: Row[]): Row[] {
  const seen = new Set<string>();
  const deduplicated: Row[] = [];

  for (const row of bronzeRows) {
    const pk = row['id'] ?? row['_id'] ?? JSON.stringify(row);
    const key = `${row['_source_table']}:${pk}`;

    if (seen.has(key)) continue;
    seen.add(key);

    const normalized: Row = {};
    for (const [k, v] of Object.entries(row)) {
      if (typeof v === 'string') {
        normalized[k] = v.trim();
      } else {
        normalized[k] = v;
      }
    }

    deduplicated.push(normalized);
  }

  log.debug({ input: bronzeRows.length, output: deduplicated.length }, 'Silver deduplication');
  return deduplicated;
}

export function transformGold(silverRows: Row[]): AggregatedRow[] {
  // Group by date for daily aggregation.
  const byDate = new Map<string, Row[]>();

  for (const row of silverRows) {
    const ingestedAt = row['_ingested_at'] as string;
    const date = ingestedAt ? ingestedAt.substring(0, 10) : 'unknown';
    if (!byDate.has(date)) {
      byDate.set(date, []);
    }
    byDate.get(date)!.push(row);
  }

  const aggregated: AggregatedRow[] = [];

  for (const [date, rows] of byDate) {
    aggregated.push({
      tenant_id: rows[0]?.['tenant_id'] ?? 'unknown',
      _source_table: rows[0]?.['_source_table'] as string,
      _source_db: rows[0]?.['_source_db'] as string,
      _ingested_at: new Date().toISOString(),
      _aggregation_type: 'daily_count',
      _period: date,
      record_count: rows.length,
    });
  }

  return aggregated;
}
