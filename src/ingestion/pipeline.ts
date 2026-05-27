import 'dotenv/config';
import { getActiveTenants, type Tenant } from '../config/tenants.js';
import { loadDbCredentials } from '../config/credentials.js';
import { extractPostgres, extractMySQL } from './extract.js';
import { transformBronze, transformSilver, transformGold } from './transform.js';
import { toParquet, uploadToR2 } from './load.js';
import {
  loadState,
  saveState,
  updateTableState,
  acquireLock,
  releaseLock,
  type IngestionState,
} from './state.js';
import { createChildLogger } from '../utils/logger.js';

const log = createChildLogger('pipeline');

interface TenantResult {
  tenantId: string;
  tablesProcessed: number;
  totalRows: number;
  durationMs: number;
  errors: string[];
}

async function ingestTenant(tenant: Tenant, state: IngestionState): Promise<TenantResult> {
  const startTime = Date.now();
  const result: TenantResult = {
    tenantId: tenant.id,
    tablesProcessed: 0,
    totalRows: 0,
    durationMs: 0,
    errors: [],
  };

  if (!acquireLock(tenant.id)) {
    result.errors.push('Could not acquire lock');
    result.durationMs = Date.now() - startTime;
    return result;
  }

  try {
    log.info({ tenantId: tenant.id, name: tenant.name, databases: tenant.databases.length }, 'Starting tenant ingestion');

    for (const dbConfig of tenant.databases) {
      try {
        // Last-ingestion timestamp for incremental extraction.
        let lastDate: Date | undefined;
        const tenantState = state.tenants[tenant.id];
        if (tenantState) {
          const dbPrefix = `${dbConfig.name}.`;
          for (const [key, ts] of Object.entries(tenantState.tables)) {
            if (key.startsWith(dbPrefix) && ts.timeColumn) {
              const d = new Date(ts.lastIngestedAt);
              if (!lastDate || d > lastDate) lastDate = d;
            }
          }
        }

        log.info({ tenantId: tenant.id, database: dbConfig.name, incremental: !!lastDate }, 'Extracting database');

        const tableStates = state.tenants[tenant.id]?.tables || {};
        const extractor = dbConfig.type === 'postgresql'
          ? extractPostgres(dbConfig, lastDate, tableStates)
          : extractMySQL(dbConfig, lastDate, tableStates);

        for await (const batch of extractor) {
          try {
            // Bronze
            const bronzeRows = transformBronze(batch.rows, tenant.id, batch.tableName, dbConfig.name);
            const bronzeParquet = await toParquet(bronzeRows, batch.tableName);
            const bronzePath = await uploadToR2(bronzeParquet, tenant.id, 'bronze', batch.tableName, tenant.bucketName, batch.isSnapshot);

            // Silver (deduplicated, trimmed)
            const silverRows = transformSilver(bronzeRows);
            const silverParquet = await toParquet(silverRows, batch.tableName);
            const silverPath = await uploadToR2(silverParquet, tenant.id, 'silver', batch.tableName, tenant.bucketName, batch.isSnapshot);

            // Gold (aggregations)
            const goldRows = transformGold(silverRows);
            if (goldRows.length > 0) {
              const goldParquet = await toParquet(goldRows, `${batch.tableName}_agg`);
              await uploadToR2(goldParquet, tenant.id, 'gold', `${batch.tableName}_agg`, tenant.bucketName, batch.isSnapshot);
            }

            updateTableState(
              state,
              tenant.id,
              dbConfig.name,
              batch.tableName,
              batch.rowCount,
              silverPath || bronzePath,
              batch.timeColumn
            );

            result.tablesProcessed++;
            result.totalRows += batch.rowCount;

            log.info({
              tenantId: tenant.id,
              table: batch.tableName,
              rows: batch.rowCount,
              bronzeKB: (bronzeParquet.length / 1024).toFixed(1),
              silverKB: (silverParquet.length / 1024).toFixed(1),
            }, 'Table batch ingested');
          } catch (batchErr) {
            const errMsg = `${batch.tableName}: ${(batchErr as Error).message}`;
            result.errors.push(errMsg);
            log.error({ tenantId: tenant.id, table: batch.tableName, error: (batchErr as Error).message }, 'Batch failed');
          }
        }
      } catch (dbErr) {
        const errMsg = `DB ${dbConfig.name}: ${(dbErr as Error).message}`;
        result.errors.push(errMsg);
        log.error({ tenantId: tenant.id, database: dbConfig.name, error: (dbErr as Error).message }, 'Database extraction failed');
      }
    }
  } finally {
    releaseLock(tenant.id);
    result.durationMs = Date.now() - startTime;
  }

  return result;
}

async function main(): Promise<void> {
  const pipelineStart = Date.now();
  log.info('=== Pipeline starting ===');

  // Try to pre-load encrypted DB credentials. If they're not configured,
  // tenants must rely on PG_*/MYSQL_* env vars instead.
  try {
    loadDbCredentials();
  } catch (err) {
    log.warn({ error: (err as Error).message }, 'DB credentials file not loaded — will fall back to env vars (PG_USER/PG_PASSWORD/etc.)');
  }

  const tenants = getActiveTenants();
  log.info({ tenantCount: tenants.length }, 'Active tenants loaded');

  const state = loadState();
  const results: TenantResult[] = [];

  // Sequential tenant processing to preserve RAM.
  for (const tenant of tenants) {
    try {
      const result = await ingestTenant(tenant, state);
      results.push(result);
      saveState(state);
    } catch (err) {
      log.error({ tenantId: tenant.id, error: (err as Error).message }, 'Tenant ingestion crashed');
      results.push({
        tenantId: tenant.id,
        tablesProcessed: 0,
        totalRows: 0,
        durationMs: 0,
        errors: [(err as Error).message],
      });
    }
  }

  // Summary
  const totalDuration = Date.now() - pipelineStart;
  const totalTables = results.reduce((sum, r) => sum + r.tablesProcessed, 0);
  const totalRows = results.reduce((sum, r) => sum + r.totalRows, 0);
  const totalErrors = results.reduce((sum, r) => sum + r.errors.length, 0);

  console.log('\n=== INGESTION SUMMARY ===');
  for (const r of results) {
    const status = r.errors.length === 0 ? 'OK' : `${r.errors.length} errors`;
    console.log(`  ${r.tenantId}: ${r.tablesProcessed} tables, ${r.totalRows} rows, ${r.durationMs}ms [${status}]`);
    for (const err of r.errors) {
      console.log(`    ERROR: ${err}`);
    }
  }
  console.log(`\nTotal: ${totalTables} tables, ${totalRows} rows, ${totalErrors} errors`);
  console.log(`Duration: ${(totalDuration / 1000).toFixed(1)}s`);
  console.log('=========================\n');

  log.info({ totalTables, totalRows, totalErrors, durationSec: (totalDuration / 1000).toFixed(1) }, 'Pipeline finished');
}

main().catch((err) => {
  log.error({ error: (err as Error).message, stack: (err as Error).stack }, 'Pipeline fatal error');
  console.error('FATAL:', err);
  process.exit(1);
});
