import cron from 'node-cron';
import 'dotenv/config';
import { getActiveTenants, type Tenant } from '../config/tenants.js';
import { extractPostgres, extractMySQL } from './extract.js';
import { transformBronze, transformSilver, transformGold } from './transform.js';
import { toParquet, uploadToR2 } from './load.js';
import { isR2Configured } from '../config/r2.js';
import { createChildLogger } from '../utils/logger.js';

const log = createChildLogger('orchestrator');
const activeLocks = new Set<string>();

async function ingestTenant(tenant: Tenant): Promise<void> {
  if (activeLocks.has(tenant.id)) {
    log.warn({ tenantId: tenant.id }, 'Ingestion already running, skipping');
    return;
  }

  activeLocks.add(tenant.id);
  const startTime = Date.now();
  let totalRows = 0;

  try {
    log.info({ tenantId: tenant.id, name: tenant.name }, 'Starting ingestion');

    for (const dbConfig of tenant.databases) {
      try {
        const extractor = dbConfig.type === 'postgresql'
          ? extractPostgres(dbConfig)
          : extractMySQL(dbConfig);

        for await (const batch of extractor) {
          // Bronze layer
          const bronzeRows = transformBronze(
            batch.rows,
            tenant.id,
            batch.tableName,
            dbConfig.name
          );

          const bronzeParquet = await toParquet(bronzeRows, batch.tableName);
          await uploadToR2(bronzeParquet, tenant.id, 'bronze', batch.tableName);

          // Silver layer
          const silverRows = transformSilver(bronzeRows);
          const silverParquet = await toParquet(silverRows, batch.tableName);
          await uploadToR2(silverParquet, tenant.id, 'silver', batch.tableName);

          // Gold layer
          const goldRows = transformGold(silverRows);
          if (goldRows.length > 0) {
            const goldParquet = await toParquet(goldRows, `${batch.tableName}_agg`);
            await uploadToR2(goldParquet, tenant.id, 'gold', `${batch.tableName}_agg`);
          }

          totalRows += batch.rowCount;
        }
      } catch (dbErr) {
        log.error(
          { tenantId: tenant.id, database: dbConfig.name, error: (dbErr as Error).message },
          'Database extraction failed'
        );
      }
    }

    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    log.info({ tenantId: tenant.id, totalRows, durationSec: duration }, 'Ingestion completed');
  } catch (err) {
    log.error({ tenantId: tenant.id, error: (err as Error).message }, 'Ingestion failed');
  } finally {
    activeLocks.delete(tenant.id);
  }
}

async function runIngestionCycle(): Promise<void> {
  log.info('Starting ingestion cycle');

  if (!isR2Configured()) {
    log.warn('R2 not configured — running in dry-run mode (extract + transform only, no upload)');
  }

  const tenants = getActiveTenants();
  log.info({ tenantCount: tenants.length }, 'Active tenants loaded');

  // Run each tenant independently — failures don't affect others.
  const results = await Promise.allSettled(tenants.map((t) => ingestTenant(t)));

  const succeeded = results.filter((r) => r.status === 'fulfilled').length;
  const failed = results.filter((r) => r.status === 'rejected').length;
  log.info({ succeeded, failed, total: tenants.length }, 'Ingestion cycle finished');
}

const cronSchedule = process.env['INGESTION_CRON'] || '*/30 * * * *';

log.info({ schedule: cronSchedule }, 'Orchestrator starting');

// Run once on startup
runIngestionCycle().catch((err) => {
  log.error({ error: (err as Error).message }, 'Initial ingestion failed');
});

// Then schedule
cron.schedule(cronSchedule, () => {
  runIngestionCycle().catch((err) => {
    log.error({ error: (err as Error).message }, 'Scheduled ingestion failed');
  });
});
