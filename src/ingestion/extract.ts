import pg from 'pg';
import type { TableState } from './state.js';
import mysql2 from 'mysql2/promise';
import type { DatabaseConfig } from '../config/tenants.js';
import { createPgPool, createMysqlConnection } from '../config/database.js';
import { createChildLogger } from '../utils/logger.js';
import type { Row } from '../utils/parquet.js';

const log = createChildLogger('extract');
const BATCH_SIZE = parseInt(process.env['INGESTION_BATCH_SIZE'] || '10000', 10);
const MAX_ROWS_PER_TABLE = parseInt(process.env['MAX_ROWS_PER_TABLE'] || '100000', 10);

const TIME_COLUMNS = ['updated_at', 'updatedat', 'modified_at', 'modifiedat', 'created_at', 'createdat', 'started_at', 'finished_at'];

export interface ExtractionResult {
  tableName: string;
  rows: Row[];
  rowCount: number;
  timeColumn?: string;
  isSnapshot?: boolean;
}

async function detectTimeColumn(
  client: pg.PoolClient,
  schema: string,
  table: string
): Promise<string | undefined> {
  const result = await client.query(
    `SELECT column_name FROM information_schema.columns
     WHERE table_schema = $1 AND table_name = $2
     AND lower(column_name) = ANY($3)
     ORDER BY array_position($3, lower(column_name))
     LIMIT 1`,
    [schema, table, TIME_COLUMNS]
  );
  return result.rows[0]?.['column_name'] as string | undefined;
}

async function detectMysqlTimeColumn(
  conn: mysql2.Connection,
  database: string,
  table: string
): Promise<string | undefined> {
  for (const col of TIME_COLUMNS) {
    const [rows] = await conn.query(
      `SELECT COLUMN_NAME FROM information_schema.COLUMNS
       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND LOWER(COLUMN_NAME) = ?
       LIMIT 1`,
      [database, table, col]
    );
    const result = rows as Record<string, string>[];
    if (result.length > 0) return result[0]!['COLUMN_NAME'];
  }
  return undefined;
}

async function getPgTables(pool: pg.Pool, schema: string = 'public'): Promise<string[]> {
  const result = await pool.query(
    `SELECT table_name FROM information_schema.tables
     WHERE table_schema = $1 AND table_type = 'BASE TABLE'
     ORDER BY table_name`,
    [schema]
  );
  return result.rows.map((r: Record<string, string>) => r['table_name'] as string);
}

async function getMysqlTables(conn: mysql2.Connection, database: string): Promise<string[]> {
  const [rows] = await conn.query(
    `SELECT TABLE_NAME FROM information_schema.tables
     WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'
     ORDER BY TABLE_NAME`,
    [database]
  );
  return (rows as Record<string, string>[]).map((r) => r['TABLE_NAME'] as string);
}

export async function* extractPostgres(
  config: DatabaseConfig,
  lastIngestion?: Date,
  tableStates?: Record<string, TableState>
): AsyncGenerator<ExtractionResult> {
  const pool = createPgPool(config);

  try {
    const client = await pool.connect();
    try {
      await client.query('SET default_transaction_read_only = on');

      const schemas = config.schemas?.length ? config.schemas : [config.schema || 'public'];

      for (const schema of schemas) {
        const tables = await getPgTables(pool, schema);

        log.info({ database: config.name, schema, tables: tables.length }, 'Found tables');

        for (const table of tables) {
          try {
            const timeColumn = await detectTimeColumn(client, schema, table);

            // Skip unchanged full-scan tables (row-count comparison)
            if (!timeColumn && tableStates) {
              const stateKey = `${config.name}.${table}`;
              const prevState = tableStates[stateKey];
              if (prevState && !prevState.timeColumn) {
                try {
                  const countResult = await client.query(`SELECT COUNT(*) as cnt FROM "${schema}"."${table}"`);
                  const currentCount = parseInt(countResult.rows[0]['cnt'] as string);
                  if (currentCount === prevState.lastRowCount) {
                    log.info({ table, database: config.name, rowCount: currentCount }, 'Table unchanged (same row count), skipping');
                    continue;
                  }
                  log.info({ table, database: config.name, prevCount: prevState.lastRowCount, currentCount }, 'Table row count changed, full scan needed');
                } catch {
                  // COUNT failed — proceed with normal extraction.
                }
              }
            }

            let offset = 0;
            let hasMore = true;

            while (hasMore) {
              let query: string;
              const params: (string | number | Date)[] = [];

              if (lastIngestion && timeColumn) {
                query = `SELECT * FROM "${schema}"."${table}" WHERE "${timeColumn}" > $1 ORDER BY "${timeColumn}" LIMIT $2 OFFSET $3`;
                params.push(lastIngestion, BATCH_SIZE, offset);
              } else {
                query = `SELECT * FROM "${schema}"."${table}" ORDER BY 1 LIMIT $1 OFFSET $2`;
                params.push(BATCH_SIZE, offset);
              }

              const result = await client.query(query, params);
              if (result.rows.length > 0) {
                yield {
                  tableName: table,
                  rows: result.rows as Row[],
                  rowCount: result.rows.length,
                  timeColumn,
                  isSnapshot: !timeColumn,
                };
              }
              hasMore = result.rows.length === BATCH_SIZE;
              offset += BATCH_SIZE;

              if (offset >= MAX_ROWS_PER_TABLE) {
                log.warn({ table, database: config.name, maxRows: MAX_ROWS_PER_TABLE }, 'Table row limit reached, moving on');
                hasMore = false;
              }
            }
          } catch (queryErr) {
            log.warn({ table, database: config.name, error: (queryErr as Error).message }, 'Failed to extract table, skipping');
          }
        }
      }
    } finally {
      client.release();
    }
  } finally {
    await pool.end();
  }
}

export async function* extractMySQL(
  config: DatabaseConfig,
  lastIngestion?: Date,
  tableStates?: Record<string, TableState>
): AsyncGenerator<ExtractionResult> {
  const conn = await createMysqlConnection(config);

  try {
    const database = config.schemas?.[0] || config.name;
    const tables = await getMysqlTables(conn, database);

    log.info({ database: config.name, tables: tables.length }, 'Found MySQL tables');

    for (const table of tables) {
      try {
        const timeColumn = await detectMysqlTimeColumn(conn, database, table);

        // Skip unchanged full-scan tables (row-count comparison)
        if (!timeColumn && tableStates) {
          const stateKey = `${config.name}.${table}`;
          const prevState = tableStates[stateKey];
          if (prevState && !prevState.timeColumn) {
            try {
              const [countRows] = await conn.query(`SELECT COUNT(*) as cnt FROM \`${table}\``);
              const currentCount = parseInt((countRows as Record<string, string>[])[0]!['cnt'] as string);
              if (currentCount === prevState.lastRowCount) {
                log.info({ table, database: config.name, rowCount: currentCount }, 'Table unchanged (same row count), skipping');
                continue;
              }
              log.info({ table, database: config.name, prevCount: prevState.lastRowCount, currentCount }, 'Table row count changed, full scan needed');
            } catch {
              // COUNT failed — proceed with normal extraction.
            }
          }
        }

        let offset = 0;
        let hasMore = true;

        while (hasMore) {
          let query: string;
          const params: (string | number | Date)[] = [];

          if (lastIngestion && timeColumn) {
            query = `SELECT * FROM \`${table}\` WHERE \`${timeColumn}\` > ? ORDER BY \`${timeColumn}\` LIMIT ? OFFSET ?`;
            params.push(lastIngestion, BATCH_SIZE, offset);
          } else {
            query = `SELECT * FROM \`${table}\` ORDER BY 1 LIMIT ? OFFSET ?`;
            params.push(BATCH_SIZE, offset);
          }

          const [rows] = await conn.query(query, params);
          const rowArray = rows as Row[];
          if (rowArray.length > 0) {
            yield {
              tableName: table,
              rows: rowArray,
              rowCount: rowArray.length,
              timeColumn,
              isSnapshot: !timeColumn,
            };
          }
          hasMore = rowArray.length === BATCH_SIZE;
          offset += BATCH_SIZE;

          if (offset >= MAX_ROWS_PER_TABLE) {
            log.warn({ table, database: config.name, maxRows: MAX_ROWS_PER_TABLE }, 'Table row limit reached, moving on');
            hasMore = false;
          }
        }
      } catch (queryErr) {
        log.warn({ table, database: config.name, error: (queryErr as Error).message }, 'Failed to extract MySQL table, skipping');
      }
    }
  } finally {
    await conn.end();
  }
}
