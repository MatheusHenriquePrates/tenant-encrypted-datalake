import pg from 'pg';
import mysql2 from 'mysql2/promise';
import type { DatabaseConfig } from './tenants.js';
import { getCredential } from './credentials.js';

function tryCredential(key: string | undefined) {
  if (!key) return null;
  try { return getCredential(key); }
  catch { return null; }
}

export function createPgPool(config: DatabaseConfig): pg.Pool {
  const cred = tryCredential(config.credentialKey);

  return new pg.Pool({
    host: cred?.host || config.host || process.env['PG_HOST'] || '127.0.0.1',
    port: cred?.port || config.port || parseInt(process.env['PG_PORT'] || '5432', 10),
    database: config.name,
    user: cred?.user || process.env['PG_USER'] || 'readonly_datalake',
    password: cred?.password || process.env['PG_PASSWORD'] || '',
    max: 3,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000,
  });
}

export async function createMysqlConnection(config: DatabaseConfig): Promise<mysql2.Connection> {
  const cred = tryCredential(config.credentialKey);

  return mysql2.createConnection({
    host: cred?.host || config.host || process.env['MYSQL_HOST'] || '127.0.0.1',
    port: cred?.port || config.port || parseInt(process.env['MYSQL_PORT'] || '3306', 10),
    database: config.name,
    user: cred?.user || process.env['MYSQL_USER'] || 'readonly_datalake',
    password: cred?.password || process.env['MYSQL_PASSWORD'] || '',
    connectTimeout: 10000,
  });
}
