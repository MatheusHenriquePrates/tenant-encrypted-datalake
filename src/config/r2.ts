import { createR2Client, getR2ConfigFromEnv } from '../security/tls.js';
import type { S3Client } from '@aws-sdk/client-s3';

let _client: S3Client | null = null;

export function getR2Client(): S3Client {
  if (!_client) {
    const config = getR2ConfigFromEnv();
    _client = createR2Client(config);
  }
  return _client;
}

export function isR2Configured(): boolean {
  return !!(
    process.env['R2_ACCOUNT_ID'] &&
    process.env['R2_ACCESS_KEY_ID'] &&
    process.env['R2_SECRET_ACCESS_KEY']
  );
}

export function buildR2Path(
  layer: 'bronze' | 'silver' | 'gold',
  tableName: string,
  timestamp?: Date,
  isSnapshot?: boolean
): string {
  const now = timestamp || new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const day = String(now.getDate()).padStart(2, '0');
  if (isSnapshot) {
    // Full-scan tables: fixed key to OVERWRITE instead of accumulate.
    return `${layer}/${tableName}/year=${year}/month=${month}/day=${day}/latest.parquet.enc`;
  }
  const ts = now.toISOString().replace(/[:.]/g, '-');
  return `${layer}/${tableName}/year=${year}/month=${month}/day=${day}/${ts}.parquet.enc`;
}
