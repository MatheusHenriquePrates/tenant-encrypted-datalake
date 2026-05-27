import { S3Client } from '@aws-sdk/client-s3';

export interface R2Config {
  accountId: string;
  accessKeyId: string;
  secretAccessKey: string;
  endpoint?: string;
}

export function createR2Client(config: R2Config): S3Client {
  const endpoint = config.endpoint || `https://${config.accountId}.r2.cloudflarestorage.com`;

  if (!endpoint.startsWith('https://')) {
    throw new Error('R2 endpoint must use HTTPS. HTTP is not allowed.');
  }

  return new S3Client({
    region: 'auto',
    endpoint,
    credentials: {
      accessKeyId: config.accessKeyId,
      secretAccessKey: config.secretAccessKey,
    },
    forcePathStyle: true,
    requestHandler: undefined,
    maxAttempts: 3,
    requestChecksumCalculation: 'WHEN_REQUIRED',
  });
}

export function getR2ConfigFromEnv(): R2Config {
  const accountId = process.env['R2_ACCOUNT_ID'];
  const accessKeyId = process.env['R2_ACCESS_KEY_ID'];
  const secretAccessKey = process.env['R2_SECRET_ACCESS_KEY'];

  if (!accountId || !accessKeyId || !secretAccessKey) {
    throw new Error(
      'R2 credentials not configured. Set R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY.'
    );
  }

  return {
    accountId,
    accessKeyId,
    secretAccessKey,
    endpoint: process.env['R2_ENDPOINT'],
  };
}
