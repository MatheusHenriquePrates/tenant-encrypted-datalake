import { PutObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import { getR2Client, buildR2Path, isR2Configured } from '../config/r2.js';
import { encryptBuffer, decryptParquetFile } from '../security/encryption.js';
import { loadMasterKey, deriveTenantKey } from '../security/keystore.js';
import { rowsToParquetBuffer, type Row } from '../utils/parquet.js';
import { createChildLogger } from '../utils/logger.js';

const log = createChildLogger('load');
const MULTIPART_THRESHOLD = 5 * 1024 * 1024; // 5MB

export async function toParquet(rows: Row[], tableName: string): Promise<Buffer> {
  return rowsToParquetBuffer(rows, tableName);
}

export async function uploadToR2(
  buffer: Buffer,
  tenantId: string,
  layer: 'bronze' | 'silver' | 'gold',
  tableName: string,
  bucketOverride?: string,
  isSnapshot?: boolean
): Promise<string> {
  if (!isR2Configured()) {
    log.warn('R2 not configured, skipping upload');
    return '';
  }

  const masterKeyPath = process.env['MASTER_KEY_PATH'] || './keys/master.key';
  const masterKey = loadMasterKey(masterKeyPath);
  const tenantKey = deriveTenantKey(masterKey, tenantId);

  const encrypted = encryptBuffer(buffer, tenantKey);
  const path = buildR2Path(layer, tableName, undefined, isSnapshot);
  const bucketName = bucketOverride || `datalake-${tenantId}`;

  const client = getR2Client();

  if (encrypted.length > MULTIPART_THRESHOLD) {
    const upload = new Upload({
      client,
      params: {
        Bucket: bucketName,
        Key: path,
        Body: encrypted,
        ContentType: 'application/octet-stream',
      },
      partSize: MULTIPART_THRESHOLD,
      queueSize: 3,
    });

    await upload.done();
  } else {
    await client.send(
      new PutObjectCommand({
        Bucket: bucketName,
        Key: path,
        Body: encrypted,
        ContentType: 'application/octet-stream',
      })
    );
  }

  log.info({ tenantId, layer, tableName, path, size: encrypted.length }, 'Uploaded to R2');
  return path;
}

export async function downloadFromR2(
  tenantId: string,
  _layer: string,
  path: string,
  bucketOverride?: string
): Promise<Buffer> {
  if (!isR2Configured()) {
    throw new Error('R2 not configured');
  }

  const client = getR2Client();
  const bucketName = bucketOverride || `datalake-${tenantId}`;

  const response = await client.send(
    new GetObjectCommand({
      Bucket: bucketName,
      Key: path,
    })
  );

  const chunks: Uint8Array[] = [];
  const stream = response.Body as AsyncIterable<Uint8Array>;
  for await (const chunk of stream) {
    chunks.push(chunk);
  }
  const encryptedBuffer = Buffer.concat(chunks);

  const masterKeyPath = process.env['MASTER_KEY_PATH'] || './keys/master.key';
  const masterKey = loadMasterKey(masterKeyPath);
  const tenantKey = deriveTenantKey(masterKey, tenantId);

  return decryptParquetFile(encryptedBuffer, tenantKey);
}
