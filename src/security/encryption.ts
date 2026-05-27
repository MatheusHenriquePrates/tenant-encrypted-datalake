import { createCipheriv, createDecipheriv, randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';

const ALGORITHM = 'aes-256-gcm';
const IV_LENGTH = 16;
const AUTH_TAG_LENGTH = 16;

export interface EncryptedPayload {
  encrypted: Buffer;
  iv: Buffer;
  authTag: Buffer;
}

export function encrypt(data: Buffer, tenantKey: Buffer): EncryptedPayload {
  const iv = randomBytes(IV_LENGTH);
  const cipher = createCipheriv(ALGORITHM, tenantKey, iv, { authTagLength: AUTH_TAG_LENGTH });

  const encrypted = Buffer.concat([cipher.update(data), cipher.final()]);
  const authTag = cipher.getAuthTag();

  return { encrypted, iv, authTag };
}

export function decrypt(encrypted: Buffer, iv: Buffer, authTag: Buffer, tenantKey: Buffer): Buffer {
  const decipher = createDecipheriv(ALGORITHM, tenantKey, iv, { authTagLength: AUTH_TAG_LENGTH });
  decipher.setAuthTag(authTag);

  return Buffer.concat([decipher.update(encrypted), decipher.final()]);
}

export function packEncrypted(payload: EncryptedPayload): Buffer {
  // Layout: [IV (16 bytes)][AuthTag (16 bytes)][Encrypted data]
  return Buffer.concat([payload.iv, payload.authTag, payload.encrypted]);
}

export function unpackEncrypted(packed: Buffer): EncryptedPayload {
  const iv = packed.subarray(0, IV_LENGTH);
  const authTag = packed.subarray(IV_LENGTH, IV_LENGTH + AUTH_TAG_LENGTH);
  const encrypted = packed.subarray(IV_LENGTH + AUTH_TAG_LENGTH);
  return { iv, authTag, encrypted };
}

export async function encryptParquetFile(filePath: string, tenantKey: Buffer): Promise<Buffer> {
  const fileData = await readFile(filePath);
  const payload = encrypt(fileData, tenantKey);
  return packEncrypted(payload);
}

export function encryptBuffer(data: Buffer, tenantKey: Buffer): Buffer {
  const payload = encrypt(data, tenantKey);
  return packEncrypted(payload);
}

export function decryptParquetFile(encryptedBuffer: Buffer, tenantKey: Buffer): Buffer {
  const { encrypted, iv, authTag } = unpackEncrypted(encryptedBuffer);
  return decrypt(encrypted, iv, authTag, tenantKey);
}
