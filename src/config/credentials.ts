import { readFileSync } from 'node:fs';
import { createDecipheriv } from 'node:crypto';
import { createChildLogger } from '../utils/logger.js';

const log = createChildLogger('credentials');

export interface DbCredential {
  host: string;
  port: number;
  user: string;
  password: string;
}

export interface DecryptedCredentials {
  [key: string]: DbCredential;
}

let _cached: DecryptedCredentials | null = null;

function decryptFile(encPath: string, masterKey: Buffer): string {
  const encData = JSON.parse(readFileSync(encPath, 'utf-8'));
  const iv = Buffer.from(encData.iv, 'hex');
  const authTag = Buffer.from(encData.authTag, 'hex');
  const decipher = createDecipheriv('aes-256-gcm', masterKey, iv);
  decipher.setAuthTag(authTag);
  let decrypted = decipher.update(encData.data, 'hex', 'utf8');
  decrypted += decipher.final('utf8');
  return decrypted;
}

export function loadDbCredentials(): DecryptedCredentials {
  if (_cached) return _cached;

  const masterKeyPath = process.env['MASTER_KEY_PATH'] || './keys/master.key';
  const credentialsPath = process.env['DB_CREDENTIALS_PATH'] || './keys/db-credentials.enc';

  const masterKey = readFileSync(masterKeyPath);
  const decrypted = decryptFile(credentialsPath, masterKey);
  _cached = JSON.parse(decrypted);

  log.info({ keys: Object.keys(_cached!) }, 'Database credentials loaded');
  return _cached!;
}

export function getCredential(key: string): DbCredential {
  const creds = loadDbCredentials();
  const credential = creds[key];
  if (!credential) {
    throw new Error(`Credential key '${key}' not found. Available: ${Object.keys(creds).join(', ')}`);
  }
  return credential;
}
