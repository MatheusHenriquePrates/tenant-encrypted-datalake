import { randomBytes, hkdfSync, createHash } from 'node:crypto';
import { readFileSync, writeFileSync, existsSync, mkdirSync } from 'node:fs';
import { dirname } from 'node:path';

const KEY_LENGTH = 32; // 256 bits

// HKDF salt — domain separator for tenant key derivation.
//
// IMPORTANT: this value is part of the crypto. It must be set ONCE for an
// installation and never changed afterwards. Changing it invalidates every
// tenant key derived from the master key, which means every encrypted file
// becomes unreadable.
//
// Default is fine for most deployments. Override via env only if you want
// stronger domain separation from other tenant-encrypted-datalake instances.
const HKDF_SALT = Buffer.from(
  process.env['HKDF_SALT'] || 'tenant-encrypted-datalake-v1',
  'utf8'
);

export function generateMasterKey(): Buffer {
  return randomBytes(KEY_LENGTH);
}

export function saveMasterKey(key: Buffer, path: string): void {
  const dir = dirname(path);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true, mode: 0o700 });
  }
  writeFileSync(path, key, { mode: 0o600 });
}

export function loadMasterKey(path: string): Buffer {
  if (!existsSync(path)) {
    throw new Error(`Master key not found at ${path}. Run 'npm run generate-master-key' first.`);
  }
  return readFileSync(path);
}

export function deriveTenantKey(masterKey: Buffer, tenantId: string): Buffer {
  const derived = hkdfSync('sha256', masterKey, HKDF_SALT, tenantId, KEY_LENGTH);
  return Buffer.from(derived);
}

export function keyFingerprint(key: Buffer): string {
  return createHash('sha256').update(key).digest('hex').substring(0, 16);
}

export function generateTenantToken(masterKey: Buffer, tenantId: string): string {
  const hmacKey = hkdfSync('sha256', masterKey, HKDF_SALT, `token:${tenantId}`, 32);
  return Buffer.from(hmacKey).toString('hex');
}

export function rotateMasterKey(
  oldKeyPath: string,
  newKeyPath: string
): { oldKey: Buffer; newKey: Buffer } {
  const oldKey = loadMasterKey(oldKeyPath);
  const newKey = generateMasterKey();
  saveMasterKey(newKey, newKeyPath);
  return { oldKey, newKey };
}

// CLI: generate master key when run directly
const isMainModule = process.argv[1]?.endsWith('keystore.js');
if (isMainModule) {
  const keyPath = process.env['MASTER_KEY_PATH'] || './keys/master.key';
  if (existsSync(keyPath)) {
    console.log(`Master key already exists at ${keyPath}`);
    const key = loadMasterKey(keyPath);
    console.log(`Fingerprint: ${keyFingerprint(key)}`);
  } else {
    const key = generateMasterKey();
    saveMasterKey(key, keyPath);
    console.log(`Master key generated at ${keyPath}`);
    console.log(`Fingerprint: ${keyFingerprint(key)}`);
  }
}
