import { readFileSync, existsSync } from 'node:fs';

export interface DatabaseConfig {
  type: 'postgresql' | 'mysql';
  credentialKey?: string;
  host?: string;
  port?: number;
  name: string;
  schema?: string;
  schemas?: string[];
}

export interface Tenant {
  id: string;
  name: string;
  bucketName: string;
  databases: DatabaseConfig[];
  active: boolean;
}

interface TenantsFile {
  tenants: Tenant[];
}

const TENANTS_PATH = process.env['TENANTS_PATH'] || './config/tenants.json';

export function loadTenants(): Tenant[] {
  if (!existsSync(TENANTS_PATH)) {
    throw new Error(`Tenants config not found at ${TENANTS_PATH}`);
  }
  const raw = readFileSync(TENANTS_PATH, 'utf-8');
  const parsed: TenantsFile = JSON.parse(raw);
  return parsed.tenants;
}

export function getActiveTenants(): Tenant[] {
  return loadTenants().filter((t) => t.active);
}

export function getTenantById(tenantId: string): Tenant | undefined {
  return loadTenants().find((t) => t.id === tenantId);
}
