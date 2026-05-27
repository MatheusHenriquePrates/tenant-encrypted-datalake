import type { FastifyRequest, FastifyReply } from 'fastify';
import { loadMasterKey, generateTenantToken } from '../security/keystore.js';
import { getTenantById } from '../config/tenants.js';
import { createChildLogger } from '../utils/logger.js';

const log = createChildLogger('auth');

const requestCounts = new Map<string, { count: number; resetAt: number }>();
const MAX_REQUESTS_PER_MINUTE = 100;

export function validateTenantToken(tenantId: string, token: string): boolean {
  const masterKeyPath = process.env['MASTER_KEY_PATH'] || './keys/master.key';
  const masterKey = loadMasterKey(masterKeyPath);
  const expectedToken = generateTenantToken(masterKey, tenantId);
  return token === expectedToken;
}

export function checkRateLimit(tenantId: string): boolean {
  const now = Date.now();
  const entry = requestCounts.get(tenantId);

  if (!entry || now > entry.resetAt) {
    requestCounts.set(tenantId, { count: 1, resetAt: now + 60000 });
    return true;
  }

  if (entry.count >= MAX_REQUESTS_PER_MINUTE) {
    return false;
  }

  entry.count++;
  return true;
}

export async function authMiddleware(
  request: FastifyRequest,
  reply: FastifyReply
): Promise<void> {
  const authHeader = request.headers['authorization'];
  const body = request.body as Record<string, unknown> | undefined;
  const params = request.params as Record<string, string> | undefined;

  const token = (body?.['token'] as string)
    || (authHeader?.startsWith('Bearer ') ? authHeader.substring(7) : undefined);

  if (!token) {
    reply.code(401).send({ error: 'Missing authentication token' });
    return;
  }

  const tenantId = (body?.['tenantId'] as string) || params?.['id'];

  if (!tenantId) {
    reply.code(400).send({ error: 'Missing tenantId' });
    return;
  }

  const tenant = getTenantById(tenantId);
  if (!tenant || !tenant.active) {
    reply.code(404).send({ error: 'Tenant not found or inactive' });
    return;
  }

  if (!validateTenantToken(tenantId, token)) {
    log.warn({ tenantId }, 'Invalid token attempt');
    reply.code(401).send({ error: 'Invalid token' });
    return;
  }

  if (!checkRateLimit(tenantId)) {
    log.warn({ tenantId }, 'Rate limit exceeded');
    reply.code(429).send({ error: 'Rate limit exceeded (100 req/min)' });
    return;
  }
}
