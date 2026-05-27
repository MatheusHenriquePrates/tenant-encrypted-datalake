import { createHash } from 'node:crypto';
import { createChildLogger } from '../utils/logger.js';

const log = createChildLogger('cache');

interface CacheEntry<T> {
  data: T;
  expiry: number;
}

const DEFAULT_TTL_MS = 5 * 60 * 1000; // 5 minutes
const MAX_ENTRIES = 1000;

export class QueryCache {
  private store = new Map<string, CacheEntry<unknown>>();
  private accessOrder: string[] = [];

  static cacheKey(tenantId: string, sql: string, params?: unknown[]): string {
    const raw = `${tenantId}:${sql}:${JSON.stringify(params || [])}`;
    return createHash('sha256').update(raw).digest('hex');
  }

  get<T>(key: string): T | undefined {
    const entry = this.store.get(key);
    if (!entry) return undefined;
    if (Date.now() > entry.expiry) {
      this.store.delete(key);
      return undefined;
    }
    return entry.data as T;
  }

  set<T>(key: string, data: T, ttlMs: number = DEFAULT_TTL_MS): void {
    if (this.store.size >= MAX_ENTRIES) {
      this.evictLRU();
    }
    this.store.set(key, { data, expiry: Date.now() + ttlMs });
    this.accessOrder.push(key);
  }

  invalidateTenant(tenantId: string): number {
    let count = 0;
    for (const [key] of this.store) {
      this.store.delete(key);
      count++;
    }
    this.accessOrder = [];
    log.info({ tenantId, cleared: count }, 'Cache invalidated for tenant');
    return count;
  }

  clear(): void {
    this.store.clear();
    this.accessOrder = [];
  }

  get size(): number {
    return this.store.size;
  }

  private evictLRU(): void {
    const toRemove = Math.max(1, Math.floor(MAX_ENTRIES * 0.1));
    const keysToRemove = this.accessOrder.splice(0, toRemove);
    for (const key of keysToRemove) {
      this.store.delete(key);
    }
    log.debug({ evicted: keysToRemove.length }, 'LRU eviction');
  }
}

export const globalCache = new QueryCache();
