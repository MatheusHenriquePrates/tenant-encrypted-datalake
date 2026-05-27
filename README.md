# tenant-encrypted-datalake

Multi-tenant data lake with **per-tenant client-side encryption** and a
**SELECT-only DuckDB proxy** that automatically filters every query by
`tenant_id`. Designed for the case where:

- You have several customers' relational data to ingest (Postgres / MySQL)
- You want to store it cheaply in S3-compatible object storage as Parquet
- You **cannot** trust the storage layer (cloud provider, ops team,
  shared bucket) to enforce tenant isolation — so isolation has to be
  cryptographic, not logical
- You still want a way to SQL-query the data without re-implementing
  half of Spark

The result: ~1.9 k LoC of TypeScript implementing a Medallion ingestion
pipeline, an HKDF-derived per-tenant key, AES-256-GCM encryption with
authenticated tags, and a Fastify proxy that opens Parquet in DuckDB
on demand, injects the tenant filter, and rejects anything that isn't a
single `SELECT`.

> **Status: reference implementation.** This is the sanitized public
> version of a system I run in production. It's intentionally small and
> single-host so the engineering is easy to read.

## Threat model — start here

This is the most important section. Skip it and the rest doesn't matter.

### What this design protects against

1. **A read of the storage bucket** by anyone — including the cloud
   provider, a misconfigured public ACL, or a compromised ops account.
   They get ciphertext + IV + auth tag, nothing else. No tenant data
   is recoverable without the master key.

2. **Cross-tenant data leakage in queries.** The proxy refuses anything
   that isn't a single `SELECT`, rejects SQL comments, blocks multi-statement
   queries, and **injects `tenant_id = '<caller>'`** into the WHERE clause
   before DuckDB executes. A tenant who somehow guesses another tenant's
   table names still can't read their rows.

3. **Compromise of one tenant's HMAC token.** Tokens are derived from the
   master key via HKDF with a token-specific info string. Knowing token
   A doesn't help compute token B.

4. **Accidental ingestion of secrets.** `transformBronze` redacts a
   denylist of common sensitive column names (`password`, `token`,
   `api_key`, `ssn`, `cpf`, …) with `[REDACTED]` before anything is
   persisted.

### What this design does NOT protect against

1. **Loss of the master key.** The master key (`./keys/master.key`)
   derives every tenant key. If it leaks, **every** tenant's encrypted
   data becomes readable. Store it like you'd store a root TLS key.
   This repo's `.gitignore` excludes `keys/` and `*.key`; double-check
   before you ever `git add -A`.

2. **A compromised host running the ingestion pipeline.** The pipeline
   process has the master key in RAM and the tenant DB credentials in
   RAM. Anything that can read its memory (root, debugger, core dump)
   gets everything.

3. **A compromised host running the proxy.** Same — the proxy holds
   the master key in memory to derive tenant keys on demand.

4. **DuckDB sandbox escape.** Queries run inside DuckDB. If DuckDB has
   a bug that allows escaping `SELECT` semantics, the parser-level
   defense (`validateSQL`) is the only thing standing in between. Keep
   DuckDB updated.

5. **Side channels** (timing, cache, network). The token comparison in
   `validateTenantToken` is **string equality**, not constant-time. The
   threat model assumes the proxy sits on a private network — if you
   expose it to untrusted clients, swap the comparison for `crypto.timingSafeEqual`.

6. **Untrusted columns making it to Gold.** The denylist in `transformBronze`
   catches obvious names. A custom column called `acct_secret_v2` will pass
   through. Audit your tenant schemas; don't rely solely on the denylist.

7. **`HKDF_SALT` rotation.** The salt is a domain separator. **Once you
   pick a value, never change it** — every previously encrypted file
   becomes unreadable. The repo default (`tenant-encrypted-datalake-v1`)
   is fine for a single deployment. Override only if you run multiple
   independent installations that should never be able to read each
   other's files.

## Architecture

```
                  ┌──────────────────────┐    ┌──────────────────────┐
                  │ Tenant A — Postgres  │    │ Tenant B — MySQL     │
                  └──────────┬───────────┘    └──────────┬───────────┘
                             │ SELECT-only readonly creds
                             ▼                            ▼
        ┌───────────────────────────────────────────────────────────┐
        │ Ingestion pipeline   (Medallion: bronze → silver → gold)  │
        │  - incremental by time column when available              │
        │  - row-count skip for full-scan tables                    │
        │  - denylist redaction of `password`, `token`, `ssn`, …    │
        └───────────────────────────────────────────────────────────┘
                             │                            │
                             │  parquet                   │  parquet
                             ▼                            ▼
        ┌───────────────────────────────────────────────────────────┐
        │ AES-256-GCM encryption                                    │
        │  - tenantKey = HKDF-SHA256(masterKey, salt, tenantId, 32) │
        │  - file payload: [16B IV][16B authTag][ciphertext]        │
        └───────────────────────────────────────────────────────────┘
                             │                            │
                             ▼                            ▼
        ┌───────────────────────────────────────────────────────────┐
        │ Cloudflare R2 (S3-compatible)                             │
        │  bucket per tenant:  datalake-{tenant_id}                 │
        │  layout:             {layer}/{table}/year=…/month=…/…     │
        └───────────────────────────────────────────────────────────┘
                             ▲
                             │ GET on demand, decrypt in memory
        ┌───────────────────────────────────────────────────────────┐
        │ DuckDB proxy (Fastify, :4500)                             │
        │  POST /query  { tenantId, token, sql }                    │
        │   1. validateTenantToken (HKDF-derived HMAC, per tenant)  │
        │   2. rate limit (100 req/min per tenant)                  │
        │   3. validateSQL (SELECT-only, no comments, no DDL/DML)   │
        │   4. fetch latest .parquet.enc from R2                    │
        │   5. decrypt → /tmp file → DuckDB read_parquet(…)         │
        │   6. injectTenantFilter (adds  WHERE tenant_id = '…')     │
        │   7. execute, cache result 5 min, return JSON             │
        └───────────────────────────────────────────────────────────┘
                             ▲
                             │ HTTP
                       Tenant query client
```

## Quick start

```bash
git clone https://github.com/MatheusHenriquePrates/tenant-encrypted-datalake.git
cd tenant-encrypted-datalake

# 1. Install deps + build
npm install
npm run build

# 2. Configure
cp .env.example .env
# edit .env — at minimum set R2_* if you want uploads
cp config/tenants.example.json config/tenants.json
# edit config/tenants.json with your tenants

# 3. Generate a master key (one time only, store it like a root TLS key)
npm run generate-master-key
# → Master key generated at ./keys/master.key
# → Fingerprint: a1b2c3d4e5f60718

# 4. Run the ingestion pipeline once (good for cron)
npm run pipeline

# 5. Or run the long-lived orchestrator (cron-loop inside the process)
npm run orchestrator

# 6. Start the query proxy (separate process)
npm run proxy
# → DuckDB Proxy listening on http://127.0.0.1:4500
```

## Querying

Compute the per-tenant token once (it's deterministic given the master key):

```bash
node -e "
  const { loadMasterKey, generateTenantToken } = require('./dist/security/keystore.js');
  const key = loadMasterKey('./keys/master.key');
  console.log(generateTenantToken(key, 'acme-co'));
"
```

Then query:

```bash
curl -sS http://localhost:4500/query \
  -H 'Content-Type: application/json' \
  -d '{
    "tenantId": "acme-co",
    "token":    "<paste-from-above>",
    "sql":      "SELECT _source_table, COUNT(*) FROM orders GROUP BY 1"
  }' | jq
```

The proxy will rewrite the SQL to
`SELECT _source_table, COUNT(*) FROM orders WHERE tenant_id = 'acme-co' GROUP BY 1`
before running it. If you try `DELETE`, `DROP`, a comment, a second
statement, or a multi-SELECT chain — the proxy refuses with HTTP 403
before DuckDB ever sees it.

## Configuration

All paths and crypto knobs are env vars — see [`.env.example`](.env.example)
for the full list with defaults and what each one does. Key ones:

| Variable | Default | What it controls |
|---|---|---|
| `MASTER_KEY_PATH` | `./keys/master.key` | Root key used to derive every tenant key |
| `HKDF_SALT` | `tenant-encrypted-datalake-v1` | Domain separator — never change after first key generation |
| `TENANTS_PATH` | `./config/tenants.json` | Tenant list (id, bucketName, databases) |
| `R2_*` | — | Cloudflare R2 credentials (S3-compatible) |
| `PROXY_PORT` / `PROXY_HOST` | `4500` / `127.0.0.1` | DuckDB proxy bind |
| `INGESTION_CRON` | `*/30 * * * *` | Schedule for the long-lived orchestrator |
| `INGESTION_BATCH_SIZE` | `10000` | Rows per DB query batch |
| `MAX_ROWS_PER_TABLE` | `100000` | Safety cap per table per cycle |

## Tenants config

`config/tenants.json` is the source of truth for what gets ingested. Each
tenant has an `id`, a `bucketName`, and a list of databases:

```json
{
  "tenants": [
    {
      "id": "acme-co",
      "name": "ACME Co.",
      "bucketName": "datalake-acme-co",
      "databases": [
        {
          "type": "postgresql",
          "credentialKey": "pg_acme",
          "name": "acme_app",
          "schemas": ["public"]
        }
      ],
      "active": true
    }
  ]
}
```

`credentialKey` looks the credential up in the encrypted credentials
file (`DB_CREDENTIALS_PATH`). If you don't use that — leave
`credentialKey` empty and the ingestion will read host/user/password
from `PG_*` / `MYSQL_*` env vars instead.

## Medallion layers

For every batch, the pipeline writes three Parquet files to R2:

- **Bronze** — raw rows, denylist-redacted, with `_ingested_at`,
  `_source_table`, `_source_db`, `tenant_id` appended
- **Silver** — bronze deduplicated by primary key (`id` or `_id`) and
  with all string columns trimmed
- **Gold** — daily aggregates (e.g. `{date: 2026-05-26, record_count: 12}`)

Paths in R2:

```
bronze/orders/year=2026/month=05/day=26/2026-05-26T03-15-22-001Z.parquet.enc
silver/orders/year=2026/month=05/day=26/2026-05-26T03-15-22-001Z.parquet.enc
gold/orders_agg/year=2026/month=05/day=26/2026-05-26T03-15-22-001Z.parquet.enc
```

Full-scan tables (no time column detected) use a fixed `latest.parquet.enc`
key so each cycle overwrites the previous file instead of accumulating.

## Layout

```
src/
├── ingestion/
│   ├── extract.ts        # Postgres + MySQL extractors, incremental + full-scan
│   ├── transform.ts      # bronze (redaction), silver (dedup+trim), gold (daily agg)
│   ├── load.ts           # encrypt + upload to R2 (multipart > 5MB)
│   ├── pipeline.ts       # one-shot CLI entry — ingest every active tenant
│   ├── orchestrator.ts   # long-lived process with INGESTION_CRON
│   └── state.ts          # incremental cursor + per-tenant file locks
├── proxy/
│   ├── server.ts         # Fastify, /health + /query + /tenants/:id/*
│   ├── auth.ts           # HKDF tenant-token validation, rate limit
│   ├── query.ts          # validateSQL + injectTenantFilter + executeQuery
│   ├── loader.ts         # fetch + decrypt parquet on demand, 5-min cache
│   └── cache.ts          # query-result LRU cache, 1k entries / 5-min TTL
├── security/
│   ├── keystore.ts       # generate / load master key, HKDF tenant derivation
│   ├── encryption.ts     # AES-256-GCM with authTag
│   └── tls.ts            # R2 client factory
├── config/
│   ├── credentials.ts    # decrypt the encrypted DB credentials file
│   ├── database.ts       # createPgPool, createMysqlConnection
│   ├── r2.ts             # R2 client + bucket path builder
│   └── tenants.ts        # load tenants.json + active filter
└── utils/
    ├── logger.ts         # pino with file + pretty transport
    └── parquet.ts        # rows → Parquet buffer via DuckDB
```

## Limitations

- **Single-host design.** State (`ingestion-state.json`, lock files) is
  on local disk. Running multiple ingestion pipelines against the same
  state path will corrupt the locks.
- **No streaming ingestion.** It's batch — a tenant table can be at most
  `MAX_ROWS_PER_TABLE` rows per cycle. For larger continuous loads,
  you'd want CDC + a queue.
- **DuckDB in-memory.** Every query spins up an in-process DuckDB,
  loads the needed Parquet files, runs the SQL, closes. Great for small
  analytical queries, bad for sub-millisecond latency.
- **No row-level access control inside a tenant.** Tenants see all rows
  for tables they have access to. If you need per-user RBAC within a
  tenant, layer it on top.
- **No tests in this public version.** The internal version has a Vitest
  suite covering encryption roundtrip, cross-tenant isolation, and SQL
  injection bypass — they're not included here because they depend on
  internal test fixtures.

## License

MIT — see [LICENSE](LICENSE).
