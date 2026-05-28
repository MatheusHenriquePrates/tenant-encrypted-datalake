<p align="center">
  <a href="#-portugu%C3%AAs"><img src="https://img.shields.io/badge/-PT--BR-39d353?style=for-the-badge&labelColor=0d1117" alt="PT-BR"/></a>
  &nbsp;
  <a href="#-english"><img src="https://img.shields.io/badge/-EN-58a6ff?style=for-the-badge&labelColor=0d1117" alt="EN"/></a>
</p>

<p align="center">
  <img src="https://capsule-render.vercel.app/api?type=slice&color=0:0d1117,100:39d353&height=180&section=header&text=tenant-encrypted-datalake&fontSize=30&fontColor=ffffff&animation=fadeIn&fontAlignY=42&desc=data%20lake%20multi-tenant%20com%20cripto%20client-side%20por%20tenant&descAlignY=68&descSize=14" width="100%" />
</p>

<p align="center">
  <img src="https://readme-typing-svg.demolab.com/?lines=%24+matheus%40devops%3A~%24+tenant-encrypted-datalake;%24+HKDF+por+tenant%2C+AES-256-GCM+com+authTag;%24+Parquet+em+R2%2C+DuckDB+proxy+SELECT-only;%24+inje%C3%A7%C3%A3o+autom%C3%A1tica+de+tenant_id+em+todo+WHERE&font=Fira%20Code&size=18&pause=1200&color=39D353&center=true&vCenter=true&width=720&height=45" />
</p>

<a id="-português"></a>

## PT-BR

```bash
matheus@devops:~$ cat sobre.txt
```

Data lake multi-tenant com **criptografia client-side por tenant** e um **proxy DuckDB SELECT-only** que filtra automaticamente toda query por `tenant_id`. Pensado pro caso onde:

- Você tem dado relacional de vários customers pra ingerir (Postgres / MySQL)
- Quer armazenar barato em object storage S3-compatible como Parquet
- **Não pode** confiar na camada de storage (cloud provider, time de ops, bucket compartilhado) pra enforcar isolamento de tenant — então o isolamento tem que ser criptográfico, não lógico
- Ainda quer um jeito de fazer SQL no dado sem reimplementar metade do Spark

O resultado: ~1.9k LoC de TypeScript implementando pipeline de ingestion Medallion, key por-tenant derivada via HKDF, criptografia AES-256-GCM com tag autenticada, e proxy Fastify que abre Parquet no DuckDB on-demand, injeta o filtro de tenant, e rejeita qualquer coisa que não seja um único `SELECT`.

> **Status: implementação de referência.** Versão pública sanitizada de um sistema que rodo em produção. Intencionalmente pequeno e single-host pra engenharia ser fácil de ler.

```bash
matheus@devops:~$ ls stack/
```

![TypeScript](https://img.shields.io/badge/-TypeScript-0d1117?style=for-the-badge&logo=typescript&logoColor=39d353) ![Node.js](https://img.shields.io/badge/-Node.js-0d1117?style=for-the-badge&logo=node.js&logoColor=39d353) ![PostgreSQL](https://img.shields.io/badge/-PostgreSQL-0d1117?style=for-the-badge&logo=postgresql&logoColor=39d353) ![MySQL](https://img.shields.io/badge/-MySQL-0d1117?style=for-the-badge&logo=mysql&logoColor=39d353) ![DuckDB](https://img.shields.io/badge/-DuckDB-0d1117?style=for-the-badge&logo=duckdb&logoColor=39d353) ![Cloudflare](https://img.shields.io/badge/-Cloudflare%20R2-0d1117?style=for-the-badge&logo=cloudflare&logoColor=39d353) ![Fastify](https://img.shields.io/badge/-Fastify-0d1117?style=for-the-badge&logo=fastify&logoColor=39d353)

```bash
matheus@devops:~$ cat threat-model.txt
```

Essa é a seção mais importante. Pula e o resto não importa.

### O que esse design protege

1. **Leitura do bucket de storage** por qualquer um — cloud provider, ACL público mal-configurado, conta de ops comprometida. Ele pega ciphertext + IV + auth tag, nada mais. Nenhum dado de tenant é recuperável sem a master key.

2. **Vazamento de dado cross-tenant em query.** O proxy recusa qualquer coisa que não seja um único `SELECT`, rejeita comentário SQL, bloqueia query multi-statement, e **injeta `tenant_id = '<caller>'`** no WHERE antes do DuckDB executar. Tenant que adivinha nome de tabela do outro ainda não lê os rows.

3. **Comprometimento do token HMAC de um tenant.** Tokens são derivados da master key via HKDF com info string específica de token. Saber token A não ajuda a computar token B.

4. **Ingestão acidental de secret.** `transformBronze` redige um denylist de nomes de coluna comuns sensíveis (`password`, `token`, `api_key`, `ssn`, `cpf`, …) com `[REDACTED]` antes de qualquer coisa ser persistida.

### O que esse design NÃO protege

1. **Perda da master key.** A master key (`./keys/master.key`) deriva toda key de tenant. Se vaza, **todo** dado encriptado de todo tenant vira legível. Guarda como guardaria uma root TLS key. `.gitignore` desse repo exclui `keys/` e `*.key` — confere antes de `git add -A`.

2. **Host comprometido rodando o pipeline.** O processo tem master key na RAM e credencial de banco na RAM. Quem lê a memória (root, debugger, core dump) tem tudo.

3. **Host comprometido rodando o proxy.** Mesmo problema — o proxy mantém master key em memória pra derivar tenant keys on-demand.

4. **Escape do sandbox do DuckDB.** Queries rodam dentro do DuckDB. Se DuckDB tem bug que permite escapar a semântica `SELECT`, a defesa em nível de parser (`validateSQL`) é a única no caminho. Mantém DuckDB atualizado.

5. **Side channels** (timing, cache, network). A comparação de token em `validateTenantToken` é **igualdade de string**, não constant-time. O threat model assume que o proxy fica em rede privada — pra clients não confiáveis, troca por `crypto.timingSafeEqual`.

6. **Coluna não confiável chegando em Gold.** O denylist em `transformBronze` pega nomes óbvios. Uma coluna `acct_secret_v2` passa. Audita os schemas dos tenants; não confia só no denylist.

7. **Rotação do `HKDF_SALT`.** O salt é separador de domínio. **Uma vez escolhido o valor, nunca muda** — todo arquivo previamente encriptado vira ilegível. Default do repo (`tenant-encrypted-datalake-v1`) tá ok pra deploy único.

```bash
matheus@devops:~$ cat arquitetura.txt
```

```
┌──────────────────────┐         ┌──────────────────────┐
│ Tenant A — Postgres  │         │ Tenant B — MySQL     │
└──────────┬───────────┘         └──────────┬───────────┘
           │  SELECT-only readonly creds                │
           ▼                                            ▼
┌───────────────────────────────────────────────────────────┐
│  Pipeline de ingestion (Medallion: bronze → silver → gold)│
│  - incremental por coluna de tempo quando disponível      │
│  - skip por row-count em tabela full-scan                 │
│  - redação por denylist de password, token, ssn, …        │
└───────────────────────────────────────────────────────────┘
           │                                            │
           │  parquet                                   │  parquet
           ▼                                            ▼
┌───────────────────────────────────────────────────────────┐
│            Criptografia AES-256-GCM                       │
│  - tenantKey = HKDF-SHA256(masterKey, salt, tenantId, 32) │
│  - payload do arquivo: [16B IV][16B authTag][ciphertext]  │
└───────────────────────────────────────────────────────────┘
           │                                            │
           ▼                                            ▼
┌───────────────────────────────────────────────────────────┐
│            Cloudflare R2 (S3-compatible)                  │
│  bucket por tenant: datalake-{tenant_id}                  │
│  layout: {layer}/{table}/year=…/month=…/…                 │
└───────────────────────────────────────────────────────────┘
                            ▲
                            │ GET on-demand, decifra em memória
┌───────────────────────────────────────────────────────────┐
│                 DuckDB proxy (Fastify, :4500)             │
│  POST /query { tenantId, token, sql }                     │
│  1. validateTenantToken (HMAC HKDF-derived, per tenant)   │
│  2. rate limit (100 req/min por tenant)                   │
│  3. validateSQL (SELECT-only, sem comentário, sem DDL/DML)│
│  4. fetch último .parquet.enc do R2                       │
│  5. decifra → arquivo /tmp → DuckDB read_parquet(…)       │
│  6. injectTenantFilter (adiciona WHERE tenant_id = '…')   │
│  7. executa, cacheia resultado 5 min, devolve JSON        │
└───────────────────────────────────────────────────────────┘
                            ▲
                            │ HTTP
                       Client de query do tenant
```

```bash
matheus@devops:~$ ./quick-start.sh
```

```bash
git clone https://github.com/MatheusHenriquePrates/tenant-encrypted-datalake.git
cd tenant-encrypted-datalake

# 1. Install + build
npm install
npm run build

# 2. Configura
cp .env.example .env
cp config/tenants.example.json config/tenants.json

# 3. Gera a master key (uma vez só)
npm run generate-master-key
# → Master key generated at ./keys/master.key

# 4. Pipeline one-shot (bom pra cron)
npm run pipeline

# 5. Ou orchestrator long-lived (cron-loop dentro do processo)
npm run orchestrator

# 6. Sobe o query proxy (processo separado)
npm run proxy
# → DuckDB Proxy listening on http://127.0.0.1:4500
```

```bash
matheus@devops:~$ cat querying.txt
```

Computa o token por-tenant uma vez (é determinístico dada a master key):

```bash
node -e "
const { loadMasterKey, generateTenantToken } = require('./dist/security/keystore.js');
const key = loadMasterKey('./keys/master.key');
console.log(generateTenantToken(key, 'acme-co'));
"
```

Depois consulta:

```bash
curl -sS http://localhost:4500/query \
  -H 'Content-Type: application/json' \
  -d '{
    "tenantId": "acme-co",
    "token": "<cola-do-anterior>",
    "sql": "SELECT _source_table, COUNT(*) FROM orders GROUP BY 1"
  }' | jq
```

O proxy reescreve o SQL pra `SELECT _source_table, COUNT(*) FROM orders WHERE tenant_id = 'acme-co' GROUP BY 1` antes de rodar. Se você tenta `DELETE`, `DROP`, comentário, segundo statement, ou multi-SELECT — o proxy recusa com HTTP 403 antes do DuckDB ver.

```bash
matheus@devops:~$ cat config.env
```

| Variável | Default | Pra quê |
|---|---|---|
| `MASTER_KEY_PATH` | `./keys/master.key` | Root key usada pra derivar toda tenant key |
| `HKDF_SALT` | `tenant-encrypted-datalake-v1` | Separador de domínio — nunca muda depois da primeira geração de key |
| `TENANTS_PATH` | `./config/tenants.json` | Lista de tenants (id, bucketName, databases) |
| `R2_*` | — | Credencial Cloudflare R2 (S3-compatible) |
| `PROXY_PORT` / `PROXY_HOST` | `4500` / `127.0.0.1` | Bind do proxy DuckDB |
| `INGESTION_CRON` | `*/30 * * * *` | Schedule do orchestrator long-lived |
| `INGESTION_BATCH_SIZE` | `10000` | Linhas por batch de query no DB |
| `MAX_ROWS_PER_TABLE` | `100000` | Cap de segurança por tabela por ciclo |

```bash
matheus@devops:~$ cat medallion-layers.txt
```

Pra cada batch, o pipeline escreve três arquivos Parquet no R2:

- **Bronze** — rows raw, redigidos pelo denylist, com `_ingested_at`, `_source_table`, `_source_db`, `tenant_id` appendados
- **Silver** — bronze dedup por primary key (`id` ou `_id`) e com colunas string trimmed
- **Gold** — agregados diários (ex: `{date: 2026-05-26, record_count: 12}`)

Tabelas full-scan (sem coluna de tempo detectada) usam key fixa `latest.parquet.enc` pra cada ciclo sobrescrever o anterior em vez de acumular.

```bash
matheus@devops:~$ cat limitacoes.txt
```

- **Design single-host.** Estado (`ingestion-state.json`, lock files) fica em disco local. Múltiplos pipelines contra o mesmo path corrompem os locks.
- **Sem ingestion streaming.** É batch — tabela de tenant tem no máximo `MAX_ROWS_PER_TABLE` linhas por ciclo. Pra cargas contínuas, usa CDC + fila.
- **DuckDB in-memory.** Cada query sobe DuckDB in-process, carrega Parquet necessário, roda SQL, fecha. Ótimo pra query analítica pequena, ruim pra latência sub-ms.
- **Sem row-level access control dentro de um tenant.** Tenants veem todas as linhas das tabelas que acessam. Pra RBAC por-user dentro de tenant, faz por cima.
- **Sem testes nessa versão pública.** A versão interna tem suite Vitest cobrindo encryption roundtrip, isolamento cross-tenant, e bypass de SQL injection.

```bash
matheus@devops:~$ cat LICENSE
```

MIT — veja [LICENSE](LICENSE).

```bash
matheus@devops:~$ contact
```

[![LinkedIn](https://img.shields.io/badge/-LinkedIn-0d1117?style=for-the-badge&logo=linkedin&logoColor=39d353)](https://www.linkedin.com/in/matheus-henrique-prates-586328234/)
[![Email](https://img.shields.io/badge/-Email-0d1117?style=for-the-badge&logo=gmail&logoColor=39d353)](mailto:mathues12398henrique@gmail.com)

```bash
matheus@devops:~$ _
```

---

<a id="-english"></a>

## EN

```bash
matheus@devops:~$ cat about.txt
```

Multi-tenant data lake with **per-tenant client-side encryption** and a **SELECT-only DuckDB proxy** that automatically filters every query by `tenant_id`.

~1.9k LoC of TypeScript implementing a Medallion ingestion pipeline, an HKDF-derived per-tenant key, AES-256-GCM encryption with authenticated tags, and a Fastify proxy that opens Parquet in DuckDB on demand, injects the tenant filter, and rejects anything that isn't a single `SELECT`.

> **Status: reference implementation.** Sanitized public version of a system run in production.

```bash
matheus@devops:~$ ls stack/
```

![TypeScript](https://img.shields.io/badge/-TypeScript-0d1117?style=for-the-badge&logo=typescript&logoColor=39d353) ![Node.js](https://img.shields.io/badge/-Node.js-0d1117?style=for-the-badge&logo=node.js&logoColor=39d353) ![PostgreSQL](https://img.shields.io/badge/-PostgreSQL-0d1117?style=for-the-badge&logo=postgresql&logoColor=39d353) ![MySQL](https://img.shields.io/badge/-MySQL-0d1117?style=for-the-badge&logo=mysql&logoColor=39d353) ![DuckDB](https://img.shields.io/badge/-DuckDB-0d1117?style=for-the-badge&logo=duckdb&logoColor=39d353) ![Cloudflare](https://img.shields.io/badge/-Cloudflare%20R2-0d1117?style=for-the-badge&logo=cloudflare&logoColor=39d353) ![Fastify](https://img.shields.io/badge/-Fastify-0d1117?style=for-the-badge&logo=fastify&logoColor=39d353)

```bash
matheus@devops:~$ cat threat-model.txt
```

**Protects against:** storage bucket reads (cloud provider, misconfigured ACL, compromised ops account); cross-tenant data leakage in queries; compromise of one tenant's HMAC token; accidental ingestion of secrets (denylist redaction).

**Does NOT protect against:** loss of the master key; compromised host running the pipeline or proxy (key + creds in RAM); DuckDB sandbox escape; side channels; untrusted columns making it to Gold; rotation of `HKDF_SALT` (never change after first key gen).

```bash
matheus@devops:~$ ./quick-start.sh
```

```bash
git clone https://github.com/MatheusHenriquePrates/tenant-encrypted-datalake.git
cd tenant-encrypted-datalake

npm install
npm run build

cp .env.example .env
cp config/tenants.example.json config/tenants.json

npm run generate-master-key
npm run pipeline
npm run proxy
```

```bash
matheus@devops:~$ cat querying.txt
```

```bash
curl -sS http://localhost:4500/query \
  -H 'Content-Type: application/json' \
  -d '{
    "tenantId": "acme-co",
    "token": "<paste-from-above>",
    "sql": "SELECT _source_table, COUNT(*) FROM orders GROUP BY 1"
  }' | jq
```

The proxy rewrites the SQL to `... WHERE tenant_id = 'acme-co' ...` before running. `DELETE`, `DROP`, comments, multi-statement → HTTP 403 before DuckDB sees it.

```bash
matheus@devops:~$ cat medallion-layers.txt
```

- **Bronze** — raw rows, denylist-redacted, with `_ingested_at`, `_source_table`, `_source_db`, `tenant_id` appended
- **Silver** — bronze deduplicated by primary key and trimmed
- **Gold** — daily aggregates

```bash
matheus@devops:~$ cat limitations.txt
```

- Single-host design (state on local disk).
- No streaming ingestion (batch only).
- DuckDB in-memory per query (analytical small queries only).
- No row-level access control inside a tenant.
- No tests in this public version.

```bash
matheus@devops:~$ cat LICENSE
```

MIT — see [LICENSE](LICENSE).

```bash
matheus@devops:~$ contact
```

[![LinkedIn](https://img.shields.io/badge/-LinkedIn-0d1117?style=for-the-badge&logo=linkedin&logoColor=39d353)](https://www.linkedin.com/in/matheus-henrique-prates-586328234/) [![Email](https://img.shields.io/badge/-Email-0d1117?style=for-the-badge&logo=gmail&logoColor=39d353)](mailto:mathues12398henrique@gmail.com)

```bash
matheus@devops:~$ _
```

<p align="center">
  <img src="https://capsule-render.vercel.app/api?type=waving&color=0:39d353,100:0d1117&height=120&section=footer" width="100%" />
</p>
