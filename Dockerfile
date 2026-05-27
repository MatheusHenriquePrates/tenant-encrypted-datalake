FROM node:20-alpine AS build

WORKDIR /app
COPY package.json package-lock.json* tsconfig.json ./
RUN npm install
COPY src ./src
RUN npm run build

FROM node:20-alpine
WORKDIR /app

# Native deps for duckdb on Alpine
RUN apk add --no-cache libc6-compat

COPY package.json package-lock.json* ./
RUN npm install --omit=dev

COPY --from=build /app/dist ./dist

# Runtime paths are configurable via env — see .env.example
RUN mkdir -p /app/keys /app/cache /app/logs /app/config \
 && addgroup -S app && adduser -S app -G app \
 && chown -R app:app /app

USER app

ENV MASTER_KEY_PATH=/app/keys/master.key \
    DB_CREDENTIALS_PATH=/app/keys/db-credentials.enc \
    STATE_PATH=/app/cache/ingestion-state.json \
    LOCKS_DIR=/app/cache/locks \
    LOG_DIR=/app/logs \
    TENANTS_PATH=/app/config/tenants.json \
    PROXY_TMP_DIR=/tmp/datalake-proxy

EXPOSE 4500

# Default: run the proxy. Override with `docker run … npm run pipeline` for ingestion.
CMD ["node", "dist/proxy/server.js"]
