# apps/api-gateway/Dockerfile
# Multi-stage build: builder → runtime

FROM node:20-alpine AS builder
WORKDIR /app

RUN corepack enable && corepack prepare pnpm@9.15.0 --activate

# Copy package manifests for dependency install layer cache
COPY package.json pnpm-workspace.yaml pnpm-lock.yaml* ./
COPY apps/api-gateway/package.json ./apps/api-gateway/

# Install dependencies
RUN pnpm install --filter @nexus/api-gateway --frozen-lockfile || pnpm install --filter @nexus/api-gateway

# Copy source and build
COPY apps/api-gateway/src ./apps/api-gateway/src
COPY apps/api-gateway/tsconfig.json ./apps/api-gateway/

WORKDIR /app/apps/api-gateway
RUN pnpm build

# ── Runtime ────────────────────────────────────────────────────────────────────
FROM node:20-alpine AS runtime
WORKDIR /app

RUN corepack enable && corepack prepare pnpm@9.15.0 --activate

COPY package.json pnpm-workspace.yaml pnpm-lock.yaml* ./
COPY apps/api-gateway/package.json ./apps/api-gateway/

RUN pnpm install --filter @nexus/api-gateway --prod --frozen-lockfile || pnpm install --filter @nexus/api-gateway --prod

COPY --from=builder /app/apps/api-gateway/dist ./apps/api-gateway/dist

WORKDIR /app/apps/api-gateway

RUN addgroup -S nexus && adduser -S nexus -G nexus
USER nexus

EXPOSE 4000

HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=3 \
  CMD wget -qO- http://localhost:4000/health || exit 1

CMD ["node", "dist/index.js"]
