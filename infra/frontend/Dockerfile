FROM node:22-alpine AS dependencies
WORKDIR /opt/app
COPY package.json package-lock.json ./
RUN npm ci

FROM node:22-alpine AS builder
WORKDIR /opt/app
ENV NEXT_TELEMETRY_DISABLED=1
COPY --from=dependencies /opt/app/node_modules ./node_modules
COPY app ./app
COPY public ./public
COPY next.config.ts postcss.config.mjs tsconfig.json ./
RUN npm run build

FROM node:22-alpine AS runner
WORKDIR /opt/app
ENV NODE_ENV=production \
    NEXT_TELEMETRY_DISABLED=1 \
    PORT=3000 \
    HOSTNAME=0.0.0.0

RUN addgroup --system --gid 1001 nodejs \
    && adduser --system --uid 1001 nextjs \
    && apk add --no-cache su-exec \
    && mkdir -p /opt/app/data/incoming \
    && chown -R nextjs:nodejs /opt/app

COPY --from=builder --chown=nextjs:nodejs /opt/app/public ./public
COPY --from=builder --chown=nextjs:nodejs /opt/app/.next/standalone ./
COPY --from=builder --chown=nextjs:nodejs /opt/app/.next/static ./.next/static
COPY --chown=root:root infra/frontend/docker-entrypoint.sh /usr/local/bin/frontend-entrypoint
RUN chmod 0555 /usr/local/bin/frontend-entrypoint

EXPOSE 3000
ENTRYPOINT ["/usr/local/bin/frontend-entrypoint"]
CMD ["node", "server.js"]
