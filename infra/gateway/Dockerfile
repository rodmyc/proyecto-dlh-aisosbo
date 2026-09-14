FROM nginx:1.29-alpine

RUN apk add --no-cache apache2-utils

COPY infra/gateway/nginx.conf /etc/nginx/conf.d/default.conf
COPY infra/gateway/docker-entrypoint.sh /usr/local/bin/dagster-gateway-entrypoint

RUN chmod 0555 /usr/local/bin/dagster-gateway-entrypoint

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/dagster-gateway-entrypoint"]
