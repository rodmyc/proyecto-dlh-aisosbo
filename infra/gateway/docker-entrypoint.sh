#!/bin/sh
set -eu

if [ -z "${DAGSTER_BASIC_AUTH_USERNAME:-}" ] || [ -z "${DAGSTER_BASIC_AUTH_PASSWORD:-}" ]; then
    echo "DAGSTER_BASIC_AUTH_USERNAME and DAGSTER_BASIC_AUTH_PASSWORD are required" >&2
    exit 1
fi

printf '%s\n' "$DAGSTER_BASIC_AUTH_PASSWORD" \
    | htpasswd -niB "$DAGSTER_BASIC_AUTH_USERNAME" > /etc/nginx/.htpasswd
chmod 0600 /etc/nginx/.htpasswd
unset DAGSTER_BASIC_AUTH_PASSWORD

exec nginx -g 'daemon off;'
