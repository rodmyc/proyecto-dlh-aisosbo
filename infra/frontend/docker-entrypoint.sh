#!/bin/sh
set -eu

mkdir -p /opt/app/data/incoming
chown nextjs:nodejs /opt/app/data/incoming

exec su-exec nextjs:nodejs "$@"
