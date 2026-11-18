#!/bin/sh
# One-shot n8n set-up, run by the n8n-import service before n8n starts:
# credentials are built from environment variables (nothing secret is committed), then every workflow in
# n8n/workflows is imported and published. Safe to re-run: imports overwrite by id.
set -eu

node /init/make_credentials.js > /tmp/credentials.json
n8n import:credentials --input=/tmp/credentials.json
rm -f /tmp/credentials.json

n8n import:workflow --separate --input=/workflows
for file in /workflows/*.json; do
  id=$(node -e "console.log(require('$file').id)")
  n8n publish:workflow --id="$id"
done
echo "n8n set-up complete"
