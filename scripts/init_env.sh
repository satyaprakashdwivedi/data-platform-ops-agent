#!/bin/sh
# Create .env from .env.example with fresh random secrets. Existing .env is never overwritten.
#   ./scripts/init_env.sh                    # then put your ANTHROPIC_API_KEY in .env
#   ./scripts/init_env.sh tests/e2e/.env.test   # CI: scripted model instead of the real API
set -eu
target=${1:-.env}
if [ -e "$target" ]; then echo "$target already exists; leaving it alone"; exit 0; fi
secret() { openssl rand -hex 24; }
sed -e "s/^POSTGRES_PASSWORD=.*/POSTGRES_PASSWORD=$(secret)/" \
    -e "s/^N8N_DB_PASSWORD=.*/N8N_DB_PASSWORD=$(secret)/" \
    -e "s/^OPS_DB_PASSWORD=.*/OPS_DB_PASSWORD=$(secret)/" \
    -e "s/^N8N_ENCRYPTION_KEY=.*/N8N_ENCRYPTION_KEY=$(secret)/" \
    -e "s/^OPS_TOOLS_TOKEN=.*/OPS_TOOLS_TOKEN=$(secret)/" \
    -e "s/^N8N_WEBHOOK_TOKEN=.*/N8N_WEBHOOK_TOKEN=$(secret)/" \
    -e "s/^APPROVER_PASSWORD=.*/APPROVER_PASSWORD=$(secret)/" \
    .env.example > "$target"
if [ "$target" != ".env" ]; then
  # test mode: point the Anthropic credential at the scripted stub (docker compose --profile test)
  sed -i.bak -e "s#^ANTHROPIC_API_KEY=.*#ANTHROPIC_API_KEY=stub-key\nANTHROPIC_BASE_URL=http://stub-llm:8100#" \
      -e "s/^N8N_WORKERS=.*/N8N_WORKERS=1/" "$target" && rm -f "$target.bak"
fi
chmod 600 "$target"
echo "wrote $target"
