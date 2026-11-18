#!/bin/sh
# Walk through the three agent workflows on a running stack (docker compose up -d first).
#   ./demo/run_demo.sh            # uses .env
# Prints the approval links; open one in a browser (log in with APPROVER_USER / APPROVER_PASSWORD).
set -eu
cd "$(dirname "$0")/.."
env_file=${1:-.env}
set -a; . "./$env_file"; set +a
ops() { curl -fsS -H "Authorization: Bearer $OPS_TOOLS_TOKEN" "$@"; }
hook() { curl -fsS -X POST "http://localhost:5678/webhook/$1" -H "X-Webhook-Token: $N8N_WEBHOOK_TOKEN" \
           -H 'Content-Type: application/json' -d "$2" >/dev/null; }
show() { ops "http://localhost:8000/notifications?limit=$1" | python3 -c '
import json, sys
for n in reversed(json.load(sys.stdin)):
    print("\n== " + n["title"] + "\n" + n["body"])
    for label, url in n["links"].items():
        print(f"   {label}: {url}")'; }

echo "1/3  Spark job failed (container killed by YARN): daily_sales_join"
hook spark-job-failed '{"app_id": "application_1700000000000_0042", "pipeline": "daily_sales_join"}'
sleep 20; show 1

echo; echo "2/3  Data quality: seeding a producer schema change on orders_enrichment, then checking"
docker compose --env-file "$env_file" exec -T postgres psql -q -U ops_tools -d ops < demo/seed_quality.sql
hook check-quality-now '{}'
sleep 20; show 1

echo; echo "3/3  Weekly Spark tuning review"
hook run-weekly-review '{}'
sleep 25; show 1

echo; echo "Approve or decline with the links above. Audit trail:"
echo "  docker compose exec postgres psql -U ops_tools -d ops -c 'select * from ops.agent_run order by run_id desc limit 5'"
