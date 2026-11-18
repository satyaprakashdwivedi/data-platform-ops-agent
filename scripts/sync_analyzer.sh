#!/bin/sh
# Vendor the spark-optimizer analyzer at a pinned commit, or (--check) verify the copy is unmodified.
#   ./scripts/sync_analyzer.sh <commit>     update the copy and the pin
#   ./scripts/sync_analyzer.sh --check      CI: fail if the vendored file differs from the pinned commit
set -eu
cd "$(dirname "$0")/.."
target=ops-tools/src/ops_tools/vendor/analyze_spark_log.py
pin_file=ops-tools/src/ops_tools/vendor/VENDORED.md
if [ "${1:-}" = "--check" ]; then
  commit=$(grep -o '[0-9a-f]\{40\}' "$pin_file" | head -1)
else
  commit=${1:?usage: sync_analyzer.sh <commit>|--check}
fi
url="https://raw.githubusercontent.com/satyaprakashdwivedi/spark-optimizer-skill/$commit/spark-optimizer/scripts/analyze_spark_log.py"
tmp=$(mktemp)
curl -fsSL "$url" -o "$tmp"
if [ "${1:-}" = "--check" ]; then
  cmp -s "$tmp" "$target" && echo "vendored analyzer matches spark-optimizer-skill@$commit" || {
    echo "vendored analyzer differs from spark-optimizer-skill@$commit"; rm -f "$tmp"; exit 1; }
else
  mv "$tmp" "$target"
  sed -i.bak "s/[0-9a-f]\{40\}/$commit/" "$pin_file" && rm -f "$pin_file.bak"
  echo "vendored analyzer updated to $commit"
fi
rm -f "$tmp"
