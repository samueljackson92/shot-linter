#!/usr/bin/env bash
set -euo pipefail

# Resolve paths
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SHOT_FILE="${1:-${SCRIPT_DIR}/MU05.csv}"
OUTPUT_FILE="${2:-${SCRIPT_DIR}/analysed_source_linting_results.parquet}"
MAPPING_FILE="${3:-/Users/rt2549/projects/fair-mast-ingestion/mappings/level2/mastu.yml}"

if [[ ! -f "$SHOT_FILE" ]]; then
  echo "Error: Shot file not found: $SHOT_FILE"
  exit 1
fi

if [[ ! -f "$MAPPING_FILE" ]]; then
  echo "Error: Mapping file not found: $MAPPING_FILE"
  exit 1
fi

# Step 1: Extract UDA signal paths from the YAML mapping file
SIGNALS=$(uv run python -c "
import yaml

with open('${MAPPING_FILE}') as f:
    data = yaml.safe_load(f)

def extract_sources(obj):
    results = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == 'source' and isinstance(v, str):
                results.append(v)
            elif k == 'source' and isinstance(v, list):
                for item in v:
                    if isinstance(item, dict) and 'channels' in item:
                        for ch in item['channels']:
                            if isinstance(ch, str):
                                results.append(ch)
            elif k == 'plasma_current' and isinstance(v, str):
                results.append(v)
            else:
                results.extend(extract_sources(v))
    elif isinstance(obj, list):
        for item in obj:
            results.extend(extract_sources(item))
    return results

sources = set(extract_sources(data))
for s in sorted(sources):
    print(s)
")

echo "Found $(echo "$SIGNALS" | wc -l | tr -d ' ') signals from mapping file"

# Step 2: Run the linter once with all shots from the CSV and all signals
echo "Linting with shots from $(basename "$SHOT_FILE") and all mapped signals..."
uv run python -m shot_linter.main \
  --shot-file "$SHOT_FILE" \
  --signals $SIGNALS \
  --output-file "$OUTPUT_FILE" \
  -n 10

echo "Done. Results saved to $OUTPUT_FILE"
