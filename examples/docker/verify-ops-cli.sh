#!/bin/bash
set -euo pipefail
docker run --rm daft-ray-ops:2.55.1 sh -c '
  for c in top ps netstat ss; do
    command -v "$c"
  done
  top -bn1 | head -2
  netstat -tln | head -3
'
