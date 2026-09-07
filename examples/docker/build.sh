#!/usr/bin/env bash
# Build daft-ray-ops image. Run from examples/docker/.
set -euo pipefail

RAY_VERSION="${RAY_VERSION:-2.55.1}"
IMAGE="${IMAGE:-daft-ray-ops:${RAY_VERSION}}"

docker build \
  --build-arg "RAY_VERSION=${RAY_VERSION}" \
  -f Dockerfile \
  -t "${IMAGE}" \
  .

echo "Built ${IMAGE}"
echo "Smoke: docker run --rm ${IMAGE} python /opt/daft-handbook/smoke.py"
