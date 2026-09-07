#!/usr/bin/env bash
# Export a local Docker image and import into k3s containerd (Docker 里有 ≠ k3s 能用).
set -euo pipefail

IMAGE="${1:-daft-ray-ops:2.55.1}"

if ! docker image inspect "${IMAGE}" >/dev/null 2>&1; then
  echo "Image not found locally: ${IMAGE}" >&2
  echo "Build first: RAY_VERSION=2.55.1 bash build.sh" >&2
  exit 1
fi

docker save "${IMAGE}" | sudo k3s ctr -n k8s.io images import -
echo "Imported ${IMAGE} into k3s containerd"
