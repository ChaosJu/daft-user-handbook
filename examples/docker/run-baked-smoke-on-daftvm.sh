#!/bin/bash
# 在 daftvm 上：构建 daft-ray-ops → 导入 k3s → 跑 baked smoke RayJob
# 用法（Windows 主机）：
#   scp -r examples/docker examples/quickstart/00-configmap-script-baked.yaml \
#       examples/quickstart/10-rayjob-baked-smoke.yaml daftvm:/tmp/daft-baked-test/
#   ssh daftvm "bash /tmp/daft-baked-test/docker/run-baked-smoke-on-daftvm.sh"
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG="/tmp/daft-baked-smoke.log"
exec > >(tee -a "$LOG") 2>&1

RAY_VERSION="${RAY_VERSION:-2.55.1}"
IMAGE="daft-ray-ops:${RAY_VERSION}"

export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
K="sudo kubectl --kubeconfig=$KUBECONFIG"

echo "=== $(date -Is) baked smoke on $(hostname) image=${IMAGE} ==="

ensure_pause() {
  local pause="rancher/mirrored-pause:3.10.2"
  if sudo k3s ctr -n k8s.io images ls | grep -q "mirrored-pause:3.10.2"; then
    return 0
  fi
  docker pull "$pause" \
    || { docker pull registry.k8s.io/pause:3.10 && docker tag registry.k8s.io/pause:3.10 "$pause"; }
  docker save "$pause" | sudo k3s ctr -n k8s.io images import -
}
ensure_pause

echo "=== docker build ${IMAGE} ==="
cd "$SCRIPT_DIR"
RAY_VERSION="$RAY_VERSION" IMAGE="$IMAGE" bash build.sh

echo "=== import ${IMAGE} -> k3s ==="
bash "$SCRIPT_DIR/import-to-k3s.sh" "$IMAGE"

QS="$ROOT"
for f in 00-configmap-script-baked.yaml 10-rayjob-baked-smoke.yaml; do
  if [ ! -f "${QS}/${f}" ]; then
    echo "FATAL: missing ${QS}/${f}" >&2
    exit 1
  fi
done

echo "=== cleanup old baked run ==="
$K delete rayjob daft-quickstart-baked -n daft-quickstart --ignore-not-found --wait=false
$K delete raycluster --all -n daft-quickstart --ignore-not-found --wait=false
sleep 5

echo "=== apply manifests ==="
$K apply -f "${QS}/00-configmap-script-baked.yaml"
$K apply -f "${QS}/10-rayjob-baked-smoke.yaml"

echo "=== watch up to 20 min ==="
for i in $(seq 1 120); do
  JS=$($K get rayjob daft-quickstart-baked -n daft-quickstart -o jsonpath='{.status.jobStatus}' 2>/dev/null || true)
  DS=$($K get rayjob daft-quickstart-baked -n daft-quickstart -o jsonpath='{.status.jobDeploymentStatus}' 2>/dev/null || true)
  RS=$($K get rayjob daft-quickstart-baked -n daft-quickstart -o jsonpath='{.status.reason}' 2>/dev/null || true)
  echo "tick=$i jobStatus=${JS:-?} deploymentStatus=${DS:-?} reason=${RS:-?}"
  $K get pods -n daft-quickstart 2>/dev/null | tail -6
  if [ "$JS" = "SUCCEEDED" ] && [ "$DS" = "Complete" ]; then
    echo "=== SUCCESS ==="
    SUB=$($K get pods -n daft-quickstart -o jsonpath='{.items[?(@.metadata.labels.job-name)].metadata.name}' 2>/dev/null | awk '{print $1}' || true)
    [ -n "$SUB" ] && $K logs -n daft-quickstart "$SUB" --tail=40 2>&1 || true
    exit 0
  fi
  if [ "$DS" = "Failed" ]; then
    echo "FAILED reason=$RS"
    break
  fi
  sleep 10
done

echo "=== debug logs ==="
$K describe rayjob daft-quickstart-baked -n daft-quickstart 2>&1 | tail -40
HEAD=$($K get pod -n daft-quickstart -l ray.io/node-type=head -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
[ -n "$HEAD" ] && $K logs -n daft-quickstart "$HEAD" -c ray-head --tail=100 2>&1 || true
SUB=$($K get pods -n daft-quickstart -o jsonpath='{.items[?(@.metadata.labels.job-name)].metadata.name}' 2>/dev/null | awk '{print $1}' || true)
[ -n "$SUB" ] && $K logs -n daft-quickstart "$SUB" --tail=120 2>&1 || true
exit 1
