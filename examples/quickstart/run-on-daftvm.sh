#!/bin/bash
# 在 daftvm 上从零跑通 examples/quickstart/
# 用法（Windows 主机）：
#   scp -r examples/quickstart daftvm:/tmp/daft-quickstart-test
#   ssh daftvm "bash /tmp/daft-quickstart-test/run-on-daftvm.sh"
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
LOG="/tmp/daft-quickstart-test.log"
exec > >(tee -a "$LOG") 2>&1

echo "=== $(date -Is) daft quickstart test on $(hostname) ==="

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "FATAL: missing $1"; exit 1; }
}

import_image_to_k3s() {
  local ref="$1"
  echo "=== import $ref -> k3s containerd ==="
  if sudo k3s ctr -n k8s.io images ls | grep -q "${ref/@sha256*/}"; then
    echo "already in k3s: $ref"
    return 0
  fi
  if docker image inspect "$ref" >/dev/null 2>&1; then
    docker save "$ref" | sudo k3s ctr -n k8s.io images import -
  else
    echo "pulling $ref with docker..."
    docker pull "$ref"
    docker save "$ref" | sudo k3s ctr -n k8s.io images import -
  fi
}

ensure_pause_image() {
  local pause="rancher/mirrored-pause:3.10.2"
  if import_image_to_k3s "$pause"; then return 0; fi
  for alt in \
    "registry.k8s.io/pause:3.10" \
    "registry.cn-hangzhou.aliyuncs.com/google_containers/pause:3.10"; do
    if docker pull "$alt"; then
      docker tag "$alt" "$pause"
      docker save "$pause" | sudo k3s ctr -n k8s.io images import -
      return 0
    fi
  done
  echo "FATAL: pause image unavailable"
  exit 1
}

install_k3s_if_needed() {
  if command -v k3s >/dev/null 2>&1 && [ -f /etc/rancher/k3s/k3s.yaml ]; then
    echo "k3s already installed"
    return 0
  fi
  echo "=== installing k3s (single-node) ==="
  curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="--write-kubeconfig-mode 644 --disable traefik" sh -
  need_cmd k3s
}

export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
KUBECTL="sudo kubectl --kubeconfig=$KUBECONFIG"

install_k3s_if_needed
need_cmd docker

echo "=== wait for k3s node ==="
for i in $(seq 1 60); do
  if $KUBECTL get nodes 2>/dev/null | grep -q ' Ready '; then
    $KUBECTL get nodes -o wide
    break
  fi
  sleep 2
  if [ "$i" -eq 60 ]; then
    echo "FATAL: k3s node not ready"
    exit 1
  fi
done

echo "=== import images (docker -> k3s) ==="
ensure_pause_image
import_image_to_k3s "quay.io/kuberay/operator:v1.6.2"
if [ -d "$ROOT/../docker" ]; then
  echo "=== build daft-ray-ops from $ROOT/../docker ==="
  (cd "$ROOT/../docker" && bash build.sh)
  bash "$ROOT/../docker/import-to-k3s.sh" daft-ray-ops:2.55.1
else
  import_image_to_k3s "daft-ray-ops:2.55.1"
fi

KUBERAY_YAML="/home/chaos/offline-build/kuberay-operator-v1.6.2.yaml"
if [ ! -f "$KUBERAY_YAML" ]; then
  echo "FATAL: missing $KUBERAY_YAML"
  exit 1
fi

echo "=== install KubeRay operator ==="
$KUBECTL create namespace kuberay-system --dry-run=client -o yaml | $KUBECTL apply -f -
$KUBECTL apply --server-side -f "$KUBERAY_YAML"
for i in $(seq 1 90); do
  if $KUBECTL -n kuberay-system rollout status deploy/kuberay-operator --timeout=5s 2>/dev/null; then
    break
  fi
  sleep 3
  if [ "$i" -eq 90 ]; then
    echo "FATAL: kuberay operator not ready"
    $KUBECTL -n kuberay-system get pods -o wide || true
    exit 1
  fi
done
$KUBECTL get crd | grep ray.io

echo "=== cleanup previous run ==="
$KUBECTL delete rayjob daft-quickstart -n daft-quickstart --ignore-not-found
$KUBECTL delete ns daft-quickstart --ignore-not-found --wait=false || true
sleep 3

echo "=== apply quickstart manifests ==="
$KUBECTL apply -f "$ROOT/00-configmap-script-baked.yaml"
$KUBECTL apply -f "$ROOT/10-rayjob-smoke.yaml"

echo "=== watch RayJob (up to 15 min) ==="
for i in $(seq 1 90); do
  $KUBECTL get rayjob daft-quickstart -n daft-quickstart 2>/dev/null || true
  JS=$($KUBECTL get rayjob daft-quickstart -n daft-quickstart -o jsonpath='{.status.jobStatus}' 2>/dev/null || true)
  DS=$($KUBECTL get rayjob daft-quickstart -n daft-quickstart -o jsonpath='{.status.jobDeploymentStatus}' 2>/dev/null || true)
  RS=$($KUBECTL get rayjob daft-quickstart -n daft-quickstart -o jsonpath='{.status.reason}' 2>/dev/null || true)
  echo "tick=$i jobStatus=$JS deploymentStatus=$DS reason=$RS"
  if [ "$JS" = "SUCCEEDED" ] && [ "$DS" = "Complete" ]; then
    echo "=== SUCCESS ==="
    break
  fi
  if [ "$DS" = "Failed" ]; then
    echo "=== FAILED ==="
    $KUBECTL describe rayjob daft-quickstart -n daft-quickstart || true
    break
  fi
  sleep 10
done

echo "=== pods ==="
$KUBECTL get pods -n daft-quickstart -o wide || true
$KUBECTL get pods -A | grep -E 'daft-quickstart|kuberay|NAMESPACE' || true

echo "=== submitter logs ==="
SUB=$($KUBECTL get pods -n daft-quickstart -o name 2>/dev/null | grep job-name | head -1 || true)
if [ -n "$SUB" ]; then
  $KUBECTL logs -n daft-quickstart "$SUB" --tail=200 || true
fi

echo "=== head logs ==="
HEAD=$($KUBECTL get pod -n daft-quickstart -l ray.io/node-type=head -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
if [ -n "$HEAD" ]; then
  $KUBECTL logs -n daft-quickstart "$HEAD" -c ray-head --tail=200 || true
fi

JS=$($KUBECTL get rayjob daft-quickstart -n daft-quickstart -o jsonpath='{.status.jobStatus}' 2>/dev/null || true)
DS=$($KUBECTL get rayjob daft-quickstart -n daft-quickstart -o jsonpath='{.status.jobDeploymentStatus}' 2>/dev/null || true)
if [ "$JS" = "SUCCEEDED" ] && [ "$DS" = "Complete" ]; then
  echo "RESULT: PASS"
  exit 0
else
  echo "RESULT: FAIL (jobStatus=$JS deploymentStatus=$DS)"
  exit 1
fi
