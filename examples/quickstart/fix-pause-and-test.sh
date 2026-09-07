#!/bin/bash
set -euo pipefail
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
KUBECTL="sudo kubectl --kubeconfig=$KUBECONFIG"
ROOT="/tmp/daft-quickstart-test"
KUBERAY_YAML="/home/chaos/offline-build/kuberay-operator-v1.6.2.yaml"
PAUSE_REF="rancher/mirrored-pause:3.10.2"
LOG="/tmp/fix-pause-and-test.log"
exec > >(tee -a "$LOG") 2>&1

echo "=== $(date -Is) fix pause image + resume test ==="

import_to_k3s() {
  local ref="$1"
  echo "=== import $ref -> k3s ==="
  if sudo k3s ctr -n k8s.io images ls | grep -q "${ref/@sha256*/}"; then
    echo "already present: $ref"
    return 0
  fi
  if docker image inspect "$ref" >/dev/null 2>&1; then
    docker save "$ref" | sudo k3s ctr -n k8s.io images import -
    return 0
  fi
  return 1
}

ensure_pause_image() {
  echo "=== ensure pause sandbox image: $PAUSE_REF ==="
  if import_to_k3s "$PAUSE_REF"; then
    return 0
  fi

  echo "direct pull failed/missing, trying alternatives..."
  for alt in \
    "registry.k8s.io/pause:3.10" \
    "registry.cn-hangzhou.aliyuncs.com/google_containers/pause:3.10" \
    "registry.aliyuncs.com/google_containers/pause:3.10"; do
    echo "try pull $alt"
    if docker pull "$alt"; then
      docker tag "$alt" "$PAUSE_REF"
      docker save "$PAUSE_REF" | sudo k3s ctr -n k8s.io images import -
      echo "imported via tag from $alt"
      return 0
    fi
  done

  echo "FATAL: could not obtain pause image"
  exit 1
}

ensure_other_images() {
  for ref in \
    "quay.io/kuberay/operator:v1.6.2" \
    "daft-ray-ops:2.55.1"; do
    if ! sudo k3s ctr -n k8s.io images ls | grep -q "${ref/@sha256*/}"; then
      if docker image inspect "$ref" >/dev/null 2>&1; then
        docker save "$ref" | sudo k3s ctr -n k8s.io images import -
      else
        docker pull "$ref"
        docker save "$ref" | sudo k3s ctr -n k8s.io images import -
      fi
    fi
  done
}

write_registries_mirror() {
  # 降低后续 sandbox / 业务镜像走 docker.io 失败的概率
  sudo mkdir -p /etc/rancher/k3s
  if [ ! -f /etc/rancher/k3s/registries.yaml ]; then
    sudo tee /etc/rancher/k3s/registries.yaml >/dev/null <<'EOF'
mirrors:
  docker.io:
    endpoint:
      - "https://docker.m.daocloud.io"
      - "https://registry-1.docker.io"
  registry.k8s.io:
    endpoint:
      - "https://registry.k8s.io"
EOF
    echo "wrote /etc/rancher/k3s/registries.yaml"
    sudo systemctl restart k3s
    sleep 15
    for i in $(seq 1 30); do
      if $KUBECTL get nodes 2>/dev/null | grep -q Ready; then break; fi
      sleep 2
    done
  fi
}

ensure_pause_image
ensure_other_images
write_registries_mirror

echo "=== k3s images (pause/ray/kuberay) ==="
sudo k3s ctr -n k8s.io images ls | grep -E 'pause|rayproject|kuberay' || true

echo "=== ensure kuberay operator ==="
$KUBECTL create namespace kuberay-system --dry-run=client -o yaml | $KUBECTL apply -f -
$KUBECTL apply --server-side -f "$KUBERAY_YAML"
$KUBECTL -n kuberay-system delete pod --all --force --grace-period=0 2>/dev/null || true
$KUBECTL -n kuberay-system rollout restart deploy/kuberay-operator
$KUBECTL -n kuberay-system rollout status deploy/kuberay-operator --timeout=300s

echo "=== apply quickstart ==="
$KUBECTL delete rayjob daft-quickstart -n daft-quickstart --ignore-not-found
$KUBECTL delete ns daft-quickstart --ignore-not-found --wait=false || true
sleep 3
$KUBECTL apply -f "$ROOT/00-configmap-script-baked.yaml"
$KUBECTL apply -f "$ROOT/10-rayjob-smoke.yaml"

echo "=== watch up to 20 min ==="
for i in $(seq 1 120); do
  JS=$($KUBECTL get rayjob daft-quickstart -n daft-quickstart -o jsonpath='{.status.jobStatus}' 2>/dev/null || true)
  DS=$($KUBECTL get rayjob daft-quickstart -n daft-quickstart -o jsonpath='{.status.jobDeploymentStatus}' 2>/dev/null || true)
  RS=$($KUBECTL get rayjob daft-quickstart -n daft-quickstart -o jsonpath='{.status.reason}' 2>/dev/null || true)
  echo "tick=$i jobStatus=${JS:-?} deploymentStatus=${DS:-?} reason=${RS:-?}"
  $KUBECTL get pods -n daft-quickstart 2>/dev/null || true
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

echo "=== logs ==="
SUB=$($KUBECTL get pods -n daft-quickstart -o name 2>/dev/null | grep submitter | head -1 || true)
[ -n "$SUB" ] && $KUBECTL logs -n daft-quickstart "$SUB" --tail=200 || true
HEAD=$($KUBECTL get pod -n daft-quickstart -l ray.io/node-type=head -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
[ -n "$HEAD" ] && $KUBECTL logs -n daft-quickstart "$HEAD" -c ray-head --tail=200 || true

JS=$($KUBECTL get rayjob daft-quickstart -n daft-quickstart -o jsonpath='{.status.jobStatus}' 2>/dev/null || true)
DS=$($KUBECTL get rayjob daft-quickstart -n daft-quickstart -o jsonpath='{.status.jobDeploymentStatus}' 2>/dev/null || true)
if [ "$JS" = "SUCCEEDED" ] && [ "$DS" = "Complete" ]; then
  echo "RESULT: PASS"
  exit 0
fi
echo "RESULT: FAIL jobStatus=$JS deploymentStatus=$DS"
exit 1
