#!/bin/bash
set -euo pipefail
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
K="sudo kubectl --kubeconfig=$KUBECONFIG"
ROOT="/tmp/daft-quickstart-test"

# pause 镜像（k3s sandbox 必需）
ensure_pause() {
  local pause="rancher/mirrored-pause:3.10.2"
  if ! sudo k3s ctr -n k8s.io images ls | grep -q "mirrored-pause:3.10.2"; then
    docker pull "$pause" || { docker pull registry.k8s.io/pause:3.10 && docker tag registry.k8s.io/pause:3.10 "$pause"; }
    docker save "$pause" | sudo k3s ctr -n k8s.io images import -
  fi
}
ensure_pause

echo "=== cleanup old run ==="
$K delete rayjob daft-quickstart -n daft-quickstart --ignore-not-found --wait=false
$K delete raycluster --all -n daft-quickstart --ignore-not-found --wait=false
$K delete job --all -n daft-quickstart --ignore-not-found
sleep 5

echo "=== re-apply ==="
$K apply -f "$ROOT/00-configmap-script-baked.yaml"
$K apply -f "$ROOT/10-rayjob-smoke.yaml"

echo "=== watch 20 min ==="
for i in $(seq 1 120); do
  JS=$($K get rayjob daft-quickstart -n daft-quickstart -o jsonpath='{.status.jobStatus}' 2>/dev/null || true)
  DS=$($K get rayjob daft-quickstart -n daft-quickstart -o jsonpath='{.status.jobDeploymentStatus}' 2>/dev/null || true)
  RS=$($K get rayjob daft-quickstart -n daft-quickstart -o jsonpath='{.status.reason}' 2>/dev/null || true)
  echo "tick=$i jobStatus=${JS:-?} deploymentStatus=${DS:-?} reason=${RS:-?}"
  $K get pods -n daft-quickstart 2>/dev/null | tail -5
  if [ "$JS" = "SUCCEEDED" ] && [ "$DS" = "Complete" ]; then echo "SUCCESS"; exit 0; fi
  if [ "$DS" = "Failed" ]; then echo "FAILED reason=$RS"; break; fi
  sleep 10
done

echo "=== head logs ==="
HEAD=$($K get pod -n daft-quickstart -l ray.io/node-type=head -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
[ -n "$HEAD" ] && $K logs -n daft-quickstart "$HEAD" -c ray-head --tail=80 2>&1 || true
SUB=$($K get pods -n daft-quickstart -o jsonpath='{.items[?(@.metadata.labels.job-name)].metadata.name}' 2>/dev/null | awk '{print $1}' || true)
[ -n "$SUB" ] && $K logs -n daft-quickstart "$SUB" --tail=120 2>&1 || true
exit 1
