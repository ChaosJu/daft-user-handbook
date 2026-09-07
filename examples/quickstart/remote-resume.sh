#!/bin/bash
set -euo pipefail
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
KUBECTL="sudo kubectl --kubeconfig=$KUBECONFIG"
ROOT="/tmp/daft-quickstart-test"
KUBERAY_YAML="/home/chaos/offline-build/kuberay-operator-v1.6.2.yaml"

$KUBECTL create namespace kuberay-system --dry-run=client -o yaml | $KUBECTL apply -f -
$KUBECTL apply --server-side -f "$KUBERAY_YAML"
$KUBECTL -n kuberay-system rollout status deploy/kuberay-operator --timeout=180s
$KUBECTL get crd | grep ray.io

$KUBECTL delete rayjob daft-quickstart -n daft-quickstart --ignore-not-found
$KUBECTL delete ns daft-quickstart --ignore-not-found --wait=false || true
sleep 2
$KUBECTL apply -f "$ROOT/00-configmap-script.yaml"
$KUBECTL apply -f "$ROOT/10-rayjob-smoke.yaml"

echo "=== initial status ==="
$KUBECTL get rayjob -n daft-quickstart -w &
WATCH_PID=$!
sleep 120
kill $WATCH_PID 2>/dev/null || true

$KUBECTL get rayjob daft-quickstart -n daft-quickstart -o wide
$KUBECTL get pods -n daft-quickstart -o wide
$KUBECTL describe rayjob daft-quickstart -n daft-quickstart | tail -40
