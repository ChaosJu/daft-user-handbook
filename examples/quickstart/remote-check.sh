#!/bin/bash
set -euo pipefail
echo "=== host ==="
hostname
uname -a
echo "=== tools ==="
for t in kubectl helm kind docker microk8s k3s; do
  if command -v "$t" >/dev/null 2>&1; then
    echo "FOUND $t -> $(command -v "$t")"
  fi
done
echo "=== kubeconfig ==="
if [ -f "$HOME/.kube/config" ]; then
  echo "kubeconfig exists"
  kubectl config current-context 2>&1 || true
  kubectl get nodes 2>&1 || true
  kubectl get crd 2>&1 | grep ray.io || true
  kubectl get pods -A 2>&1 | grep -E 'kuberay|NAMESPACE' || true
else
  echo "no ~/.kube/config"
  ls -la /etc/rancher/k3s/k3s.yaml 2>/dev/null || true
  snap list 2>/dev/null | grep -E 'microk8s|kubectl' || true
fi
