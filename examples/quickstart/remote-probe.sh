#!/bin/bash
echo "=== docker ps ==="
docker ps -a --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}' 2>&1 | head -30
echo "=== docker images (ray/kuberay/kind) ==="
docker images --format 'table {{.Repository}}:{{.Tag}}\t{{.Size}}' 2>&1 | grep -iE 'ray|kuberay|kind|k8s|kube' || true
echo "=== snap ==="
snap list 2>/dev/null | grep -iE 'microk8s|kubectl|helm' || echo "(none)"
echo "=== common bin paths ==="
ls -la /usr/local/bin/kubectl /usr/local/bin/kind /usr/local/bin/helm 2>&1 || true
ls -la ~/bin/kubectl ~/.local/bin/kubectl 2>&1 || true
echo "=== k3s ==="
systemctl is-active k3s 2>/dev/null || true
echo "=== memory/cpu ==="
free -h
nproc
