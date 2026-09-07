#!/bin/bash
set -euo pipefail
echo "=== sudo ==="
sudo -n true 2>/dev/null && echo "passwordless sudo OK" || echo "sudo needs password"
echo "=== disk ==="
df -h /
echo "=== existing k3s/kind dirs ==="
ls -la /var/lib/rancher/k3s 2>/dev/null || echo "no k3s"
ls -la ~/.kube 2>/dev/null || echo "no .kube"
