#!/bin/bash
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
K="sudo kubectl --kubeconfig=$KUBECONFIG"
$K get rayjob -n daft-quickstart -o wide
echo "=== completed submitter logs ==="
P=$($K get pods -n daft-quickstart --field-selector=status.phase=Succeeded -o jsonpath='{.items[-1].metadata.name}' 2>/dev/null || true)
[ -n "$P" ] && $K logs -n daft-quickstart "$P" --tail=80
