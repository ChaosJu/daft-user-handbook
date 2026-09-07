#!/bin/bash
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
K="sudo kubectl --kubeconfig=$KUBECONFIG"
HEAD=$($K get pod -n daft-quickstart -l ray.io/node-type=head -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
echo "head=$HEAD"
$K logs -n daft-quickstart "$HEAD" -c ray-head --tail=200 2>&1 | grep -A5 -E 'collect|where|sort|PyDataFrame|SUCCESS|Job.*succeeded|uv run' || $K logs -n daft-quickstart "$HEAD" -c ray-head --tail=80
