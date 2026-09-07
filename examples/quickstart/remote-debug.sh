#!/bin/bash
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
K="sudo kubectl --kubeconfig=$KUBECONFIG"
echo "=== rayjob ==="
$K get rayjob -n daft-quickstart -o wide
$K describe rayjob daft-quickstart -n daft-quickstart | tail -30
echo "=== pods ==="
$K get pods -n daft-quickstart -o wide
echo "=== submitter logs (all) ==="
for p in $($K get pods -n daft-quickstart -o name | grep -v head | grep -v worker); do
  echo "--- $p ---"
  $K logs -n daft-quickstart "$p" --tail=100 2>&1 || true
done
echo "=== head logs ==="
HEAD=$($K get pod -n daft-quickstart -l ray.io/node-type=head -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
if [ -n "$HEAD" ]; then
  $K describe pod -n daft-quickstart "$HEAD" | tail -40
  $K logs -n daft-quickstart "$HEAD" -c ray-head --tail=150 2>&1 || true
fi
echo "=== worker logs ==="
W=$($K get pod -n daft-quickstart -l ray.io/node-type=worker -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
if [ -n "$W" ]; then
  $K logs -n daft-quickstart "$W" -c ray-worker --tail=80 2>&1 || true
fi
echo "=== node describe (memory pressure?) ==="
$K describe node | tail -30
