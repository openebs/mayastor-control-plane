#!/bin/bash
set -exo pipefail

sudo kubeadm init --config /tmp/kubeadm_config.yaml \
  --ignore-preflight-errors=Swap,NumCPU,SystemVerification

[ -d "$HOME"/.kube ] || mkdir -p "$HOME"/.kube
sudo cp /etc/kubernetes/admin.conf $HOME/.kube/config
sudo chown $(id -u):$(id -g) $HOME/.kube/config

while ! nc -z localhost 6443; do
  echo "...Waiting on k8s API server to give a sign of life"
  sleep 5
done

kubectl apply -f ${cni_url}
wget https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.6.3/components.yaml
sed 's/- --secure-port=4443/- --secure-port=4443\n        - --kubelet-insecure-tls/' components.yaml | kubectl apply -f -
