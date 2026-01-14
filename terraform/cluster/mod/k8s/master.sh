#!/bin/bash
set -exo pipefail

sudo kubeadm init --config /tmp/kubeadm_config.yaml \
  --ignore-preflight-errors=Swap,NumCPU,SystemVerification

[ -d "$HOME"/.kube ] || mkdir -p "$HOME"/.kube
sudo cp /etc/kubernetes/admin.conf $HOME/.kube/config
sudo chown $(id -u):$(id -g) $HOME/.kube/config

# When this doesn't complete, you may debug the created containers, example:
# sudo crictl --runtime-endpoint unix:///var/run/containerd/containerd.sock ps -a
# CONTAINER           IMAGE               CREATED             STATE               NAME                      ATTEMPT             POD ID              POD
# 186b1d4e98b4e       07d562355feda       19 seconds ago      Exited              kube-apiserver            8                   c2caa089e0d8d       kube-apiserver-ksmaster-1
# 721c771742872       097a9f9514c71       25 seconds ago      Exited              kube-controller-manager   8                   2873b68d1a3d8       kube-controller-manager-ksmaster-1
# ff1706cee6f41       c1f0d1cc8af40       16 minutes ago      Running             kube-scheduler            0                   47bf977e98d1f       kube-scheduler-ksmaster-1
# ff01d32504e7f       3861cfcd7c04c       16 minutes ago      Running             etcd                      0                   85a15c8f5d6e6       etcd-ksmaster-1
# sudo crictl --runtime-endpoint unix:///var/run/containerd/containerd.sock logs 721c771742872
# ..
# Error: invalid argument "LegacyServiceAccountTokenNoAutoGeneration=false" for "--feature-gates" flag: unrecognized feature gate: LegacyServiceAccountTokenNoAutoGeneration
while ! nc -z localhost 6443; do
  echo "...Waiting on k8s API server to give a sign of life"
  sleep 5
done

kubectl apply -f ${cni_url}
wget https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.8.0/components.yaml
sed 's/- --secure-port=10250/- --secure-port=10250\n        - --kubelet-insecure-tls/' components.yaml | kubectl apply -f -
