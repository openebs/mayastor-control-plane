#!/bin/bash
set -x

DISTRO=$(cat /etc/os-release | awk '/^NAME="/ {print $1}' | awk -F\" '{print $2}')
if [ ! "$DISTRO" == "Ubuntu" ]; then
  echo "Script only tested on Ubuntu"
  exit 1
fi

sudo apt-get update && sudo apt-get install -y apt-transport-https curl \
  ca-certificates software-properties-common


curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -
cat <<EOF | sudo tee /etc/apt/sources.list.d/kubernetes.list
deb https://apt.kubernetes.io/ kubernetes-xenial main
EOF


curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
#sudo mkdir -p /etc/apt/keyrings
#curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo add-apt-repository --yes "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"

#echo \
#  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
#  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

if [ -n "${kube_version}" ]; then
  KUBE_VERSION="=${kube_version}"
fi

sudo apt-get update
sudo apt-get install -y -o Options::=--force-confdef \
  -o Dpkg::Options::=--force-confnew kubelet$KUBE_VERSION kubeadm$KUBE_VERSION kubectl$KUBE_VERSION docker-ce \
  containerd.io docker-ce-cli

sudo apt-mark hold kubelet kubeadm kubectl

sudo tee /etc/docker/daemon.json >/dev/null <<EOF
{
  "exec-opts": ["native.cgroupdriver=systemd"],
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "100m"
  },
  "storage-driver":  "overlay2",
  "insecure-registries" : ["kvmhost:5000"]
}
EOF

KVM_HOST_IP=$(ip -f inet addr show ens3 | awk '/inet / {print $2}' | awk -F. '{print $1"."$2"."$3".1"}')
echo "$KVM_HOST_IP kvmhost" | sudo tee -a /etc/hosts

sudo mkdir -p /etc/systemd/system/docker.service.d

sudo systemctl daemon-reload
sudo systemctl restart docker

sudo tee /etc/modules-load.d/containerd.conf >/dev/null <<EOF
overlay
br_netfilter
EOF

sudo modprobe overlay
sudo modprobe br_netfilter

# Setup required sysctl params, these persist across reboots.
sudo sysctl --system
sudo mkdir -p /etc/containerd
sudo containerd config default | sudo tee /etc/containerd/config.toml

sudo sed -i 's/\[plugins."io.containerd.grpc.v1.cri".registry.mirrors\]/\[plugins."io.containerd.grpc.v1.cri".registry.mirrors\]\n        \[plugins."io.containerd.grpc.v1.cri".registry.mirrors."kvmhost:5000"\]\n          endpoint = \["http:\/\/kvmhost:5000"\]/g' /etc/containerd/config.toml
sudo sed -i 's/\[plugins."io.containerd.grpc.v1.cri".registry.configs\]/\[plugins."io.containerd.grpc.v1.cri".registry.configs\]\n        \[plugins."io.containerd.grpc.v1.cri".registry.configs."kvmhost:5000".tls\]\n          insecure_skip_verify = true/g' /etc/containerd/config.toml

# Rebooting a node does not kill the containers they are running resulting
# in a vary long timeout before the system moves forward.
#
# There are various bugs upstream detailing this and it supposed to be fixed a few weeks ago (June)
# This is *not* that fix rather a work around. The containers will not shut down "cleanly".
sudo mkdir -p /etc/systemd/system/containerd.service.d
sudo tee /etc/sysctl.d/system/containerd.service.d/override.conf >/dev/null <<EOF
[Service]
KillMode=mixed
EOF


# Restart containerd
sudo systemctl restart containerd
