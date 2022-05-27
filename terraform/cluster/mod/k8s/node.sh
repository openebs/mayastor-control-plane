#!/bin/bash
set -ex

function addKernelModules() {
    for module in $@; do
        sudo modprobe $module
        echo $module | sudo tee -a /etc/modules-load.d/kvm.conf
    done
}

function addHugePages() {
    echo $1 | sudo tee /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
    echo "vm.nr_hugepages = $1" | sudo tee -a /etc/sysctl.d/10-kubeadm.conf
}

sudo grep -qa container=lxc /proc/1/environ || (
    addHugePages ${nr_hugepages}
    addKernelModules xfs

    DISTRO=$(cat /etc/os-release | awk '/^NAME="/ {print $1}' | awk -F\" '{print $2}')
    case $DISTRO in
        Ubuntu)
            echo "loading nvme kernel modules"
            sudo apt -y install `apt search linux-modules-extra | fgrep \`uname -r\` | sed -e "s/,.*//"`
            addKernelModules nvme-tcp
            ;;
        *)
            echo "nvme kernel modules not loaded!"
            ;;
    esac
)

until $(nc -z ${master_ip} 6443); do
  echo "Waiting for API server to respond"
  sleep 5
done

sudo kubeadm join --token=${token} ${master_ip}:6443 \
  --discovery-token-unsafe-skip-ca-verification \
  --ignore-preflight-errors=Swap,SystemVerification
