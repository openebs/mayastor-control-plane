#!/usr/bin/env bash

# Cleans up the iptables rules added by bdd tests

set -euo pipefail

echo "Cleaning all IPTABLE rules added by tests..."

# First backup the iptables rules
sudo bash -c "iptables-save > iptables.backup"
# Remove the rules filtering by comment "added by bdd tests"
sed -i '/.*--comment.* "added by bdd tests"/d' iptables.backup
# Restore the rules
sudo iptables-restore < iptables.backup
# Remove the temporary backup file
sudo rm iptables.backup
