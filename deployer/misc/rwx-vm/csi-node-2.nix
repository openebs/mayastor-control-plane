{ config, pkgs, ... }:
{
  boot.loader.systemd-boot.enable = true;
  boot.loader.efi.canTouchEfiVariables = true;
  boot.kernelModules = [ "nvme-tcp" ];
  boot.consoleLogLevel = 7;

  services.getty.autologinUser = "root";
  users.users.root.initialPassword = "a";

  # enable for ssh access
  # services.openssh.enable = true;
  services.openssh.settings.PermitRootLogin = "yes";
  services.openssh.settings.PasswordAuthentication = true;

  virtualisation.vmVariant = {
    virtualisation = {
      forwardPorts = [
        # enable for ssh access
        # { from = "host"; host.port = 2222; guest.port = 22; }

        # csi-node binds on vm on port 50055, and host ip can be used on $host:50056
        # 50055 cannot be used on the host, because the csi-node in docker, has its port mapped to host 50055
        { from = "host"; host.port = 50056; guest.port = 50055; }
        # csi-node proxy that converts http to unix socket
        { from = "host"; host.port = 50059; guest.port = 50059; }
        # agent-ha-node service (the cluster agent calls replace-path on this, via the host ip 10.1.0.1 on the docker network)
        { from = "host"; host.port = 50070; guest.port = 50070; }
      ];
      memorySize = 512;

      sharedDirectories = {
        workspace = {
          source = builtins.getEnv "WORKSPACE_ROOT";
          target = "/workspace";
        };
      };
    };

    systemd.services = {
      csi-node = {
        wantedBy = [ "multi-user.target" ];
        serviceConfig = {
          # User environment file for custom setup, example:
          # ARGS=--nvme-nr-io-queues 1 --node-name app-node-2 --csi-socket /var/tmp/csi-app-node-2.sock --grpc-endpoint [::]:50055
          # EnvironmentFile = "/workspace/tmp/run/csi-node.env";
          # ExecStart = "/workspace/target/debug/csi-node $ARGS";
          ExecStart = "/workspace/target/debug/csi-node --nvme-nr-io-queues 1 --node-name app-node-2 --csi-socket /var/tmp/csi-app-node-2.sock --grpc-endpoint [::]:50055 --grpc-auto-tls";
        };
      };
      agent-ha-node = {
        wantedBy = [ "multi-user.target" ];
        requires = [ "csi-node.service" ];
        after = [ "network-online.target" ];
        wants = [ "network-online.target" ];
        serviceConfig = {
          ExecStart = "/workspace/target/debug/agent-ha-node -napp-node-2 -g[::]:50070 --grpc-auto-tls --csi-socket /var/tmp/csi-app-node-2.sock --cluster-agent https://10.1.0.1:11500 --fake-grpc-endpoint 10.1.0.1:50070";
        };
      };
      # this is required to convert TCP into unix-socket which is what the csi node plugin listen on
      csi-node-proxy = {
        wantedBy = [ "multi-user.target" ];
        after = [ "csi-node.service" ];
        serviceConfig = {
          ExecStart = "${pkgs.socat}/bin/socat TCP-LISTEN:50059,fork UNIX-CONNECT:/var/tmp/csi-app-node-2.sock";
          Restart = "always";
        };
      };
    };
  };

  networking.firewall.enable = false;

  environment.systemPackages = with pkgs; [
    nvme-cli
    iptables
  ];

  system.stateVersion = config.system.nixos.release;
}
