let
  sources = import ../nix/sources.nix;
  pkgs = import sources.nixpkgs {
    overlays = [ (_: _: { inherit sources; }) (import ../nix/overlay.nix { }) ];
  };
in
with pkgs;
mkShell {
  name = "control-plane-tf-shell";
  buildInputs = [
    git
    jq
    utillinux
    which
    (terraform.withPlugins (p: [ p.libvirt p.null p.template p.lxd p.kubernetes p.helm p.local ]))
    tflint
  ];
}
