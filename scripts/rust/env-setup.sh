#!/usr/bin/env bash

# Meant to be called only from the nix-shell shellHook

# these are provided by the shellHook
dev_rustup=${dev_rustup:-}
devrustup_moth=${devrustup_moth:-}
rustup_channel=${rustup_channel:-}

if [ -z "$CI" ]; then
  if ! diff -r --exclude Cargo.lock "$RUST_TOOLCHAIN" "$RUST_TOOLCHAIN_NIX" &>/dev/null; then
    rm -rf "$RUST_TOOLCHAIN"
    mkdir -p "$RUST_TOOLCHAIN" 2>/dev/null
    cp -r "$RUST_TOOLCHAIN_NIX"/* "$RUST_TOOLCHAIN"
    chmod -R +w "$RUST_TOOLCHAIN"
  fi
  if [ "$dev_rustup" == "1" ] && [ "$IN_NIX_SHELL" == "impure" ]; then
    cowsay "$devrustup_moth"
    unset dev_rustup
    unset USE_NIX_RUST
    RUSTUP_CUSTOM=
    path_remove () { export PATH=`echo -n $PATH | awk -v RS=: -v ORS=: '$0 != "'$1'"' | sed 's/:$//'`; }
    path_remove "$RUST_TOOLCHAIN_NIX/bin"

    # https://discourse.nixos.org/t/nix-shell-with-rustup/22452
    if [ -f /usr/bin/ldd ]; then
      NIX_LDD=$(ldd --version | head -n 1 | awk '{ print $NF }')
      USR_LDD=$(/usr/bin/ldd --version | head -n 1 | awk '{ print $NF }')
      if [ "$NIX_LDD" != "$USR_LDD" ]; then
        RUSTUP_CUSTOM="1"
      fi
    fi
    if [ -n "$RUSTUP_CUSTOM" ]; then
      cat <<EOF >rust-toolchain.toml
[toolchain]
path = "$RUST_TOOLCHAIN"
EOF
      # Use rust-toolchain.toml so the IDE can work correctly but use the
      # RUSTUP_TOOLCHAIN under nix so we can use rustup properly
      export RUSTUP_TOOLCHAIN="$rustup_channel"
      # Adding ~/.cargo/bin to the path let's us carry on using rustup but it lowers its
      # priority: https://github.com/rust-lang/cargo/pull/11023
      export PATH=$RUST_TOOLCHAIN/bin:$PATH:~/.cargo/bin
    else
      cat <<EOF >rust-toolchain.toml
[toolchain]
channel = "$rustup_channel"
components = [ "rust-src" ]
EOF
    fi
    if ! rustup toolchain list | grep "$rustup_channel" >/dev/null; then
      rustup toolchain install "$rustup_channel"
    fi
  elif [ -n "$USE_NIX_RUST" ]; then
    export PATH=$RUST_TOOLCHAIN/bin:$PATH
  fi
fi