#!/bin/bash
set -Eeuo pipefail

run() {
    while [[ "$#" -gt 0 ]]; do case $1 in
        -i|--install-dir) USER_INSTALL_DIR="$2"; shift;shift;;
        *) usage "Unknown parameter passed: $1"; shift;shift;;
    esac; done

    INSTALL_DIR=${USER_INSTALL_DIR:-"$HOME/.matano"}
    INSTALL_CODE_DIR="$INSTALL_DIR/matano"

    echo "🟡 Installing matano..."

    [ ! -d "$INSTALL_CODE_DIR" ] && git clone --quiet https://github.com/matanolabs/matano.git "$INSTALL_CODE_DIR"
    {
        pushd "$INSTALL_CODE_DIR/cli" && npm run full-install && popd
    } > /dev/null 2>&1

    # printf "eval $(matano autocomplete:script zsh)" >> ~/.zshrc; source ~/.zshrc

    printf '\033[1A\033[K' # Clear line
    echo "✅ Installed Matano. Run matano to get started."
}

run "$@"
