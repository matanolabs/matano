#!/bin/bash
set -euo pipefail

run() {
    INSTALL_DIR="$HOME/.matano"
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

run
