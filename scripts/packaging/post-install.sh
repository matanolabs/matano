#!/bin/bash
set -eu

while [[ $# -gt 0 ]]; do case $1 in
	-b | --bin-dir)
		PARSED_BIN_DIR="$2"
		shift
		shift
		;;
	*)
		usage "Unknown parameter passed: $1"
		shift
		shift
		;;
	esac done

BIN_DIR=${PARSED_BIN_DIR:-/usr/local/bin}

chown -R "$USER" "$PWD"
chmod -R 775 "$PWD"
ln -sf "$PWD"/matano "$BIN_DIR"/matano
chown -h "$USER" "$BIN_DIR"/matano
# -h only on mac
{
	chmod -h 775 "$BIN_DIR"/matano || true
} >/dev/null 2>&1

printf "Installed Matano. Run matano to get started.\n\n"
./matano
