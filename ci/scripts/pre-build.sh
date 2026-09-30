#!/bin/bash
# Copyright (c) 2026 ADBC Drivers Contributors
# Licensed under the Apache License, Version 2.0.

set -euo pipefail

: "${PROTOC_TAG:=v33.5}"

main() {
    local -r platform="${2}"
    local -r arch="${3}"
    local -r destination="$(pwd)/protoc"
    local archive

    case "${platform}/${arch}" in
        linux/amd64) archive="protoc-33.5-linux-x86_64.zip" ;;
        linux/arm64) archive="protoc-33.5-linux-aarch_64.zip" ;;
        macos/arm64) archive="protoc-33.5-osx-aarch_64.zip" ;;
        windows/amd64) archive="protoc-33.5-win64.zip" ;;
        *) echo "Unsupported platform: ${platform}/${arch}" >&2; exit 1 ;;
    esac

    gh release download \
        --repo protocolbuffers/protobuf \
        --pattern "${archive}" \
        --output protoc.zip \
        "${PROTOC_TAG}"
    unzip -q protoc.zip -d "${destination}"
    echo "export PROTOC=${destination}/bin/protoc" > .env.build
}

main "$@"
