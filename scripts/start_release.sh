#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
app_root="${repo_root}/Memento"

cd "$app_root"

BIN="memento"

if [[ -f "./target/release/${BIN}" ]] && ! file "./target/release/${BIN}" | grep -q "ELF"; then
    echo "[imaginos] ${BIN}: binary is not ELF (corrupt or wrong arch) — removing it"
    rm -f "./target/release/${BIN}"
fi

if [[ -f "./target/release/${BIN}" ]]; then
    set +e
    timeout 5 "./target/release/${BIN}" --help >/dev/null 2>&1
    probe_status=$?
    set -e
    # Only remove on exec-failure codes (126=permission/linker, 127=not-found).
    # Exit 1 = runtime error (socket in use, DB not ready) — keep the binary.
    # Exit 124 = killed by timeout — normal for a daemon with no --help flag.
    if [[ "${probe_status}" == "126" || "${probe_status}" == "127" ]]; then
        echo "[imaginos] ${BIN}: binary cannot run on this host (exit ${probe_status}) — removing it"
        rm -f "./target/release/${BIN}"
    fi
fi

if [[ ! -x "./target/release/${BIN}" ]]; then
    # Check ~/bin/ — used on nodes where target/ is excluded from Syncthing
    if [[ -x "${HOME}/bin/${BIN}" ]]; then
        echo "[imaginos] ${BIN}: using pre-deployed binary at ${HOME}/bin/${BIN}"
        exec "${HOME}/bin/${BIN}"
    elif [[ "${IMAGINOS_ALLOW_RELEASE_BUILD:-0}" == "1" ]]; then
        echo "[imaginos] ${BIN}: release binary missing; building because IMAGINOS_ALLOW_RELEASE_BUILD=1"
        cargo build --release --bin "${BIN}"
    else
        echo "[imaginos] ${BIN}: release binary missing; refusing compile-on-boot"
        echo "[imaginos] build ${BIN} ahead of time or set IMAGINOS_ALLOW_RELEASE_BUILD=1 for an emergency rebuild"
        exit 1
    fi
fi

# Clean up a stale socket left by the probe or a previous crash before binding.
MEMENTO_SOCKET="${MEMENTO_SOCKET:-/tmp/memento.sock}"
if [[ -S "${MEMENTO_SOCKET}" ]]; then
    rm -f "${MEMENTO_SOCKET}"
fi

exec "./target/release/${BIN}"
