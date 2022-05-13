#!/bin/bash

export TOOLCHAIN_ROOT="/opt/rust/rustup/toolchains/stable-x86_64-unknown-linux-gnu"
export GCC_ROOT="$TOOLCHAIN_ROOT/lib/rustlib/x86_64-unknown-linux-musl/lib/self-contained"

args=()
for arg in "$@"; do
    if [[ $arg = *"Bdynamic"* ]]; then
        args+=() # we do not want this arg
    elif [[ $arg = *"crti.o"* ]]; then
        args+=("$arg" "$GCC_ROOT/crtbeginS.o" "-Bstatic")
    elif [[ $arg = *"crtn.o"* ]]; then
        args+=("-lgcc" "-lgcc_eh" "-lc" "$GCC_ROOT/crtend.o" "$arg")
    else
        args+=("$arg")
    fi
done

echo "RUNNING WITH ARGS: ${args[@]}"
musl-gcc "${args[@]}"
