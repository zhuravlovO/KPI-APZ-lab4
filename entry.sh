#!/usr/bin/env sh

bin="$1"
shift

if [ -z "$bin" ]; then
  echo "Error: binary is not defined"
  exit 1
fi

if [ ! -x "./$bin" ]; then
  echo "Error: './$bin' not found or not executable"
  ls -l
  exit 1
fi

echo "Starting binary: $bin"
exec "./$bin" "$@"
