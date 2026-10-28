#!/bin/sh
set -eu
# Files contain literal values, never shell code. Preserve embedded PEM newlines.
for secret in /run/secrets/*; do
    [ -f "$secret" ] || continue
    name=${secret##*/}
    case "$name" in
        ''|[0-9]*|*[!A-Za-z0-9_]*) echo 'Invalid secret filename' >&2; exit 1 ;;
    esac
    [ "$(wc -c < "$secret")" -le 65536 ] || { echo 'Secret exceeds 64 KiB' >&2; exit 1; }
    value=$(cat "$secret")
    export "$name=$value"
done
exec /usr/local/bin/coldctl --data-dir /var/lib/coldctl "$@"
