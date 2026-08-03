#!/bin/sh
# Build the downloadable SDK package before serving. Deterministic from the
# packaged bytes, so every task ends up with the same version. A failure here
# only costs SDK connectivity, so it must not stop the service from starting.
set -e

python manage.py preparesdk || echo "preparesdk failed; /sdk/version will report NOT_DEFINED"

exec "$@"
