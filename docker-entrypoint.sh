#!/bin/sh
# Build the downloadable SDK package before serving. Deterministic from the
# packaged bytes, so every task ends up with the same version.
#
# A failure here is fatal on purpose: /gbm/health would still answer 200, so
# the deploy would go green while every SDK client fails to connect.
set -e

python manage.py preparesdk

exec "$@"
