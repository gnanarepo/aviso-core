#!/usr/bin/env bash
# What the three GBM APIs answer, in a form that can be committed.
#
#   scripts/api_fingerprint.sh <base-url> <tenant>
#
# Prints one record per endpoint: status, byte size, how many entries, the
# field names present, and a sha256 of the canonicalised body. No values --
# these are live customer records and the point is to compare runs, not to
# publish deals. A byte-identical body gives an identical hash, so a
# before/after pair proves parity without either side leaving the machine.
#
# Auth comes from the environment so it stays out of argv and out of git:
#   INTERNAL_API_KEY=...   header auth, the path that exists today
#   SESSION_COOKIE=...     session auth, the path this branch adds
#
# Run it against the stack before deploying and again after; diff the output.
set -uo pipefail

BASE="${1:?usage: api_fingerprint.sh <base-url> <tenant>}"
TENANT="${2:?usage: api_fingerprint.sh <base-url> <tenant>}"
DEAL="${DEAL_ID:-006RP00000TBErl}"
PERIOD="${PERIOD:-2026Q3}"
TIMEOUT="${TIMEOUT:-25}"

auth=(-H "X-Tenant-Name: $TENANT")
if [ -n "${INTERNAL_API_KEY:-}" ]; then
    auth+=(-H "Internal-Api-Key: $INTERNAL_API_KEY")
    auth_mode=internal-api-key
elif [ -n "${SESSION_COOKIE:-}" ]; then
    auth+=(-H "Cookie: $SESSION_COOKIE")
    auth_mode=session
else
    echo "set INTERNAL_API_KEY or SESSION_COOKIE" >&2
    exit 1
fi

# Structure only: how many entries, which fields, and the hash of the body.
shape() {
    local file="$1"
    if [ ! -s "$file" ]; then
        echo '{"parsed": false, "empty": true}'
        return
    fi
    if ! jq -e . "$file" >/dev/null 2>&1; then
        jq -n --arg sha "$(sha256sum <"$file" | cut -d' ' -f1)" \
              '{parsed: false, sha256: $sha}'
        return
    fi
    jq -S -c . "$file" | tr -d '\n' > "$file.canon"
    jq -n --argjson entries "$(jq 'if type == "array" then length
                                  elif type == "object" then (keys | length)
                                  else 1 end' "$file")" \
          --argjson fields "$(jq -c 'if type == "array"
                                     then (map(select(type == "object") | keys) | add // [] | unique)
                                     elif type == "object" then keys
                                     else [] end' "$file")" \
          --arg sha "$(sha256sum <"$file.canon" | cut -d' ' -f1)" \
          '{parsed: true, entries: $entries, fields: $fields, sha256: $sha}'
    rm -f "$file.canon"
}

fingerprint() {
    local label="$1" method="$2" path="$3" body="${4:-}"
    local tmp meta status size args
    tmp=$(mktemp)

    args=(-s --max-time "$TIMEOUT" -o "$tmp" -w '%{http_code} %{size_download}')
    [ "$method" = POST ] && args+=(-X POST -H 'Content-Type: application/json' -d "$body")

    # curl still writes -w on a timeout, but exits non-zero; 000 means no reply.
    meta=$(curl "${args[@]}" "${auth[@]}" "$BASE$path")
    status=${meta%% *}
    size=${meta##* }

    jq -n --arg label "$label" --arg method "$method" --arg path "$path" \
          --argjson status "${status:-0}" --argjson bytes "${size:-0}" \
          --argjson shape "$(shape "$tmp")" \
          '{label: $label, method: $method, path: $path,
            status: $status, bytes: $bytes,
            timed_out: ($status == 0)} + $shape'
    rm -f "$tmp"
}

{
    fingerprint basic_results GET "/gbm/basic_results/?id_list=$DEAL"
    fingerprint deals_results GET "/gbm/deals_results/?period=$PERIOD"
    fingerprint drilldown_fields GET \
        "/gbm/v2/drilldown_fields/?period=$PERIOD&owner_mode=false"
    fingerprint drilldown_fields_post POST \
        "/gbm/v2/drilldown_fields/?period=$PERIOD&owner_mode=false" \
        '{"fields_list":["as_of_Account"]}'
} | jq -s --arg base "$BASE" --arg tenant "$TENANT" --arg period "$PERIOD" \
          --arg auth "$auth_mode" --arg deal "$DEAL" \
          '{base: $base, tenant: $tenant, period: $period, deal: $deal,
            auth: $auth, endpoints: .}'
