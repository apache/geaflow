#!/usr/bin/env bash
set -euo pipefail

# One-click smoke test for CASTS decision service.
#
# What it does:
# 1) Ensures `.venv` exists and runs `uv sync` into it (no activation required).
# 2) Starts `uvicorn` on HOST:PORT (unless a healthy service is already running there).
# 3) Runs HTTP checks for `/health` and `POST /casts/decision`.
# 4) Shuts down the service if this script started it.

HOST="${GEAFLOW_AI_CASTS_HOST:-127.0.0.1}"
PORT="${GEAFLOW_AI_CASTS_PORT:-5001}"
TIMEOUT_SECONDS="${GEAFLOW_AI_CASTS_TIMEOUT_SECONDS:-20}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLUGIN_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_DIR="${PLUGIN_DIR}/logs"
LOG_FILE="${LOG_DIR}/smoke_casts.log"

mkdir -p "${LOG_DIR}"
cd "${PLUGIN_DIR}"

if [[ ! -d .venv ]]; then
  python3.11 -m venv .venv
fi

VENV_PY="${PLUGIN_DIR}/.venv/bin/python"
if [[ ! -x "${VENV_PY}" ]]; then
  echo "[smoke] ERROR: venv python not found at ${VENV_PY}"
  exit 1
fi

cleanup() {
  if [[ "${STARTED_HERE:-false}" == "true" && -n "${SERVER_PID:-}" ]]; then
    kill "${SERVER_PID}" >/dev/null 2>&1 || true
    for _ in {1..50}; do
      if kill -0 "${SERVER_PID}" >/dev/null 2>&1; then
        sleep 0.1
      else
        break
      fi
    done
  fi
}
trap cleanup EXIT

echo "[smoke] Sync deps (casts venv) ..."
uv sync --extra dev

BASE_URL="http://${HOST}:${PORT}"
STARTED_HERE="false"
SERVER_PID=""

health_ok() {
  local body
  body="$(curl -fsS "${BASE_URL}/health" 2>/dev/null || true)"
  if [[ -z "${body}" ]]; then
    return 1
  fi
  "${VENV_PY}" -c 'import json,sys; obj=json.loads(sys.argv[1]); assert obj.get("status") == "UP"' \
    "${body}" >/dev/null 2>&1
}

if health_ok; then
  echo "[smoke] CASTS already running at ${BASE_URL}"
else
  echo "[smoke] Starting CASTS at ${BASE_URL} ..."
  "${VENV_PY}" -m uvicorn api.app:app --host "${HOST}" --port "${PORT}" --log-level info >"${LOG_FILE}" 2>&1 &
  SERVER_PID="$!"
  disown "${SERVER_PID}" >/dev/null 2>&1 || true
  STARTED_HERE="true"

  deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))
  while true; do
    if health_ok; then
      break
    fi
    if [[ $(date +%s) -ge ${deadline} ]]; then
      echo "[smoke] ERROR: CASTS did not become healthy within ${TIMEOUT_SECONDS}s"
      echo "[smoke] Log: ${LOG_FILE}"
      tail -n 200 "${LOG_FILE}" || true
      exit 1
    fi
    sleep 0.2
  done
fi

echo "[smoke] Health OK"

post_json() {
  local path="$1"
  local body="$2"
  local expected_code="$3"

  local tmp
  tmp="$(mktemp)"
  local code
  code="$(curl -sS -o "${tmp}" -w "%{http_code}" -X POST "${BASE_URL}${path}" \
    -H "Content-Type: application/json" \
    -d "${body}" || true)"
  local resp
  resp="$(cat "${tmp}")"
  rm -f "${tmp}"

  if [[ "${code}" != "${expected_code}" ]]; then
    echo "[smoke] ERROR: POST ${path} expected HTTP ${expected_code}, got ${code}"
    echo "[smoke] Response:"
    echo "${resp}"
    exit 1
  fi

  printf "%s" "${resp}"
}

echo "[smoke] Test: empty scope rejected (expect 422)"
resp="$(post_json "/casts/decision" '{
  "api_version":"v1",
  "scope":{},
  "trace":{},
  "payload":{
    "goal":"find friends",
    "traversal":{"structural_signature":"V()","step_index":0},
    "node":{"label":"Person","properties":{"type":"Person","name":"Alice"}},
    "graph_schema":{
      "schema_fingerprint":"fp_smoke",
      "valid_outgoing_labels":["friend"],
      "valid_incoming_labels":[]
    }
  }
}' "422")"
printf "%s" "${resp}" | "${VENV_PY}" -c 'import json,sys; obj=json.load(sys.stdin); assert obj.get("ok") is False'
echo "[smoke]   OK"

echo "[smoke] Test: /casts/decision"
resp="$(post_json "/casts/decision" '{
  "api_version":"v1",
  "scope":{"run_id":"run_smoke"},
  "trace":{},
  "payload":{
    "goal":"find friends",
    "traversal":{"structural_signature":"V()","step_index":0},
    "node":{"label":"Person","properties":{"type":"Person","name":"Alice"}},
    "graph_schema":{
      "schema_fingerprint":"fp_smoke",
      "valid_outgoing_labels":["friend"],
      "valid_incoming_labels":[]
    }
  }
}' "200")"
printf "%s" "${resp}" | "${VENV_PY}" -c \
  'import json,sys; obj=json.load(sys.stdin); assert obj.get("ok") is True; p=obj.get("payload") or {}; d=p.get("decision") or ""; assert isinstance(d,str) and d.strip()'
echo "[smoke]   OK"

echo
echo "[smoke] SMOKE OK: casts service"
echo "[smoke] Base URL: ${BASE_URL}"
if [[ "${STARTED_HERE}" == "true" ]]; then
  echo "[smoke] Log: ${LOG_FILE}"
fi

