#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0.
#
# One-command reproduction of the GraphRAG pipeline:
#   verify toolchain -> import + publish READY -> start service -> wait ready
#   -> six-method benchmark -> collect artifact paths
#
# Every step writes into one run directory so a result can always be traced back
# to the exact manifest, bundle, and benchmark output that produced it.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DATASET="2wikimultihopqa"
LIMIT=""
SKIP_BUILD="0"
SKIP_SERVICE="0"
PORT="${GEAFLOW_SERVER_PORT:-18080}"
RUNS_ROOT="${GEAFLOW_RUNS_ROOT:-${ROOT}/tmp/graphrag/data/runs}"

usage() {
    cat <<'EOF'
Usage: tools/graphrag/reproduce.sh [options]

Runs import -> publish READY -> service readiness -> six-method benchmark.

Options:
  --dataset <name>   2wikimultihopqa (default) or hotpotqa
  --limit <n>        restrict the benchmark to the first n queries (smoke runs)
  --skip-build       reuse an existing bundle instead of re-publishing
  --skip-service     run only the artifact and benchmark steps
  --port <port>      service port for the local run (default 18080)
  -h, --help         show this message

Environment:
  GEAFLOW_VECTORS     precomputed query vectors JSONL (required for vector methods)
  GEAFLOW_BUNDLE      existing READY bundle to reuse with --skip-build
  GEAFLOW_QUERIES     query JSONL for the dataset
  GEAFLOW_GOLD        gold JSONL/zip; enables Recall/MRR/NDCG/RR scoring
  GEAFLOW_RUNS_ROOT   output root (default tmp/graphrag/data/runs)
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dataset) DATASET="$2"; shift 2 ;;
        --limit) LIMIT="$2"; shift 2 ;;
        --skip-build) SKIP_BUILD="1"; shift ;;
        --skip-service) SKIP_SERVICE="1"; shift ;;
        --port) PORT="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

case "${DATASET}" in
    2wikimultihopqa) MANIFEST="${ROOT}/tools/graphrag/manifests/2wikimultihopqa-dev-v2.json" ;;
    hotpotqa) MANIFEST="${ROOT}/tools/graphrag/manifests/hotpotqa-distractor-v2.json" ;;
    *) echo "unsupported dataset: ${DATASET}" >&2; exit 2 ;;
esac

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_DIR="${RUNS_ROOT}/reproduce-${DATASET}-${STAMP}"
BUNDLE="${GEAFLOW_BUNDLE:-${RUN_DIR}/bundle}"
mkdir -p "${RUN_DIR}"
MANIFEST_OUT="${RUN_DIR}/reproduce-manifest.json"

log() { printf '[reproduce] %s\n' "$*" >&2; }
fail() { printf '[reproduce] ERROR: %s\n' "$*" >&2; exit 1; }

require() {
    command -v "$1" >/dev/null 2>&1 || fail "required tool is missing: $1"
}

log "run directory: ${RUN_DIR}"

# --- 1. toolchain -------------------------------------------------------------
require java
require mvn
require python3
[[ -f "${MANIFEST}" ]] || fail "manifest not found: ${MANIFEST}"

# --- 2. import and publish READY -------------------------------------------
if [[ "${SKIP_BUILD}" == "1" ]]; then
    [[ -d "${BUNDLE}" && -f "${BUNDLE}/READY.json" ]] \
        || fail "--skip-build requires GEAFLOW_BUNDLE pointing at a READY bundle (got '${BUNDLE}')"
    log "reusing READY bundle: ${BUNDLE}"
else
    VECTORS="${GEAFLOW_VECTORS:-}"
    if [[ -z "${VECTORS}" ]]; then
        fail "GEAFLOW_VECTORS must point at precomputed query vectors; the vector channel and
       Vector Overfetch baseline cannot be reproduced without them"
    fi
    [[ -f "${VECTORS}" ]] || fail "vector file not found: ${VECTORS}"
    log "publishing READY bundle from ${MANIFEST}"
    PYTHONPATH="${ROOT}" python3 "${ROOT}/tools/graphrag/publish_bundle.py" \
        --manifest "${MANIFEST}" \
        --vectors "${VECTORS}" \
        --output "${BUNDLE}"
    [[ -f "${BUNDLE}/READY.json" ]] || fail "publish did not produce READY.json"
fi

# --- 3. service readiness -----------------------------------------------------
if [[ "${SKIP_SERVICE}" == "1" ]]; then
    log "skipping service start (--skip-service)"
else
    log "building service classpath"
    CLASSPATH_FILE="${RUN_DIR}/classpath.txt"
    mvn -o -q -pl geaflow-ai -am -DskipTests -Dmaven.javadoc.skip=true \
        -Djacoco.skip=true -Drat.skip=true \
        dependency:build-classpath -Dmdep.outputFile="${CLASSPATH_FILE}" >/dev/null
    SERVICE_CLASSPATH="${ROOT}/geaflow-ai/target/classes:$(cat "${CLASSPATH_FILE}")"
    SERVICE_LOG="${RUN_DIR}/service.log"
    GEAFLOW_RETRIEVAL_ARTIFACT_BUNDLE_ROOT="${BUNDLE}" \
    GEAFLOW_RETRIEVAL_ARTIFACT_GRAPH_NAME="$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['graph_name'])" "${BUNDLE}/READY.json")" \
    GEAFLOW_RETRIEVAL_ARTIFACT_GRAPH_VERSION="$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['version'])" "${BUNDLE}/READY.json")" \
    GEAFLOW_RETRIEVAL_ARTIFACT_INDEX_VERSION="$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['version'])" "${BUNDLE}/READY.json")" \
    GEAFLOW_RETRIEVAL_ARTIFACT_VECTOR_SOURCE="$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['vector_source'])" "${BUNDLE}/READY.json")" \
    GEAFLOW_RETRIEVAL_ARTIFACT_VECTOR_VERSION="$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['vector_version'])" "${BUNDLE}/READY.json")" \
    GEAFLOW_SERVER_PORT="${PORT}" java -cp "${SERVICE_CLASSPATH}" \
        org.apache.geaflow.ai.GeaFlowMemoryServer >"${SERVICE_LOG}" 2>&1 &
    SERVICE_PID=$!
    trap 'kill ${SERVICE_PID} 2>/dev/null || true' EXIT

    log "waiting for /ready on port ${PORT}"
    ready="0"
    for _ in $(seq 1 60); do
        if curl -fsS "http://127.0.0.1:${PORT}/ready" >"${RUN_DIR}/ready.json" 2>/dev/null; then
            ready="1"
            break
        fi
        sleep 2
    done
    [[ "${ready}" == "1" ]] || fail "service never became ready; see ${SERVICE_LOG}"
fi

# --- 4. six-method benchmark --------------------------------------------------
QUERIES="${GEAFLOW_QUERIES:-${RUN_DIR}/queries.jsonl}"
if [[ ! -f "${QUERIES}" ]]; then
    fail "query set not found: ${QUERIES}
       Provide GEAFLOW_QUERIES pointing at a JSONL query file for this dataset"
fi
# A full run embeds every response body; gzip keeps the artifact on disk.
BENCH_OUT="${RUN_DIR}/${DATASET}-six-methods.jsonl.gz"
BENCH_ARGS=(
    --url "http://127.0.0.1:${PORT}/api/v1/retrievals" \
    --queries "${QUERIES}" \
    --output "${BENCH_OUT}" \
    --graph-name "$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['graph_name'])" "${BUNDLE}/READY.json")" \
    --graph-version "$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['version'])" "${BUNDLE}/READY.json")" \
    --index-version "$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['version'])" "${BUNDLE}/READY.json")" \
    --vectors "${GEAFLOW_VECTORS:-}" \
    --seed 0
)
[[ -n "${LIMIT}" ]] && BENCH_ARGS+=(--limit "${LIMIT}")

# Gold targets turn the run from a smoke test into a scored evaluation. They are optional so a
# cache-less environment can still exercise the wire protocol, and the CSV then leaves the
# evidence-quality columns empty instead of inventing numbers.
GOLD="${GEAFLOW_GOLD:-}"
if [[ -n "${GOLD}" && -e "${GOLD}" ]]; then
    BENCH_ARGS+=(--gold "${GOLD}")
    if [[ -f "${BUNDLE}/chunks.jsonl" ]]; then
        BENCH_ARGS+=(--chunks "${BUNDLE}/chunks.jsonl")
    fi
else
    log "no GEAFLOW_GOLD set; evidence-quality metrics will be omitted from this run"
fi
log "running six-method benchmark"
PYTHONPATH="${ROOT}" python3 "${ROOT}/tools/graphrag/benchmark_http.py" "${BENCH_ARGS[@]}"

# --- 5. run manifest ----------------------------------------------------------
python3 - "${MANIFEST_OUT}" "${RUN_DIR}" "${DATASET}" <<'PY'
import json, sys
from pathlib import Path
out, run_dir, dataset = sys.argv[1], Path(sys.argv[2]), sys.argv[3]
Path(out).write_text(json.dumps({
    'dataset': dataset,
    'runDirectory': str(run_dir),
    'artifacts': sorted(str(p.relative_to(run_dir)) for p in run_dir.rglob('*') if p.is_file()),
}, indent=2, sort_keys=True), encoding='utf-8')
PY

log "artifacts written under ${RUN_DIR}"
cat "${MANIFEST_OUT}"