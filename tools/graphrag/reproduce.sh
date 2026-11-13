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
# Retrieval engine: "http" serves the bundle and drives it over the wire, "batch" calls the Java
# batch CLI in-process. Both use the same Java retrieval code, so they must agree.
ENGINE="${GEAFLOW_ENGINE:-http}"
RUNS_ROOT="${GEAFLOW_RUNS_ROOT:-${ROOT}/tmp/graphrag/data/runs}"

# Conventional input locations. A dataset name alone is enough when the pipeline
# has been run once before, so a normal reproduction needs no environment exports.
DATA_ROOT="${GEAFLOW_DATA_ROOT:-${ROOT}/tmp/graphrag/data}"

# discover_input <name> <glob> <label>
discover_input() {
    local name="$1" pattern="$2" label="$3"
    local found
    found="$(ls -1 ${pattern} 2>/dev/null | head -1 || true)"
    if [[ -z "${found}" ]]; then
        fail "${label} not found for dataset '${DATASET}'.
       Looked for: ${pattern}
       Set ${name} explicitly to point at it."
    fi
    printf '%s\n' "${found}"
}

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
  --engine <kind>    http (default) or batch: drive retrieval over HTTP or via the Java batch CLI
  -h, --help         show this message

Environment (all optional; conventional locations are discovered automatically):
  GEAFLOW_DATA_ROOT   dataset/data root (default tmp/graphrag/data)
  GEAFLOW_VECTORS     precomputed query vectors .npz (required for vector methods)
  GEAFLOW_BUNDLE      existing READY bundle to reuse with --skip-build
  GEAFLOW_QUERIES     query JSONL for the dataset
  GEAFLOW_GOLD        gold JSONL/zip/parquet; enables Recall/MRR/NDCG/EM-F1 scoring
  GEAFLOW_ANSWERS     answer endpoint URL (default http://127.0.0.1:<port>/api/v1/answers when the
                      served engine is used); enables real EM/F1 instead of leaving them empty

Nothing needs to be exported for a repeat run on a machine that already holds the
dataset cache: queries, vectors and gold are discovered under GEAFLOW_DATA_ROOT by
dataset name. Exports exist to override those defaults.
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
        --engine)
            case "$2" in
                http|batch) ENGINE="$2" ;;
                *) echo "--engine must be http or batch" >&2; exit 2 ;;
            esac
            shift 2 ;;
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
    # Convention: tmp/graphrag/data/runs/<dataset>-query-vectors*.npz, else an explicit export.
    VECTORS="${GEAFLOW_VECTORS:-}"
    if [[ -z "${VECTORS}" ]]; then
        VECTORS="$(ls -1 "${DATA_ROOT}"/runs/"${DATASET}"-query-vectors*.npz \
            "${DATA_ROOT}"/runs/"${DATASET}"*-vectors*.npz 2>/dev/null | head -1 || true)"
    fi
    if [[ -z "${VECTORS}" ]]; then
        fail "no precomputed query vectors found for dataset '${DATASET}'.
       Looked under: ${DATA_ROOT}/runs/${DATASET}-query-vectors*.npz
       Set GEAFLOW_VECTORS to point at the .npz file; the vector channel and the
       equal-budget Vector Overfetch baseline cannot be reproduced without it."
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
    # Offline dependency:build-classpath can succeed while writing a zero-byte file, which then
    # fails at service start with a confusing NoClassDefFoundError. Reuse a known-good classpath
    # when one is supplied, and never continue with an empty one.
    if [[ -n "${GEAFLOW_CLASSPATH:-}" && -s "${GEAFLOW_CLASSPATH}" ]]; then
        cp "${GEAFLOW_CLASSPATH}" "${CLASSPATH_FILE}"
    fi
    if [[ ! -s "${CLASSPATH_FILE}" ]]; then
        mvn -o -q -pl geaflow-ai -am -DskipTests -Dmaven.javadoc.skip=true \
            -Djacoco.skip=true -Drat.skip=true \
            dependency:build-classpath -Dmdep.outputFile="${CLASSPATH_FILE}" >/dev/null
    fi
    [[ -s "${CLASSPATH_FILE}" ]] || fail "empty service classpath at ${CLASSPATH_FILE}; set GEAFLOW_CLASSPATH to a known-good classpath file"
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
# Convention: tmp/graphrag/data/queries/<dataset>.jsonl, else an explicit export.
QUERIES="${GEAFLOW_QUERIES:-}"
if [[ -z "${QUERIES}" && -f "${RUN_DIR}/queries.jsonl" ]]; then
    QUERIES="${RUN_DIR}/queries.jsonl"
fi
if [[ -z "${QUERIES}" ]]; then
    QUERIES="$(ls -1 "${DATA_ROOT}"/queries/"${DATASET}"*.jsonl \
        "${DATA_ROOT}"/queries/"${DATASET}"*.json 2>/dev/null | head -1 || true)"
fi
if [[ -z "${QUERIES}" || ! -f "${QUERIES}" ]]; then
    fail "query set not found for dataset '${DATASET}'.
       Looked for: ${DATA_ROOT}/queries/${DATASET}*.jsonl
       Set GEAFLOW_QUERIES to point at a JSONL query file."
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

# EM/F1 need the answer endpoint: /api/v1/retrievals never returns an 'answer' field, so without
# this flag the two answer columns stay empty by design. The answer layer is extractive, so expect
# near-zero EM against short gold answers -- that is a real result, not a wiring failure.
# Default to the locally served endpoint when one is running; GEAFLOW_ANSWERS overrides it.
if [[ -z "${GEAFLOW_ANSWERS:-}" && "${SKIP_SERVICE}" != "1" ]]; then
    GEAFLOW_ANSWERS="http://127.0.0.1:${PORT}/api/v1/answers"
fi
if [[ -n "${GEAFLOW_ANSWERS:-}" ]]; then
    BENCH_ARGS+=(--answers-url "${GEAFLOW_ANSWERS}")
fi

# Gold targets turn the run from a smoke test into a scored evaluation. They are optional so a
# cache-less environment can still exercise the wire protocol, and the CSV then leaves the
# evidence-quality columns empty instead of inventing numbers.
#
# Discovery walks the cache directory with `find` rather than `ls`. `ls <dir> <dir>/*` prints a
# `dirname:` header and reports the directory itself first, so `head -1` yielded a path with a
# trailing colon that `-e` rejects -- which silently dropped gold and left every evidence-quality
# column empty for a cache laid out as `cache/hotpotqa-hf/<file>`.
GOLD="${GEAFLOW_GOLD:-}"
if [[ -z "${GOLD}" ]]; then
    GOLD="$(find "${DATA_ROOT}/cache" -maxdepth 2 \
        \( -name "${DATASET}*" -o -path "${DATA_ROOT}/cache/${DATASET}*/*" \) \
        -type f 2>/dev/null | sort | head -1 || true)"
fi
if [[ -z "${GOLD}" ]]; then
    # Fall back to a directory whose own name differs from the dataset name (for example the
    # HotpotQA cache lives under `cache/hotpotqa-hf/`). The loader takes a file, not a directory,
    # so prefer a readable gold artifact inside it over the directory path itself.
    CACHE_DIR="$(find "${DATA_ROOT}/cache" -maxdepth 1 -type d -name "${DATASET}*" 2>/dev/null \
        | sort | head -1 || true)"
    if [[ -n "${CACHE_DIR}" ]]; then
        GOLD="$(find "${CACHE_DIR}" -maxdepth 1 -type f \
            \( -name '*.parquet' -o -name '*.json' -o -name '*.jsonl' -o -name '*.zip' \) \
            2>/dev/null | sort | head -1 || true)"
    fi
fi
if [[ -n "${GOLD}" && -e "${GOLD}" ]]; then
    BENCH_ARGS+=(--gold "${GOLD}")
    if [[ -f "${BUNDLE}/chunks.jsonl" ]]; then
        BENCH_ARGS+=(--chunks "${BUNDLE}/chunks.jsonl")
    fi
else
    log "no GEAFLOW_GOLD set; evidence-quality metrics will be omitted from this run"
fi
log "running six-method benchmark (engine=${ENGINE})"
if [[ "${ENGINE}" == "batch" ]]; then
    # The design states the Python tooling invokes "the Java CLI in batch mode" and that retrieval
    # policy, budget and trace stay in Java so there is one behavioural source of truth. This path
    # exercises exactly that: the CLI drives the same RecallService the service uses, so a batch run
    # and a served run cannot silently diverge.
    if [[ -z "${SERVICE_CLASSPATH:-}" ]]; then
        CLASSPATH_FILE="${RUN_DIR}/classpath.txt"
        mvn -o -q -pl geaflow-ai -am -DskipTests -Dmaven.javadoc.skip=true \
            -Djacoco.skip=true -Drat.skip=true \
            dependency:build-classpath -Dmdep.outputFile="${CLASSPATH_FILE}" >/dev/null
        SERVICE_CLASSPATH="${ROOT}/geaflow-ai/target/classes:$(cat "${CLASSPATH_FILE}")"
    fi
    BATCH_OUT="${RUN_DIR}/${DATASET}-batch.jsonl"
    java -cp "${SERVICE_CLASSPATH}" org.apache.geaflow.ai.retrieval.service.RetrievalBatchCli \
        --bundle "${BUNDLE}" \
        --queries "${QUERIES}" \
        --output "${BATCH_OUT}" \
        --graph-name "$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['graph_name'])" "${BUNDLE}/READY.json")" \
        --graph-version "$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['version'])" "${BUNDLE}/READY.json")" \
        --index-version "$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['version'])" "${BUNDLE}/READY.json")" \
        --vector-source "$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['vector_source'])" "${BUNDLE}/READY.json")" \
        --vector-version "$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['vector_version'])" "${BUNDLE}/READY.json")" \
        --mode "${GEAFLOW_BATCH_MODE:-ADAPTIVE_HYBRID}" \
        --execution-mode "${GEAFLOW_BATCH_EXECUTION:-CASCADED}" \
        --top-k 10 --max-candidates 100 --timeout-ms 10000 --token-budget 4096 \
        --primary-constraint CANDIDATES
    log "batch results: ${BATCH_OUT}"
    log "note: batch mode writes raw JSONL only; run tools/graphrag/eval to score it"
else
    PYTHONPATH="${ROOT}" python3 "${ROOT}/tools/graphrag/benchmark_http.py" "${BENCH_ARGS[@]}"
fi

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