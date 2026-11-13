#!/usr/bin/env bash
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
#
# Fusion-weight sweep for the adaptive routing module.
#
# ablate_adaptive.sh neutralises one decision component per condition; this script instead varies
# the graph/text fusion ratio, which the component ablation identified as the dominant term. Only
# the ratio matters to reciprocal-rank fusion - the no-router (1.0:1.0) and no-graph-fusion
# (0.25:0.25) conditions produced identical rankings - so the text weight is held at 1.0 and the
# graph weight carries the sweep. The shipped default of 2.0:0.25 is the ratio 8.
#
# Budget note: --ablation multi-hop halves the candidate window, so --max-candidates 200 yields the
# candidateLimit = 100 the 500-query report used. Running with the default 100 would produce a
# 50-candidate run that is not comparable with that report.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DATA_ROOT="${GEAFLOW_DATA_ROOT:-${ROOT}/tmp/graphrag/data}"
RUNS_ROOT="${GEAFLOW_RUNS_ROOT:-${DATA_ROOT}/runs}"
DATASET="2wikimultihopqa"
LIMIT="200"
PORT="18091"
WEIGHTS="0.25,0.5,1.0,2.0,4.0,8.0"
TEXT_WEIGHT="1.0"
MAX_CANDIDATES="200"
OUT_DIR=""

usage() {
    cat <<'EOF'
Usage: tools/graphrag/sweep_fusion.sh [options]

Restarts the service once per graph/text fusion ratio and runs FIXED_HYBRID and ADAPTIVE_HYBRID
over the same query slice. FIXED_HYBRID does not read the routing weights, so it acts as a control
whose numbers must stay flat across the sweep.

Options:
  --dataset <name>        2wikimultihopqa (default) or hotpotqa
  --limit <n>             queries per ratio (default 200)
  --port <port>           service port (default 18091)
  --weights <list>        comma-separated graph weights (default 0.25,0.5,1.0,2.0,4.0,8.0)
  --text-weight <w>       fixed text weight (default 1.0)
  --max-candidates <n>    pre-ablation candidate window (default 200 -> candidateLimit 100)
  --out <dir>             output directory (default under runs/fusion-sweep/)

Environment (optional, as in reproduce.sh):
  GEAFLOW_VECTORS, GEAFLOW_BUNDLE, GEAFLOW_QUERIES, GEAFLOW_GOLD, GEAFLOW_CLASSPATH

Nothing is uploaded and nothing outside the run directory is written.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dataset) DATASET="$2"; shift 2 ;;
        --limit) LIMIT="$2"; shift 2 ;;
        --port) PORT="$2"; shift 2 ;;
        --weights) WEIGHTS="$2"; shift 2 ;;
        --text-weight) TEXT_WEIGHT="$2"; shift 2 ;;
        --max-candidates) MAX_CANDIDATES="$2"; shift 2 ;;
        --out) OUT_DIR="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

log() { echo "[sweep] $*" >&2; }

[[ -n "${OUT_DIR}" ]] || OUT_DIR="${RUNS_ROOT}/fusion-sweep/${DATASET}-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "${OUT_DIR}"

BUNDLE="${GEAFLOW_BUNDLE:-${RUNS_ROOT}/${DATASET}-bundle}"
[[ -d "${BUNDLE}" ]] || { log "missing bundle: ${BUNDLE}"; exit 2; }
[[ -f "${BUNDLE}/READY.json" ]] || { log "missing READY.json in ${BUNDLE}"; exit 2; }

GOLD="${GEAFLOW_GOLD:-}"
if [[ -z "${GOLD}" ]]; then
    GOLD="$(find "${DATA_ROOT}/cache" -maxdepth 2 \
        \( -name "${DATASET}*" -o -path "${DATA_ROOT}/cache/${DATASET}*/*" \) \
        -type f 2>/dev/null | sort | head -1 || true)"
fi
if [[ -z "${GOLD}" || ! -e "${GOLD}" ]]; then
    log "no gold found under ${DATA_ROOT}/cache; quality columns will be empty"
    GOLD=""
fi

log "bundle:    ${BUNDLE}"
log "gold:      ${GOLD:-<none>}"
log "output:    ${OUT_DIR}"
log "limit:     ${LIMIT}"
log "ratios:    graph=${WEIGHTS} with text=${TEXT_WEIGHT}"

CLASSPATH_FILE="${OUT_DIR}/classpath.txt"
if [[ -n "${GEAFLOW_CLASSPATH:-}" && -s "${GEAFLOW_CLASSPATH}" ]]; then
    # Reusing a known-good classpath avoids the offline dependency:build-classpath goal, which can
    # exit successfully while writing a zero-byte file.
    cp "${GEAFLOW_CLASSPATH}" "${CLASSPATH_FILE}"
fi
if [[ ! -s "${CLASSPATH_FILE}" ]]; then
    log "building service classpath"
    ( cd "${ROOT}" && mvn -o -q -pl geaflow-ai -am -DskipTests -Dmaven.javadoc.skip=true \
        -Djacoco.skip=true -Drat.skip=true dependency:build-classpath \
        -Dmdep.outputFile="${CLASSPATH_FILE}" >/dev/null )
fi
[[ -s "${CLASSPATH_FILE}" ]] || { log "empty classpath: ${CLASSPATH_FILE}; set GEAFLOW_CLASSPATH"; exit 2; }
SERVICE_CLASSPATH="${ROOT}/geaflow-ai/target/classes:$(cat "${CLASSPATH_FILE}")"

GRAPH_NAME="$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['graph_name'])" "${BUNDLE}/READY.json")"
GRAPH_VERSION="$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['version'])" "${BUNDLE}/READY.json")"
VECTOR_SOURCE="$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['vector_source'])" "${BUNDLE}/READY.json")"
VECTOR_VERSION="$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['vector_version'])" "${BUNDLE}/READY.json")"

IFS=',' read -r -a WEIGHT_LIST <<< "${WEIGHTS}"
for weight in "${WEIGHT_LIST[@]}"; do
    cond_dir="${OUT_DIR}/graph-${weight}"
    mkdir -p "${cond_dir}"
    log "--- graph-weight=${weight} text-weight=${TEXT_WEIGHT} ---"
    service_log="${cond_dir}/service.log"
    env \
        GEAFLOW_RETRIEVAL_ARTIFACT_BUNDLE_ROOT="${BUNDLE}" \
        GEAFLOW_RETRIEVAL_ARTIFACT_GRAPH_NAME="${GRAPH_NAME}" \
        GEAFLOW_RETRIEVAL_ARTIFACT_GRAPH_VERSION="${GRAPH_VERSION}" \
        GEAFLOW_RETRIEVAL_ARTIFACT_INDEX_VERSION="${GRAPH_VERSION}" \
        GEAFLOW_RETRIEVAL_ARTIFACT_VECTOR_SOURCE="${VECTOR_SOURCE}" \
        GEAFLOW_RETRIEVAL_ARTIFACT_VECTOR_VERSION="${VECTOR_VERSION}" \
        GEAFLOW_RETRIEVAL_GRAPH_FUSION_GRAPH_WEIGHT="${weight}" \
        GEAFLOW_RETRIEVAL_GRAPH_FUSION_TEXT_WEIGHT="${TEXT_WEIGHT}" \
        GEAFLOW_SERVER_PORT="${PORT}" \
        java -cp "${SERVICE_CLASSPATH}" org.apache.geaflow.ai.GeaFlowMemoryServer \
        >"${service_log}" 2>&1 &
    service_pid=$!

    ready="0"
    for _ in $(seq 1 60); do
        if curl -fsS "http://127.0.0.1:${PORT}/ready" >"${cond_dir}/ready.json" 2>/dev/null; then
            ready="1"
            break
        fi
        # The port can still be held by the previous condition's JVM during shutdown; wait rather
        # than silently benchmarking against a stale server.
        if ! kill -0 "${service_pid}" 2>/dev/null; then
            break
        fi
        sleep 1
    done
    if [[ "${ready}" != "1" ]]; then
        log "service did not become ready for graph-weight=${weight}; see ${service_log}"
        kill "${service_pid}" 2>/dev/null || true
        exit 1
    fi
    # The ready response must come from this condition's own JVM, not a leftover one.
    if ! kill -0 "${service_pid}" 2>/dev/null; then
        log "service for graph-weight=${weight} exited while another server held port ${PORT}"
        exit 1
    fi

    PYTHONPATH="${ROOT}${PYTHONPATH:+:${PYTHONPATH}}" \
    python3 "${ROOT}/tools/graphrag/benchmark_http.py" \
        --url "http://127.0.0.1:${PORT}/api/v1/retrievals" \
        --limit "${LIMIT}" --ablation multi-hop --max-candidates "${MAX_CANDIDATES}" \
        --graph-name "${GRAPH_NAME}" --graph-version "${GRAPH_VERSION}" \
        --index-version "${GRAPH_VERSION}" \
        --output "${cond_dir}/${DATASET}-graph-${weight}.csv" \
        --answers-url "http://127.0.0.1:${PORT}/api/v1/answers" \
        ${GEAFLOW_VECTORS:+--vectors "${GEAFLOW_VECTORS}"} \
        ${GEAFLOW_QUERIES:+--queries "${GEAFLOW_QUERIES}"} \
        ${GOLD:+--gold "${GOLD}"} \
        $([[ -f "${BUNDLE}/chunks.jsonl" ]] && echo --chunks "${BUNDLE}/chunks.jsonl") \
        2>&1 | tee "${cond_dir}/benchmark.log"

    kill "${service_pid}" 2>/dev/null || true
    wait "${service_pid}" 2>/dev/null || true
done

log "writing combined summary"
python3 - "${OUT_DIR}" <<'PY'
import csv, glob, os, sys
out = sys.argv[1]
rows = []
for path in sorted(glob.glob(os.path.join(out, 'graph-*', '*-graph-*.csv'))):
    ratio = os.path.basename(os.path.dirname(path)).replace('graph-', '')
    with open(path, newline='', encoding='utf-8') as stream:
        for row in csv.DictReader(stream):
            row['graphWeight'] = ratio
            rows.append(row)
if not rows:
    print('no sweep results found', file=sys.stderr)
    sys.exit(1)
rows.sort(key=lambda row: (float(row['graphWeight']), row['method']))
fields = ['graphWeight'] + [k for k in rows[0].keys() if k != 'graphWeight']
combined = os.path.join(out, 'fusion-sweep.csv')
with open(combined, 'w', newline='', encoding='utf-8') as stream:
    writer = csv.DictWriter(stream, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)
print(combined)
PY

log "done: ${OUT_DIR}"
