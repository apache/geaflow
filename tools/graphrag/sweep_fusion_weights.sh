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
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
################################################################################
#
# Grid sweep for the adaptive fusion weights.
#
# The component ablation established that the shipped graph-weight=2.0 / text-weight=0.25
# pairing costs ranking quality on 2Wiki. This script sweeps a small, declared grid so the
# recommendation is measured rather than argued: each cell restarts the service with one
# pair of weights and runs FIXED_HYBRID and ADAPTIVE_HYBRID on the same query slice.
#
# The grid is declared up front rather than chosen after seeing results, so the comparison
# is a search rather than a cherry-pick.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DATA_ROOT="${GEAFLOW_DATA_ROOT:-${ROOT}/tmp/graphrag/data}"
RUNS_ROOT="${GEAFLOW_RUNS_ROOT:-${DATA_ROOT}/runs}"
DATASET="2wikimultihopqa"
LIMIT="200"
PORT="18100"
OUT_DIR=""

usage() {
    cat <<'EOF'
Usage: tools/graphrag/sweep_fusion_weights.sh [options]

Sweeps graph-weight x text-weight for the adaptive fusion, restarting the service for
each cell. Reports quality and cost per cell so a recommended default is evidence-based.

Options:
  --dataset <name>  2wikimultihopqa (default) or hotpotqa
  --limit <n>       queries per cell (default 200)
  --port <port>     service port (default 18100)
  --out <dir>       output directory (default under runs/weight-sweep/)
  -h, --help        show this help

Environment: GEAFLOW_VECTORS, GEAFLOW_BUNDLE, GEAFLOW_QUERIES, GEAFLOW_GOLD as in
reproduce.sh. Nothing is uploaded; only the run directory is written.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dataset) DATASET="$2"; shift 2 ;;
        --limit) LIMIT="$2"; shift 2 ;;
        --port) PORT="$2"; shift 2 ;;
        --out) OUT_DIR="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

log() { echo "[sweep] $*" >&2; }

[[ -n "${OUT_DIR}" ]] || OUT_DIR="${RUNS_ROOT}/weight-sweep/${DATASET}-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "${OUT_DIR}"

# Grid: graph-weight x text-weight. 2.0/0.25 is the shipped pair; 1.0/1.0 removes the graph
# preference entirely. Values below 1.0 let the text channels lead.
GRAPH_WEIGHTS="2.0 1.0 0.25"
TEXT_WEIGHTS="1.0 0.25"

BUNDLE="${GEAFLOW_BUNDLE:-${RUNS_ROOT}/${DATASET}-bundle}"
[[ -d "${BUNDLE}" ]] || { log "missing bundle: ${BUNDLE}"; exit 2; }
[[ -f "${BUNDLE}/READY.json" ]] || { log "missing READY.json in ${BUNDLE}"; exit 2; }

GOLD="${GEAFLOW_GOLD:-}"
if [[ -z "${GOLD}" ]]; then
    GOLD="$(find "${DATA_ROOT}/cache" -maxdepth 2 \( -name "${DATASET}*" -o -path "${DATA_ROOT}/cache/${DATASET}*/*" \) -type f 2>/dev/null | sort | head -1 || true)"
fi
if [[ -z "${GOLD}" || ! -e "${GOLD}" ]]; then
    log "no gold found; quality columns will be empty"
    GOLD=""
fi

CLASSPATH_FILE="${OUT_DIR}/classpath.txt"
if [[ ! -s "${CLASSPATH_FILE}" ]]; then
    log "building service classpath"
    ( cd "${ROOT}" && mvn -o -q -pl geaflow-ai -am -DskipTests -Dmaven.javadoc.skip=true \
        -Djacoco.skip=true -Drat.skip=true dependency:build-classpath \
        -Dmdep.outputFile="${CLASSPATH_FILE}" >/dev/null )
fi
[[ -s "${CLASSPATH_FILE}" ]] || { log "classpath build produced no output"; exit 1; }
SERVICE_CLASSPATH="${ROOT}/geaflow-ai/target/classes:$(cat "${CLASSPATH_FILE}")"

GRAPH_NAME="$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['graph_name'])" "${BUNDLE}/READY.json")"
GRAPH_VERSION="$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['version'])" "${BUNDLE}/READY.json")"
VECTOR_SOURCE="$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['vector_source'])" "${BUNDLE}/READY.json")"
VECTOR_VERSION="$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['vector_version'])" "${BUNDLE}/READY.json")"

log "bundle: ${BUNDLE}"
log "gold:   ${GOLD:-<none>}"
log "output: ${OUT_DIR}"
log "grid:   graph-weight in (${GRAPH_WEIGHTS}) x text-weight in (${TEXT_WEIGHTS})"

for gw in ${GRAPH_WEIGHTS}; do
    for tw in ${TEXT_WEIGHTS}; do
        cell="gw${gw}_tw${tw}"
        cell_dir="${OUT_DIR}/${cell}"
        mkdir -p "${cell_dir}"
        log "--- ${cell} ---"

        service_log="${cell_dir}/service.log"
        GEAFLOW_RETRIEVAL_ARTIFACT_BUNDLE_ROOT="${BUNDLE}" \
        GEAFLOW_RETRIEVAL_ARTIFACT_GRAPH_NAME="${GRAPH_NAME}" \
        GEAFLOW_RETRIEVAL_ARTIFACT_GRAPH_VERSION="${GRAPH_VERSION}" \
        GEAFLOW_RETRIEVAL_ARTIFACT_INDEX_VERSION="${GRAPH_VERSION}" \
        GEAFLOW_RETRIEVAL_ARTIFACT_VECTOR_SOURCE="${VECTOR_SOURCE}" \
        GEAFLOW_RETRIEVAL_ARTIFACT_VECTOR_VERSION="${VECTOR_VERSION}" \
        GEAFLOW_RETRIEVAL_GRAPH_FUSION_GRAPH_WEIGHT="${gw}" \
        GEAFLOW_RETRIEVAL_GRAPH_FUSION_TEXT_WEIGHT="${tw}" \
        GEAFLOW_SERVER_PORT="${PORT}" \
            java -cp "${SERVICE_CLASSPATH}" org.apache.geaflow.ai.GeaFlowMemoryServer \
            >"${service_log}" 2>&1 &
        service_pid=$!

        ready="0"
        for _ in $(seq 1 60); do
            if curl -fsS "http://127.0.0.1:${PORT}/ready" >"${cell_dir}/ready.json" 2>/dev/null; then
                ready="1"
                break
            fi
            sleep 1
        done
        if [[ "${ready}" != "1" ]]; then
            log "service did not become ready for ${cell}; see ${service_log}"
            kill "${service_pid}" 2>/dev/null || true
            exit 1
        fi

        bench_args=(--url "http://127.0.0.1:${PORT}/api/v1/retrievals"
                    --limit "${LIMIT}" --ablation multi-hop
                    --graph-name "${GRAPH_NAME}" --graph-version "${GRAPH_VERSION}"
                    --index-version "${GRAPH_VERSION}"
                    --output "${cell_dir}/${DATASET}-${cell}.csv"
                    --answers-url "http://127.0.0.1:${PORT}/api/v1/answers")
        [[ -n "${GEAFLOW_VECTORS:-}" ]] && bench_args+=(--vectors "${GEAFLOW_VECTORS}")
        [[ -n "${GEAFLOW_QUERIES:-}" ]] && bench_args+=(--queries "${GEAFLOW_QUERIES}")
        [[ -n "${GOLD}" ]] && bench_args+=(--gold "${GOLD}")
        [[ -f "${BUNDLE}/chunks.jsonl" ]] && bench_args+=(--chunks "${BUNDLE}/chunks.jsonl")

        PYTHONPATH="${ROOT}${PYTHONPATH:+:${PYTHONPATH}}" \
        python3 "${ROOT}/tools/graphrag/benchmark_http.py" "${bench_args[@]}" \
            2>&1 | tee "${cell_dir}/benchmark.log"

        kill "${service_pid}" 2>/dev/null || true
        wait "${service_pid}" 2>/dev/null || true
    done
done

log "writing combined summary"
python3 - "${OUT_DIR}" <<'PY'
import csv, glob, json, os, sys
out = sys.argv[1]
rows = []
for path in sorted(glob.glob(os.path.join(out, '*', '*.csv'))):
    cell = os.path.basename(os.path.dirname(path))
    with open(path, newline='', encoding='utf-8') as stream:
        for row in csv.DictReader(stream):
            row['cell'] = cell
            rows.append(row)
if not rows:
    print('no cell results found', file=sys.stderr)
    sys.exit(1)
fields = ['cell'] + [k for k in rows[0].keys() if k != 'cell']
combined = os.path.join(out, 'weight-sweep.csv')
with open(combined, 'w', newline='', encoding='utf-8') as stream:
    writer = csv.DictWriter(stream, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)
# Rank cells by MRR so the winner is explicit.
best = sorted((r for r in rows if r['method'] == 'ADAPTIVE_HYBRID'),
              key=lambda r: float(r['mrr'] or 0), reverse=True)
print(combined)
print('\nrank by MRR (ADAPTIVE_HYBRID):')
for r in best[:5]:
    print(f"  {r['cell']:18} MRR={float(r['mrr']):.4f} R@10={float(r['evidenceRecallAtK']):.4f} "
          f"edges={float(r['meanEdgesExamined']):.1f}")
PY

log "done: ${OUT_DIR}"