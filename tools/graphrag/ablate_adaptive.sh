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
# Component-level ablation runner for the adaptive routing module.
#
# The scenario ablations in benchmark_http.py (--ablation) swap which channels run. They answer
# "what if this channel were absent", not "what is this decision component contributing". The
# routing parameters are server-side configuration, so a component ablation means restarting the
# service with one parameter neutralised and re-running the same two methods against it.
#
# Each condition sets exactly one environment variable, so the delta between two conditions is
# attributable to that component and nothing else.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DATA_ROOT="${GEAFLOW_DATA_ROOT:-${ROOT}/tmp/graphrag/data}"
RUNS_ROOT="${GEAFLOW_RUNS_ROOT:-${DATA_ROOT}/runs}"
DATASET="2wikimultihopqa"
LIMIT="200"
PORT="18090"
MAX_CANDIDATES="100"
OUT_DIR=""
CONDITIONS=""

usage() {
    cat <<'EOF'
Usage: tools/graphrag/ablate_adaptive.sh [options]

Runs the adaptive routing module with one decision component neutralised at a time.
Every condition restarts the service with exactly one overridden setting, then runs
FIXED_HYBRID and ADAPTIVE_HYBRID over the same query slice.

Options:
  --dataset <name>    2wikimultihopqa (default) or hotpotqa
  --limit <n>         queries per condition (default 200)
  --max-candidates <n> pre-ablation candidate window (default 100; use 200 to match the
                      500-query report, since multi-hop halves it)
  --port <port>       service port (default 18090)
  --conditions <list> comma-separated subset; default is all of them
  --out <dir>         output directory (default under runs/ablations-component/)

Conditions:
  baseline            no override; the shipped configuration
  no-router           graph-weight=1.0, text-weight=1.0: fusion no longer favours the graph
  no-replan           max-replan-count=0: the policy cannot reshape the budget mid-request
  no-probe-share      probe-share=0.5: the text probe gets an even half of the budget
  no-graph-fusion     graph-weight=0.25: graph ranked like a text channel
  no-idf-trigger      high-average-idf=1000: the lexical-confidence rule can never fire
  no-degree-trigger   high-candidate-degree=1000000: the hub rule can never fire
  no-cascade-gate     min-probe-evidence=1, min-probe-score-gap=0: the graph stage
                      almost always runs (min-probe-evidence cannot be 0: it is validated)

Environment (optional, as in reproduce.sh):
  GEAFLOW_VECTORS, GEAFLOW_BUNDLE, GEAFLOW_QUERIES, GEAFLOW_GOLD

Nothing is uploaded and nothing outside the run directory is written.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dataset) DATASET="$2"; shift 2 ;;
        --limit) LIMIT="$2"; shift 2 ;;
        --max-candidates) MAX_CANDIDATES="$2"; shift 2 ;;
        --port) PORT="$2"; shift 2 ;;
        --conditions) CONDITIONS="$2"; shift 2 ;;
        --out) OUT_DIR="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

log() { echo "[ablate] $*" >&2; }

[[ -n "${OUT_DIR}" ]] || OUT_DIR="${RUNS_ROOT}/ablations-component/${DATASET}-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "${OUT_DIR}"

# Each condition is a single environment override. Empty means "use the shipped default".
condition_env() {
    case "$1" in
        baseline)            echo "" ;;
        no-router)           echo "GEAFLOW_RETRIEVAL_GRAPH_FUSION_GRAPH_WEIGHT=1.0 GEAFLOW_RETRIEVAL_GRAPH_FUSION_TEXT_WEIGHT=1.0" ;;
        no-replan)           echo "GEAFLOW_RETRIEVAL_MAX_REPLAN_COUNT=0" ;;
        no-probe-share)      echo "GEAFLOW_RETRIEVAL_GRAPH_PROBE_SHARE=0.5" ;;
        # Zero, not 0.25: the shipped default is already 0.25 against a text weight of 1.0, so
        # setting 0.25 here would reproduce the baseline instead of ablating anything. Removing the
        # graph rank term entirely is the remaining meaningful question.
        no-graph-fusion)     echo "GEAFLOW_RETRIEVAL_GRAPH_FUSION_GRAPH_WEIGHT=0.0" ;;
        no-idf-trigger)      echo "GEAFLOW_RETRIEVAL_HIGH_AVERAGE_IDF=1000.0" ;;
        no-degree-trigger)   echo "GEAFLOW_RETRIEVAL_HIGH_CANDIDATE_DEGREE=1000000" ;;
        no-cascade-gate)     echo "GEAFLOW_RETRIEVAL_CASCADE_MIN_PROBE_SCORE_GAP=0.0 GEAFLOW_RETRIEVAL_CASCADE_MIN_PROBE_EVIDENCE=1" ;;
        # Diffusion-ceiling probes rather than component ablations: with the shipped growth limit
        # the graph retriever stops itself after the first hop (96 of 96 post-hop traces end with
        # DIFFUSION_GROWTH_LIMIT), so the second hop the policy plans never executes. These
        # conditions test whether the hop-boundary rules have any work once that ceiling is raised.
        growth-4)           echo "GEAFLOW_RETRIEVAL_HIGH_GROWTH_RATE=4.0" ;;
        growth-8)           echo "GEAFLOW_RETRIEVAL_HIGH_GROWTH_RATE=8.0" ;;
        *) return 1 ;;
    esac
}

ALL_CONDITIONS="baseline no-router no-replan no-probe-share no-graph-fusion no-idf-trigger no-degree-trigger no-cascade-gate"
[[ -n "${CONDITIONS}" ]] || CONDITIONS="$(echo "${ALL_CONDITIONS}" | tr ' ' ',')"

# Resolve the bundle and its artifact coordinates once; every condition reuses them.
BUNDLE="${GEAFLOW_BUNDLE:-${RUNS_ROOT}/${DATASET}-bundle}"
[[ -d "${BUNDLE}" ]] || { log "missing bundle: ${BUNDLE}"; exit 2; }
[[ -f "${BUNDLE}/READY.json" ]] || { log "missing READY.json in ${BUNDLE}"; exit 2; }

# Gold is what turns this from a latency probe into a quality ablation. It has to be discovered the
# same way reproduce.sh does it, or every quality column comes back empty and the conditions look
# identical because only their cost numbers differ.
GOLD="${GEAFLOW_GOLD:-}"
if [[ -z "${GOLD}" ]]; then
    GOLD="$(find "${DATA_ROOT}/cache" -maxdepth 2 \
        \( -name "${DATASET}*" -o -path "${DATA_ROOT}/cache/${DATASET}*/*" \) \
        -type f 2>/dev/null | sort | head -1 || true)"
fi
if [[ -z "${GOLD}" ]]; then
    CACHE_DIR="$(find "${DATA_ROOT}/cache" -maxdepth 1 -type d -name "${DATASET%%-*}*" 2>/dev/null \
        | sort | head -1 || true)"
    if [[ -n "${CACHE_DIR}" ]]; then
        GOLD="$(find "${CACHE_DIR}" -maxdepth 1 -type f \
            \( -name '*.parquet' -o -name '*.json' -o -name '*.jsonl' -o -name '*.zip' \) \
            2>/dev/null | sort | head -1 || true)"
    fi
fi
if [[ -z "${GOLD}" || ! -e "${GOLD}" ]]; then
    log "no gold found under ${DATA_ROOT}/cache; quality columns will be empty"
    GOLD=""
fi

log "bundle:    ${BUNDLE}"
log "gold:      ${GOLD:-<none>}"
log "output:    ${OUT_DIR}"
log "limit:     ${LIMIT}"
log "conditions: ${CONDITIONS}"

CLASSPATH_FILE="${OUT_DIR}/classpath.txt"
# A non-empty file is required: a build interrupted by a signal leaves a zero-byte
# classpath behind, and reusing it fails later with a confusing NoClassDefFoundError.
# The offline dependency:build-classpath goal can also succeed while writing nothing, so an
# existing non-empty classpath can be supplied explicitly and the result is verified either way.
if [[ -n "${GEAFLOW_CLASSPATH:-}" && -s "${GEAFLOW_CLASSPATH}" ]]; then
    cp "${GEAFLOW_CLASSPATH}" "${CLASSPATH_FILE}"
fi
if [[ ! -s "${CLASSPATH_FILE}" ]]; then
    log "building service classpath"
    ( cd "${ROOT}" && mvn -o -q -pl geaflow-ai -am -DskipTests -Dmaven.javadoc.skip=true \
        -Djacoco.skip=true -Drat.skip=true dependency:build-classpath \
        -Dmdep.outputFile="${CLASSPATH_FILE}" >/dev/null )
fi
[[ -s "${CLASSPATH_FILE}" ]] || { log "empty classpath: ${CLASSPATH_FILE}; set GEAFLOW_CLASSPATH to a known-good one"; exit 2; }
SERVICE_CLASSPATH="${ROOT}/geaflow-ai/target/classes:$(cat "${CLASSPATH_FILE}")"

GRAPH_NAME="$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['graph_name'])" "${BUNDLE}/READY.json")"
GRAPH_VERSION="$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['version'])" "${BUNDLE}/READY.json")"
VECTOR_SOURCE="$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['vector_source'])" "${BUNDLE}/READY.json")"
VECTOR_VERSION="$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['vector_version'])" "${BUNDLE}/READY.json")"

IFS=',' read -r -a CONDITION_LIST <<< "${CONDITIONS}"

for condition in "${CONDITION_LIST[@]}"; do
    if ! env_override="$(condition_env "${condition}")"; then
        log "unknown condition: ${condition}"
        exit 2
    fi
    cond_dir="${OUT_DIR}/${condition}"
    mkdir -p "${cond_dir}"
    log "--- ${condition} ---"

    service_log="${cond_dir}/service.log"
    # shellcheck disable=SC2086
    env \
        GEAFLOW_RETRIEVAL_ARTIFACT_BUNDLE_ROOT="${BUNDLE}" \
        GEAFLOW_RETRIEVAL_ARTIFACT_GRAPH_NAME="${GRAPH_NAME}" \
        GEAFLOW_RETRIEVAL_ARTIFACT_GRAPH_VERSION="${GRAPH_VERSION}" \
        GEAFLOW_RETRIEVAL_ARTIFACT_INDEX_VERSION="${GRAPH_VERSION}" \
        GEAFLOW_RETRIEVAL_ARTIFACT_VECTOR_SOURCE="${VECTOR_SOURCE}" \
        GEAFLOW_RETRIEVAL_ARTIFACT_VECTOR_VERSION="${VECTOR_VERSION}" \
        GEAFLOW_SERVER_PORT="${PORT}" \
        ${env_override} \
        java -cp "${SERVICE_CLASSPATH}" org.apache.geaflow.ai.GeaFlowMemoryServer \
        >"${service_log}" 2>&1 &
    service_pid=$!

    ready="0"
    for _ in $(seq 1 60); do
        if curl -fsS "http://127.0.0.1:${PORT}/ready" >"${cond_dir}/ready.json" 2>/dev/null; then
            ready="1"
            break
        fi
        sleep 1
    done
    if [[ "${ready}" != "1" ]]; then
        log "service did not become ready for ${condition}; see ${service_log}"
        kill "${service_pid}" 2>/dev/null || true
        exit 1
    fi

    # --ablation multi-hop only selects the FIXED_HYBRID/ADAPTIVE_HYBRID pair; the request itself is
    # not reshaped because apply_ablation only halves the budget, which would confound a component
    # comparison. The component difference comes from the service environment above, not from here.
    bench_args=(--url "http://127.0.0.1:${PORT}/api/v1/retrievals"
                --limit "${LIMIT}" --ablation multi-hop --max-candidates "${MAX_CANDIDATES}"
                --graph-name "${GRAPH_NAME}" --graph-version "${GRAPH_VERSION}"
                --index-version "${GRAPH_VERSION}"
                --output "${cond_dir}/${DATASET}-${condition}.csv"
                --answers-url "http://127.0.0.1:${PORT}/api/v1/answers")
    [[ -n "${GEAFLOW_VECTORS:-}" ]] && bench_args+=(--vectors "${GEAFLOW_VECTORS}")
    [[ -n "${GEAFLOW_QUERIES:-}" ]] && bench_args+=(--queries "${GEAFLOW_QUERIES}")
    [[ -n "${GOLD}" ]] && bench_args+=(--gold "${GOLD}")
    [[ -f "${BUNDLE}/chunks.jsonl" ]] && bench_args+=(--chunks "${BUNDLE}/chunks.jsonl")

    PYTHONPATH="${ROOT}${PYTHONPATH:+:${PYTHONPATH}}" \
    python3 "${ROOT}/tools/graphrag/benchmark_http.py" "${bench_args[@]}" \
        2>&1 | tee "${cond_dir}/benchmark.log"

    kill "${service_pid}" 2>/dev/null || true
    wait "${service_pid}" 2>/dev/null || true
done

log "writing combined summary"
python3 - "${OUT_DIR}" <<'PY'
import csv, glob, json, os, sys
out = sys.argv[1]
rows = []
for path in sorted(glob.glob(os.path.join(out, '*', '*-*.csv'))):
    condition = os.path.basename(os.path.dirname(path))
    with open(path, newline='', encoding='utf-8') as stream:
        for row in csv.DictReader(stream):
            row['condition'] = condition
            rows.append(row)
if not rows:
    print('no condition results found', file=sys.stderr)
    sys.exit(1)
fields = ['condition'] + [k for k in rows[0].keys() if k != 'condition']
combined = os.path.join(out, 'component-ablation.csv')
with open(combined, 'w', newline='', encoding='utf-8') as stream:
    writer = csv.DictWriter(stream, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)
print(combined)
PY

log "done: ${OUT_DIR}"