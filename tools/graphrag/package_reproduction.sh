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
# Package the reference reproduction assets: the READY bundle, the query vectors and the
# reference benchmark CSV of each dataset, plus a REPRODUCE.json descriptor and SHA-256
# checksums. The archives are what a reviewer downloads to reproduce the reported scores
# without rebuilding anything.
#
# The reference bundles predate build_code_revision recording, so they are published as-is:
# rebuilding them with newer code changes the graph snapshot and therefore the graph metrics.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DATA_ROOT="${GEAFLOW_DATA_ROOT:-${ROOT}/tmp/graphrag/data}"
OUT="${1:-${ROOT}/tmp/graphrag/reproduction-packages}"
WIKI_RUN="${WIKI_RUN:-${DATA_ROOT}/runs/reproduce-2wikimultihopqa-20261113T083113Z}"
HOT_RUN="${HOT_RUN:-${DATA_ROOT}/runs/reproduce-hotpotqa-20261113T085612Z}"

mkdir -p "${OUT}"

package() {
    local dataset="$1" bundle="$2" vectors="$3" run_dir="$4" csv_name="$5"
    local stage="${OUT}/${dataset}"
    [[ -f "${bundle}/READY.json" ]] || { echo "missing READY.json in ${bundle}" >&2; exit 2; }
    [[ -f "${vectors}" ]] || { echo "missing query vectors: ${vectors}" >&2; exit 2; }
    [[ -f "${run_dir}/${csv_name}" ]] || { echo "missing reference CSV: ${run_dir}/${csv_name}" >&2; exit 2; }
    rm -rf "${stage}"
    mkdir -p "${stage}/vectors" "${stage}/reference"
    cp -r "${bundle}" "${stage}/bundle"
    cp "${vectors}" "${stage}/vectors/"
    cp "${run_dir}/${csv_name}" "${stage}/reference/"
    python3 - "${dataset}" "${stage}" "${run_dir}" <<'PY'
import json, sys
from pathlib import Path
dataset, stage, run_dir = sys.argv[1:4]
ready = json.loads((Path(stage) / 'bundle' / 'READY.json').read_text())
meta = {
    'dataset': dataset,
    'bundle_version': ready['version'],
    'code_revision_of_reported_run': 'fee160853',
    'build_code_revision': ready.get('build_code_revision', 'unknown'),
    'reference_run_directory': run_dir.rsplit('/', 1)[-1],
    'notes': ('Reference bundle used for the reported scores. It predates build_code_revision '
              'recording; do not rebuild it with newer code if you need identical graph figures.'),
}
(Path(stage) / 'REPRODUCE.json').write_text(json.dumps(meta, indent=2) + '\n')
PY
    tar -czf "${OUT}/${dataset}-reproduction.tar.gz" -C "${OUT}" "${dataset}"
    rm -rf "${stage}"
    echo "packed ${dataset}-reproduction.tar.gz"
}

package 2wikimultihopqa \
    "${DATA_ROOT}/runs/2wikimultihopqa-bundle" \
    "${DATA_ROOT}/runs/2wikimultihopqa-query-vectors-gte.npz" \
    "${WIKI_RUN}" "2wikimultihopqa-six-methods.csv"
package hotpotqa \
    "${DATA_ROOT}/runs/hotpotqa-bundle-gte" \
    "${DATA_ROOT}/runs/hotpotqa-query-vectors-gte.npz" \
    "${HOT_RUN}" "hotpotqa-six-methods.csv"

( cd "${OUT}" && sha256sum ./*-reproduction.tar.gz > SHA256SUMS.txt )
echo "checksums written to ${OUT}/SHA256SUMS.txt"
cat "${OUT}/SHA256SUMS.txt"

cat <<EOF

Publish them with (requires a token that can create releases):
  gh release create graphrag-repro-v1 -R <owner>/<repo> \\
    ${OUT}/2wikimultihopqa-reproduction.tar.gz \\
    ${OUT}/hotpotqa-reproduction.tar.gz \\
    ${OUT}/SHA256SUMS.txt \\
    --title "GraphRAG reproduction bundles (reference)"
EOF
