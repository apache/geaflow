# Reproducible Dataset Ingestion

The pinned manifests cover HotpotQA official distractor Parquet and 2WikiMultiHopQA
official dev ZIP. Raw data and vector files stay in the local cache.

Install the optional Parquet reader and point the cache variable at the directory that
contains the manifest-relative paths:

```bash
python -m pip install -r tools/graphrag/requirements-ingest.txt
export GRAPH_RAG_CACHE="$PWD/tmp/graphrag/data/cache"
```

Validate either local source without downloading it:

```bash
python -m tools.graphrag.ingest.manifest_cli tools/graphrag/manifests/hotpotqa-distractor-v2.json
python -m tools.graphrag.ingest.manifest_cli tools/graphrag/manifests/2wikimultihopqa-dev-v2.json
```

Build a Python canonical release, then build and publish the native Graph/BM25/Vector
bundle with the matching precomputed chunk vectors:

```bash
python tools/graphrag/publish_bundle.py \
  --manifest tools/graphrag/manifests/hotpotqa-distractor-v2.json \
  --vectors tmp/graphrag/local_adapters/hotpotqa/chunk-vectors-gte.jsonl \
  --output tmp/graphrag/data/published/hotpotqa-distractor

python tools/graphrag/publish_bundle.py \
  --manifest tools/graphrag/manifests/2wikimultihopqa-dev-v2.json \
  --vectors tmp/graphrag/data/cache/2wikimultihopqa-dev-vectors.jsonl \
  --output tmp/graphrag/data/published/2wikimultihopqa-dev
```

Create that 2Wiki vector file from the pinned local encoder and canonical chunks first:

```bash
python tools/graphrag/embed_chunks.py \
  --chunks tmp/graphrag/data/releases/2wikimultihopqa-dev/e9b966b931bee494/chunks.jsonl \
  --model "$GTE_MODEL_DIR" \
  --output tmp/graphrag/data/cache/2wikimultihopqa-dev-vectors.jsonl \
  --device cuda
```

The command uses local caches by default. Add `--allow-download` only when an explicit
download is intended. The vector JSONL must contain one `chunkId` and numeric `vector`
array per canonical chunk. Publishing to an existing output directory reuses it only
when READY metadata is identical; conflicting output is rejected.

The native Java index builders currently accept in-memory chunk lists and vector maps.
The importer and embedding exporter stream their inputs, while native artifact building
still requires memory proportional to the corpus and vector set.
