"""Build a checksummed canonical GraphRAG release from generic JSON/JSONL records."""

import argparse
import hashlib
import json
from pathlib import Path

from tools.graphrag.ingest.normalize import ChunkingConfig
from tools.graphrag.ingest.pipeline import run_import
from tools.graphrag.ingest.sources.source_loader import SourceLoader, SourceManifest


def build(manifest_path, output_root, canonical_input=None, chunk_size=None, overlap=None, batch_size=256,
          vector_source=None, vector_version=None, allow_download=False):
    manifest = SourceManifest.from_file(Path(manifest_path))
    raw = json.loads(Path(manifest_path).read_text(encoding='utf-8'))
    if canonical_input:
        source = Path(canonical_input)
        manifest = SourceManifest(manifest.dataset, manifest.dataset_release, manifest.split,
                                  manifest.source_uri, str(source),
                                  _sha256(source),
                                  manifest.parser_version, manifest.license_url,
                                  manifest.license_status)
    chunking = ChunkingConfig(chunk_size or raw.get('chunk_size', 1200),
                              overlap if overlap is not None else raw.get('overlap', 120))
    result = run_import(
        manifest, Path(output_root),
        SourceLoader(batch_size=batch_size, allow_download=allow_download), chunking,
        vector_source=vector_source or raw.get('vector_source', 'unknown'),
        vector_version=vector_version or raw.get('vector_version', 'unknown'),
        random_seed=raw.get('random_seed', 0),
        entity_schema_version=raw.get('entity_schema_version', 'chunk-entity-cooccurrence-v2'),
        entity_extractor_version=raw.get('entity_extractor_version', 'text-cooccurrence-entity-v2'))
    return result


def _sha256(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', required=True, type=Path)
    parser.add_argument('--canonical-input', type=Path)
    parser.add_argument('--output-root', required=True, type=Path)
    parser.add_argument('--chunk-size', type=int, default=1200)
    parser.add_argument('--overlap', type=int, default=120)
    parser.add_argument('--batch-size', type=int, default=256)
    parser.add_argument('--vector-source', default='unknown')
    parser.add_argument('--vector-version', default='unknown')
    parser.add_argument('--allow-download', action='store_true')
    args = parser.parse_args()
    print(json.dumps(build(args.manifest, args.output_root, args.canonical_input,
                           args.chunk_size, args.overlap, args.batch_size,
                           args.vector_source, args.vector_version, args.allow_download),
                          sort_keys=True, indent=2))


if __name__ == '__main__':
    main()
