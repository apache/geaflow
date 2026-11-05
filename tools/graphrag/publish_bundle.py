"""Build a canonical release and publish its native Java retrieval bundle."""

import argparse
import json
import os
import subprocess
import tempfile
from pathlib import Path

from tools.graphrag.build_release import build


ROOT = Path(__file__).resolve().parents[2]
MAIN_CLASS = "org.apache.geaflow.ai.retrieval.ingest.CanonicalJsonlArtifactBridgeCli"


def publish(manifest_path, vectors_path, output, allow_download=False, parent_bundle=None):
    manifest_path = Path(manifest_path).resolve()
    vectors_path = Path(vectors_path).resolve()
    output = Path(output).resolve()
    manifest_config = json.loads(manifest_path.read_text(encoding="utf-8"))
    with tempfile.TemporaryDirectory(prefix="graphrag-publish-") as temporary:
        temporary = Path(temporary)
        release_root = temporary / "release"
        release = build(manifest_path, release_root, batch_size=256,
                        allow_download=allow_download)
        release_directory = release_root / release["version"]
        java_manifest = _java_manifest(release, manifest_config)
        java_manifest_path = temporary / "java-manifest.json"
        java_manifest_path.write_text(json.dumps(java_manifest, sort_keys=True), encoding="utf-8")
        classpath_file = temporary / "classpath.txt"
        _run([
            "mvn", "-pl", "geaflow-ai", "-am", "-DskipTests", "-Dmaven.javadoc.skip=true",
            "package",
            "dependency:build-classpath",
            "-Dmdep.outputFile=" + str(classpath_file),
        ])
        dependency_classpath = classpath_file.read_text(encoding="utf-8").strip()
        classpath = os.pathsep.join((str(ROOT / "geaflow-ai/target/classes"), dependency_classpath))
        _run([
            "java", "-cp", classpath, MAIN_CLASS,
            "--release", str(release_directory),
            "--output", str(output),
            "--manifest", str(java_manifest_path),
            "--vectors", str(vectors_path),
            "--graph-name", release["dataset"],
            "--graph-version", release["version"],
            "--index-version", release["version"],
            "--vector-source", release["vector_source"],
            "--vector-version", release["vector_version"],
        ] + (["--parent-bundle", str(Path(parent_bundle).resolve())]
             if parent_bundle is not None else []))
        ready = json.loads((output / "READY.json").read_text(encoding="utf-8"))
        return ready


def _java_manifest(ready, config):
    policy = ready["policy_version"]
    chunk_size = config.get("chunk_size", 1200)
    overlap = config.get("overlap", 120)
    return {
        "manifestVersion": "v1",
        "dataset": ready["dataset"],
        "datasetRelease": ready["dataset_release"],
        "split": ready["split"],
        "sourceUri": ready.get("source_uri"),
        "cachePath": ready.get("source_provenance", {}).get("resolved_cache_path"),
        "sha256": ready["sha256"],
        "preprocessingVersion": ready["preprocessing_version"],
        "chunking": {"policyVersion": policy, "chunkSize": chunk_size, "overlap": overlap},
        "graphSchemaVersion": ready["entity_schema_version"],
        "vectorSource": ready["vector_source"],
        "vectorVersion": ready["vector_version"],
        "randomSeed": ready["random_seed"],
        # The design lists the entity extraction configuration and the budget configuration among
        # the values every build must store. Both were previously dropped here, so two builds
        # differing only in extractor version or budget could not be told apart from metadata.
        "entityExtractorVersion": ready.get("entity_extractor_version"),
        "budgetConfiguration": _budget_configuration(config),
    }


def _budget_configuration(config):
    """Returns the recorded budget, or None when the manifest declares none.

    Absence must stay absent rather than defaulting: "never recorded" and "recorded as the
    standard budget" are different facts about a build.
    """
    declared = config.get("budget_configuration")
    if not declared:
        return None
    return {
        "primaryConstraint": declared.get("primary_constraint", "candidates"),
        "maxCandidates": int(declared["max_candidates"]),
        "timeoutMs": int(declared["timeout_ms"]),
        "tokenBudget": int(declared["token_budget"]),
        "maxEdgeScans": int(declared["max_edge_scans"]),
    }


def _run(command):
    subprocess.run(command, cwd=ROOT, check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--vectors", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--allow-download", action="store_true")
    parser.add_argument("--parent-bundle", type=Path,
                        help="READY bundle whose graph snapshot is the parent of this graph delta")
    args = parser.parse_args()
    ready = publish(args.manifest, args.vectors, args.output, args.allow_download,
                    args.parent_bundle)
    print(json.dumps(ready, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
