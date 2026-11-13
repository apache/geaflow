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

"""Score exported query/evidence pairs with the pinned MiniLM cross-encoder."""

import argparse
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import re
import sys
import time


MODEL_ID = "cross-encoder/ms-marco-MiniLM-L6-v2"
MODEL_REVISION = "233902d25c440f23af6f7d6e94d2946bac0bee0a"
DEPENDENCIES = {"torch": "2.8.0", "transformers": "4.57.1", "numpy": "2.3.5"}
QUERY_FIELDS = {"schemaVersion", "queryId", "queryText", "poolSha256", "candidates"}
CANDIDATE_FIELDS = {"evidenceId", "text"}


def _object_without_duplicates(pairs):
    result = {}
    for name, value in pairs:
        if name in result:
            raise ValueError("Duplicate JSON field: " + name)
        result[name] = value
    return result


def _reject_constant(value):
    raise ValueError("Non-finite JSON number: " + value)


def _require_fields(value, expected, name):
    if not isinstance(value, dict) or set(value) != expected:
        raise ValueError(name + " must contain exactly: " + ", ".join(sorted(expected)))


def _require_text(value, name):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(name + " must be a nonempty string")


def file_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def iter_queries(path, max_candidates=100):
    """Validate one query at a time without retaining all evidence texts."""
    if type(max_candidates) is not int or not 100 <= max_candidates <= 1000:
        raise ValueError("max-candidates must be between 100 and 1000")
    query_ids = set()
    with Path(path).open("r", encoding="utf-8", newline="") as source:
        for line_number, line in enumerate(source, 1):
            try:
                row = json.loads(line, object_pairs_hook=_object_without_duplicates,
                                 parse_constant=_reject_constant)
                _require_fields(row, QUERY_FIELDS, "Query")
                if type(row["schemaVersion"]) is not int or row["schemaVersion"] != 1:
                    raise ValueError("schemaVersion must be integer 1")
                _require_text(row["queryId"], "queryId")
                _require_text(row["queryText"], "queryText")
                if row["queryId"] in query_ids:
                    raise ValueError("Duplicate queryId: " + row["queryId"])
                fingerprint = row["poolSha256"]
                if not isinstance(fingerprint, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", fingerprint):
                    raise ValueError("poolSha256 must contain 64 hexadecimal characters")
                candidates = row["candidates"]
                if not isinstance(candidates, list) or len(candidates) > max_candidates:
                    raise ValueError("candidates must be a list containing at most "
                                     + str(max_candidates) + " entries")
                evidence_ids = set()
                for candidate in candidates:
                    _require_fields(candidate, CANDIDATE_FIELDS, "Candidate")
                    _require_text(candidate["evidenceId"], "evidenceId")
                    _require_text(candidate["text"], "candidate text")
                    if candidate["evidenceId"] in evidence_ids:
                        raise ValueError("Duplicate evidenceId: " + candidate["evidenceId"])
                    evidence_ids.add(candidate["evidenceId"])
                query_ids.add(row["queryId"])
                yield row
            except (ValueError, TypeError) as error:
                raise ValueError("Input line " + str(line_number) + ": " + str(error)) from error
    if not query_ids:
        raise ValueError("Input must contain at least one query")


def scan_queries(path, max_candidates=100):
    query_count = pair_count = 0
    for row in iter_queries(path, max_candidates):
        query_count += 1
        pair_count += len(row["candidates"])
    return query_count, pair_count, file_sha256(path)


def read_queries(path, max_candidates=100):
    """Compatibility helper; the scoring CLI uses the streaming iterator instead."""
    return list(iter_queries(path, max_candidates)), file_sha256(path)


def peak_rss_bytes():
    """OS process-lifetime high-water mark, including model load and Python allocations."""
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        class MemoryCounters(ctypes.Structure):
            _fields_ = [("cb", wintypes.DWORD), ("page_faults", wintypes.DWORD)] + [
                (name, ctypes.c_size_t) for name in ("peak_working_set", "working_set",
                                                    "peak_paged", "paged", "peak_nonpaged",
                                                    "nonpaged", "pagefile", "peak_pagefile")]

        counters = MemoryCounters()
        counters.cb = ctypes.sizeof(counters)
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.GetCurrentProcess.restype = wintypes.HANDLE
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        psapi.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.POINTER(MemoryCounters),
                                               wintypes.DWORD]
        if not psapi.GetProcessMemoryInfo(kernel.GetCurrentProcess(), ctypes.byref(counters), counters.cb):
            raise ctypes.WinError(ctypes.get_last_error())
        return counters.peak_working_set
    import resource

    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value if sys.platform == "darwin" else value * 1024


def check_environment(device="cpu"):
    if device not in ("cpu", "cuda"):
        raise ValueError("device must be cpu or cuda")
    versions = {}
    failures = []
    if not (3, 11) <= sys.version_info[:2] <= (3, 13):
        failures.append("Python 3.11 through 3.13 is required")
    for package, expected in DEPENDENCIES.items():
        try:
            actual = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            failures.append(package + "==" + expected + " is missing")
            continue
        versions[package] = actual
        if actual.split("+", 1)[0] != expected:
            failures.append(package + " must be " + expected + "; found " + actual)
    if failures:
        instructions = ("Run geaflow-ai/bin/setup-temporal-cross-encoder.ps1 -Device " + device)
        raise RuntimeError("; ".join(failures) + "\n" + instructions)
    if device == "cuda":
        os.environ["CUBLAS_WORKSPACE_CONFIG"] = ":4096:8"
        import torch

        if not torch.cuda.is_available() or torch.cuda.device_count() < 1 or not torch.version.cuda:
            raise RuntimeError("CUDA is unavailable. Check the NVIDIA driver and run "
                               "geaflow-ai/bin/setup-temporal-cross-encoder.ps1 -Device cuda; "
                               "CPU fallback is disabled.")
        properties = torch.cuda.get_device_properties(0)
        versions.update({"cudaVersion": torch.version.cuda, "deviceName": properties.name,
                         "deviceMemoryBytes": properties.total_memory})
    return versions


class TransformerScorer:
    def __init__(self, cache_dir, batch_size, max_length, threads, local_files_only, device="cpu"):
        if device not in ("cpu", "cuda"):
            raise ValueError("device must be cpu or cuda")
        if device == "cuda":
            os.environ["CUBLAS_WORKSPACE_CONFIG"] = ":4096:8"
        # Keep imports lazy so protocol validation and unit tests need no model dependencies.
        import torch
        from transformers import AutoModelForSequenceClassification, AutoTokenizer

        torch.manual_seed(0)
        torch.set_num_threads(threads)
        torch.set_num_interop_threads(1)
        torch.use_deterministic_algorithms(True)
        if device == "cuda":
            torch.backends.cuda.matmul.allow_tf32 = False
            torch.backends.cudnn.allow_tf32 = False
            torch.backends.cudnn.benchmark = False
        options = {"revision": MODEL_REVISION, "cache_dir": str(cache_dir),
                   "local_files_only": local_files_only, "trust_remote_code": False}
        self.tokenizer = AutoTokenizer.from_pretrained(MODEL_ID, **options)
        self.model = AutoModelForSequenceClassification.from_pretrained(
            MODEL_ID, **options, use_safetensors=True, attn_implementation="eager",
            dtype=torch.float32)
        if self.model.config.num_labels != 1:
            raise ValueError("The pinned model must have exactly one output label")
        self.model.to(device=device, dtype=torch.float32)
        self.model.eval()
        self.torch = torch
        self.batch_size = batch_size
        self.max_length = max_length
        self.device = device

    def synchronize(self):
        if self.device == "cuda":
            self.torch.cuda.synchronize()

    def memory_metrics(self):
        if self.device == "cuda":
            return {"cudaPeakAllocatedBytes": self.torch.cuda.max_memory_allocated(),
                    "cudaPeakReservedBytes": self.torch.cuda.max_memory_reserved(),
                    "cudaPeakScope": "process_lifetime_including_model_load"}
        return {}

    def __call__(self, query_text, evidence_texts):
        scores = []
        with self.torch.inference_mode():
            for start in range(0, len(evidence_texts), self.batch_size):
                texts = evidence_texts[start:start + self.batch_size]
                tokens = self.tokenizer([query_text] * len(texts), texts, padding=True,
                                        truncation=True, max_length=self.max_length,
                                        return_tensors="pt")
                tokens = {name: tensor.to(device=self.device) for name, tensor in tokens.items()}
                logits = self.model(**tokens).logits
                if tuple(logits.shape) != (len(texts), 1):
                    raise ValueError("Unexpected model output shape: " + str(tuple(logits.shape)))
                scores.extend(float(value) for value in logits[:, 0].cpu().tolist())
        return scores


def validate_output_paths(output, metadata):
    if output.resolve() == metadata.resolve():
        raise ValueError("Scores and metadata must have different paths")
    for path in (output, metadata):
        if path.exists():
            raise FileExistsError("Refusing to overwrite: " + str(path))


def write_scores(rows, input_sha256, output, metadata, scorer, batch_size=16,
                 max_length=512, threads=1, load_nanos=0, versions=None, progress=None, device="cpu",
                 query_count=None, max_candidates=100, input_path=None):
    if device not in ("cpu", "cuda"):
        raise ValueError("device must be cpu or cuda")
    if type(max_candidates) is not int or not 100 <= max_candidates <= 1000:
        raise ValueError("max-candidates must be between 100 and 1000")
    versions = versions or {}
    cuda_details = {}
    if device == "cuda":
        for key in ("cudaVersion", "deviceName"):
            _require_text(versions.get(key), key)
            cuda_details[key] = versions[key]
        memory = versions.get("deviceMemoryBytes")
        if type(memory) is not int or memory <= 0:
            raise ValueError("deviceMemoryBytes must be a positive integer")
        cuda_details["deviceMemoryBytes"] = memory
    output, metadata = Path(output), Path(metadata)
    validate_output_paths(output, metadata)
    output.parent.mkdir(parents=True, exist_ok=True)
    metadata.parent.mkdir(parents=True, exist_ok=True)
    if query_count is None:
        try:
            query_count = len(rows)
        except TypeError:
            pass
    inference_nanos = 0
    pair_count = 0
    processed_queries = 0
    scores_digest = hashlib.sha256()
    wall_started = time.monotonic_ns()
    with output.open("xb") as scores_file:
        for index, row in enumerate(rows, 1):
            candidates = row["candidates"]
            if len(candidates) > max_candidates:
                raise ValueError("Candidate pool exceeds max-candidates")
            if candidates and device == "cuda":
                scorer.synchronize()
            started = time.perf_counter_ns() if candidates else 0
            values = list(scorer(row["queryText"], [item["text"] for item in candidates])) if candidates else []
            if candidates and device == "cuda":
                scorer.synchronize()
            elapsed = time.perf_counter_ns() - started if candidates else 0
            if len(values) != len(candidates):
                raise ValueError("Scorer returned the wrong number of scores for " + row["queryId"])
            if any(type(value) not in (int, float) or not math.isfinite(value) for value in values):
                raise ValueError("Scorer returned a non-finite or nonnumeric score for " + row["queryId"])
            result = {"schemaVersion": 1, "queryId": row["queryId"],
                      "poolSha256": row["poolSha256"], "elapsedNanos": elapsed,
                      "scores": [{"evidenceId": candidate["evidenceId"], "logit": float(value)}
                                 for candidate, value in zip(candidates, values)]}
            line = (json.dumps(result, ensure_ascii=False, separators=(",", ":"),
                               allow_nan=False) + "\n").encode("utf-8")
            scores_file.write(line)
            scores_digest.update(line)
            inference_nanos += elapsed
            pair_count += len(candidates)
            processed_queries = index
            if progress is not None and (index == 1 or index % 10 == 0 or index == query_count):
                print("Scored " + str(index) + "/" + str(query_count or "?") + " queries; "
                      + str(pair_count) + " pairs",
                      file=progress, flush=True)
    if not processed_queries or (query_count is not None and processed_queries != query_count):
        raise ValueError("Input query count changed while scoring")
    if input_path is not None and file_sha256(input_path) != input_sha256:
        raise ValueError("Input changed after validation")
    memory = {}
    try:
        memory["peakRssBytes"] = peak_rss_bytes()
    except (OSError, ImportError) as error:
        memory.update({"peakRssBytes": None, "peakRssError": str(error)})
    memory["peakRssScope"] = "process_lifetime_including_model_load"
    memory["peakRssMethod"] = "GetProcessMemoryInfo.PeakWorkingSetSize" if os.name == "nt" else "getrusage.ru_maxrss"
    if hasattr(scorer, "memory_metrics"):
        measured = scorer.memory_metrics()
        if isinstance(measured, dict):
            memory.update(measured)
    result = {"schemaVersion": 1, "modelId": MODEL_ID, "modelRevision": MODEL_REVISION,
              "device": device, "batchSize": batch_size, "maxLength": max_length,
              "threads": threads, "inputSha256": input_sha256,
              "scoresSha256": scores_digest.hexdigest(),
              "queryCount": processed_queries, "pairCount": pair_count, "loadNanos": load_nanos,
              "inferenceNanos": inference_nanos, "pythonVersion": platform.python_version(),
              "torchVersion": versions.get("torch", "unknown"),
              "transformersVersion": versions.get("transformers", "unknown"),
              "scoreKind": "raw_logit", "maxCandidates": max_candidates,
              "timingScope": "complete_pool",
              "queryElapsedNanosScope": "scores.jsonl elapsedNanos measures every candidate: tokenizer+forward+device synchronization",
              "loadNanosScope": "tokenizer and model load plus selected-device transfer; excludes input validation",
              "scoringWallNanos": time.monotonic_ns() - wall_started,
              "scoringWallScope": "stream reading, inference, score writing, final input hash; excludes model load and preflight"}
    result.update(cuda_details)
    result.update(memory)
    # Metadata is the completion marker: leave it absent if scoring fails.
    with metadata.open("x", encoding="utf-8", newline="\n") as metadata_file:
        metadata_file.write(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False) + "\n")
    return result


def _positive_integer(text):
    value = int(text)
    if value <= 0:
        raise argparse.ArgumentTypeError("Value must be positive")
    return value


def _candidate_limit(text):
    value = int(text)
    if not 100 <= value <= 1000:
        raise argparse.ArgumentTypeError("max-candidates must be between 100 and 1000")
    return value


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--metadata", type=Path)
    parser.add_argument("--cache-dir", type=Path)
    parser.add_argument("--batch-size", type=_positive_integer, default=16)
    parser.add_argument("--max-length", type=_positive_integer, default=512)
    parser.add_argument("--max-candidates", type=_candidate_limit, default=100)
    parser.add_argument("--threads", type=_positive_integer, default=1)
    parser.add_argument("--device", choices=("cpu", "cuda"), default="cpu")
    parser.add_argument("--local-files-only", action="store_true")
    parser.add_argument("--check-environment", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.check_environment:
            print(json.dumps(check_environment(args.device), sort_keys=True))
            return 0
        if any(value is None for value in (args.input, args.output, args.metadata, args.cache_dir)):
            parser.error("--input, --output, --metadata and --cache-dir are required for scoring")
        if args.max_length > 512:
            raise ValueError("The pinned MiniLM model supports max-length at most 512")
        validate_output_paths(args.output, args.metadata)
        query_count, _, input_sha256 = scan_queries(args.input, args.max_candidates)
        versions = check_environment(args.device)
        print("Loading pinned " + MODEL_ID + " on " + args.device.upper() + "...",
              file=sys.stderr, flush=True)
        started = time.perf_counter_ns()
        scorer = TransformerScorer(args.cache_dir, args.batch_size, args.max_length,
                                   args.threads, args.local_files_only, device=args.device)
        if args.device == "cuda":
            scorer.synchronize()
        load_nanos = time.perf_counter_ns() - started
        write_scores(iter_queries(args.input, args.max_candidates), input_sha256,
                     args.output, args.metadata, scorer,
                     args.batch_size, args.max_length, args.threads, load_nanos,
                     versions, sys.stderr, device=args.device, query_count=query_count,
                     max_candidates=args.max_candidates, input_path=args.input)
        print("Completed local scoring: " + str(args.metadata), file=sys.stderr, flush=True)
        return 0
    except Exception as error:
        print("[ERROR] " + str(error), file=sys.stderr, flush=True)
        return 2


if __name__ == "__main__":
    sys.exit(main())
