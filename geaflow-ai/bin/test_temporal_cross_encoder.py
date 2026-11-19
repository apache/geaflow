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

"""Protocol tests use only the standard library and never load model weights."""

import hashlib
from contextlib import nullcontext
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch


MODULE_PATH = Path(__file__).with_name("score-temporal-cross-encoder.py")
SPEC = importlib.util.spec_from_file_location("temporal_cross_encoder", MODULE_PATH)
SCORER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SCORER)


class CrossEncoderProtocolTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.input = self.root / "input.jsonl"
        self.output = self.root / "scores.jsonl"
        self.metadata = self.root / "metadata.json"

    @staticmethod
    def query(query_id="q1"):
        return {"schemaVersion": 1, "queryId": query_id, "queryText": "地球 is warming?",
                "poolSha256": "a" * 64,
                "candidates": [{"evidenceId": "e2", "text": "Second evidence"},
                               {"evidenceId": "e1", "text": "First evidence"}]}

    def write_input(self, rows):
        self.input.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
                              encoding="utf-8")

    def test_rejects_unexpected_fields_types_and_candidate_limits(self):
        invalid = []
        row = self.query()
        row["gold"] = ["e1"]
        invalid.append(row)
        row = self.query()
        row["candidates"][0]["label"] = "SUPPORTS"
        invalid.append(row)
        row = self.query()
        row["schemaVersion"] = True
        invalid.append(row)
        row = self.query()
        row["poolSha256"] = "not-a-fingerprint"
        invalid.append(row)
        row = self.query()
        row["queryText"] = " "
        invalid.append(row)
        row = self.query()
        row["candidates"] = [{"evidenceId": str(index), "text": "text"} for index in range(101)]
        invalid.append(row)
        for row in invalid:
            with self.subTest(row=row):
                self.write_input([row])
                with self.assertRaises(ValueError):
                    SCORER.read_queries(self.input)

    def test_rejects_duplicate_query_evidence_and_json_fields(self):
        self.write_input([self.query(), self.query()])
        with self.assertRaisesRegex(ValueError, "Duplicate queryId"):
            SCORER.read_queries(self.input)
        row = self.query()
        row["candidates"][1]["evidenceId"] = "e2"
        self.write_input([row])
        with self.assertRaisesRegex(ValueError, "Duplicate evidenceId"):
            SCORER.read_queries(self.input)
        self.input.write_text('{"schemaVersion":1,"schemaVersion":1}\n', encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Duplicate JSON field"):
            SCORER.read_queries(self.input)

    def test_scores_only_text_preserves_order_hashes_and_progress(self):
        self.write_input([self.query("q" + str(index)) for index in range(11)])
        rows, fingerprint = SCORER.read_queries(self.input)
        calls = []

        def fake(query_text, evidence_texts):
            calls.append((query_text, evidence_texts))
            return [-2.5, 3.25]

        progress = io.StringIO()
        result = SCORER.write_scores(rows, fingerprint, self.output, self.metadata, fake,
                                     versions={"torch": "fake", "transformers": "fake"},
                                     progress=progress)
        output = [json.loads(line) for line in self.output.read_text(encoding="utf-8").splitlines()]
        self.assertEqual(calls[0], ("地球 is warming?", ["Second evidence", "First evidence"]))
        self.assertEqual(output[0]["scores"], [{"evidenceId": "e2", "logit": -2.5},
                                                {"evidenceId": "e1", "logit": 3.25}])
        self.assertEqual(output[0]["poolSha256"], "a" * 64)
        self.assertGreaterEqual(output[0]["elapsedNanos"], 0)
        self.assertEqual(result["inputSha256"], hashlib.sha256(self.input.read_bytes()).hexdigest())
        self.assertEqual(result["scoresSha256"], hashlib.sha256(self.output.read_bytes()).hexdigest())
        self.assertEqual(result["pairCount"], 22)
        self.assertEqual(result["queryCount"], 11)
        self.assertEqual(result["modelId"], SCORER.MODEL_ID)
        self.assertEqual(result["modelRevision"], SCORER.MODEL_REVISION)
        self.assertEqual(json.loads(self.metadata.read_text(encoding="utf-8")), result)
        self.assertIn("Scored 1/11", progress.getvalue())
        self.assertIn("Scored 10/11", progress.getvalue())
        self.assertIn("Scored 11/11", progress.getvalue())

    def test_empty_candidate_query_is_preserved_without_inference(self):
        row = self.query()
        row["candidates"] = []
        self.write_input([row])
        rows, fingerprint = SCORER.read_queries(self.input)

        def forbidden(*unused):
            self.fail("An empty candidate pool must not call the scorer")

        result = SCORER.write_scores(rows, fingerprint, self.output, self.metadata, forbidden)
        self.assertEqual(result["pairCount"], 0)
        output = json.loads(self.output.read_text(encoding="utf-8"))
        self.assertEqual(output["scores"], [])
        self.assertEqual(output["elapsedNanos"], 0)

    def test_default_limit_is_100_and_larger_pools_require_explicit_bound(self):
        row = self.query()
        row["candidates"] = [{"evidenceId": "e" + str(index), "text": "evidence " + str(index)}
                             for index in range(500)]
        self.write_input([row])
        with self.assertRaisesRegex(ValueError, "at most 100 entries"):
            SCORER.read_queries(self.input)
        count, pairs, digest = SCORER.scan_queries(self.input, 500)
        self.assertEqual((count, pairs), (1, 500))
        self.assertEqual(digest, SCORER.file_sha256(self.input))
        for limit in (99, 1001, True):
            with self.subTest(limit=limit), self.assertRaisesRegex(ValueError, "100 and 1000"):
                list(SCORER.iter_queries(self.input, limit))
        row["candidates"].append({"evidenceId": "e500", "text": "one too many"})
        self.write_input([row])
        with self.assertRaisesRegex(ValueError, "at most 500 entries"):
            list(SCORER.iter_queries(self.input, 500))

    def test_streaming_iterator_does_not_preload_later_queries(self):
        self.write_input([self.query()])
        with self.input.open("a", encoding="utf-8") as source:
            source.write('{"not":"a query"}\n')
        rows = SCORER.iter_queries(self.input)
        self.assertEqual(next(rows)["queryId"], "q1")
        with self.assertRaisesRegex(ValueError, "Input line 2"):
            next(rows)

    def test_main_streams_500_pools_and_hashes_without_whole_file_read(self):
        first, second = self.query("q1"), self.query("q2")
        for row in (first, second):
            row["candidates"] = [{"evidenceId": "e" + str(index), "text": "证据 " + str(index)}
                                 for index in range(500)]
        self.write_input([first, second])
        calls = []

        def fake(query, texts):
            calls.append(len(texts))
            return [index / 10 for index in range(len(texts))]

        with patch.object(SCORER, "check_environment", return_value={}), \
                patch.object(SCORER, "TransformerScorer", return_value=fake), \
                patch.object(SCORER, "read_queries", side_effect=AssertionError("preloaded input")), \
                patch.object(Path, "read_bytes", side_effect=AssertionError("whole-file read")), \
                patch("sys.stderr", new=io.StringIO()) as progress:
            result = SCORER.main(["--input", str(self.input), "--output", str(self.output),
                                  "--metadata", str(self.metadata), "--cache-dir", str(self.root),
                                  "--max-candidates", "500"])
        self.assertEqual(result, 0)
        self.assertEqual(calls, [500, 500])
        self.assertIn("Scored 2/2 queries", progress.getvalue())
        metadata = json.loads(self.metadata.read_text(encoding="utf-8"))
        self.assertEqual(metadata["maxCandidates"], 500)
        self.assertEqual(metadata["queryCount"], 2)
        self.assertEqual(metadata["pairCount"], 1000)
        self.assertEqual(metadata["inputSha256"], SCORER.file_sha256(self.input))
        self.assertEqual(metadata["scoresSha256"], SCORER.file_sha256(self.output))
        self.assertEqual(metadata["timingScope"], "complete_pool")
        self.assertEqual(metadata["peakRssScope"], "process_lifetime_including_model_load")
        self.assertGreater(metadata["peakRssBytes"], 0)
        self.assertGreaterEqual(metadata["scoringWallNanos"], metadata["inferenceNanos"])
        self.assertIn("excludes model load", metadata["scoringWallScope"])

    def test_generator_without_length_and_count_mismatch_completion(self):
        rows = (self.query("q" + str(index)) for index in range(2))
        result = SCORER.write_scores(rows, "a" * 64, self.output, self.metadata,
                                     lambda query, texts: [-1, 1])
        self.assertEqual(result["queryCount"], 2)
        self.assertEqual(result["pairCount"], 4)
        self.output.unlink()
        self.metadata.unlink()
        with self.assertRaisesRegex(ValueError, "query count changed"):
            SCORER.write_scores(iter([self.query()]), "a" * 64, self.output, self.metadata,
                                 lambda query, texts: [-1, 1], query_count=2)
        self.assertFalse(self.metadata.exists())

    def test_input_change_after_validation_does_not_publish_completion(self):
        row = self.query()
        self.write_input([row])
        digest = SCORER.file_sha256(self.input)

        def tamper(query, texts):
            changed = self.query()
            changed["queryText"] = "changed while scoring"
            self.write_input([changed])
            return [-1, 1]

        with self.assertRaisesRegex(ValueError, "Input changed after validation"):
            SCORER.write_scores(iter([row]), digest, self.output, self.metadata, tamper,
                                 input_path=self.input)
        self.assertTrue(self.output.exists())
        self.assertFalse(self.metadata.exists())

    @unittest.skipUnless(SCORER.os.name == "nt", "Windows memory API")
    def test_windows_peak_rss_uses_pointer_sized_process_handle(self):
        import ctypes
        from ctypes import wintypes

        wide_handle = (1 << 48) + 17
        kernel, psapi = Mock(), Mock()
        kernel.GetCurrentProcess.return_value = wide_handle

        def memory(handle, counters, size):
            self.assertEqual(handle, wide_handle)
            self.assertEqual(size, ctypes.sizeof(counters._obj))
            counters._obj.peak_working_set = 123456
            return 1

        psapi.GetProcessMemoryInfo.side_effect = memory
        with patch.object(ctypes, "WinDLL", side_effect=[kernel, psapi]):
            self.assertEqual(SCORER.peak_rss_bytes(), 123456)
        self.assertIs(kernel.GetCurrentProcess.restype, wintypes.HANDLE)
        self.assertIs(psapi.GetProcessMemoryInfo.argtypes[0], wintypes.HANDLE)

    def test_invalid_scores_leave_no_completion_metadata(self):
        for values in ([float("nan"), 0], [float("inf"), 0], [True, 0], ["1", 0], [1]):
            with self.subTest(values=values):
                self.output.unlink(missing_ok=True)
                with self.assertRaises(ValueError):
                    SCORER.write_scores([self.query()], "a" * 64, self.output, self.metadata,
                                         lambda query, texts: values)
                self.assertFalse(self.metadata.exists())

    def test_model_failure_leaves_no_completion_metadata(self):
        def fail(query, texts):
            raise RuntimeError("fake inference failure")

        with self.assertRaisesRegex(RuntimeError, "fake inference failure"):
            SCORER.write_scores([self.query()], "a" * 64, self.output, self.metadata, fail)
        self.assertFalse(self.metadata.exists())

    def test_refuses_overwriting_either_output(self):
        for existing in (self.output, self.metadata):
            with self.subTest(existing=existing):
                existing.write_text("keep this file", encoding="utf-8")
                with self.assertRaises(FileExistsError):
                    SCORER.write_scores([self.query()], "a" * 64, self.output, self.metadata,
                                         lambda query, texts: [0, 0])
                self.assertEqual(existing.read_text(encoding="utf-8"), "keep this file")
                existing.unlink()
        with self.assertRaises(ValueError):
            SCORER.write_scores([self.query()], "a" * 64, self.output, self.output,
                                 lambda query, texts: [0, 0])

    def test_check_environment_does_not_load_model(self):
        with patch.object(SCORER, "check_environment", return_value={"torch": "2.8.0+cpu"}), \
                patch.object(SCORER, "TransformerScorer", side_effect=AssertionError("loaded model")), \
                patch("sys.stdout", new=io.StringIO()):
            self.assertEqual(SCORER.main(["--check-environment"]), 0)

    def test_cpu_environment_check_does_not_import_torch(self):
        with patch.object(SCORER.importlib.metadata, "version",
                          side_effect=lambda name: SCORER.DEPENDENCIES[name]), \
                patch.dict(sys.modules, {"torch": None}):
            self.assertEqual(SCORER.check_environment(), SCORER.DEPENDENCIES)

    def test_cuda_environment_requires_available_gpu_without_fallback(self):
        for available, count, cuda_version in ((False, 0, None), (True, 0, "12.8"),
                                                (True, 1, None)):
            with self.subTest(available=available, count=count, cuda_version=cuda_version):
                torch = SimpleNamespace(cuda=SimpleNamespace(is_available=lambda: available,
                                                             device_count=lambda: count),
                                        version=SimpleNamespace(cuda=cuda_version))
                with patch.object(SCORER.importlib.metadata, "version",
                                  side_effect=lambda name: SCORER.DEPENDENCIES[name]), \
                        patch.dict(sys.modules, {"torch": torch}), \
                        patch.dict(SCORER.os.environ):
                    with self.assertRaisesRegex(RuntimeError, "-Device cuda"):
                        SCORER.check_environment("cuda")

    def test_cuda_environment_reports_actual_device_without_loading_model(self):
        properties = SimpleNamespace(name="Fake GPU", total_memory=8589934592)
        torch = SimpleNamespace(cuda=SimpleNamespace(is_available=lambda: True,
                                                     device_count=lambda: 1,
                                                     get_device_properties=lambda index: properties),
                                version=SimpleNamespace(cuda="12.8"))
        with patch.object(SCORER.importlib.metadata, "version",
                          side_effect=lambda name: SCORER.DEPENDENCIES[name]), \
                patch.dict(sys.modules, {"torch": torch}), patch.dict(SCORER.os.environ), \
                patch.object(SCORER, "TransformerScorer", side_effect=AssertionError("loaded model")):
            result = SCORER.check_environment("cuda")
            self.assertEqual(result["cudaVersion"], "12.8")
            self.assertEqual(result["deviceName"], "Fake GPU")
            self.assertEqual(result["deviceMemoryBytes"], 8589934592)
            self.assertEqual(SCORER.os.environ["CUBLAS_WORKSPACE_CONFIG"], ":4096:8")

    def test_cuda_scoring_synchronizes_timer_and_records_device(self):
        events = []
        scorer = Mock(side_effect=lambda query, texts: events.append("score") or [-1, 1])
        scorer.synchronize.side_effect = lambda: events.append("sync")
        ticks = iter((10, 30))

        def clock():
            events.append("clock")
            return next(ticks)

        versions = {"torch": "2.8.0+cu128", "transformers": "4.57.1", "cudaVersion": "12.8",
                    "deviceName": "Fake GPU", "deviceMemoryBytes": 8589934592}
        with patch.object(SCORER.time, "perf_counter_ns", side_effect=clock):
            result = SCORER.write_scores([self.query()], "a" * 64, self.output, self.metadata,
                                         scorer, versions=versions, device="cuda")
        self.assertEqual(events, ["sync", "clock", "score", "sync", "clock"])
        self.assertEqual(result["device"], "cuda")
        self.assertEqual(result["cudaVersion"], "12.8")
        self.assertEqual(result["deviceName"], "Fake GPU")
        self.assertEqual(result["deviceMemoryBytes"], 8589934592)
        self.assertEqual(result["inferenceNanos"], 20)

    def test_cuda_memory_telemetry_reports_allocated_reserved_and_scope(self):
        scorer = Mock(return_value=[-1, 1])
        scorer.memory_metrics.return_value = {"cudaPeakAllocatedBytes": 1234,
                                              "cudaPeakReservedBytes": 2048,
                                              "cudaPeakScope": "process_lifetime_including_model_load"}
        versions = {"cudaVersion": "12.8", "deviceName": "Fake GPU",
                    "deviceMemoryBytes": 8589934592}
        with patch.object(SCORER, "peak_rss_bytes", return_value=4096):
            result = SCORER.write_scores(iter([self.query()]), "a" * 64,
                                         self.output, self.metadata, scorer,
                                         versions=versions, device="cuda")
        self.assertEqual(result["peakRssBytes"], 4096)
        self.assertEqual(result["cudaPeakAllocatedBytes"], 1234)
        self.assertEqual(result["cudaPeakReservedBytes"], 2048)
        self.assertEqual(result["cudaPeakScope"], "process_lifetime_including_model_load")
        self.assertEqual(result["timingScope"], "complete_pool")
        self.assertIn("every candidate", result["queryElapsedNanosScope"])

    def test_cuda_scoring_requires_device_metadata_before_output(self):
        with self.assertRaisesRegex(ValueError, "cudaVersion"):
            SCORER.write_scores([self.query()], "a" * 64, self.output, self.metadata,
                                 Mock(), device="cuda")
        self.assertFalse(self.output.exists())
        self.assertFalse(self.metadata.exists())

    def test_transformer_moves_model_and_tokens_to_selected_device(self):
        tensor = Mock()
        tensor.to.return_value = tensor
        logits = Mock()
        logits.shape = (2, 1)
        logits.__getitem__ = Mock(return_value=SimpleNamespace(
            cpu=lambda: SimpleNamespace(tolist=lambda: [-1.5, 2.0])))
        model = Mock()
        model.config.num_labels = 1
        model.return_value.logits = logits
        tokenizer = Mock(return_value={"input_ids": tensor, "attention_mask": tensor})
        torch = SimpleNamespace(manual_seed=Mock(), set_num_threads=Mock(),
                                set_num_interop_threads=Mock(), use_deterministic_algorithms=Mock(),
                                float32="float32", inference_mode=nullcontext,
                                cuda=SimpleNamespace(synchronize=Mock(),
                                                     max_memory_allocated=Mock(return_value=8192),
                                                     max_memory_reserved=Mock(return_value=16384)),
                                backends=SimpleNamespace(cuda=SimpleNamespace(matmul=SimpleNamespace()),
                                                         cudnn=SimpleNamespace()))
        transformers = SimpleNamespace(
            AutoTokenizer=SimpleNamespace(from_pretrained=Mock(return_value=tokenizer)),
            AutoModelForSequenceClassification=SimpleNamespace(from_pretrained=Mock(return_value=model)))
        with patch.dict(sys.modules, {"torch": torch, "transformers": transformers}), \
                patch.dict(SCORER.os.environ):
            scorer = SCORER.TransformerScorer(self.root, 16, 512, 1, True, device="cuda")
            self.assertEqual(scorer("query", ["first", "second"]), [-1.5, 2.0])
            scorer.synchronize()
            self.assertEqual(scorer.memory_metrics(), {
                "cudaPeakAllocatedBytes": 8192, "cudaPeakReservedBytes": 16384,
                "cudaPeakScope": "process_lifetime_including_model_load"})
        model.to.assert_called_once_with(device="cuda", dtype="float32")
        self.assertEqual(tensor.to.call_count, 2)
        tensor.to.assert_called_with(device="cuda")
        torch.cuda.synchronize.assert_called_once_with()
        self.assertFalse(torch.backends.cuda.matmul.allow_tf32)
        self.assertFalse(torch.backends.cudnn.allow_tf32)
        expected = {"revision": SCORER.MODEL_REVISION, "cache_dir": str(self.root),
                    "local_files_only": True, "trust_remote_code": False}
        transformers.AutoTokenizer.from_pretrained.assert_called_once_with(SCORER.MODEL_ID,
                                                                           **expected)
        transformers.AutoModelForSequenceClassification.from_pretrained.assert_called_once_with(
            SCORER.MODEL_ID, **expected, use_safetensors=True, attn_implementation="eager",
            dtype="float32")

    def test_main_passes_device_and_waits_for_model_load(self):
        self.write_input([self.query()])
        scorer = Mock(return_value=[-1, 1])
        versions = {"cudaVersion": "12.8", "deviceName": "Fake GPU",
                    "deviceMemoryBytes": 8589934592}
        with patch.object(SCORER, "check_environment", return_value=versions) as check, \
                patch.object(SCORER, "TransformerScorer", return_value=scorer) as create, \
                patch("sys.stderr", new=io.StringIO()):
            result = SCORER.main(["--input", str(self.input), "--output", str(self.output),
                                  "--metadata", str(self.metadata), "--cache-dir", str(self.root),
                                  "--device", "cuda"])
        self.assertEqual(result, 0)
        check.assert_called_once_with("cuda")
        create.assert_called_once_with(self.root, 16, 512, 1, False, device="cuda")
        self.assertEqual(scorer.synchronize.call_count, 3)


if __name__ == "__main__":
    unittest.main()
