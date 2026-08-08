
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import atexit
import copy
import logging
import os
import threading
import time
from dataclasses import dataclass

try:
    import torch
    torch.set_num_threads(1)
except ImportError:
    torch = None


@dataclass
class _ModelSlot:
    transform: object
    model: object
    version: str


class TorchInferSession(object):

    def __init__(self, transform_class, hot_reload_options=None) -> None:
        self._template_transform = transform_class
        self._transform_cls = transform_class.__class__
        self._logger = logging.getLogger(__name__)

        options = hot_reload_options or {}
        model_path = options.get("model_path")
        if model_path:
            self._model_path = model_path
        else:
            self._model_path = os.path.join(os.getcwd(), "model.pt")
        self._model_path = os.path.abspath(self._model_path)

        model_root = os.path.dirname(self._model_path)
        self._version_file = os.path.abspath(
            options.get("model_version_file") or os.path.join(model_root, "model.version")
        )

        self._poll_interval_sec = max(float(options.get("poll_interval_sec", 1.0)), 0.01)
        self._backoff_sec = max(float(options.get("backoff_sec", 10.0)), 0.0)
        self._warmup_enabled = bool(options.get("warmup_enabled", True))
        self._hot_reload_enabled = bool(options.get("hot_reload_enabled", True))

        self._state_lock = threading.Lock()
        self._watcher_stop_event = threading.Event()
        self._watcher_thread = None
        self._loading_version = None
        self._last_failed_version = None
        self._next_retry_ts = 0.0
        self._closed = False

        init_version = self._read_reload_fingerprint() or "bootstrap"
        self.model_active = self._build_slot(init_version, reuse_template=True)
        self.model_standby = self._build_slot(init_version)

        self._logger.info(
            "infer hot reload initialized active_version=%s standby_version=%s model_path=%s version_file=%s "
            "poll_interval_sec=%.3f backoff_sec=%.3f warmup_enabled=%s hot_reload_enabled=%s",
            self.model_active.version,
            self.model_standby.version if self.model_standby is not None else None,
            self._model_path,
            self._version_file,
            self._poll_interval_sec,
            self._backoff_sec,
            self._warmup_enabled,
            self._hot_reload_enabled,
        )
        if self._hot_reload_enabled:
            self._watcher_thread = threading.Thread(
                target=self._watch_reload_loop,
                name="infer-hot-reload-watcher",
                daemon=True,
            )
            self._watcher_thread.start()
        atexit.register(self.close)

    def run(self, *inputs):
        self._ensure_open()
        active_slot = self._get_active_slot()
        return self._run_with_slot(active_slot, *inputs)

    def close(self):
        with self._state_lock:
            if self._closed:
                return
            self._closed = True
            self._watcher_stop_event.set()
            watcher_thread = self._watcher_thread
        if watcher_thread is not None and watcher_thread.is_alive() and watcher_thread is not threading.current_thread():
            watcher_thread.join(timeout=1.0)
        with self._state_lock:
            self._watcher_thread = None
            self._loading_version = None
            self._last_failed_version = None
            self._next_retry_ts = 0.0
            self.model_standby = None
            self.model_active = None

    def _ensure_open(self):
        with self._state_lock:
            if self._closed:
                raise RuntimeError("infer session already closed")

    def _watch_reload_loop(self):
        while not self._watcher_stop_event.wait(self._poll_interval_sec):
            try:
                self.maybe_reload()
            except Exception:
                self._logger.exception("infer hot reload watcher loop failed")

    def maybe_reload(self):
        if not self._hot_reload_enabled or self._closed:
            return

        now = time.monotonic()
        candidate_version = self._read_reload_fingerprint()
        if not candidate_version:
            return

        with self._state_lock:
            if self._closed:
                return
            if self._loading_version is not None:
                return
            active_slot = self.model_active
            standby_slot = self.model_standby
            active_version = active_slot.version if active_slot is not None else None
            standby_version = standby_slot.version if standby_slot is not None else None
            if active_slot is None or standby_slot is None:
                return
            if candidate_version == active_version:
                return
            if self._last_failed_version == candidate_version and now < self._next_retry_ts:
                return
            self._loading_version = candidate_version
            self._logger.info(
                "infer hot reload loading standby candidate_version=%s active_version=%s standby_version=%s",
                candidate_version,
                active_version,
                standby_version,
            )

        self._load_and_swap(candidate_version, active_slot, standby_slot)

    def _get_active_slot(self):
        with self._state_lock:
            return self.model_active

    def _load_and_swap(self, candidate_version, active_slot, standby_slot):
        start_ts = time.monotonic()
        warmup_ms = 0
        try:
            self._load_slot(standby_slot)
            load_done_ts = time.monotonic()
            warmup_ms = self._warmup_slot(standby_slot)

            with self._state_lock:
                if self._closed:
                    self._loading_version = None
                    return
                old_version = active_slot.version if active_slot is not None else None
                standby_slot.version = candidate_version
                self.model_active = standby_slot
                self.model_standby = active_slot
                self._loading_version = None
                self._last_failed_version = None
                self._next_retry_ts = 0.0

            load_ms = int((load_done_ts - start_ts) * 1000)
            self._logger.info(
                "infer hot reload switched switch_success=true candidate_version=%s active_version=%s "
                "load_ms=%s warmup_ms=%s",
                candidate_version,
                old_version,
                load_ms,
                warmup_ms,
            )
        except Exception:
            fail_ts = time.monotonic()
            with self._state_lock:
                self._loading_version = None
                self._last_failed_version = candidate_version
                self._next_retry_ts = fail_ts + self._backoff_sec
                active_version = self.model_active.version if self.model_active is not None else None

            self._logger.exception(
                "infer hot reload failed switch_success=false candidate_version=%s active_version=%s "
                "next_retry_sec=%.3f warmup_ms=%s",
                candidate_version,
                active_version,
                self._backoff_sec,
                warmup_ms,
            )

    def _build_slot(self, version, reuse_template=False):
        transform = self._build_transform(reuse_template)
        model = self._load_model(transform)
        return _ModelSlot(transform=transform, model=model, version=version)

    def _load_slot(self, slot):
        slot.model = self._load_model(slot.transform)

    def _build_transform(self, reuse_template):
        if reuse_template:
            return self._template_transform
        try:
            return self._transform_cls()
        except Exception:
            return copy.deepcopy(self._template_transform)

    def _load_model(self, transform):
        if not hasattr(transform, "load_model"):
            return getattr(transform, "model", None)

        load_result = transform.load_model(self._model_path)
        if callable(load_result):
            return load_result
        model = getattr(transform, "model", None)
        if callable(model):
            return model
        return None

    def _warmup_slot(self, slot):
        if not self._warmup_enabled:
            return 0

        warmup_inputs = self._get_warmup_inputs(slot.transform)
        if warmup_inputs is None:
            raise RuntimeError("infer hot reload warmup inputs missing")

        start_ts = time.monotonic()
        self._run_with_slot(slot, *warmup_inputs)
        return int((time.monotonic() - start_ts) * 1000)

    def _get_warmup_inputs(self, transform):
        warmup_func = getattr(transform, "get_warmup_inputs", None)
        if not callable(warmup_func):
            return None

        inputs = warmup_func()
        if inputs is None:
            return None
        if isinstance(inputs, tuple):
            return inputs
        if isinstance(inputs, list):
            return tuple(inputs)
        return (inputs,)

    def _run_with_slot(self, slot, *inputs):
        pre_result = slot.transform.transform_pre(*inputs)
        if slot.model is not None:
            model_inputs = self._extract_model_inputs(pre_result)
            model_result = self._invoke_model(slot.model, model_inputs)
            return slot.transform.transform_post(model_result)
        return slot.transform.transform_post(self._extract_post_inputs(pre_result))

    def _extract_post_inputs(self, pre_result):
        if isinstance(pre_result, (tuple, list)) and len(pre_result) == 2:
            return pre_result[0]
        return pre_result

    def _extract_model_inputs(self, pre_result):
        if isinstance(pre_result, (tuple, list)) and len(pre_result) == 2:
            return pre_result[1]
        return pre_result

    def _invoke_model(self, model, model_inputs):
        if isinstance(model_inputs, tuple):
            return model(*model_inputs)
        if isinstance(model_inputs, list):
            return model(*model_inputs)
        return model(model_inputs)

    def _read_reload_fingerprint(self):
        model_signature = self._file_signature(self._model_path)
        version_signature = self._file_signature(self._version_file)
        if model_signature is None and version_signature is None:
            return None

        fingerprint_parts = [
            "model={}".format(model_signature or "missing"),
            "manifest={}".format(version_signature or "missing"),
        ]
        manifest_version = self._read_manifest_version()
        if manifest_version:
            fingerprint_parts.append("token={}".format(manifest_version))
        return "|".join(fingerprint_parts)

    def _file_signature(self, path):
        try:
            stat_result = os.stat(path)
        except FileNotFoundError:
            return None
        except Exception:
            self._logger.exception("infer hot reload stat failed path=%s", path)
            return None
        return "{}:{}".format(int(stat_result.st_mtime_ns), stat_result.st_size)

    def _read_manifest_version(self):
        try:
            with open(self._version_file, "r", encoding="utf-8") as version_file:
                version = version_file.read().strip()
                return version or None
        except FileNotFoundError:
            return None
        except Exception:
            self._logger.exception(
                "infer hot reload read manifest failed version_file=%s",
                self._version_file,
            )
            return None
