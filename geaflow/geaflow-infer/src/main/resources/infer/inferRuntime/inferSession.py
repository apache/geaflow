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

        self._poll_interval_sec = float(options.get("poll_interval_sec", 1.0))
        self._backoff_sec = float(options.get("backoff_sec", 10.0))
        self._warmup_enabled = bool(options.get("warmup_enabled", True))
        self._hot_reload_enabled = bool(options.get("hot_reload_enabled", True))

        self._state_lock = threading.Lock()
        self._loader_thread = None
        self._loading_version = None
        self._last_failed_version = None
        self._next_retry_ts = 0.0
        self._next_check_ts = 0.0
        self._standby_slot = None

        init_version = self._read_manifest_version() or "bootstrap"
        init_slot = self._build_slot(init_version, reuse_template=True)
        self._active_slot = init_slot

        self._logger.info(
            "infer hot reload initialized active_version=%s model_path=%s version_file=%s "
            "poll_interval_sec=%.3f backoff_sec=%.3f warmup_enabled=%s hot_reload_enabled=%s",
            init_slot.version,
            self._model_path,
            self._version_file,
            self._poll_interval_sec,
            self._backoff_sec,
            self._warmup_enabled,
            self._hot_reload_enabled,
        )

    def run(self, *inputs):
        self.maybe_reload()
        active_slot = self._get_active_slot()
        return self._run_with_slot(active_slot, *inputs)

    def maybe_reload(self):
        if not self._hot_reload_enabled:
            return

        now = time.monotonic()
        if now < self._next_check_ts:
            return

        with self._state_lock:
            now = time.monotonic()
            if now < self._next_check_ts:
                return
            self._next_check_ts = now + self._poll_interval_sec

            active_version = self._active_slot.version
            candidate_version = self._read_manifest_version()
            if not candidate_version or candidate_version == active_version:
                return
            if self._loading_version == candidate_version:
                return

            if self._last_failed_version == candidate_version and now < self._next_retry_ts:
                return

            if self._loader_thread is not None and self._loader_thread.is_alive():
                return

            self._loading_version = candidate_version
            self._loader_thread = threading.Thread(
                target=self._load_and_swap,
                args=(candidate_version,),
                name="infer-hot-reload",
                daemon=True,
            )
            self._loader_thread.start()
            self._logger.info(
                "infer hot reload scheduled candidate_version=%s active_version=%s",
                candidate_version,
                active_version,
            )

    def _get_active_slot(self):
        with self._state_lock:
            return self._active_slot

    def _load_and_swap(self, candidate_version):
        start_ts = time.monotonic()
        warmup_ms = 0
        try:
            standby_slot = self._build_slot(candidate_version)
            load_done_ts = time.monotonic()
            warmup_ms = self._warmup_slot(standby_slot)

            with self._state_lock:
                old_version = self._active_slot.version
                self._standby_slot = standby_slot
                self._active_slot = standby_slot
                self._standby_slot = None
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
                self._standby_slot = None
                self._loading_version = None
                self._last_failed_version = candidate_version
                self._next_retry_ts = fail_ts + self._backoff_sec
                active_version = self._active_slot.version

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
            return 0

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
