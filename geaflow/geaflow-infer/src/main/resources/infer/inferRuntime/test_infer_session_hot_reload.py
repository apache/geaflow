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

import os
import sys
import tempfile
import threading
import time
import unittest

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from inferSession import TorchInferSession


def _atomic_publish(file_path, content):
    temp_path = file_path + ".tmp"
    with open(temp_path, "w", encoding="utf-8") as write_file:
        write_file.write(content)
    os.replace(temp_path, file_path)


class ReloadableTransform(object):
    load_count = 0
    fail_versions = set()
    load_sleep_sec = 0.0

    def __init__(self):
        self.input_size = 1
        self._version = ""

    def load_model(self, model_path):
        type(self).load_count += 1
        if type(self).load_sleep_sec > 0:
            time.sleep(type(self).load_sleep_sec)
        with open(model_path, "r", encoding="utf-8") as read_file:
            self._version = read_file.read().strip()
        if self._version in type(self).fail_versions:
            raise RuntimeError("failed to load model version {}".format(self._version))

        version = self._version

        def _call(data):
            return "{}:{}".format(version, data)

        return _call

    def transform_pre(self, *args):
        value = args[0]
        if value == "slow":
            time.sleep(0.2)
        return value

    def transform_post(self, value):
        return value

    def get_warmup_inputs(self):
        return ("warmup",)


class TorchInferSessionHotReloadTest(unittest.TestCase):

    def setUp(self):
        ReloadableTransform.load_count = 0
        ReloadableTransform.fail_versions = set()
        ReloadableTransform.load_sleep_sec = 0.0
        self.temp_dir = tempfile.TemporaryDirectory()
        self.model_path = os.path.join(self.temp_dir.name, "model.pt")
        self.manifest_path = os.path.join(self.temp_dir.name, "model.version")
        _atomic_publish(self.model_path, "v1")
        _atomic_publish(self.manifest_path, "v1")

    def tearDown(self):
        self.temp_dir.cleanup()

    def _build_session(self, backoff_sec=0.3):
        transform = ReloadableTransform()
        return TorchInferSession(
            transform,
            {
                "model_path": self.model_path,
                "model_version_file": self.manifest_path,
                "poll_interval_sec": 0.1,
                "backoff_sec": backoff_sec,
                "warmup_enabled": True,
                "hot_reload_enabled": True,
            },
        )

    def _wait_until(self, predicate, timeout_sec=3.0):
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            if predicate():
                return True
            time.sleep(0.02)
        return False

    def test_manifest_change_triggers_reload(self):
        session = self._build_session()
        self.assertEqual("v1:request", session.run("request"))

        _atomic_publish(self.model_path, "v2")
        _atomic_publish(self.manifest_path, "v2")

        switched = self._wait_until(lambda: session.run("request") == "v2:request")
        self.assertTrue(switched)

    def test_single_flight_reload(self):
        ReloadableTransform.load_sleep_sec = 0.2
        session = self._build_session()
        self.assertEqual(1, ReloadableTransform.load_count)

        _atomic_publish(self.model_path, "v2")
        _atomic_publish(self.manifest_path, "v2")

        calls = []

        def _trigger_reload():
            session.maybe_reload()
            calls.append(1)

        threads = [threading.Thread(target=_trigger_reload) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(6, len(calls))
        loaded = self._wait_until(lambda: ReloadableTransform.load_count >= 2)
        self.assertTrue(loaded)
        time.sleep(0.25)
        self.assertEqual(2, ReloadableTransform.load_count)

    def test_switch_does_not_affect_inflight_request(self):
        session = self._build_session()

        _atomic_publish(self.model_path, "v2")
        _atomic_publish(self.manifest_path, "v2")

        result_holder = {}

        def _run_slow_request():
            result_holder["result"] = session.run("slow")

        slow_thread = threading.Thread(target=_run_slow_request)
        slow_thread.start()
        slow_thread.join()

        self.assertEqual("v1:slow", result_holder["result"])
        switched = self._wait_until(lambda: session.run("request") == "v2:request")
        self.assertTrue(switched)

    def test_failed_reload_keeps_active_and_backoff(self):
        session = self._build_session(backoff_sec=0.4)

        ReloadableTransform.fail_versions = {"bad"}
        _atomic_publish(self.model_path, "bad")
        _atomic_publish(self.manifest_path, "bad")

        failed = self._wait_until(lambda: session.run("request") == "v1:request")
        self.assertTrue(failed)
        first_failed_count = ReloadableTransform.load_count
        self.assertGreaterEqual(first_failed_count, 2)

        time.sleep(0.1)
        session.run("request")
        self.assertEqual(first_failed_count, ReloadableTransform.load_count)

        time.sleep(0.45)
        session.run("request")
        retried = self._wait_until(lambda: ReloadableTransform.load_count > first_failed_count)
        self.assertTrue(retried)


if __name__ == "__main__":
    unittest.main()
