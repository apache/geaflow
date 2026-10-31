#
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
#

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class DockerPreflightTest(unittest.TestCase):
    def run_preflight(self, installed=True, fail_command=""):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            calls = directory / "calls"
            if installed:
                docker = directory / "docker"
                docker.write_text(
                    "#!/bin/bash\n"
                    'printf "%s\\n" "$*" >> "$DOCKER_TEST_CALLS"\n'
                    'if [[ "$1" == "$DOCKER_TEST_FAIL" ]]; then exit 1; fi\n'
                    'echo "Docker $1 succeeded"\n'
                )
                docker.chmod(0o755)
            environment = dict(os.environ)
            environment.update({"PATH": str(directory), "DOCKER_TEST_CALLS": str(calls),
                                "DOCKER_TEST_FAIL": fail_command})
            result = subprocess.run(
                ["/bin/bash", str(Path(__file__).with_name("check-docker.sh"))],
                env=environment, capture_output=True, text=True,
            )
            return result, calls.read_text().splitlines() if calls.exists() else []

    def test_missing_cli_fails(self):
        result, calls = self.run_preflight(installed=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Docker CLI is required", result.stderr)
        self.assertEqual(calls, [])

    def test_unreachable_daemon_fails_before_info(self):
        result, calls = self.run_preflight(fail_command="version")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Docker daemon is unreachable", result.stderr)
        self.assertEqual(calls, ["version"])

    def test_unhealthy_daemon_fails(self):
        result, calls = self.run_preflight(fail_command="info")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Docker daemon health check failed", result.stderr)
        self.assertEqual(calls, ["version", "info"])

    def test_healthy_daemon_passes(self):
        result, calls = self.run_preflight()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls, ["version", "info"])


if __name__ == "__main__":
    unittest.main()
