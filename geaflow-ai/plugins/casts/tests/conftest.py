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

"""
Test configuration for CASTS.

Ensures required environment variables are set for configuration loading.
"""

import os
from pathlib import Path
import sys

# Ensure parent of module root is on sys.path for `import casts`.
module_root_parent = Path(__file__).resolve().parents[2]
if str(module_root_parent) not in sys.path:
    sys.path.insert(0, str(module_root_parent))

# Ensure the CASTS plugin root is importable so tests can import `core.*` and
# `harness.*` directly (pytest sets rootdir to the plugin directory).
plugin_root = Path(__file__).resolve().parents[1]
if str(plugin_root) not in sys.path:
    sys.path.insert(0, str(plugin_root))


def _ensure_env() -> None:
    os.environ.setdefault("EMBEDDING_ENDPOINT", "http://localhost")
    os.environ.setdefault("EMBEDDING_APIKEY", "test-embedding-key")
    os.environ.setdefault("EMBEDDING_MODEL", "text-embedding-v3")
    os.environ.setdefault("LLM_ENDPOINT", "http://localhost")
    os.environ.setdefault("LLM_APIKEY", "test-llm-key")
    os.environ.setdefault("LLM_MODEL", "test-llm-model")


_ensure_env()
