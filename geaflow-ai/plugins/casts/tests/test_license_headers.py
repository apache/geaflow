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

from __future__ import annotations

from pathlib import Path

LICENSE_HEADER_TOKEN = "Licensed to the Apache Software Foundation (ASF) under one"

SKIP_DIRS = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "__pycache__",
    "build",
    "dist",
}


def _python_files_under(root: Path) -> list[Path]:
    python_files: list[Path] = []
    for path in root.rglob("*.py"):
        if any(part in SKIP_DIRS for part in path.parts):
            continue
        python_files.append(path)
    return python_files


def _has_license_header(path: Path) -> bool:
    # Be robust to:
    # - shebangs (`#!/usr/bin/env python3`)
    # - encoding lines (`# -*- coding: utf-8 -*-`)
    # - minor whitespace differences (`# Licensed...` vs `#  Licensed...`)
    try:
        head_lines = path.read_text(encoding="utf-8", errors="replace").splitlines()[:30]
    except OSError:
        return False
    return LICENSE_HEADER_TOKEN in "\n".join(head_lines)


def test_all_python_files_have_license_header() -> None:
    plugin_root = Path(__file__).resolve().parents[1]

    missing = sorted(
        (path.relative_to(plugin_root) for path in _python_files_under(plugin_root) if not _has_license_header(path)),
        key=lambda p: str(p),
    )

    assert not missing, (
        "Missing ASF license header in the following Python files (expected within the first 30 lines):\n"
        + "\n".join(f"- {path}" for path in missing)
    )

