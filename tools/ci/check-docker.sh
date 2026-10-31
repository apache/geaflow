#!/usr/bin/env bash
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

set -euo pipefail

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker CLI is required for the container integration tests." >&2
  exit 1
fi

# A client binary alone does not prove that the daemon is reachable.
if ! docker version; then
  echo "Docker daemon is unreachable. Check the service, socket permissions and DOCKER_HOST." >&2
  exit 1
fi
if ! docker info; then
  echo "Docker daemon health check failed; container integration tests cannot run." >&2
  exit 1
fi
