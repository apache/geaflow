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

# prepare path
BASE_LOG_DIR=/tmp/logs
GEAFLOW_LOG_DIR=$BASE_LOG_DIR/geaflow
mkdir -p "$GEAFLOW_LOG_DIR"
if [[ ! -e "$GEAFLOW_HOME/logs" && ! -L "$GEAFLOW_HOME/logs" ]]; then
  ln -s "$BASE_LOG_DIR" "$GEAFLOW_HOME/logs"
fi

# Keep the JVM as PID 1 so startup failures and termination reach the container runtime.
cd "$GEAFLOW_HOME"
exec "$JAVA_HOME/bin/java" -Xmx2048m \
  -cp "$GEAFLOW_HOME/boot/geaflow-kubernetes-operator-bootstrap.jar" \
  org.springframework.boot.loader.JarLauncher \
  --is_local_mode="${IS_LOCAL_MODE:-false}" \
  --k8s_master_url="${K8S_MASTER_URL:-http://127.0.0.1:8000/}" \
  --server.port="${HTTP_SERVER_PORT:-8080}" "$@"
