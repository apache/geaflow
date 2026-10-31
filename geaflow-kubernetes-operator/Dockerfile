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

# Java 11 runtime; the operator does not execute job JVMs in its own container.
FROM docker.io/library/eclipse-temurin:11-jre-jammy@sha256:0c567a8bd671331808b89a96976fabb305d57b123dec4fa62a20bfb89f2b32b9

VOLUME /tmp

ENV GEAFLOW_INSTALL_PATH=/opt
ENV GEAFLOW_HOME=$GEAFLOW_INSTALL_PATH/geaflow

ENV IS_LOCAL_MODE=false
ENV K8S_MASTER_URL=http://127.0.0.1:8000/
ENV HTTP_SERVER_PORT=8080


# copy jar
WORKDIR $GEAFLOW_HOME/
COPY target/boot/geaflow-kubernetes-operator-bootstrap-*-executable.jar \
    $GEAFLOW_HOME/boot/geaflow-kubernetes-operator-bootstrap.jar
COPY docker $GEAFLOW_HOME/

EXPOSE $HTTP_SERVER_PORT

ENTRYPOINT ["bash", "/opt/geaflow/bin/start-process.sh"]
