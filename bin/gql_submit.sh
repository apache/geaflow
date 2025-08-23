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

if [ -z "$JAVA_HOME" ] ; then
  JAVACMD=`which java`
else
  JAVACMD="$JAVA_HOME/bin/java"
fi

if [ ! -x "$JAVACMD" ] ; then
  echo "The JAVA_HOME environment variable is not defined correctly" >&2
  exit 1
fi

DIR=`dirname "$0"`
DIR=`cd "$DIR"; pwd`

# build classpath
GEAFLOW_JAR_DIR=${DIR}/../geaflow/geaflow-deploy/geaflow-assembly/target/

echo $GEAFLOW_JAR_DIR
FAT_JAR=`ls $GEAFLOW_JAR_DIR | grep "^geaflow-assembly-.*[^sources].jar"`
CLASSPATH="$GEAFLOW_JAR_DIR/$FAT_JAR"

TMP_DIR=/tmp/geaflow
GEAFLOW_LOG_DIR=${TMP_DIR}/logs
GEAFLOW_LOG_PATH=$GEAFLOW_LOG_DIR/local.log
mkdir -p $GEAFLOW_LOG_DIR

LOG4j_PROPERTIES_FILE_NAME=log4j.properties

while [ "$1" ]; do
  case "$1" in
  --gql)
    GQL_FILE="$2"
    shift
    ;;
  --profiler)
    ASYNC_PROFILER_SHELL_PATH="$2"
    shift
    ;;
  --args)
    JOB_ARGS="$2"
    shift
    ;;
  esac
  shift
  done

agent_port=8088
master_port=8090

if ! command -v lsof &> /dev/null; then
    echo "lsof is not installed. Using default port numbers."
else
    check_port() {
        local port=$1
        if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
            return 0 # 端口被占用
        else
            return 1 # 端口未被占用
        fi
    }

    find_available_port() {
        local start_port=$1
        while check_port $start_port; do
            ((start_port++))
        done
        echo $start_port
    }

    while check_port $agent_port; do
        agent_port=$(find_available_port $agent_port)
    done
    master_port=$((agent_port + 1))
    while check_port $master_port; do
        master_port=$(find_available_port $master_port)
    done
fi

DEFAULT_JOB_ARGS=$(cat <<EOF
{
  "job": {
    "geaflow.log.dir": "/tmp/geaflow/logs",
    "geaflow.agent.http.port": "${agent_port}",
    "geaflow.master.http.port": "${master_port}",
    "AGENT_PROFILER_PATH": "${ASYNC_PROFILER_SHELL_PATH}"
  }
}
EOF
)
JOB_ARGS=${JOB_ARGS:-${DEFAULT_JOB_ARGS}}
echo "JOB_ARGS:  ${JOB_ARGS}"

echo "GQL_FILE: $GQL_FILE"
mkdir -p /tmp/geaflow/gql
cat $GQL_FILE > /tmp/geaflow/gql/user.gql

CLASSPATH=$CLASSPATH:/tmp/geaflow/gql/

echo "CLASSPATH:$CLASSPATH"
echo -e "\033[32mView dashboard via http://localhost:${master_port}.
See logs via url http://localhost:${master_port}/#/components/master/logs or at local path ${GEAFLOW_LOG_PATH}\033[32m"

$JAVACMD -cp "$CLASSPATH" \
  -DclusterType=LOCAL \
  -Dlog.file=${GEAFLOW_LOG_PATH} \
  -Dlog4j.configuration=${LOG4j_PROPERTIES_FILE_NAME} \
  org.apache.geaflow.dsl.runtime.engine.GeaFlowGqlClient "${JOB_ARGS}" > ${GEAFLOW_LOG_PATH} 2>&1

echo "Finished"