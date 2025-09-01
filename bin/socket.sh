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
echo "CLASSPATH:$CLASSPATH"

$JAVACMD -cp "$CLASSPATH" -Dlog4j.configuration= org.apache.geaflow.dsl.connector.socket.server.SocketServer "$@"
