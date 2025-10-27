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

function checkDocker() {
  which docker &> /dev/null || { echo -e "\033[31mdocker is not installed\033[0m"; return 1; }
  docker version &> /dev/null || { echo -e "\033[31mdocker is not running\033[0m"; return 1; }
}

function checkMinikube() {
  checkDocker || return 1
  which minikube &> /dev/null || { echo -e "\033[31mminikube is not installed\033[0m"; return 1; }
  minikube status &> /dev/null || { echo -e "\033[31mminikube is not running\033[0m"; return 1; }
}

function checkMaven() {
  which java &> /dev/null || { echo -e "\033[31mjava is not installed\033[0m"; return 1; }
  which mvn &> /dev/null || { echo -e "\033[31mmaven is not installed\033[0m"; return 1; }
}

USE_UBUNTU="true"
# parse args
for arg in $*
do
  if [[ "$arg" = "--all" ]]; then
    ALL="true"
  elif [[ "$arg" =~ --module=(.*) ]]; then
    MODULE=${BASH_REMATCH[1]}
  elif [[ "$arg" =~ --output=(.*) ]]; then
    OUTPUT=${BASH_REMATCH[1]}
  elif [[ "$arg" = "--help" ]]; then
    HELP="true"
  elif [[ "$arg" = "--centos" ]]; then
    USE_UBUNTU="false"
  elif [[ "$arg" = "--ubuntu" ]]; then
    USE_UBUNTU="true"
  else
    ERROR="$arg"
  fi
done

# prepare base images
ARCH=$(uname -m)
if [[ "$ARCH" = "x86_64" ]]; then
  if [[ "$USE_UBUNTU" = "true" ]]; then
    DOCKER_FILE="Dockerfile-ubuntu"
  else
    DOCKER_FILE="Dockerfile-centos"
  fi
  GEAFLOW_IMAGE_NAME="geaflow"
  CONSOLE_IMAGE_NAME="geaflow-console"
elif [[ "$ARCH" = "arm64" || "$ARCH" = "aarch64" ]]; then
  if [[ "$USE_UBUNTU" = "true" ]]; then
    DOCKER_FILE="Dockerfile-arm64-ubuntu"
  else
    DOCKER_FILE="Dockerfile-arm64-centos"
  fi
  GEAFLOW_IMAGE_NAME="geaflow-arm"
  CONSOLE_IMAGE_NAME="geaflow-console-arm"
else
  echo -e "\033[31munknown arch $ARCH, only support x86_64,arm64,aarch64\033[0m"
  exit 1
fi

BASE_DIR=$(cd $(dirname $0); pwd)
GEAFLOW_DIR=$BASE_DIR/geaflow
GEAFLOW_DOCKER_DIR=$GEAFLOW_DIR/geaflow-deploy/docker
GEAFLOW_PACKAGE_DIR=$GEAFLOW_DIR/geaflow-deploy/geaflow-assembly/target
GEAFLOW_CONSOLE_DIR=$BASE_DIR/geaflow-console
GEAFLOW_CONSOLE_DOCKER_DIR=$GEAFLOW_CONSOLE_DIR
GEAFLOW_CONSOLE_PACKAGE_DIR=$GEAFLOW_CONSOLE_DIR/target

# print help message
if [[ -n "$ERROR" ]]; then
  echo -e "\033[31millegal argument found: $ERROR\033[0m"
fi
if [[ $# -eq 0 || -n "$HELP" || -n "$ERROR" ]]; then
  echo -n 'Usage: build.sh [-options]
Options:
    --all                       Build package and image of all modules.
    --module=<name>             Build given module name, default all. values: geaflow|geaflow-console
    --output=<type>             Build given output type, default all. values: package|image
    --help                      Show this help message.
    --centos                    Build Centos docker.
    --ubuntu                    build ubuntu docker.
'
  exit 1
fi

# prepare config
if [[ -n "$ALL" ]]; then
  checkMaven || exit 1
  checkDocker || exit 1
  MODULE=""
  OUTPUT=""
fi

if [[ -z "$MODULE" ]]; then
  BUILD_GEAFLOW="true"
  BUILD_GEAFLOW_CONSOLE="true"
  BUILD_GEAFLOW_WEB="true"
  MODULES="geaflow,geaflow-console"
  MVN_BUILD_DIR=$BASE_DIR
elif [[ "$MODULE" = "geaflow" ]]; then
  BUILD_GEAFLOW="true"
  MODULES=$MODULE
  MVN_BUILD_DIR=$GEAFLOW_DIR
elif [[ "$MODULE" = "geaflow-console" ]]; then
  BUILD_GEAFLOW_CONSOLE="true"
  MODULES=$MODULE
  MVN_BUILD_DIR=$GEAFLOW_CONSOLE_DIR
else
  echo -e "\033[31millegal module name '$MODULE'\033[0m"
  exit 1
fi

if [[ -z "$OUTPUT" ]]; then
  BUILD_PACKAGE="true"
  BUILD_IMAGE="true"
  OUTPUTS="package,image"
elif [[ "$OUTPUT" = "package" ]]; then
  BUILD_PACKAGE="true"
  OUTPUTS="package"
elif [[ "$OUTPUT" = "image" ]]; then
  BUILD_IMAGE="true"
  OUTPUTS="image"
else
  echo -e "\033[31millegal output type '$OUTPUT'\033[0m"
  exit 1
fi
echo "prepare build $OUTPUTS for $MODULES ..."

function buildJarPackage() {
  echo -e "\033[32mrun maven build in $MVN_BUILD_DIR ...\033[0m"

  checkMaven || return 1

  cd $MVN_BUILD_DIR
  mvn clean install -DskipTests -Dcheckstyle.skip -T4 || return 1
}


function buildGeaflowImage() {
  echo -e "\033[32mbuild geaflow image ...\033[0m"
  ARCHIVE=$(find $GEAFLOW_PACKAGE_DIR -name 'geaflow-*-bin.tar.gz')
  if [[ ! -f $ARCHIVE ]]; then
    echo -e "\033[31mgeaflow tar not found, please build with " \
    "--module=geaflow --output=package\033[0m"
    return 1
  fi

  checkMinikube
  MINIKUBE_INSTALLED=$?

  TMPDIR=${GEAFLOW_DOCKER_DIR}/_TMP_
  PACKAGE_NAME=geaflow

  cleanup() {
      echo "cleanup:${TMPDIR}"
      rm -rf "${TMPDIR}"
  }
  trap cleanup EXIT

  mkdir -p "${TMPDIR}"

  cp "${ARCHIVE}" "${TMPDIR}/"
  GEAFLOW_ENGINE_FILE=$(find ${TMPDIR} -name 'geaflow-*-bin.tar.gz')

  GEAFLOW_ENGINE_TAR=_TMP_/geaflow.tar.gz

  cd $GEAFLOW_DOCKER_DIR
  mv ${GEAFLOW_ENGINE_FILE} ${GEAFLOW_ENGINE_TAR}
  if [[ $MINIKUBE_INSTALLED = "0" ]]; then
    echo "build geaflow image in minikube env"
    eval $(minikube docker-env 2> /dev/null) &> /dev/null
    docker build -f $DOCKER_FILE --network=host --build-arg geaflow_engine_tar=${GEAFLOW_ENGINE_TAR} -t $GEAFLOW_IMAGE_NAME:0.1 .
    RETURN_CODE=$?
    eval $(minikube docker-env --unset 2> /dev/null) &> /dev/null
  else
    echo -e '\033[31mbuild geaflow image in local env\033[0m'
    docker build -f $DOCKER_FILE --network=host --build-arg geaflow_engine_tar=${GEAFLOW_ENGINE_TAR} -t $GEAFLOW_IMAGE_NAME:0.1 .
    RETURN_CODE=$?
  fi

  return $RETURN_CODE
}

function buildGeaflowConsoleImage() {
  echo -e "\033[32mbuild geaflow-console image ...\033[0m"
  eval $(minikube docker-env --unset 2> /dev/null) &> /dev/null
  JAR=$(find $GEAFLOW_PACKAGE_DIR -name 'geaflow-assembly-*.jar' | grep -v '\-sources\.jar')
  if [[ ! -f $JAR ]]; then
    echo -e "\033[31mgeaflow jar not found, please build with " \
      "--module=geaflow --output=package\033[0m"
    return 1
  fi
  JAR=$(find $GEAFLOW_CONSOLE_PACKAGE_DIR -name 'geaflow-console-bootstrap-*-executable.jar')
  if [[ ! -f $JAR ]]; then
    echo -e "\033[31mgeaflow-console jar not found, please build with" \
      "--module=geaflow-console --output=package\033[0m"
    return 1
  fi

  checkDocker || return 1

  cd $BASE_DIR
  docker build -f geaflow-console/$DOCKER_FILE --network=host -t $CONSOLE_IMAGE_NAME:0.1 .
}

# build package
if [[ -n $BUILD_PACKAGE ]]; then
  if [[ -n "$BUILD_GEAFLOW" || -n "$BUILD_GEAFLOW_CONSOLE" ]]; then
    buildJarPackage || exit $?
  fi
fi

# build image
if [[ -n "$BUILD_IMAGE" ]]; then
  if [[ -n "$BUILD_GEAFLOW" ]]; then
    buildGeaflowImage || exit $?
  fi

  if [[ -n "$BUILD_GEAFLOW_CONSOLE" ]]; then
    buildGeaflowConsoleImage || exit $?
  fi
fi

echo -e "\033[32mbuild success !\033[0m"