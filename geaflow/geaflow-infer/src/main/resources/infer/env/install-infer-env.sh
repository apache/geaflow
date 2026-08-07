#!/bin/bash

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

CURRENT_DIR="$(cd "$1" && pwd)"
REQUIREMENTS_PATH=$2
MINICOMDA_OSS_URL=$3
# $4: Framework type: TORCH (default) or PADDLE
FRAMEWORK_TYPE="${4:-TORCH}"
# $5: Whether to enable GPU for Paddle: true or false (default false)
PADDLE_GPU_ENABLE="${5:-false}"
# $6: CUDA version for Paddle GPU wheel selection (default 11.7)
PADDLE_CUDA_VERSION="${6:-11.7}"

PYTHON_EXEC=$CURRENT_DIR/conda/bin/python3

echo "execute shell at path ${CURRENT_DIR}"
echo "install requirements path ${REQUIREMENTS_PATH}"
echo "framework type: ${FRAMEWORK_TYPE}"

MINICONDA_INSTALL=$CURRENT_DIR/miniconda.sh
[ ! -e $MINICONDA_INSTALL ] && touch $MINICONDA_INSTALL

function install_miniconda() {
    if [ ! -f "$CONDA_INSTALL" ]; then
        print_function "STEP" "download miniconda oss ${MINICOMDA_OSS_URL}..."
        download $MINICOMDA_OSS_URL $MINICONDA_INSTALL
        chmod +x $MINICONDA_INSTALL
        if [ $? -ne 0 ]; then
            echo "Please manually chmod +x $MINICONDA_INSTALL"
            exit 1
        fi
        if [ -d "$CURRENT_DIR/conda" ]; then
            rm -rf "$CURRENT_DIR/conda"
            if [ $? -ne 0 ]; then
                echo "Please manually rm -rf $CURRENT_DIR/conda directory.\
                Then retry to exec the script."
                exit 1
            fi
        fi
        print_function "STEP" "download miniconda... [SUCCESS]"
    fi

    if [ ! -d "$CURRENT_DIR/conda" ]; then
        print_function "STEP" "installing conda..."
        $MINICONDA_INSTALL -f -b -p $CURRENT_DIR/conda >/dev/null 2>&1

        print_function "STEP" "psutil cython pip..."
        $PYTHON_EXEC -m pip install --upgrade pip >/dev/null 2>&1
        $PYTHON_EXEC -m pip install psutil >/dev/null 2>&1
        $PYTHON_EXEC -m pip install Cython >/dev/null 2>&1
        print_function "STEP" "psutil cython pip... [SUCCESS]"

        print_function "STEP" "build share memory lib package"
        cd $CURRENT_DIR/../inferFiles || exit
        $PYTHON_EXEC setup.py build_ext --inplace > /dev/null 2>&1
        print_function "STEP" "build share memory lib [SUCCESS]"

        if [ $? -ne 0 ]; then
            echo "install miniconda failed"
            exit 1
        fi
        print_function "STEP" "install conda ... [SUCCESS]"
    fi
}

# Install requirements.
function install_requirements() {
    if [ -f "${REQUIREMENTS_PATH}" ]; then
        print_function "STEP" "installing requirements..."
        max_retry_times=3
        retry_times=0
        source $CURRENT_DIR/conda/bin/activate
        install_command="conda run -p $CURRENT_DIR/conda $PYTHON_EXEC -m pip install --ignore-installed -r ${REQUIREMENTS_PATH}"
        ${install_command} >/dev/null 2>&1
        status=$?
        while [[ ${status} -ne 0 ]] && [[ ${retry_times} -lt ${max_retry_times} ]]; do
            retry_times=$((retry_times + 1))
            # sleep 3 seconds and then reinstall.
            sleep 3
            echo "$PYTHON_EXEC -m pip install -r ${REQUIREMENTS_PATH} retrying ${retry_times}/${max_retry_times}"
            ${install_command} >/dev/null 2>&1
            status=$?
        done
        if [[ ${status} -ne 0 ]]; then
            echo "python -m pip install -r ${REQUIREMENTS_PATH} failed after retrying ${max_retry_times} times.\
                You can retry to execute the script again."
            exit 1
        fi
        print_function "STEP" "$PYTHON_EXEC -m pip install -r ${REQUIREMENTS_PATH}... [SUCCESS]"
    fi
}

# Install PaddlePaddle framework (CPU or GPU) before installing pgl/paddlespatial.
# This function is invoked only when FRAMEWORK_TYPE=PADDLE.
function install_paddlepaddle() {
    print_function "STEP" "installing PaddlePaddle (gpu=${PADDLE_GPU_ENABLE}, cuda=${PADDLE_CUDA_VERSION})..."
    source $CURRENT_DIR/conda/bin/activate

    max_retry_times=3
    retry_times=0

    if [[ "${PADDLE_GPU_ENABLE}" == "true" ]]; then
        # Derive wheel post-fix from CUDA version: "11.7" -> "117", "12.0" -> "120"
        cuda_postfix=$(echo "${PADDLE_CUDA_VERSION}" | tr -d '.')
        PADDLE_WHEEL="paddlepaddle-gpu==2.6.0.post${cuda_postfix}"
        echo "Installing GPU PaddlePaddle: ${PADDLE_WHEEL}"
    else
        PADDLE_WHEEL="paddlepaddle==2.6.0"
        echo "Installing CPU PaddlePaddle: ${PADDLE_WHEEL}"
    fi

    PADDLE_INSTALL_CMD="conda run -p $CURRENT_DIR/conda $PYTHON_EXEC -m pip install ${PADDLE_WHEEL} \
        -i https://pypi.tuna.tsinghua.edu.cn/simple"

    ${PADDLE_INSTALL_CMD} >/dev/null 2>&1
    status=$?
    while [[ ${status} -ne 0 ]] && [[ ${retry_times} -lt ${max_retry_times} ]]; do
        retry_times=$((retry_times + 1))
        sleep 3
        echo "PaddlePaddle install retrying ${retry_times}/${max_retry_times}"
        ${PADDLE_INSTALL_CMD} >/dev/null 2>&1
        status=$?
    done
    if [[ ${status} -ne 0 ]]; then
        echo "PaddlePaddle installation failed after ${max_retry_times} retries."
        exit 1
    fi
    print_function "STEP" "PaddlePaddle installed... [SUCCESS]"
}

function print_function() {
    local STAGE_LENGTH=48
    local left_edge_len=
    local right_edge_len=
    local str
    case "$1" in
        "STAGE")
            left_edge_len=$(((STAGE_LENGTH-${#2})/2))
            right_edge_len=$((STAGE_LENGTH-${#2}-left_edge_len))
            str="$(seq -s "=" $left_edge_len | tr -d "[:digit:]")""$2""$(seq -s "=" $right_edge_len | tr -d "[:digit:]")"
            ;;
        "STEP")
            str="$2"
            ;;
        *)
            str="seq -s "=" $STAGE_LENGTH | tr -d "[:digit:]""
            ;;
    esac
    echo $str | tee -a $LOG_FILE
}

function download() {
    local DOWNLOAD_STATUS=
    if hash "wget" 2>/dev/null; then
        wget "$1" -O "$2" -q -T1200 -t3
        DOWNLOAD_STATUS="$?"
    else
        curl "$1" -o "$2" --progress-bar --connect-timeout 1200 --retry 3
        DOWNLOAD_STATUS="$?"
    fi
    if [ $DOWNLOAD_STATUS -ne 0 ]; then
        echo "Download failed.You can try again"
        exit $DOWNLOAD_STATUS
    fi
}


STEP=0
if [ $STEP -lt 1 ]; then
  install_miniconda
  STEP=1
  print_function "STEP" "install miniconda... [SUCCESS]"
fi

# For PADDLE framework, install PaddlePaddle BEFORE the requirements.txt
# because pgl and paddlespatial depend on paddlepaddle being present first.
if [[ "${FRAMEWORK_TYPE}" == "PADDLE" ]]; then
  install_paddlepaddle
  print_function "STEP" "install paddlepaddle... [SUCCESS]"
fi

if [ $STEP -lt 2 ]; then
  install_requirements ${REQUIREMENTS_PATH}
  STEP=2
  print_function "STEP" "install requirements... [SUCCESS]"
fi

print_function "STAGE"  "install environment... [SUCCESS]"
