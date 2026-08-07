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
# Setup Python environment for GeaFlow tests

# Check if conda is available
if ! command -v conda &> /dev/null; then
    echo "Conda not found, trying to initialize it..."
    source /Users/windwheel/.zshrc || source /Users/windwheel/.bash_profile
fi

# Activate pytorch_env
eval "$(conda shell.bash hook)"
conda activate pytorch_env

# Verify Python and modules are available
echo "Python version:"
python3 --version

echo "Checking required modules..."
python3 -c "import torch; print('✓ PyTorch version:', torch.__version__)"
python3 -c "import numpy; print('✓ NumPy version:', numpy.__version__)"
python3 -c "print('✓ All required modules available')"

# Export Python executable path
export PYTHON_EXECUTABLE=$(which python3)
echo "Python executable: $PYTHON_EXECUTABLE"
