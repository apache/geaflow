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

param(
    [string]$PythonExecutable = 'python',
    [ValidateSet('cpu', 'cuda')][string]$Device = 'cpu'
)

$ErrorActionPreference = 'Stop'
$Device = $Device.ToLowerInvariant()
$taskModuleRoot = Split-Path -Parent $PSScriptRoot
$taskEnvironmentName = $(if ($Device -eq 'cuda') { 'cross-encoder-cuda-venv' } else { 'cross-encoder-venv' })
$taskEnvironmentRoot = Join-Path $taskModuleRoot ('target/' + $taskEnvironmentName)
$taskEnvironmentPython = Join-Path $taskEnvironmentRoot 'Scripts/python.exe'
$taskWorker = Join-Path $PSScriptRoot 'score-temporal-cross-encoder.py'
$taskRequirements = Join-Path $PSScriptRoot 'requirements-cross-encoder.txt'

if (Test-Path -LiteralPath $taskEnvironmentRoot) {
    if (-not (Test-Path -LiteralPath $taskEnvironmentPython -PathType Leaf)) {
        throw "Existing environment has no Python executable: $taskEnvironmentRoot. Choose a complete environment; no files were removed."
    }
} else {
    & $PythonExecutable '-c' "import sys; assert (3, 11) <= sys.version_info[:2] <= (3, 13), 'Python 3.11 through 3.13 is required'"
    if ($LASTEXITCODE -ne 0) {
        throw 'Use -PythonExecutable with Python 3.11 through 3.13. No global packages were changed.'
    }
    & $PythonExecutable '-m' 'venv' $taskEnvironmentRoot
    if ($LASTEXITCODE -ne 0) {
        throw "Could not create the local Python environment: $taskEnvironmentRoot"
    }
}

& $taskEnvironmentPython '-c' "import sys; assert (3, 11) <= sys.version_info[:2] <= (3, 13), 'Python 3.11 through 3.13 is required'"
if ($LASTEXITCODE -ne 0) {
    throw 'The existing local environment requires Python 3.11 through 3.13; it was not deleted or replaced.'
}
$taskTorchIndex = 'https://download.pytorch.org/whl/cpu'
$taskTorchVersion = 'torch==2.8.0+cpu'
if ($Device -eq 'cuda') {
    $taskTorchIndex = 'https://download.pytorch.org/whl/cu128'
    $taskTorchVersion = 'torch==2.8.0+cu128'
}
& $taskEnvironmentPython '-m' 'pip' 'install' '--index-url' $taskTorchIndex $taskTorchVersion
if ($LASTEXITCODE -ne 0) {
    throw "$Device PyTorch installation failed. Check network access and rerun this setup script."
}
& $taskEnvironmentPython '-m' 'pip' 'install' '-r' $taskRequirements
if ($LASTEXITCODE -ne 0) {
    throw 'Cross-encoder dependency installation failed. Check network access and rerun this setup script.'
}
& $taskEnvironmentPython $taskWorker '--check-environment' '--device' $Device
if ($LASTEXITCODE -ne 0) {
    throw 'Cross-encoder dependency validation failed. Review the output above; no inference was run.'
}
Write-Host "Cross-encoder $Device environment ready: $taskEnvironmentPython"
Write-Host 'The model will be downloaded to the module cache during the first evaluation.'
