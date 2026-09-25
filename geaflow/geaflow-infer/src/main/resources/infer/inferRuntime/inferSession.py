#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import os
import torch
torch.set_num_threads(1)

from baseInferSession import BaseInferSession


class TorchInferSession(BaseInferSession):
    """
    PyTorch-backed inference session.

    Wraps a user-defined TransFormFunction instance that uses torch models.
    Inherits BaseInferSession to ensure framework-agnostic dispatch from infer_server.py.
    """

    def __init__(self, transform_class) -> None:
        super().__init__(transform_class)

    def run(self, *inputs):
        a, b = self._transform.transform_pre(*inputs)
        return self._transform.transform_post(a)

