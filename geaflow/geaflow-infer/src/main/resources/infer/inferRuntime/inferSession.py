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

class TorchInferSession(object):
    def __init__(self, transform_class) -> None:
        self._transform = transform_class
        self._model_path = os.getcwd() + "/model.pt"
        if not hasattr(transform_class, "load_model"):
            raise RuntimeError("transform class must define load_model(model_path)")
        self._model = transform_class.load_model(self._model_path)
        self._legacy_mode = self._model is None
        if not self._legacy_mode and not callable(self._model):
            raise RuntimeError("load_model(model_path) must return a callable model")

    def run(self, *inputs):
        if self._legacy_mode:
            return self._transform.transform_post(self._transform.transform_pre(*inputs))

        feature = self._transform.transform_pre(*inputs)
        if isinstance(feature, tuple):
            model_args = feature
        elif isinstance(feature, list):
            model_args = tuple(feature)
        else:
            model_args = (feature,)
        res = self._model(*model_args)
        return self._transform.transform_post(res)
