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

"""
PaddlePaddle inference session for the GeaFlow-Infer framework.

This module implements PaddleInferSession, a concrete BaseInferSession that executes
user-defined TransFormFunction instances backed by PaddlePaddle / PGL models.

Key design points
-----------------
* paddle.Tensor cannot be pickled directly. All tensor data crossing the shared-memory
  bridge MUST be in numpy / Python-native form. The conversion is the responsibility
  of the user's transform_pre / transform_post methods, but PaddleInferSession also
  guards against accidental Tensor leakage at the session boundary.
* Two execution modes are supported:
  - "dynamic" (default): paddle.jit.load or plain Python eager mode – suitable for
    development and debugging.
  - "static": paddle.inference.create_predictor – suitable for production, provides
    TensorRT / MKLDNN acceleration. Activated when the transform class sets
    infer_mode = "static".
* Thread count is capped at 1 to match the single-threaded worker loop in
  infer_server.py and avoid over-subscription with multiple Python workers.
"""

import os
import traceback

import paddle

paddle.set_num_threads(1)

from baseInferSession import BaseInferSession


class PaddleInferSession(BaseInferSession):
    """
    PaddlePaddle-backed inference session.

    Constructor loads the model via the user's TransFormFunction.load_model()
    (which may use paddle.load, paddle.jit.load, or paddle.inference depending
    on the user's preference) and selects either dynamic or static inference mode.
    """

    def __init__(self, transform_class):
        """
        Args:
            transform_class: Instance of a user-defined class that inherits
                             TransFormFunction and has been already initialised
                             (including internal model loading).
        """
        super().__init__(transform_class)
        self._infer_mode = getattr(transform_class, "infer_mode", "dynamic")
        print(
            f"[PaddleInferSession] initialised. "
            f"device={paddle.get_device()}, infer_mode={self._infer_mode}"
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, *inputs):
        """
        Execute one inference round.

        Calls transform_pre() then transform_post() on the wrapped transform class.
        Any paddle.Tensor in the output is coerced to a plain Python list so that
        the pickle bridge can serialise the result without framework dependencies.

        Args:
            *inputs: Arguments forwarded from the Java side via the data bridge.

        Returns:
            Serialisable Python object (list / dict / scalar).
        """
        try:
            pre_result, aux = self._transform.transform_pre(*inputs)
            post_result = self._transform.transform_post(pre_result)
            return self._coerce_to_native(post_result)
        except paddle.fluid.core.PaddleException as paddle_err:
            raise RuntimeError(
                f"[PaddleInferSession] PaddlePaddle exception: {paddle_err}"
            ) from paddle_err
        except Exception as exc:
            raise RuntimeError(
                f"[PaddleInferSession] inference error: {exc}\n"
                + traceback.format_exc()
            ) from exc

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _coerce_to_native(obj):
        """
        Recursively convert paddle.Tensor → list so the result is picklable.

        Args:
            obj: Arbitrary Python object that may contain paddle.Tensor.

        Returns:
            obj with all paddle.Tensor replaced by Python lists.
        """
        if isinstance(obj, paddle.Tensor):
            return obj.numpy().tolist()
        if isinstance(obj, (list, tuple)):
            converted = [PaddleInferSession._coerce_to_native(item) for item in obj]
            return type(obj)(converted)
        if isinstance(obj, dict):
            return {k: PaddleInferSession._coerce_to_native(v) for k, v in obj.items()}
        return obj
