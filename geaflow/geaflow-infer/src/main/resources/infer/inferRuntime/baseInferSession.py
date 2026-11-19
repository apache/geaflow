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
Framework-agnostic abstract base class for inference sessions.

This module defines the BaseInferSession abstract class that all framework-specific
session implementations (TorchInferSession, PaddleInferSession) must inherit from.
This ensures runtime polymorphism and framework-agnostic dispatch in infer_server.py.
"""

import abc


class BaseInferSession(abc.ABC):
    """
    Abstract base class for all inference sessions.

    Concrete implementations must wrap a user-defined TransFormFunction subclass
    and expose a uniform run() interface so that infer_server.py can dispatch
    inference calls without knowing the underlying deep-learning framework.
    """

    def __init__(self, transform_class):
        """
        Initialise the session with a user-defined transform class instance.

        Args:
            transform_class: An instance of a class that inherits TransFormFunction
                             and implements load_model / transform_pre / transform_post.
        """
        self._transform = transform_class

    @abc.abstractmethod
    def run(self, *inputs):
        """
        Execute one inference round.

        Args:
            *inputs: Positional arguments unpacked from the shared-memory data bridge.
                     Typically: (vertex_id, feature_list, neighbor_features_map, ...).

        Returns:
            The post-processed result ready for serialisation back to the Java side.
        """
        raise NotImplementedError
