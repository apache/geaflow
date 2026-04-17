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


class GCNTestTransform(object):
    input_size = 1

    def load_model(self, model_path):
        if not os.path.exists(model_path):
            raise RuntimeError("missing model file: %s" % model_path)
        # For test purpose, we don't load a real torch model. The built-in GCN UDF
        # validates the Java->Python payload protocol end-to-end.
        self._loaded = True
        return self._infer_model

    def _infer_model(self, payload):
        if not self._loaded:
            raise RuntimeError("model not loaded")

        center_node_id = payload.get("center_node_id")
        sampled_nodes = payload.get("sampled_nodes") or []
        node_features = payload.get("node_features") or []
        edge_index = payload.get("edge_index") or [[], []]
        edge_weight = payload.get("edge_weight") or []

        if center_node_id is None:
            raise RuntimeError("missing center_node_id")
        if not sampled_nodes:
            raise RuntimeError("missing sampled_nodes")
        if not node_features:
            raise RuntimeError("missing node_features")
        if center_node_id not in sampled_nodes:
            raise RuntimeError("center_node_id not in sampled_nodes")
        center_index = sampled_nodes.index(center_node_id)
        embedding = node_features[center_index] if 0 <= center_index < len(node_features) else []

        # Deterministic test output:
        # - prediction: local index + 1
        # - confidence: number of edges in edge_weight
        prediction = int(center_index) + 1
        confidence = float(len(edge_weight))

        return {
            "node_id": center_node_id,
            "embedding": embedding,
            "prediction": prediction,
            "confidence": confidence,
            "debug_edge_index_shape": [len(edge_index), len(edge_index[0]) if edge_index else 0],
        }

    def transform_pre(self, payload):
        if payload is None:
            raise RuntimeError("missing payload")
        return (payload,)

    def transform_post(self, res):
        return res


class GCNBatchMarkerTransform(object):
    input_size = 1

    def load_model(self, model_path):
        if not os.path.exists(model_path):
            raise RuntimeError("missing model file: %s" % model_path)
        return self._infer_model

    def _infer_model(self, payload):
        center_node_id = payload.get("center_node_id")
        sampled_nodes = payload.get("sampled_nodes") or []
        node_features = payload.get("node_features") or []
        if center_node_id not in sampled_nodes:
            raise RuntimeError("center_node_id not in sampled_nodes")
        center_index = sampled_nodes.index(center_node_id)
        embedding = node_features[center_index] if 0 <= center_index < len(node_features) else []
        return {
            "node_id": center_node_id,
            "embedding": embedding,
            "prediction": int(center_node_id),
            "confidence": float(center_node_id),
        }

    def transform_pre(self, payload):
        return (payload,)

    def transform_post(self, res):
        return res


class GCNDirectedEdgeCountTransform(object):
    input_size = 1

    def load_model(self, model_path):
        if not os.path.exists(model_path):
            raise RuntimeError("missing model file: %s" % model_path)
        return self._infer_model

    def _infer_model(self, payload):
        center_node_id = payload.get("center_node_id")
        sampled_nodes = payload.get("sampled_nodes") or []
        node_features = payload.get("node_features") or []
        edge_index = payload.get("edge_index") or [[], []]
        if center_node_id not in sampled_nodes:
            raise RuntimeError("center_node_id not in sampled_nodes")
        center_index = sampled_nodes.index(center_node_id)
        outgoing_edges = 0
        row = edge_index[0] if len(edge_index) > 0 else []
        col = edge_index[1] if len(edge_index) > 1 else []
        for src, target in zip(row, col):
            if src == center_index and target != center_index:
                outgoing_edges += 1
        embedding = node_features[center_index] if 0 <= center_index < len(node_features) else []
        return {
            "node_id": center_node_id,
            "embedding": embedding,
            "prediction": outgoing_edges,
            "confidence": float(outgoing_edges),
        }

    def transform_pre(self, payload):
        return (payload,)

    def transform_post(self, res):
        return res
