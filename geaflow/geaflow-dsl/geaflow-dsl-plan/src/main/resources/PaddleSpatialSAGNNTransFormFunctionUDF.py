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

"""
PaddleSpatial SA-GNN Transform Function for GeaFlow-Infer Framework.

This module implements the Spatial Adaptive Graph Neural Network (SA-GNN) algorithm
for generating node embeddings using PaddlePaddle and PGL (Paddle Graph Learning).

SA-GNN Architecture (from PaddleSpatial):
  - SpatialLocalAGG: local GCN-like aggregation using degree-normalised message passing
  - SpatialOrientedAGG: direction-aware aggregation that partitions neighbours into
    spatial sectors based on coordinates and aggregates each sector independently
  - SpatialAttnProp: location-aware multi-head attention propagation

This file is the user-provided TransFormFunctionUDF.py that the GeaFlow-Infer framework
loads automatically. It should be deployed inside the user-defined UDF jar.

Input protocol (from Java SAGNN.java):
  args[0]: vertex_id   – Object, the vertex identifier
  args[1]: vertex_feats – List[float], feature vector; last 2 elements are [coord_x, coord_y]
  args[2]: nbr_feats   – Map[int, List[List[float]]], layer → list of neighbour feature vectors

Output:
  A List[float] representing the node embedding vector (length = output_dim).

Configuration:
  Set the following JVM configuration keys:
    geaflow.infer.env.user.transform.classname = SAGNNTransFormFunction
    geaflow.infer.framework.type               = PADDLE
    geaflow.infer.env.paddle.gpu.enable        = false   (set true for GPU clusters)
"""

import abc
import os
import traceback
from typing import Dict, List, Optional, Tuple, Union

import numpy as np
import paddle
import paddle.nn as nn
import paddle.nn.functional as F
import pgl
from pgl.nn import functional as GF

# ───────────────────────────────────────────────────────────────────────────────
# Abstract base (mirrors the one in TransFormFunctionUDF.py so users can copy
# this file as their UDF without depending on the torch-only original)
# ───────────────────────────────────────────────────────────────────────────────

class TransFormFunction(abc.ABC):
    """Framework-agnostic abstract base class for GeaFlow-Infer transform functions."""

    def __init__(self, input_size: int):
        self.input_size = input_size

    @abc.abstractmethod
    def load_model(self, *args):
        pass

    @abc.abstractmethod
    def transform_pre(self, *args):
        pass

    @abc.abstractmethod
    def transform_post(self, *args):
        pass


# ───────────────────────────────────────────────────────────────────────────────
# PaddleSpatial SA-GNN building blocks
# ───────────────────────────────────────────────────────────────────────────────

class SpatialLocalAGG(nn.Layer):
    """
    Local GCN aggregation layer from PaddleSpatial SA-GNN.

    Performs degree-normalised message passing on a pgl.Graph instance.
    Optionally applies a linear projection before aggregation.
    """

    def __init__(self, input_dim: int, hidden_dim: int,
                 transform: bool = True, activation=None):
        super(SpatialLocalAGG, self).__init__()
        self.transform = transform
        if self.transform:
            self.linear = nn.Linear(input_dim, hidden_dim, bias_attr=False)
        self.activation = activation

    def forward(self, graph: pgl.Graph, feature: paddle.Tensor) -> paddle.Tensor:
        norm = GF.degree_norm(graph)
        if self.transform:
            feature = self.linear(feature)
        feature = feature * norm
        output = graph.send_recv(feature, "sum")
        output = output * norm
        if self.activation is not None:
            output = self.activation(output)
        return output


class SpatialOrientedAGG(nn.Layer):
    """
    Direction-aware aggregation layer from PaddleSpatial SA-GNN.

    Partitions edges into ``num_sectors`` spatial sectors based on the
    relative angle between source and destination node coordinates.
    Each sector is aggregated independently via SpatialLocalAGG and
    the results are concatenated then projected.

    Coordinates are expected in the node feature dict under the key 'coord'
    with shape (num_nodes, 2).
    """

    def __init__(self, input_dim: int, hidden_dim: int,
                 num_sectors: int = 8, transform: bool = True, activation=None):
        super(SpatialOrientedAGG, self).__init__()
        self.num_sectors = num_sectors
        linear_input_dim = (hidden_dim if transform else input_dim) * (num_sectors + 1)
        self.linear = nn.Linear(linear_input_dim, hidden_dim, bias_attr=False)
        self.conv_layers = nn.LayerList([
            SpatialLocalAGG(input_dim, hidden_dim, transform, activation=lambda x: x)
            for _ in range(num_sectors + 1)
        ])

    def _partition_edges_by_sector(
            self, g: pgl.Graph
    ) -> List[List[Tuple[int, int]]]:
        """Return edge lists partitioned into num_sectors+1 directional buckets."""
        subgraph_edges = [[] for _ in range(self.num_sectors + 1)]
        g_np = g.numpy()
        coords = g_np.node_feat.get('coord')  # (N, 2)
        for src, dst in g_np.edges:
            if coords is not None:
                rel = coords[dst] - coords[src]
                if rel[0] == 0 and rel[1] == 0:
                    sec = 0
                else:
                    rel[0] += 1e-9
                    angle = np.arctan(rel[1] / rel[0])
                    angle += np.pi * int(angle < 0)
                    angle += np.pi * int(rel[0] < 0)
                    sec = int(angle / (np.pi / self.num_sectors))
                    sec = min(sec, self.num_sectors)
            else:
                sec = 0
            subgraph_edges[sec].append((int(src), int(dst)))
        return subgraph_edges

    def forward(self, graph: pgl.Graph, feature: paddle.Tensor) -> paddle.Tensor:
        from pgl.sampling.custom import subgraph as pgl_subgraph
        partitioned = self._partition_edges_by_sector(graph)
        g_np = graph.numpy()
        h_list = []
        for i, conv in enumerate(self.conv_layers):
            sub_g = pgl_subgraph(g_np, g_np.nodes, edges=partitioned[i])
            sub_g = sub_g.tensor()
            h_list.append(conv(sub_g, feature))
        feat_h = paddle.concat(h_list, axis=-1)
        feat_h = paddle.cast(feat_h, 'float32')
        return self.linear(feat_h)


class SAGNNModel(nn.Layer):
    """
    Full SA-GNN model composing local + oriented aggregation layers.

    Architecture:
      Layer 0: SpatialLocalAGG (GCN-like, fast)
      Layer 1: SpatialOrientedAGG (direction-aware, richer spatial context)
      Projection: Linear(hidden_dim → output_dim)

    The model is intentionally kept simple so that it can run on a mini-graph
    containing only the centre node and its sampled neighbours (the data that
    the SAGNN.java algorithm sends to the Python process).
    """

    def __init__(self, input_dim: int, hidden_dim: int, output_dim: int,
                 num_sectors: int = 8):
        super(SAGNNModel, self).__init__()
        self.local_agg = SpatialLocalAGG(
            input_dim, hidden_dim, transform=True, activation=F.relu)
        self.oriented_agg = SpatialOrientedAGG(
            hidden_dim, hidden_dim, num_sectors=num_sectors,
            transform=True, activation=None)
        self.proj = nn.Linear(hidden_dim, output_dim)

    def forward(self, graph: pgl.Graph, feature: paddle.Tensor) -> paddle.Tensor:
        h = self.local_agg(graph, feature)
        h = F.relu(h)
        h = self.oriented_agg(graph, h)
        h = F.relu(h)
        h = self.proj(h)
        return h


# ───────────────────────────────────────────────────────────────────────────────
# Main Transform Function
# ───────────────────────────────────────────────────────────────────────────────

class SAGNNTransFormFunction(TransFormFunction):
    """
    SA-GNN Transform Function for GeaFlow-Infer (PaddlePaddle backend).

    This class is the entry point for the Python inference sub-process.
    It is instantiated once per Python worker process and handles multiple
    infer() calls from the Java side.

    Feature vector convention
    -------------------------
    The Java SAGNN algorithm sends a feature vector for each vertex.
    The LAST TWO elements of this vector are treated as (coord_x, coord_y),
    i.e. the spatial coordinates used by SA-GNN's oriented aggregation.
    The preceding elements are the semantic node features.

    If coordinates are unavailable (feature_dim <= 2), the model degrades
    gracefully to SpatialLocalAGG-only aggregation using zero coordinates.

    Model files
    -----------
    The model weights are expected at:
        <cwd>/sagnn_model.pdparams        (state_dict, loaded with paddle.load)
    If the file does not exist, the model is randomly initialised (useful for
    testing the pipeline without a pre-trained model).

    infer_mode
    ----------
    Set self.infer_mode = "static" to enable paddle.inference acceleration.
    The static path requires exported model files (sagnn_model.pdmodel /
    sagnn_model.pdiparams) created with paddle.jit.save.
    """

    # Expose infer_mode so PaddleInferSession can read it.
    infer_mode: str = "dynamic"

    def __init__(self):
        # input_size=3: (vertex_id, vertex_features, neighbor_features_map)
        super().__init__(input_size=3)
        print("[SAGNNTransFormFunction] Initialising …")

        # ── device selection ──────────────────────────────────────────────
        device = "gpu" if paddle.is_compiled_with_cuda() else "cpu"
        paddle.set_device(device)
        print(f"[SAGNNTransFormFunction] Using device: {paddle.get_device()}")

        # ── model hyper-parameters ────────────────────────────────────────
        # input_dim = feature_dim - 2 (coords excluded from semantic features)
        # Set conservatively; the forward pass handles variable-length inputs
        # via padding / truncation.
        self.feature_dim: int = 64    # total feature vector length (incl. coords)
        self.coord_dim: int = 2       # last N elements are coordinates
        self.input_dim: int = self.feature_dim - self.coord_dim  # 62
        self.hidden_dim: int = 128
        self.output_dim: int = 64
        self.num_sectors: int = 8

        # ── load model ────────────────────────────────────────────────────
        model_path = os.path.join(os.getcwd(), "sagnn_model.pdparams")
        self.load_model(model_path)

    # ------------------------------------------------------------------
    # TransFormFunction interface
    # ------------------------------------------------------------------

    def load_model(self, model_path: str):
        """
        Initialise the SAGNNModel and optionally load pre-trained weights.

        Args:
            model_path: Path to a .pdparams state-dict file produced by
                        ``paddle.save(model.state_dict(), model_path)``.
                        If the file does not exist, a randomly-initialised
                        model is used (useful for integration testing).
        """
        self.model = SAGNNModel(
            input_dim=self.input_dim,
            hidden_dim=self.hidden_dim,
            output_dim=self.output_dim,
            num_sectors=self.num_sectors,
        )
        if os.path.exists(model_path):
            try:
                state_dict = paddle.load(model_path)
                self.model.set_state_dict(state_dict)
                print(f"[SAGNNTransFormFunction] Loaded weights from {model_path}")
            except Exception as exc:
                print(f"[SAGNNTransFormFunction] WARNING: failed to load weights "
                      f"({exc}). Using random initialisation.")
        else:
            print(f"[SAGNNTransFormFunction] Model file not found at {model_path}. "
                  f"Using random initialisation.")
        self.model.eval()

    def transform_pre(self, *args) -> Tuple[List[float], object]:
        """
        Build a mini PGL graph from the received data and run SA-GNN inference.

        Args:
            args[0]: vertex_id  – vertex identifier (any hashable type)
            args[1]: vertex_feats – List[float], length == self.feature_dim
                     Convention: last 2 elements are [coord_x, coord_y]
            args[2]: nbr_feats_map – Dict[int, List[List[float]]]
                     Maps layer index → list of neighbour feature vectors.
                     Only layer key 1 (first hop) is used to build the mini-graph.

        Returns:
            (embedding_list, vertex_id)
        """
        try:
            vertex_id = args[0]
            vertex_feats_raw: List[float] = args[1] if args[1] else []
            nbr_feats_map: Dict = args[2] if args[2] else {}

            # ── parse vertex features ──────────────────────────────────────
            v_feat, v_coord = self._split_feat_coord(vertex_feats_raw)

            # ── collect first-hop neighbour features ───────────────────────
            layer1_nbrs: List[List[float]] = nbr_feats_map.get(1, [])
            if not layer1_nbrs:
                # Also try key 0 (Python dict from Java may use 0-indexed layers)
                layer1_nbrs = nbr_feats_map.get(0, [])

            nbr_feats_list, nbr_coords_list = [], []
            for nf in layer1_nbrs:
                nf_feat, nf_coord = self._split_feat_coord(nf)
                nbr_feats_list.append(nf_feat)
                nbr_coords_list.append(nf_coord)

            # ── build mini PGL graph ───────────────────────────────────────
            graph, all_feats, all_coords = self._build_mini_graph(
                v_feat, v_coord, nbr_feats_list, nbr_coords_list
            )

            # ── run SA-GNN forward pass ────────────────────────────────────
            feature_tensor = paddle.to_tensor(all_feats, dtype='float32')
            coord_tensor = paddle.to_tensor(all_coords, dtype='float32')

            # Attach coordinates to graph node_feat for SpatialOrientedAGG
            graph.node_feat['coord'] = coord_tensor

            with paddle.no_grad():
                embeddings = self.model(graph, feature_tensor)  # (num_nodes, output_dim)

            # The centre node is always node 0 in our mini-graph
            centre_embedding = embeddings[0].numpy().tolist()
            return centre_embedding, vertex_id

        except Exception as exc:
            print(f"[SAGNNTransFormFunction] ERROR in transform_pre: {exc}")
            traceback.print_exc()
            return [0.0] * self.output_dim, args[0] if args else None

    def transform_post(self, *args) -> List[float]:
        """
        Post-process the embedding returned by transform_pre.

        Args:
            args[0]: embedding – List[float] or nested result from transform_pre

        Returns:
            Flat List[float] embedding ready for serialisation.
        """
        if not args:
            return [0.0] * self.output_dim
        result = args[0]
        if isinstance(result, (list, tuple)) and result and isinstance(result[0], (list, tuple)):
            # Unwrap one nesting level (shouldn't happen, but be defensive)
            result = result[0]
        if isinstance(result, paddle.Tensor):
            result = result.numpy().tolist()
        return result if isinstance(result, list) else list(result)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _split_feat_coord(
            self, feat_vec: List[float]
    ) -> Tuple[List[float], List[float]]:
        """
        Split a combined feature vector into semantic features + coordinates.

        The last ``self.coord_dim`` elements are coordinates; everything
        before them is the semantic feature.

        Args:
            feat_vec: Raw feature vector from the Java side.

        Returns:
            (semantic_feat, coord)  both padded / truncated to fixed sizes.
        """
        if not feat_vec:
            return ([0.0] * self.input_dim, [0.0] * self.coord_dim)

        arr = list(feat_vec)

        if len(arr) > self.feature_dim:
            arr = arr[:self.feature_dim]
        elif len(arr) < self.feature_dim:
            arr = arr + [0.0] * (self.feature_dim - len(arr))

        semantic = arr[: self.input_dim]
        coords = arr[self.input_dim :]
        return semantic, coords

    def _build_mini_graph(
            self,
            centre_feat: List[float],
            centre_coord: List[float],
            nbr_feats: List[List[float]],
            nbr_coords: List[List[float]],
    ) -> Tuple[pgl.Graph, np.ndarray, np.ndarray]:
        """
        Build a PGL mini-graph with the centre node (id=0) and its neighbours.

        Graph structure:
          Node 0: centre vertex
          Nodes 1..K: sampled neighbours (K = len(nbr_feats))
          Edges: directed from each neighbour to the centre node
                 (i → 0 for i in 1..K)

        Args:
            centre_feat:  Semantic features for the centre node.
            centre_coord: Spatial coordinates for the centre node.
            nbr_feats:    List of semantic features for each neighbour.
            nbr_coords:   List of spatial coordinates for each neighbour.

        Returns:
            (graph, all_features_array, all_coords_array)
            where all_features_array has shape (num_nodes, input_dim)
            and   all_coords_array   has shape (num_nodes, coord_dim).
        """
        num_nbrs = len(nbr_feats)
        num_nodes = 1 + num_nbrs

        # Build edge list: every neighbour sends a message to the centre node
        if num_nbrs > 0:
            edges = [(i + 1, 0) for i in range(num_nbrs)]
        else:
            # Self-loop so the graph is non-empty and SA-GNN can still run
            edges = [(0, 0)]

        # Stack feature arrays
        all_feats = np.array(
            [centre_feat] + nbr_feats, dtype=np.float32
        )  # (num_nodes, input_dim)

        all_coords = np.array(
            [centre_coord] + nbr_coords, dtype=np.float32
        )  # (num_nodes, coord_dim)

        graph = pgl.Graph(num_nodes=num_nodes, edges=edges)
        graph.tensor()
        return graph, all_feats, all_coords
