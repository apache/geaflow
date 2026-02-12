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

"""Graph data utilities for CASTS simulations.

This module supports two data sources:

1. Synthetic graph data with Zipf-like distribution (default).
2. Real transaction/relationship data loaded from CSV files under ``real_graph_data/``
   (or a custom loader via ``GraphGeneratorConfig.real_data_loader``).

Use :class:`GraphGenerator` as the unified in-memory representation. The simulation
engine and other components should treat it as read-only.
"""

from dataclasses import dataclass
import random

import networkx as nx

from core.constants import EDGE_LABEL_KEY, EDGE_TARGET_KEY, NODE_ID_KEY, NODE_TYPE_KEY
from core.types import GraphEdges, GraphNodes, JsonDict
from data.real_graph_loader import RealGraphLoader, default_real_graph_loader


@dataclass
class GraphGeneratorConfig:
    """Configuration for building graph data.

    Attributes:
        use_real_data: Whether to build from real CSV files instead of synthetic data.
        real_data_dir: Directory containing the ``*.csv`` relationship tables.
        real_subgraph_size: Maximum number of nodes to keep when sampling a
            connected subgraph from real data. If ``None``, use the full graph.
        real_data_loader: Optional callable to load real graph nodes/edges.
    """

    use_real_data: bool = False
    real_data_dir: str | None = None
    real_subgraph_size: int | None = None
    real_data_loader: RealGraphLoader | None = None


class GraphGenerator:
    """Unified graph container used by the simulation.

    - By default, it generates synthetic graph data with realistic business
      entity relationships.
    - When ``config.use_real_data`` is True, it instead loads nodes/edges from
      ``real_graph_data`` CSV files and optionally samples a connected subgraph
      to control size while preserving edge integrity.
    """

    def __init__(self, size: int = 30, config: GraphGeneratorConfig | None = None):
        self.nodes: GraphNodes = {}
        self.edges: GraphEdges = {}

        self.config = config or GraphGeneratorConfig()
        self.source_label = "synthetic"

        if self.config.use_real_data:
            loader = self.config.real_data_loader or default_real_graph_loader
            self.nodes, self.edges = loader(self.config)
            self.source_label = "real"
        else:
            self._generate_zipf_data(size)

    def to_networkx(self) -> nx.DiGraph:
        """Convert to NetworkX graph for visualization and analysis."""
        G: nx.DiGraph = nx.DiGraph()
        for node_id, node in self.nodes.items():
            G.add_node(node_id, **node)
        for node_id, edge_list in self.edges.items():
            for edge in edge_list:
                G.add_edge(node_id, edge[EDGE_TARGET_KEY], label=edge[EDGE_LABEL_KEY])
        return G

    # ------------------------------------------------------------------
    # Synthetic data (existing behavior)
    # ------------------------------------------------------------------

    def _generate_zipf_data(self, size: int) -> None:
        """Generate graph data following Zipf distribution for realistic entity distributions."""
        # Use concrete, realistic business roles instead of abstract types
        # Approximate Zipf: "Retail SME" is most common, "FinTech Startup" is rarest
        business_types = [
            "Retail SME",  # Most common - small retail businesses
            "Logistics Partner",  # Medium frequency - logistics providers
            "Enterprise Vendor",  # Medium frequency - large vendors
            "Regional Distributor",  # Less common - regional distributors
            "FinTech Startup",  # Rarest - fintech companies
        ]
        # Weights approximating 1/k distribution
        type_weights = [100, 50, 25, 12, 6]
        
        business_categories = ["retail", "wholesale", "finance", "manufacturing"]
        regions = ["NA", "EU", "APAC", "LATAM"]
        risk_levels = ["low", "medium", "high"]

        # Generate nodes
        for i in range(size):
            node_type = random.choices(business_types, weights=type_weights, k=1)[0]
            status = "active" if random.random() < 0.8 else "inactive"
            age = random.randint(18, 60)
            
            node: JsonDict = {
                NODE_ID_KEY: str(i),
                NODE_TYPE_KEY: node_type,
                "status": status,
                "age": age,
                "category": random.choice(business_categories),
                "region": random.choice(regions),
                "risk": random.choices(risk_levels, weights=[60, 30, 10])[0],
            }
            self.nodes[str(i)] = node
            self.edges[str(i)] = []

        # Generate edges with realistic relationship labels
        edge_labels = ["related", "friend", "knows", "supplies", "manages"]
        for i in range(size):
            num_edges = random.randint(1, 4)
            for _ in range(num_edges):
                target = random.randint(0, size - 1)
                if target != i:
                    label = random.choice(edge_labels)
                    # Ensure common "Retail SME" has more 'related' edges
                    # and "Logistics Partner" has more 'friend' edges for interesting simulation
                    if self.nodes[str(i)]["type"] == "Retail SME" and random.random() < 0.7:
                        label = "related"
                    elif (
                        self.nodes[str(i)]["type"] == "Logistics Partner"
                        and random.random() < 0.7
                    ):
                        label = "friend"

                    self.edges[str(i)].append(
                        {EDGE_TARGET_KEY: str(target), EDGE_LABEL_KEY: label}
                    )
