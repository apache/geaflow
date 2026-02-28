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

"""Data source implementations for CASTS system.

This module provides concrete implementations of the DataSource interface
for both synthetic and real data sources.
"""

from collections import deque
import csv
from pathlib import Path
import random

import networkx as nx

from core.config import DefaultConfiguration
from core.interfaces import Configuration, DataSource, GoalGenerator, GraphSchema
from core.schema import InMemoryGraphSchema
from core.types import GraphEdges, GraphNodes, JsonDict


class SyntheticBusinessGraphGoalGenerator(GoalGenerator):
    """Goal generator for (Synthetic) business/financial graphs."""

    def __init__(self):
        # Emphasize multi-hop + relation types to give the LLM
        # a clearer signal about traversable edges.
        self._goals = [
            (
                "Map how risk propagates through multi-hop business "
                "relationships (friend, supplier, partner, investor, "
                "customer) based on available data",
                "Score is based on the number of hops and the variety of relationship types "
                "(friend, supplier, partner, etc.) traversed. Paths that stay within one "
                "relationship type are less valuable.",
            ),
            (
                "Discover natural community structures that emerge from "
                "active entity interactions along friend and partner "
                "relationships",
                "Score is based on the density of connections found. Paths that identify nodes "
                "with many shared 'friend' or 'partner' links are more valuable. Simple long "
                "chains are less valuable.",
            ),
            (
                "Recommend smarter supplier alternatives by walking "
                "along supplier and customer chains and learning from "
                "historical risk-category patterns",
                "Score is based on ability to traverse 'supplier' and 'customer' chains. "
                "The longer the chain, the better. Paths that don't follow these "
                "relationships should be penalized.",
            ),
            (
                "Trace fraud signals across investor / partner / customer "
                "relationship chains using real-time metrics, without "
                "assuming globally optimal paths",
                "Score is based on the length and complexity of chains involving 'investor', "
                "'partner', and 'customer' relationships. Paths that connect disparate parts "
                "of the graph are more valuable.",
            ),
            (
                "Uncover hidden cross-region business connections through "
                "accumulated domain knowledge and repeated traversals over "
                "friend / partner edges",
                "Score is based on the ability to connect nodes from different 'region' "
                "properties using 'friend' or 'partner' edges. A path that starts in 'NA' "
                "and ends in 'EU' is high value.",
            ),
        ]
        self._goal_weights = [100.0, 60.0, 40.0, 25.0, 15.0]

    @property
    def goal_texts(self) -> list[str]:
        return [g[0] for g in self._goals]

    @property
    def goal_weights(self) -> list[float]:
        return self._goal_weights.copy()

    def select_goal(self, node_type: str | None = None) -> tuple[str, str]:
        """Select a goal and its rubric based on weights."""
        selected_goal, selected_rubric = random.choices(
            self._goals, weights=self._goal_weights, k=1
        )[0]
        return selected_goal, selected_rubric


class RealBusinessGraphGoalGenerator(GoalGenerator):
    """Goal generator for real financial graph data.

    Goals are written as QA-style descriptions over the actual
    entity / relation types present in the CSV graph, so that
    g explicitly reflects the observed schema.
    """

    def __init__(self, node_types: set[str], edge_labels: set[str]):
        self._node_types = node_types
        self._edge_labels = edge_labels

        person = "Person" if "Person" in node_types else "person node"
        company = "Company" if "Company" in node_types else "company node"
        account = "Account" if "Account" in node_types else "account node"
        loan = "Loan" if "Loan" in node_types else "loan node"

        invest = "invest" if "invest" in edge_labels else "invest relation"
        guarantee = "guarantee" if "guarantee" in edge_labels else "guarantee relation"
        transfer = "transfer" if "transfer" in edge_labels else "transfer relation"
        withdraw = "withdraw" if "withdraw" in edge_labels else "withdraw relation"
        repay = "repay" if "repay" in edge_labels else "repay relation"
        deposit = "deposit" if "deposit" in edge_labels else "deposit relation"
        apply = "apply" if "apply" in edge_labels else "apply relation"
        own = "own" if "own" in edge_labels else "ownership relation"

        # Construct goals aligned to observable relations in the real graph.
        self._goals = [
            (
                f"""Given a {person}, walk along {invest} / {own} / {guarantee} / {apply} edges to reach related {company} or {loan} nodes and return representative paths.""",  # noqa: E501
                f"""Score is based on whether a path connects a {person} to a {company} or {loan}. Bonus for using multiple relation types and 2-4 hop paths. Single-hop paths score lower.""",  # noqa: E501
            ),
            (
                f"""Starting from an {account}, follow {transfer} / {withdraw} / {repay} / {deposit} edges to trace money flows and reach a {loan} or another {account} within 2-4 hops.""",  # noqa: E501
                f"""Score is based on staying on transaction edges and reaching a {loan} or a multi-hop {account} chain. Paths that stop immediately or use unrelated links score lower.""",  # noqa: E501
            ),
            (
                f"""For a single {company}, traverse {own} and {apply} relations to reach both {account} and {loan} nodes, and include {guarantee} if available.""",  # noqa: E501
                f"""Score is based on covering ownership and loan-related steps in the same path. Higher scores for paths that include both {account} and {loan} and use {guarantee}.""",  # noqa: E501
            ),
            (
                f"""Between {person} and {company} nodes, find short chains using {invest} / {own} / {guarantee} relations to explain related-party links.""",  # noqa: E501
                f"""Score is based on discovering paths that include both {person} and {company} within 2-3 steps. Using more than one relation type increases the score.""",  # noqa: E501
            ),
            (
                f"""From a {company}, explore multi-hop {invest} or {guarantee} relations to reach multiple other {company} nodes and summarize the cluster.""",  # noqa: E501
                f"""Score increases with the number of distinct {company} nodes reached within 2-4 hops. Simple single-edge paths score lower.""",  # noqa: E501
            ),
            (
                f"""Starting at a {loan}, follow incoming {repay} links to {account} nodes, then use incoming {own} links to reach related {person} or {company} owners.""",  # noqa: E501
                f"""Score is based on reaching at least one owner ({person} or {company}) via {repay} -> {own} within 2-3 hops. Paths that end at {account} score lower.""",  # noqa: E501
            ),
        ]

        # Heuristic weight distribution; can be tuned by future statistics
        self._goal_weights = [100.0, 90.0, 80.0, 70.0, 60.0, 50.0]

    @property
    def goal_texts(self) -> list[str]:
        return [g[0] for g in self._goals]

    @property
    def goal_weights(self) -> list[float]:
        return self._goal_weights.copy()

    def select_goal(self, node_type: str | None = None) -> tuple[str, str]:
        """Weighted random selection; optionally bias by node_type.

        If ``node_type`` is provided, slightly bias towards goals whose
        text mentions that type; otherwise fall back to simple
        weighted random sampling over all goals.
        """

        # Simple heuristic: filter a small candidate subset by node_type
        candidates: list[tuple[str, str]] = self._goals
        weights: list[float] = self._goal_weights

        if node_type is not None:
            node_type_lower = node_type.lower()
            filtered: list[tuple[tuple[str, str], float]] = []

            for goal_tuple, w in zip(self._goals, self._goal_weights, strict=False):
                text = goal_tuple[0]
                if node_type_lower in text.lower():
                    # Slightly boost weights for goals matching the same type.
                    filtered.append((goal_tuple, w * 2))

            if filtered:
                c_tuple, w_tuple = zip(*filtered, strict=False)
                candidates = list(c_tuple)
                weights = list(w_tuple)

        selected_goal, selected_rubric = random.choices(candidates, weights=weights, k=1)[0]
        return selected_goal, selected_rubric


class SyntheticDataSource(DataSource):
    """Synthetic graph data source with Zipf distribution."""

    def __init__(self, size: int = 30):
        """Initialize synthetic data source.

        Args:
            size: Number of nodes to generate
        """
        self._nodes: GraphNodes = {}
        self._edges: GraphEdges = {}
        self._source_label = "synthetic"
        # NOTE: For synthetic graphs we assume the generated data is immutable
        # after initialization. If you mutate `nodes` / `edges` at runtime, you
        # must call `get_schema()` again so a fresh InMemoryGraphSchema (and
        # fingerprint) is built.
        self._goal_generator: GoalGenerator | None = None
        self._generate_zipf_data(size)
        self._schema = InMemoryGraphSchema(self._nodes, self._edges)
        self._goal_generator = SyntheticBusinessGraphGoalGenerator()

    @property
    def nodes(self) -> GraphNodes:
        return self._nodes

    @property
    def edges(self) -> GraphEdges:
        return self._edges

    @property
    def source_label(self) -> str:
        return self._source_label

    def get_node(self, node_id: str) -> JsonDict | None:
        return self._nodes.get(node_id)

    def get_neighbors(self, node_id: str, edge_label: str | None = None) -> list[str]:
        """Get neighbor node IDs for a given node."""
        if node_id not in self._edges:
            return []

        neighbors = []
        for edge in self._edges[node_id]:
            if edge_label is None or edge["label"] == edge_label:
                neighbors.append(edge["target"])
        return neighbors

    def get_schema(self) -> GraphSchema:
        """Get the graph schema for this data source.

        If node or edge data are mutated after initialization, call this again
        to rebuild a fresh schema view.
        """
        if self._schema is None:
            self._schema = InMemoryGraphSchema(self._nodes, self._edges)
        return self._schema

    def get_goal_generator(self) -> GoalGenerator:
        """Get the goal generator for this data source."""
        if self._goal_generator is None:
            self._goal_generator = SyntheticBusinessGraphGoalGenerator()
        return self._goal_generator

    def get_starting_nodes(
        self,
        goal: str,
        recommended_node_types: list[str],
        count: int,
        min_degree: int = 2,
    ) -> list[str]:
        """Select starting nodes using LLM-recommended node types.

        For synthetic data, this is straightforward because all nodes
        are guaranteed to have at least 1 outgoing edge by construction.

        Args:
            goal: The traversal goal text (for logging)
            recommended_node_types: Node types recommended by LLM
            count: Number of starting nodes to return
            min_degree: Minimum outgoing degree for fallback selection

        Returns:
            List of node IDs suitable for starting traversal
        """
        # Tier 1: LLM-recommended node types
        if recommended_node_types:
            candidates = [
                node_id
                for node_id, node in self._nodes.items()
                if node.get("type") in recommended_node_types
            ]

            if len(candidates) >= count:
                return random.sample(candidates, k=count)

        # Tier 2: Degree-based fallback
        candidates = [
            node_id
            for node_id in self._nodes.keys()
            if len(self._edges.get(node_id, [])) >= min_degree
        ]

        if len(candidates) >= count:
            return random.sample(candidates, k=count)

        # Tier 3: Emergency fallback - any nodes with at least 1 edge
        candidates = [
            node_id for node_id in self._nodes.keys() if len(self._edges.get(node_id, [])) >= 1
        ]

        if len(candidates) >= count:
            return random.sample(candidates, k=count)

        # Last resort: take any nodes
        all_nodes = list(self._nodes.keys())
        if len(all_nodes) >= count:
            return random.sample(all_nodes, k=count)

        return all_nodes

    def _generate_zipf_data(self, size: int):
        """Generate synthetic data following Zipf distribution."""
        business_types = [
            "Retail SME",
            "Logistics Partner",
            "Enterprise Vendor",
            "Regional Distributor",
            "FinTech Startup",
        ]
        type_weights = [100, 50, 25, 12, 6]

        business_categories = ["retail", "wholesale", "finance", "manufacturing"]
        regions = ["NA", "EU", "APAC", "LATAM"]
        risk_levels = ["low", "medium", "high"]

        # Generate nodes
        for i in range(size):
            node_type = random.choices(business_types, weights=type_weights, k=1)[0]
            status = "active" if random.random() < 0.8 else "inactive"
            age = random.randint(18, 60)

            node = {
                "id": str(i),
                "type": node_type,
                "category": random.choice(business_categories),
                "region": random.choice(regions),
                "risk": random.choice(risk_levels),
                "status": status,
                "age": age,
            }
            self._nodes[str(i)] = node

        # Generate edges with more structured, denser relationship patterns
        edge_labels = ["friend", "supplier", "partner", "investor", "customer"]

        # Baseline randomness: ensure each node has some edges.
        for i in range(size):
            base_degree = random.randint(1, 3)  # Ensure at least one edge.
            for _ in range(base_degree):
                target_id = str(random.randint(0, size - 1))
                if target_id == str(i):
                    continue
                label = random.choice(edge_labels)
                edge = {"target": target_id, "label": label}
                self._edges.setdefault(str(i), []).append(edge)

        # Structural bias: different business types favor certain relations
        # to help the LLM learn stable patterns.
        for i in range(size):
            src_id = str(i)
            node_type = self._nodes[src_id]["type"]

            # Retail SME: more customer / supplier edges
            if node_type == "Retail SME":
                extra_labels = ["customer", "supplier"]
                extra_edges = 2
            # Logistics Partner: more partner / supplier edges
            elif node_type == "Logistics Partner":
                extra_labels = ["partner", "supplier"]
                extra_edges = 2
            # Enterprise Vendor: more supplier / investor edges
            elif node_type == "Enterprise Vendor":
                extra_labels = ["supplier", "investor"]
                extra_edges = 2
            # Regional Distributor: more partner / customer edges
            elif node_type == "Regional Distributor":
                extra_labels = ["partner", "customer"]
                extra_edges = 2
            # FinTech Startup: more investor / partner edges
            else:  # 'FinTech Startup'
                extra_labels = ["investor", "partner"]
                extra_edges = 3  # Slightly higher to test deeper paths.

            for _ in range(extra_edges):
                target_id = str(random.randint(0, size - 1))
                if target_id == src_id:
                    continue
                label = random.choice(extra_labels)
                edge = {"target": target_id, "label": label}
                self._edges.setdefault(src_id, []).append(edge)

        # Optional: increase global "friend" connectivity to reduce isolated components.
        for i in range(size):
            src_id = str(i)
            if random.random() < 0.3:  # 30% of nodes add an extra friend edge.
                target_id = str(random.randint(0, size - 1))
                if target_id != src_id:
                    edge = {"target": target_id, "label": "friend"}
                    self._edges.setdefault(src_id, []).append(edge)


class RealDataSource(DataSource):
    """Real graph data source loaded from CSV files."""

    def __init__(self, data_dir: str, max_nodes: int | None = None):
        """Initialize real data source.

        Args:
            data_dir: Directory containing CSV files
            max_nodes: Maximum number of nodes to load (for sampling)
        """
        self._nodes: GraphNodes = {}
        self._edges: GraphEdges = {}
        self._source_label = "real"
        self._data_dir = Path(data_dir)
        self._max_nodes = max_nodes
        self._config = DefaultConfiguration()

        # Schema is now lazily loaded and will be constructed on the first
        # call to `get_schema()` after the data is loaded.
        self._schema: GraphSchema | None = None
        self._schema_dirty = True  # Start with a dirty schema
        self._goal_generator: GoalGenerator | None = None

        # Caches for starting node selection
        self._node_out_edges: dict[str, list[str]] | None = None
        self._nodes_by_type: dict[str, list[str]] | None = None

        self._load_real_graph()

        # Defer goal generator creation until schema is accessed
        # self._goal_generator = RealBusinessGraphGoalGenerator(node_types, edge_labels)

    @property
    def nodes(self) -> GraphNodes:
        return self._nodes

    @property
    def edges(self) -> GraphEdges:
        return self._edges

    @property
    def source_label(self) -> str:
        return self._source_label

    def get_node(self, node_id: str) -> JsonDict | None:
        return self._nodes.get(node_id)

    def get_neighbors(self, node_id: str, edge_label: str | None = None) -> list[str]:
        """Get neighbor node IDs for a given node."""
        if node_id not in self._edges:
            return []

        neighbors = []
        for edge in self._edges[node_id]:
            if edge_label is None or edge["label"] == edge_label:
                neighbors.append(edge["target"])
        return neighbors

    def reload(self):
        """Reload data from source and invalidate the schema and goal generator."""
        self._load_real_graph()
        self._schema_dirty = True
        self._goal_generator = None
        # Invalidate caches
        self._node_out_edges = None
        self._nodes_by_type = None

    def get_schema(self) -> GraphSchema:
        """Get the graph schema for this data source.

        The schema is created on first access and recreated if the data
        source has been reloaded. If the underlying data mutates, call
        ``reload()`` or request the schema again to rebuild caches.
        """
        if self._schema is None or self._schema_dirty:
            self._schema = InMemoryGraphSchema(self._nodes, self._edges)
            self._schema_dirty = False
        return self._schema

    def get_goal_generator(self) -> GoalGenerator:
        """Get the goal generator for this data source."""
        if self._goal_generator is None:
            # The goal generator depends on the schema, so ensure it's fresh.
            schema = self.get_schema()
            self._goal_generator = RealBusinessGraphGoalGenerator(
                node_types=schema.node_types, edge_labels=schema.edge_labels
            )
        return self._goal_generator

    def get_starting_nodes(
        self,
        goal: str,
        recommended_node_types: list[str],
        count: int,
        min_degree: int = 2,
    ) -> list[str]:
        """Select starting nodes using LLM-recommended node types.

        For real data, connectivity varies, so we rely on caches and fallbacks.

        Args:
            goal: The traversal goal text (for logging)
            recommended_node_types: Node types recommended by LLM
            count: Number of starting nodes to return
            min_degree: Minimum outgoing degree for fallback selection

        Returns:
            List of node IDs suitable for starting traversal
        """
        # Ensure caches are built
        if self._nodes_by_type is None:
            self._build_nodes_by_type_cache()
        if self._node_out_edges is None:
            self._build_node_out_edges_cache()

        # Add assertions for type checker to know caches are not None
        assert self._nodes_by_type is not None
        assert self._node_out_edges is not None

        # Tier 1: LLM-recommended node types
        if recommended_node_types:
            candidates = []
            for node_type in recommended_node_types:
                if node_type in self._nodes_by_type:
                    candidates.extend(self._nodes_by_type[node_type])

            if len(candidates) >= count:
                return random.sample(candidates, k=count)

        # Tier 2: Degree-based fallback
        candidates = [
            node_id for node_id, edges in self._node_out_edges.items() if len(edges) >= min_degree
        ]

        if len(candidates) >= count:
            return random.sample(candidates, k=count)

        # Tier 3: Emergency fallback - any nodes with at least 1 edge
        candidates = [node_id for node_id, edges in self._node_out_edges.items() if len(edges) >= 1]

        if len(candidates) >= count:
            return random.sample(candidates, k=count)

        # Last resort: take any nodes
        all_nodes = list(self._nodes.keys())
        if len(all_nodes) >= count:
            return random.sample(all_nodes, k=count)

        return all_nodes

    def _build_node_out_edges_cache(self):
        """Build cache mapping node_id -> list of outgoing edge labels."""
        self._node_out_edges = {}
        for node_id in self._nodes.keys():
            edge_labels = [edge["label"] for edge in self._edges.get(node_id, [])]
            self._node_out_edges[node_id] = edge_labels

    def _build_nodes_by_type_cache(self):
        """Build cache mapping node_type -> list of node IDs."""
        self._nodes_by_type = {}
        for node_id, node in self._nodes.items():
            node_type = node.get("type")
            if node_type:
                if node_type not in self._nodes_by_type:
                    self._nodes_by_type[node_type] = []
                self._nodes_by_type[node_type].append(node_id)

    def _load_real_graph(self):
        """Load graph data from CSV files."""
        data_dir = Path(self._data_dir)
        if not data_dir.exists():
            raise ValueError(f"Data directory not found: {self._data_dir}")

        # Load nodes from various entity CSV files
        self._load_nodes_from_csv(data_dir / "Person.csv", "Person")
        self._load_nodes_from_csv(data_dir / "Company.csv", "Company")
        self._load_nodes_from_csv(data_dir / "Account.csv", "Account")
        self._load_nodes_from_csv(data_dir / "Loan.csv", "Loan")
        self._load_nodes_from_csv(data_dir / "Medium.csv", "Medium")

        # Load edges from relationship CSV files
        self._load_edges_from_csv(
            data_dir / "PersonInvestCompany.csv", "Person", "Company", "invest"
        )
        self._load_edges_from_csv(
            data_dir / "PersonGuaranteePerson.csv", "Person", "Person", "guarantee"
        )
        self._load_edges_from_csv(
            data_dir / "CompanyInvestCompany.csv", "Company", "Company", "invest"
        )
        self._load_edges_from_csv(
            data_dir / "CompanyGuaranteeCompany.csv", "Company", "Company", "guarantee"
        )
        self._load_edges_from_csv(
            data_dir / "AccountTransferAccount.csv", "Account", "Account", "transfer"
        )
        self._load_edges_from_csv(
            data_dir / "AccountWithdrawAccount.csv", "Account", "Account", "withdraw"
        )
        self._load_edges_from_csv(data_dir / "AccountRepayLoan.csv", "Account", "Loan", "repay")
        self._load_edges_from_csv(data_dir / "LoanDepositAccount.csv", "Loan", "Account", "deposit")
        self._load_edges_from_csv(data_dir / "PersonApplyLoan.csv", "Person", "Loan", "apply")
        self._load_edges_from_csv(data_dir / "CompanyApplyLoan.csv", "Company", "Loan", "apply")
        self._load_edges_from_csv(data_dir / "PersonOwnAccount.csv", "Person", "Account", "own")
        self._load_edges_from_csv(data_dir / "CompanyOwnAccount.csv", "Company", "Account", "own")
        self._load_edges_from_csv(
            data_dir / "MediumSignInAccount.csv", "Medium", "Account", "signin"
        )

        # Sample subgraph if max_nodes is specified
        if self._max_nodes and len(self._nodes) > self._max_nodes:
            self._sample_subgraph()

        # Enhance connectivity
        self._add_owner_links()
        self._add_shared_medium_links()

        # Build caches for starting node selection
        self._build_node_out_edges_cache()
        self._build_nodes_by_type_cache()

    def _add_shared_medium_links(self):
        """Add edges between account owners who share a login medium."""
        medium_to_accounts = {}
        signin_edges: list[tuple[str, str]] = self._find_edges_by_label(
            "signin",
            "Medium",
            "Account",
        )

        for medium_id, account_id in signin_edges:
            if medium_id not in medium_to_accounts:
                medium_to_accounts[medium_id] = []
            medium_to_accounts[medium_id].append(account_id)

        # Build owner map
        owner_map = {}
        person_owns: list[tuple[str, str]] = self._find_edges_by_label(
            "own",
            "Person",
            "Account",
        )
        company_owns: list[tuple[str, str]] = self._find_edges_by_label(
            "own",
            "Company",
            "Account",
        )
        for src, tgt in person_owns:
            owner_map[tgt] = src
        for src, tgt in company_owns:
            owner_map[tgt] = src

        new_edges = 0
        for _, accounts in medium_to_accounts.items():
            if len(accounts) > 1:
                # Get all unique owners for these accounts
                owners = {owner_map.get(acc_id) for acc_id in accounts if owner_map.get(acc_id)}

                if len(owners) > 1:
                    owner_list = list(owners)
                    # Add edges between all pairs of owners
                    for i in range(len(owner_list)):
                        for j in range(i + 1, len(owner_list)):
                            owner1_id = owner_list[i]
                            owner2_id = owner_list[j]
                            self._add_edge_if_not_exists(owner1_id, owner2_id, "shared_medium")
                            self._add_edge_if_not_exists(owner2_id, owner1_id, "shared_medium")
                            new_edges += 2

        if new_edges > 0:
            print(
                f"Connectivity enhancement: Added {new_edges} "
                "'shared_medium' edges based on login data."
            )

    def _add_owner_links(self):
        """Add edges between owners of accounts that have transactions."""
        # Build an owner map: account_id -> owner_id
        owner_map = {}
        person_owns: list[tuple[str, str]] = self._find_edges_by_label(
            "own",
            "Person",
            "Account",
        )
        company_owns: list[tuple[str, str]] = self._find_edges_by_label(
            "own",
            "Company",
            "Account",
        )

        for src, tgt in person_owns:
            owner_map[tgt] = src
        for src, tgt in company_owns:
            owner_map[tgt] = src

        # Find all transfer edges
        transfer_edges: list[tuple[str, str]] = self._find_edges_by_label(
            "transfer",
            "Account",
            "Account",
        )

        new_edges = 0
        for acc1_id, acc2_id in transfer_edges:
            owner1_id = owner_map.get(acc1_id)
            owner2_id = owner_map.get(acc2_id)

            if owner1_id and owner2_id and owner1_id != owner2_id:
                # Add a 'related_to' edge in both directions
                self._add_edge_if_not_exists(owner1_id, owner2_id, "related_to")
                self._add_edge_if_not_exists(owner2_id, owner1_id, "related_to")
                new_edges += 2

        if new_edges > 0:
            print(
                f"Connectivity enhancement: Added {new_edges} "
                "'related_to' edges based on ownership."
            )

    def _find_edges_by_label(
        self, label: str, from_type: str, to_type: str
    ) -> list[tuple[str, str]]:
        """Helper to find all edges of a certain type."""
        edges = []

        # Check for special cases in the config first.
        default_special_cases: dict[str, str] = {}
        special_cases = self._config.get(
            "EDGE_FILENAME_MAPPING_SPECIAL_CASES", default_special_cases
        )
        key = label
        if from_type:
            key = f"{label.lower()}_{from_type.lower()}"  # e.g., "own_person"

        filename = special_cases.get(key, special_cases.get(label))

        # If not found, fall back to the standard naming convention.
        if not filename:
            filename = f"{from_type}{label.capitalize()}{to_type}.csv"

        filepath = self._data_dir / filename

        try:
            with open(filepath, encoding="utf-8") as f:
                reader = csv.reader(f, delimiter="|")
                for row in reader:
                    if len(row) >= 2:
                        src_id = f"{from_type}_{row[0]}"
                        tgt_id = f"{to_type}_{row[1]}"
                        if src_id in self._nodes and tgt_id in self._nodes:
                            edges.append((src_id, tgt_id))
        except FileNotFoundError:
            # This is expected if a certain edge type file doesn't exist.
            pass
        except UnicodeDecodeError as e:
            print(f"Warning: Unicode error reading {filepath}: {e}")
        except Exception as e:
            print(f"Warning: An unexpected error occurred while reading {filepath}: {e}")
        return edges

    def _add_edge_if_not_exists(self, src_id, tgt_id, label):
        """Adds an edge if it doesn't already exist."""
        if src_id not in self._edges:
            self._edges[src_id] = []

        # Check if a similar edge already exists
        for edge in self._edges[src_id]:
            if edge["target"] == tgt_id and edge["label"] == label:
                return  # Edge already exists

        self._edges[src_id].append({"target": tgt_id, "label": label})

    def _load_nodes_from_csv(self, filepath: Path, entity_type: str):
        """Load nodes from a CSV file using actual column names as attributes."""
        if not filepath.exists():
            return

        try:
            with open(filepath, encoding="utf-8") as f:
                # Use DictReader to get actual column names
                reader = csv.DictReader(f, delimiter="|")
                if not reader.fieldnames:
                    return

                # First column is the ID field
                id_field = reader.fieldnames[0]

                for row in reader:
                    raw_id = row.get(id_field)
                    if not raw_id:  # Skip empty IDs
                        continue

                    node_id = f"{entity_type}_{raw_id}"
                    node = {
                        "id": node_id,
                        "type": entity_type,
                        "raw_id": raw_id,
                    }

                    # Add all fields using their real column names
                    for field_name, field_value in row.items():
                        if field_name != id_field and field_value:
                            node[field_name] = field_value

                    self._nodes[node_id] = node
        except Exception as e:
            print(f"Warning: Error loading {filepath}: {e}")

    def _load_edges_from_csv(self, filepath: Path, from_type: str, to_type: str, label: str):
        """Load edges from a CSV file."""
        if not filepath.exists():
            return

        try:
            with open(filepath, encoding="utf-8") as f:
                reader = csv.reader(f, delimiter="|")
                for row in reader:
                    if len(row) >= 2:
                        src_id = f"{from_type}_{row[0]}"
                        tgt_id = f"{to_type}_{row[1]}"

                        # Only add edge if both nodes exist
                        if src_id in self._nodes and tgt_id in self._nodes:
                            edge = {"target": tgt_id, "label": label}
                            if src_id not in self._edges:
                                self._edges[src_id] = []
                            self._edges[src_id].append(edge)
        except Exception as e:
            print(f"Warning: Error loading {filepath}: {e}")

    def _sample_subgraph(self):
        """Sample a connected subgraph to limit size.

        We first find the largest weakly connected component, then perform a
        BFS-style expansion from a random seed node inside that component
        until we reach ``max_nodes``. This preserves local structure better
        than uniform random sampling over all nodes in the component.
        """
        if not self._max_nodes or len(self._nodes) <= self._max_nodes:
            return

        # Build networkx graph for sampling
        G = nx.DiGraph()
        for node_id, node in self._nodes.items():
            G.add_node(node_id, **node)
        for src_id, edge_List in self._edges.items():
            for edge in edge_List:
                G.add_edge(src_id, edge["target"], label=edge["label"])

        # Find largest connected component
        if not G.nodes():
            return

        # For directed graphs, use weakly connected components
        largest_cc = max(nx.weakly_connected_components(G), key=len)

        # If largest component is bigger than max_nodes, grow a neighborhood
        # around a random seed instead of uniform sampling.
        #
        # Important: in this dataset, BFS from an Account node can quickly fill
        # the budget with Account->Account transfer edges and miss other types
        # (Person/Company/Loan/Medium). To keep the sample useful for goal-driven
        # traversal while staying data-agnostic, we prioritize expanding into
        # *previously unseen node types* first.
        if len(largest_cc) > self._max_nodes:
            # Choose a seed type uniformly to avoid always starting from the
            # dominant type (often Account) when max_nodes is small.
            nodes_by_type: dict[str, list[str]] = {}
            for node_id in largest_cc:
                node_type = G.nodes[node_id].get("type", "Unknown")
                nodes_by_type.setdefault(node_type, []).append(node_id)
            seed_type = random.choice(list(nodes_by_type.keys()))
            seed = random.choice(nodes_by_type[seed_type])
            visited: set[str] = {seed}
            queue: deque[str] = deque([seed])
            seen_types: set[str] = {G.nodes[seed].get("type", "Unknown")}

            while queue and len(visited) < self._max_nodes:
                current = queue.popleft()

                # Collect candidate neighbors (both directions) to preserve
                # weak connectivity while allowing richer expansion.
                candidates: list[str] = []
                for _, nbr in G.out_edges(current):
                    candidates.append(nbr)
                for nbr, _ in G.in_edges(current):
                    candidates.append(nbr)

                # Deduplicate while keeping a stable order.
                deduped: list[str] = []
                seen = set()
                for nbr in candidates:
                    if nbr in seen:
                        continue
                    seen.add(nbr)
                    deduped.append(nbr)

                # Randomize, then prefer nodes that introduce a new type.
                random.shuffle(deduped)
                deduped.sort(
                    key=lambda nid: (
                        0 if G.nodes[nid].get("type", "Unknown") not in seen_types else 1
                    )
                )

                for nbr in deduped:
                    if nbr not in largest_cc or nbr in visited:
                        continue
                    visited.add(nbr)
                    queue.append(nbr)
                    seen_types.add(G.nodes[nbr].get("type", "Unknown"))
                    if len(visited) >= self._max_nodes:
                        break

            sampled_nodes = visited
        else:
            sampled_nodes = largest_cc

        # Filter nodes and edges to sampled subset
        self._nodes = {
            node_id: node for node_id, node in self._nodes.items() if node_id in sampled_nodes
        }
        self._edges = {
            src_id: [edge for edge in edges if edge["target"] in sampled_nodes]
            for src_id, edges in self._edges.items()
            if src_id in sampled_nodes
        }


class DataSourceFactory:
    """Factory for creating appropriate data sources."""

    @staticmethod
    def create(config: Configuration) -> DataSource:
        """Create a data source based on configuration.

        Args:
            config: The configuration object.

        Returns:
            Configured DataSource instance
        """
        if config.get_bool("SIMULATION_USE_REAL_DATA"):
            data_dir = config.get_str("SIMULATION_REAL_DATA_DIR")
            max_nodes = config.get_int("SIMULATION_REAL_SUBGRAPH_SIZE")
            return RealDataSource(data_dir=data_dir, max_nodes=max_nodes)
        else:
            size = config.get_int("SIMULATION_GRAPH_SIZE")
            return SyntheticDataSource(size=size)
