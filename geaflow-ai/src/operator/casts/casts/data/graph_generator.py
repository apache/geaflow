"""Graph data utilities for CASTS simulations.

This module supports two data sources:

1. Synthetic graph data with Zipf-like distribution (default).
2. Real transaction/relationship data loaded from CSV files under ``real_graph_data/``.

Use :class:`GraphGenerator` as the unified in-memory representation. The simulation
engine and other components should treat it as read-only.
"""

import csv
from dataclasses import dataclass
from pathlib import Path
import random
from typing import Any, Dict, List, Optional, Set, Tuple

import networkx as nx


@dataclass
class GraphGeneratorConfig:
    """Configuration for building graph data.

    Attributes:
        use_real_data: Whether to build from real CSV files instead of synthetic data.
        real_data_dir: Directory containing the ``*.csv`` relationship tables.
        real_subgraph_size: Maximum number of nodes to keep when sampling a
            connected subgraph from real data. If ``None``, use the full graph.
    """

    use_real_data: bool = False
    real_data_dir: Optional[str] = None
    real_subgraph_size: Optional[int] = None


class GraphGenerator:
    """Unified graph container used by the simulation.

    - By default, it generates synthetic graph data with realistic business
      entity relationships.
    - When ``config.use_real_data`` is True, it instead loads nodes/edges from
      ``real_graph_data`` CSV files and optionally samples a connected subgraph
      to control size while preserving edge integrity.
    """

    def __init__(self, size: int = 30, config: Optional[GraphGeneratorConfig] = None):
        self.nodes: Dict[str, Dict[str, Any]] = {}
        self.edges: Dict[str, List[Dict[str, str]]] = {}

        self.config = config or GraphGeneratorConfig()
        self.source_label = "synthetic"

        if self.config.use_real_data:
            self._load_real_graph()
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
                G.add_edge(node_id, edge['target'], label=edge['label'])
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
            
            node = {
                "id": str(i),
                "type": node_type,
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

                    self.edges[str(i)].append({"target": str(target), "label": label})

    # ------------------------------------------------------------------
    # Real data loading and subgraph sampling
    # ------------------------------------------------------------------

    def _load_real_graph(self) -> None:
        """Load nodes and edges from real CSV data.

        The current implementation treats each business/financial entity as a
        node and the relation tables as directed edges. It then optionally
        samples a connected subgraph to keep the graph size manageable.
        """

        data_dir = self._resolve_data_dir()

        # Load entity tables as nodes
        entity_files = {
            "Person": "Person.csv",
            "Company": "Company.csv",
            "Account": "Account.csv",
            "Loan": "Loan.csv",
            "Medium": "Medium.csv",
        }

        node_attributes: Dict[Tuple[str, str], Dict[str, Any]] = {}

        for entity_type, filename in entity_files.items():
            path = data_dir / filename
            if not path.exists():
                continue

            with path.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle, delimiter="|")
                for row in reader:
                    # Assume there is an ``id`` column; if not, fall back to
                    # the first column name as primary key.
                    if "id" in row:
                        raw_id = row["id"]
                    else:
                        first_key = next(iter(row.keys()))
                        raw_id = row[first_key]

                    node_key = (entity_type, raw_id)
                    attrs = dict(row)
                    # Normalize type-style fields so simulation code can rely on
                    # a unified "type" key for both synthetic and real graphs.
                    attrs["entity_type"] = entity_type
                    attrs["type"] = entity_type
                    self_id = f"{entity_type}:{raw_id}"
                    attrs["id"] = self_id
                    node_attributes[node_key] = attrs

        # Load relationship tables as edges (directed)
        # Each mapping: (source_type, target_type, filename, source_field, target_field, label)
        relation_specs = [
            ("Person", "Company", "PersonInvestCompany.csv", "investorId", "companyId", "invests"),
            (
                "Person",
                "Person",
                "PersonGuaranteePerson.csv",
                "fromId",
                "toId",
                "guarantees",
            ),
            ("Person", "Loan", "PersonApplyLoan.csv", "personId", "loanId", "applies_loan"),
            ("Company", "Loan", "CompanyApplyLoan.csv", "companyId", "loanId", "applies_loan"),
            (
                "Company",
                "Company",
                "CompanyGuaranteeCompany.csv",
                "fromId",
                "toId",
                "guarantees",
            ),
            (
                "Company",
                "Company",
                "CompanyInvestCompany.csv",
                "investorId",
                "companyId",
                "invests",
            ),
            ("Company", "Account", "CompanyOwnAccount.csv", "companyId", "accountId", "owns"),
            ("Person", "Account", "PersonOwnAccount.csv", "personId", "accountId", "owns"),
            ("Loan", "Account", "LoanDepositAccount.csv", "loanId", "accountId", "deposit_to"),
            (
                "Account",
                "Account",
                "AccountTransferAccount.csv",
                "fromId",
                "toId",
                "transfers",
            ),
            (
                "Account",
                "Account",
                "AccountWithdrawAccount.csv",
                "fromId",
                "toId",
                "withdraws",
            ),
            ("Account", "Loan", "AccountRepayLoan.csv", "accountId", "loanId", "repays"),
            ("Medium", "Account", "MediumSignInAccount.csv", "mediumId", "accountId", "binds"),
        ]

        edges: Dict[str, List[Dict[str, str]]] = {}

        def ensure_node(entity_type: str, raw_id: str) -> Optional[str]:
            key = (entity_type, raw_id)
            if key not in node_attributes:
                return None
            node_id = node_attributes[key]["id"]
            return node_id

        for src_type, tgt_type, filename, src_field, tgt_field, label in relation_specs:
            path = data_dir / filename
            if not path.exists():
                continue

            with path.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle, delimiter="|")
                for row in reader:
                    src_raw = row.get(src_field)
                    tgt_raw = row.get(tgt_field)
                    if not src_raw or not tgt_raw:
                        continue

                    src_id = ensure_node(src_type, src_raw)
                    tgt_id = ensure_node(tgt_type, tgt_raw)
                    if src_id is None or tgt_id is None:
                        continue

                    edges.setdefault(src_id, []).append({"target": tgt_id, "label": label})

        # If requested, sample a connected subgraph
        if self.config.real_subgraph_size is not None:
            node_ids, edges = self._sample_connected_subgraph(
                node_attributes, edges, self.config.real_subgraph_size
            )
            # Rebuild node_attributes restricted to sampled IDs
            node_attributes = {
                (attrs["entity_type"], attrs["id"].split(":", 1)[1]): attrs
                for (etype, raw_id), attrs in node_attributes.items()
                if attrs["id"] in node_ids
            }

        # Finalize into self.nodes / self.edges using string IDs only
        self.nodes = {}
        self.edges = {}
        for _, attrs in node_attributes.items():
            self.nodes[attrs["id"]] = attrs
            self.edges.setdefault(attrs["id"], [])

        for src_id, edge_list in edges.items():
            if src_id not in self.edges:
                continue
            for edge in edge_list:
                if edge["target"] in self.nodes:
                    self.edges[src_id].append(edge)

    def _sample_connected_subgraph(
        self,
        node_attributes: Dict[Tuple[str, str], Dict[str, Any]],
        edges: Dict[str, List[Dict[str, str]]],
        max_size: int,
    ) -> Tuple[Set[str], Dict[str, List[Dict[str, str]]]]:
        """Sample a connected subgraph while preserving edge integrity.

        Strategy:
            1. Build an undirected view of the real graph using current nodes/edges.
            2. Randomly pick a seed node and perform BFS until ``max_size`` nodes
               are reached or the component is exhausted.
            3. Restrict the edge set to edges whose both endpoints are within
               the sampled node set.
        """

        if not node_attributes:
            return set(), {}

        # Build adjacency for undirected BFS
        adj: Dict[str, Set[str]] = {}

        def add_undirected(u: str, v: str) -> None:
            adj.setdefault(u, set()).add(v)
            adj.setdefault(v, set()).add(u)

        for src_id, edge_list in edges.items():
            for edge in edge_list:
                tgt_id = edge["target"]
                add_undirected(src_id, tgt_id)

        all_node_ids: List[str] = [attrs["id"] for attrs in node_attributes.values()]
        seed = random.choice(all_node_ids)

        visited: Set[str] = {seed}
        queue: List[str] = [seed]

        while queue and len(visited) < max_size:
            current = queue.pop(0)
            for neighbor in adj.get(current, set()):
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append(neighbor)
                    if len(visited) >= max_size:
                        break

        # Restrict edges to sampled node set and keep them directed
        new_edges: Dict[str, List[Dict[str, str]]] = {}
        for src_id, edge_list in edges.items():
            if src_id not in visited:
                continue
            for edge in edge_list:
                if edge["target"] in visited:
                    new_edges.setdefault(src_id, []).append(edge)

        return visited, new_edges

    def _resolve_data_dir(self) -> Path:
        """Resolve the directory that contains real graph CSV files."""

        project_root = Path(__file__).resolve().parents[2]

        if self.config.real_data_dir:
            configured = Path(self.config.real_data_dir)
            if not configured.is_absolute():
                configured = project_root / configured
            if not configured.is_dir():
                raise FileNotFoundError(f"Real data directory not found: {configured}")
            return configured

        default_candidates = [
            project_root / "data" / "real_graph_data",
            project_root / "real_graph_data",
        ]
        for candidate in default_candidates:
            if candidate.is_dir():
                return candidate

        raise FileNotFoundError(
            "Unable to locate real graph data directory. "
            "Provide GraphGeneratorConfig.real_data_dir explicitly."
        )
