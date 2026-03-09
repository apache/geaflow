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

"""Real graph data loader for CASTS simulations."""

from __future__ import annotations

import csv
from pathlib import Path
import random
from typing import TYPE_CHECKING, Callable

from core.constants import EDGE_LABEL_KEY, EDGE_TARGET_KEY, NODE_ID_KEY, NODE_TYPE_KEY
from core.types import GraphEdges, GraphNodes, JsonDict

if TYPE_CHECKING:
    from harness.data.graph_generator import GraphGeneratorConfig

RealGraphLoader = Callable[
    ["GraphGeneratorConfig"],
    tuple[GraphNodes, GraphEdges],
]


def default_real_graph_loader(
    config: GraphGeneratorConfig,
) -> tuple[GraphNodes, GraphEdges]:
    """Load nodes and edges from real CSV data.

    The loader treats each business/financial entity as a node and relation tables
    as directed edges. It optionally samples a connected subgraph to keep the
    graph size manageable.
    """

    data_dir = _resolve_data_dir(config.real_data_dir)

    # Load entity tables as nodes
    entity_files = {
        "Person": "Person.csv",
        "Company": "Company.csv",
        "Account": "Account.csv",
        "Loan": "Loan.csv",
        "Medium": "Medium.csv",
    }

    node_attributes: dict[tuple[str, str], JsonDict] = {}

    for entity_type, filename in entity_files.items():
        path = data_dir / filename
        if not path.exists():
            continue

        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle, delimiter="|")
            for row in reader:
                # Assume there is an ``id`` column; if not, fall back to
                # the first column name as primary key.
                if NODE_ID_KEY in row:
                    raw_id = row[NODE_ID_KEY]
                else:
                    first_key = next(iter(row.keys()))
                    raw_id = row[first_key]

                node_key = (entity_type, raw_id)
                attrs = dict(row)
                # Normalize type-style fields so simulation code can rely on
                # a unified "type" key for both synthetic and real graphs.
                attrs["entity_type"] = entity_type
                attrs[NODE_TYPE_KEY] = entity_type
                self_id = f"{entity_type}:{raw_id}"
                attrs[NODE_ID_KEY] = self_id
                node_attributes[node_key] = attrs

    # Load relationship tables as edges (directed)
    # Each mapping: (source_type, target_type, filename, source_field, target_field, label)
    relation_specs = [
        ("Person", "Company", "PersonInvestCompany.csv", "investorId", "companyId", "invests"),
        ("Person", "Person", "PersonGuaranteePerson.csv", "fromId", "toId", "guarantees"),
        ("Person", "Loan", "PersonApplyLoan.csv", "personId", "loanId", "applies_loan"),
        ("Company", "Loan", "CompanyApplyLoan.csv", "companyId", "loanId", "applies_loan"),
        ("Company", "Company", "CompanyGuaranteeCompany.csv", "fromId", "toId", "guarantees"),
        ("Company", "Company", "CompanyInvestCompany.csv", "investorId", "companyId", "invests"),
        ("Company", "Account", "CompanyOwnAccount.csv", "companyId", "accountId", "owns"),
        ("Person", "Account", "PersonOwnAccount.csv", "personId", "accountId", "owns"),
        ("Loan", "Account", "LoanDepositAccount.csv", "loanId", "accountId", "deposit_to"),
        ("Account", "Account", "AccountTransferAccount.csv", "fromId", "toId", "transfers"),
        ("Account", "Account", "AccountWithdrawAccount.csv", "fromId", "toId", "withdraws"),
        ("Account", "Loan", "AccountRepayLoan.csv", "accountId", "loanId", "repays"),
        ("Medium", "Account", "MediumSignInAccount.csv", "mediumId", "accountId", "binds"),
    ]

    edges: dict[str, list[dict[str, str]]] = {}

    def ensure_node(entity_type: str, raw_id: str) -> str | None:
        key = (entity_type, raw_id)
        if key not in node_attributes:
            return None
        return node_attributes[key][NODE_ID_KEY]

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

                edges.setdefault(src_id, []).append(
                    {EDGE_TARGET_KEY: tgt_id, EDGE_LABEL_KEY: label}
                )

    # If requested, sample a connected subgraph
    if config.real_subgraph_size is not None:
        node_ids, edges = _sample_connected_subgraph(
            node_attributes, edges, config.real_subgraph_size
        )
        # Rebuild node_attributes restricted to sampled IDs
        node_attributes = {
            (attrs["entity_type"], attrs[NODE_ID_KEY].split(":", 1)[1]): attrs
            for (etype, raw_id), attrs in node_attributes.items()
            if attrs[NODE_ID_KEY] in node_ids
        }

    # Finalize into nodes / edges using string IDs only
    nodes: GraphNodes = {}
    normalized_edges: dict[str, list[dict[str, str]]] = {}
    for _, attrs in node_attributes.items():
        nodes[attrs[NODE_ID_KEY]] = attrs
        normalized_edges.setdefault(attrs[NODE_ID_KEY], [])

    for src_id, edge_list in edges.items():
        if src_id not in normalized_edges:
            continue
        for edge in edge_list:
            if edge[EDGE_TARGET_KEY] in nodes:
                normalized_edges[src_id].append(edge)

    return nodes, normalized_edges


def _sample_connected_subgraph(
    node_attributes: dict[tuple[str, str], JsonDict],
    edges: dict[str, list[dict[str, str]]],
    max_size: int,
) -> tuple[set[str], dict[str, list[dict[str, str]]]]:
    """Sample a connected subgraph while preserving edge integrity."""

    if not node_attributes:
        return set(), {}

    # Build adjacency for undirected BFS
    adj: dict[str, set[str]] = {}

    def add_undirected(u: str, v: str) -> None:
        adj.setdefault(u, set()).add(v)
        adj.setdefault(v, set()).add(u)

    for src_id, edge_list in edges.items():
        for edge in edge_list:
            tgt_id = edge[EDGE_TARGET_KEY]
            add_undirected(src_id, tgt_id)

    all_node_ids: list[str] = [attrs[NODE_ID_KEY] for attrs in node_attributes.values()]
    seed = random.choice(all_node_ids)

    visited: set[str] = {seed}
    queue: list[str] = [seed]

    while queue and len(visited) < max_size:
        current = queue.pop(0)
        for neighbor in adj.get(current, set()):
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append(neighbor)
                if len(visited) >= max_size:
                    break

    # Restrict edges to sampled node set and keep them directed
    new_edges: dict[str, list[dict[str, str]]] = {}
    for src_id, edge_list in edges.items():
        if src_id not in visited:
            continue
        for edge in edge_list:
            if edge[EDGE_TARGET_KEY] in visited:
                new_edges.setdefault(src_id, []).append(edge)

    return visited, new_edges


def _resolve_data_dir(real_data_dir: str | None) -> Path:
    """Resolve the directory that contains real graph CSV files."""

    project_root = Path(__file__).resolve().parents[2]

    if real_data_dir:
        configured = Path(real_data_dir)
        if not configured.is_absolute():
            configured = project_root / configured
        if not configured.is_dir():
            raise FileNotFoundError(f"Real data directory not found: {configured}")
        return configured

    default_candidates = [
        project_root / "harness" / "data" / "real_graph_data",
        project_root / "real_graph_data",
    ]
    for candidate in default_candidates:
        if candidate.is_dir():
            return candidate

    raise FileNotFoundError(
        "Unable to locate real graph data directory. "
        "Provide GraphGeneratorConfig.real_data_dir explicitly."
    )
