"""Data source implementations for CASTS system.

This module provides concrete implementations of the DataSource interface
for both synthetic and real data sources.
"""

from collections import deque
import csv
from pathlib import Path
import random
from typing import Any, Dict, List, Optional, Set, Tuple

import networkx as nx

from casts.core.config import DefaultConfiguration
from casts.core.interfaces import Configuration, DataSource, GoalGenerator, GraphSchema
from casts.core.schema import InMemoryGraphSchema


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
        self._goal_weights = [100, 60, 40, 25, 15]

    @property
    def goal_texts(self) -> List[str]:
        return [g[0] for g in self._goals]

    @property
    def goal_weights(self) -> List[int]:
        return self._goal_weights.copy()

    def select_goal(self, node_type: Optional[str] = None) -> Tuple[str, str]:
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
        guarantee = (
            "guarantee" if "guarantee" in edge_labels else "guarantee relation"
        )
        transfer = "transfer" if "transfer" in edge_labels else "transfer relation"
        withdraw = "withdraw" if "withdraw" in edge_labels else "withdraw relation"
        repay = "repay" if "repay" in edge_labels else "repay relation"
        deposit = "deposit" if "deposit" in edge_labels else "deposit relation"
        apply = "apply" if "apply" in edge_labels else "apply relation"
        own = "own" if "own" in edge_labels else "ownership relation"

        # Construct a set of risk / AML / relationship-analysis oriented goals
        self._goals = [
            (
                f"""Given a {person}, walk along {invest} / {guarantee} / {own} / {apply} edges to analyse multi-hop connections to high-risk {company} and {loan} nodes for credit-risk QA.""",
                f"""Score is based on identifying paths connecting a {person} to a high-risk {company} or {loan}. The shorter the path, the higher the score. Paths that fail to reach a risky entity receive 0 points.""",
            ),
            (
                f"""Starting from an {account}, follow {transfer} / {withdraw} / {repay} / {deposit} transaction edges to trace money flows to suspicious {loan} nodes or unusually active {person} nodes, producing evidence paths for risk QA.""",
                f"""Score is based on following transaction-related edges ({transfer}, {repay}, etc.) to a suspicious node. The path must follow the flow of money. Paths that use non-financial links are penalized.""",
            ),
            (
                f"""For a single {company}, combine its {own} {account} nodes, {apply} loans, and roles as a {guarantee} provider to build explanatory QA that evaluates risk concentration in the overall guarantee network.""",
                f"""Score is based on identifying how many distinct risk-related paths (ownership, loans, guarantees) originate from a single {company}. Higher scores for paths that show high concentration.""",
            ),
            (
                f"""Between {person} and {company} nodes, explore chained {invest} / {own} / {apply} / {guarantee} relations to discover potential related parties and benefit-transfer paths, and generate audit-style QA in natural language.""",
                f"""Score is based on finding a chain of at least 3 steps connecting a {person} to a {company} through investment, ownership, or guarantee links. The more varied the links, the better.""",
            ),
            (
                f"""Pick a high-risk {loan} node and expand along {repay} / {deposit} / {transfer} edges to find abnormal money cycles and key {account} nodes, providing evidence for AML-style QA.""",
                """Score is highest for paths that form a cycle (e.g., A->B->C->A) representing potential money laundering. The closer the path is to a closed loop, the higher the score.""",
            ),
            (
                f"""Between {company} nodes, walk multi-hop {invest} and {guarantee} relations to identify tightly cross-invested or mutually guaranteed company clusters and explain their structural patterns in QA form.""",
                """Score is based on identifying reciprocal relationships (e.g., Company A invests in B, and B invests in A) or short cycles of investment/guarantee between companies. Simple one-way paths are less valuable.""",
            ),
            (
                f"""For a given {person}, answer through how many {apply} / {own} / {guarantee} / {invest} chains they are indirectly exposed to high-risk {loan} or high-risk {company} nodes, and return representative paths.""",
                f"""Score is based on the path length connecting a {person} to a high-risk entity. Longer, more indirect paths that successfully connect to the target are valuable. Paths that don't terminate at a risky entity are penalized.""",
            ),
        ]

        # Heuristic weight distribution; can be tuned by future statistics
        self._goal_weights = [100, 90, 80, 70, 60, 50, 40]

    @property
    def goal_texts(self) -> List[str]:
        return [g[0] for g in self._goals]

    @property
    def goal_weights(self) -> List[int]:
        return self._goal_weights.copy()

    def select_goal(self, node_type: Optional[str] = None) -> Tuple[str, str]:
        """Weighted random selection; optionally bias by node_type.

        If ``node_type`` is provided, slightly bias towards goals whose
        text mentions that type; otherwise fall back to simple
        weighted random sampling over all goals.
        """

        # Simple heuristic: filter a small candidate subset by node_type
        candidates: list[tuple[str, str]] = self._goals
        weights: list[int] = self._goal_weights

        if node_type is not None:
            node_type_lower = node_type.lower()
            filtered: List[Tuple[Tuple[str, str], int]] = []

            for goal_tuple, w in zip(self._goals, self._goal_weights, strict=False):
                text = goal_tuple[0]
                if node_type_lower in text.lower():
                    # 同类型的目标权重放大一些
                    filtered.append((goal_tuple, w * 2))

            if filtered:
                c_tuple, w_tuple = zip(*filtered, strict=False)
                candidates = list(c_tuple)
                weights = list(w_tuple)

        selected_goal, selected_rubric = random.choices(
            candidates, weights=weights, k=1
        )[0]
        return selected_goal, selected_rubric


class SyntheticDataSource(DataSource):
    """Synthetic graph data source with Zipf distribution."""

    def __init__(self, size: int = 30):
        """Initialize synthetic data source.
        
        Args:
            size: Number of nodes to generate
        """
        self._nodes: Dict[str, Dict[str, Any]] = {}
        self._edges: Dict[str, List[Dict[str, str]]] = {}
        self._source_label = "synthetic"
        # NOTE: For synthetic graphs we assume the generated data is immutable
        # after initialization. If you mutate `nodes` / `edges` at runtime, you
        # must call `get_schema()` again so a fresh InMemoryGraphSchema (and
        # fingerprint) is built.
        self._goal_generator: Optional[GoalGenerator] = None
        self._generate_zipf_data(size)
        self._schema = InMemoryGraphSchema(self._nodes, self._edges)
        self._goal_generator = SyntheticBusinessGraphGoalGenerator()

    @property
    def nodes(self) -> Dict[str, Dict[str, Any]]:
        return self._nodes

    @property
    def edges(self) -> Dict[str, List[Dict[str, str]]]:
        return self._edges

    @property
    def source_label(self) -> str:
        return self._source_label

    def get_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        return self._nodes.get(node_id)

    def get_neighbors(self, node_id: str, edge_label: Optional[str] = None) -> List[str]:
        """Get neighbor node IDs for a given node."""
        if node_id not in self._edges:
            return []

        neighbors = []
        for edge in self._edges[node_id]:
            if edge_label is None or edge['label'] == edge_label:
                neighbors.append(edge['target'])
        return neighbors

    def get_schema(self) -> GraphSchema:
        """Get the graph schema for this data source."""
        if self._schema is None:
            self._schema = InMemoryGraphSchema(self._nodes, self._edges)
        return self._schema

    def get_goal_generator(self) -> GoalGenerator:
        """Get the goal generator for this data source."""
        if self._goal_generator is None:
            self._goal_generator = SyntheticBusinessGraphGoalGenerator()
        return self._goal_generator

    def _generate_zipf_data(self, size: int):
        """Generate synthetic data following Zipf distribution."""
        business_types = [
            'Retail SME',
            'Logistics Partner',
            'Enterprise Vendor',
            'Regional Distributor',
            'FinTech Startup',
        ]
        type_weights = [100, 50, 25, 12, 6]

        business_categories = ['retail', 'wholesale', 'finance', 'manufacturing']
        regions = ['NA', 'EU', 'APAC', 'LATAM']
        risk_levels = ['low', 'medium', 'high']

        # Generate nodes
        for i in range(size):
            node_type = random.choices(business_types, weights=type_weights, k=1)[0]
            status = 'active' if random.random() < 0.8 else 'inactive'
            age = random.randint(18, 60)

            node = {
                'id': str(i),
                'type': node_type,
                'category': random.choice(business_categories),
                'region': random.choice(regions),
                'risk': random.choice(risk_levels),
                'status': status,
                'age': age,
            }
            self._nodes[str(i)] = node

        # Generate edges with more structured, denser relationship patterns
        edge_labels = ['friend', 'supplier', 'partner', 'investor', 'customer']

        # 基础随机度：保证每个点有一定随机边
        for i in range(size):
            base_degree = random.randint(1, 3)  # 原来是 0~3，现在保证至少 1 条
            for _ in range(base_degree):
                target_id = str(random.randint(0, size - 1))
                if target_id == str(i):
                    continue
                label = random.choice(edge_labels)
                edge = {'target': target_id, 'label': label}
                self._edges.setdefault(str(i), []).append(edge)

        # 结构性“偏好”：不同业务类型偏向某些关系，有利于 LLM 学习到稳定模板
        for i in range(size):
            src_id = str(i)
            node_type = self._nodes[src_id]['type']

            # Retail SME: more customer / supplier edges
            if node_type == 'Retail SME':
                extra_labels = ['customer', 'supplier']
                extra_edges = 2
            # Logistics Partner: more partner / supplier edges
            elif node_type == 'Logistics Partner':
                extra_labels = ['partner', 'supplier']
                extra_edges = 2
            # Enterprise Vendor: more supplier / investor edges
            elif node_type == 'Enterprise Vendor':
                extra_labels = ['supplier', 'investor']
                extra_edges = 2
            # Regional Distributor: more partner / customer edges
            elif node_type == 'Regional Distributor':
                extra_labels = ['partner', 'customer']
                extra_edges = 2
            # FinTech Startup: more investor / partner edges
            else:  # 'FinTech Startup'
                extra_labels = ['investor', 'partner']
                extra_edges = 3  # 稍微高一点，帮你测试深度路径

            for _ in range(extra_edges):
                target_id = str(random.randint(0, size - 1))
                if target_id == src_id:
                    continue
                label = random.choice(extra_labels)
                edge = {'target': target_id, 'label': label}
                self._edges.setdefault(src_id, []).append(edge)

        # 可选：轻微增加“friend”全局连通性，避免太多孤立子图
        for i in range(size):
            src_id = str(i)
            if random.random() < 0.3:  # 30% 节点额外加一条 friend 边
                target_id = str(random.randint(0, size - 1))
                if target_id != src_id:
                    edge = {'target': target_id, 'label': 'friend'}
                    self._edges.setdefault(src_id, []).append(edge)


class RealDataSource(DataSource):
    """Real graph data source loaded from CSV files."""

    def __init__(self, data_dir: str, max_nodes: Optional[int] = None):
        """Initialize real data source.

        Args:
            data_dir: Directory containing CSV files
            max_nodes: Maximum number of nodes to load (for sampling)
        """
        self._nodes: Dict[str, Dict[str, Any]] = {}
        self._edges: Dict[str, List[Dict[str, str]]] = {}
        self._source_label = "real"
        self._data_dir = Path(data_dir)
        self._max_nodes = max_nodes
        self._config = DefaultConfiguration()

        # Schema is now lazily loaded and will be constructed on the first
        # call to `get_schema()` after the data is loaded.
        self._schema: Optional[GraphSchema] = None
        self._schema_dirty = True  # Start with a dirty schema
        self._goal_generator: Optional[GoalGenerator] = None
        self._load_real_graph()

        # Defer goal generator creation until schema is accessed
        # self._goal_generator = RealBusinessGraphGoalGenerator(node_types, edge_labels)

    @property
    def nodes(self) -> Dict[str, Dict[str, Any]]:
        return self._nodes

    @property
    def edges(self) -> Dict[str, List[Dict[str, str]]]:
        return self._edges

    @property
    def source_label(self) -> str:
        return self._source_label

    def get_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        return self._nodes.get(node_id)

    def get_neighbors(self, node_id: str, edge_label: Optional[str] = None) -> List[str]:
        """Get neighbor node IDs for a given node."""
        if node_id not in self._edges:
            return []

        neighbors = []
        for edge in self._edges[node_id]:
            if edge_label is None or edge['label'] == edge_label:
                neighbors.append(edge['target'])
        return neighbors

    def reload(self):
        """Reload data from source and invalidate the schema and goal generator."""
        self._load_real_graph()
        self._schema_dirty = True
        self._goal_generator = None

    def get_schema(self) -> GraphSchema:
        """Get the graph schema for this data source.

        The schema is created on first access and recreated if the data
        source has been reloaded.
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

    def _add_shared_medium_links(self):
        """Add edges between account owners who share a login medium."""
        medium_to_accounts = {}
        signin_edges: list[tuple[str, str]] = self._find_edges_by_label('signin', 'Medium', 'Account')

        for medium_id, account_id in signin_edges:
            if medium_id not in medium_to_accounts:
                medium_to_accounts[medium_id] = []
            medium_to_accounts[medium_id].append(account_id)

        # Build owner map
        owner_map = {}
        person_owns: list[tuple[str, str]] = self._find_edges_by_label('own', 'Person', 'Account')
        company_owns: list[tuple[str, str]] = self._find_edges_by_label('own', 'Company', 'Account')
        for src, tgt in person_owns:
            owner_map[tgt] = src
        for src, tgt in company_owns:
            owner_map[tgt] = src

        new_edges = 0
        for medium_id, accounts in medium_to_accounts.items():
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
                            self._add_edge_if_not_exists(owner1_id, owner2_id, 'shared_medium')
                            self._add_edge_if_not_exists(owner2_id, owner1_id, 'shared_medium')
                            new_edges += 2

        if new_edges > 0:
            print(f"Connectivity enhancement: Added {new_edges} 'shared_medium' edges based on login data.")

    def _add_owner_links(self):
        """Add edges between owners of accounts that have transactions."""
        # Build an owner map: account_id -> owner_id
        owner_map = {}
        person_owns: list[tuple[str, str]] = self._find_edges_by_label('own', 'Person', 'Account')
        company_owns: list[tuple[str, str]] = self._find_edges_by_label('own', 'Company', 'Account')

        for src, tgt in person_owns:
            owner_map[tgt] = src
        for src, tgt in company_owns:
            owner_map[tgt] = src

        # Find all transfer edges
        transfer_edges: list[tuple[str, str]] = self._find_edges_by_label('transfer', 'Account', 'Account')

        new_edges = 0
        for acc1_id, acc2_id in transfer_edges:
            owner1_id = owner_map.get(acc1_id)
            owner2_id = owner_map.get(acc2_id)

            if owner1_id and owner2_id and owner1_id != owner2_id:
                # Add a 'related_to' edge in both directions
                self._add_edge_if_not_exists(owner1_id, owner2_id, 'related_to')
                self._add_edge_if_not_exists(owner2_id, owner1_id, 'related_to')
                new_edges += 2

        if new_edges > 0:
            print(f"Connectivity enhancement: Added {new_edges} 'related_to' edges based on ownership.")

    def _find_edges_by_label(
        self, label: str, from_type: str, to_type: str
    ) -> list[tuple[str, str]]:
        """Helper to find all edges of a certain type."""
        edges = []

        # Check for special cases in the config first.
        special_cases = self._config.get("EDGE_FILENAME_MAPPING_SPECIAL_CASES", {})
        key = label
        if from_type:
            key = f"{label.lower()}_{from_type.lower()}" # e.g., "own_person"

        filename = special_cases.get(key, special_cases.get(label))

        # If not found, fall back to the standard naming convention.
        if not filename:
            filename = f"{from_type}{label.capitalize()}{to_type}.csv"

        filepath = self._data_dir / filename

        try:
            with open(filepath, encoding='utf-8') as f:
                reader = csv.reader(f, delimiter='|')
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
            if edge['target'] == tgt_id and edge['label'] == label:
                return  # Edge already exists

        self._edges[src_id].append({'target': tgt_id, 'label': label})



    def _load_nodes_from_csv(self, filepath: Path, entity_type: str):
        """Load nodes from a CSV file using actual column names as attributes."""
        if not filepath.exists():
            return

        try:
            with open(filepath, encoding='utf-8') as f:
                # Use DictReader to get actual column names
                reader = csv.DictReader(f, delimiter='|')
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
                        'id': node_id,
                        'type': entity_type,
                        'raw_id': raw_id,
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
            with open(filepath, encoding='utf-8') as f:
                reader = csv.reader(f, delimiter='|')
                for row in reader:
                    if len(row) >= 2:
                        src_id = f"{from_type}_{row[0]}"
                        tgt_id = f"{to_type}_{row[1]}"

                        # Only add edge if both nodes exist
                        if src_id in self._nodes and tgt_id in self._nodes:
                            edge = {'target': tgt_id, 'label': label}
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
        for src_id, edge_list in self._edges.items():
            for edge in edge_list:
                G.add_edge(src_id, edge['target'], label=edge['label'])

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
            nodes_by_type: Dict[str, List[str]] = {}
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
                candidates: List[str] = []
                for _, nbr in G.out_edges(current):
                    candidates.append(nbr)
                for nbr, _ in G.in_edges(current):
                    candidates.append(nbr)

                # Deduplicate while keeping a stable order.
                deduped: List[str] = []
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
                        0
                        if G.nodes[nid].get("type", "Unknown") not in seen_types
                        else 1
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
            node_id: node
            for node_id, node in self._nodes.items()
            if node_id in sampled_nodes
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
            data_dir = config.get_str('SIMULATION_REAL_DATA_DIR')
            max_nodes = config.get_int('SIMULATION_REAL_SUBGRAPH_SIZE')
            return RealDataSource(data_dir=data_dir, max_nodes=max_nodes)
        else:
            size = config.get_int('SIMULATION_GRAPH_SIZE', 30)
            return SyntheticDataSource(size=size)
