# CASTS Architecture Documentation

## Overview

The CASTS (Context-Aware Strategy Cache System) project is designed with a clean, modular architecture that ensures clear separation of concerns between core logic, external services, data management, and simulation execution.

## Architecture Structure

```
casts/
├── __init__.py                      # Main package entry point
├── core/                            # Core models, services, and configuration
│   ├── __init__.py
│   ├── config.py                    # Configuration management
│   ├── interfaces.py                # Abstract interfaces: GraphSchema, GoalGenerator, DataSource
│   ├── models.py                    # Context, StrategyKnowledgeUnit
│   ├── services.py                  # StrategyCache
│   ├── schema.py                    # InMemoryGraphSchema implementation
│   └── gremlin_state.py             # GremlinStateMachine
├── services/                        # External service integrations
│   ├── __init__.py
│   ├── embedding.py                 # EmbeddingService
│   ├── llm_oracle.py                # LLMOracle
│   └── path_judge.py                # PathJudge: generic LLM-based path evaluator
├── data/                            # Data generation and management
│   ├── __init__.py
│   ├── graph_generator.py           # GraphGenerator
│   └── sources.py                   # DataSourceFactory, implementations, and goal generators
│                                    # - SyntheticDataSource, RealDataSource
│                                    # - SyntheticBusinessGraphGoalGenerator, RealBusinessGraphGoalGenerator, etc.
├── simulation/                      # Simulation framework + evaluation
│   ├── __init__.py
│   ├── engine.py                    # SimulationEngine
│   ├── executor.py                  # TraversalExecutor
│   ├── metrics.py                   # MetricsCollector
│   ├── runner.py                    # Main entry point
│   ├── visualizer.py                # SimulationVisualizer
│   └── evaluator.py                 # PathEvaluator + BatchEvaluator (LLM-based verifier)
└── utils/                           # Utility functions
    ├── __init__.py
    └── helpers.py                   # Helper functions for signatures, fingerprints, etc.
```

### Simulation Engine Features

- `casts/simulation/executor.py` always generates **Level 2 (canonical)** signatures by appending the full decision string (e.g., `out('friend')`, `has('type','Person')`) to the traversal path. This ensures all edge labels and filter parameters are preserved in the knowledge base.
- The executor natively supports bidirectional traversal templates (`both('label')` and `bothE('label')`), merging inbound and outbound edges.
- Signature abstraction for matching purposes is handled separately by `StrategyCache` at query time (see Section 2.1 and 2.3).
- Execution logging for all edge modes is normalized to keep diagnostics readable and lint-compliant.
- Traversal errors are trapped via a narrow set of runtime exceptions so simulations keep running even if a malformed SKU decision occurs.
- The simulation engine does not own hard-coded business goals; all traversal objectives come from the `DataSource`'s `GoalGenerator`, keeping experiments domain-agnostic.

### LLM-Based Path Evaluation (Verifier)

- The module `casts/simulation/evaluator.py` implements `PathEvaluator` and `BatchEvaluator` for scoring full traversal paths.
- `PathEvaluator` decomposes each path into five dimensions with fixed weights (summing to 100):
  - **Query effectiveness (0–35)** – The primary quality signal, driven by an LLM-based judge.
  - **Strategy reusability (0–25)** – SKU reuse, structural signature depth, and decision pattern stability.
  - **Cache hit efficiency (0–20)** – Tier1/Tier2 hit rates vs. LLM fallbacks along the path.
  - **Decision consistency (0–15)** – Direction/type transition regularity across steps.
  - **Information utility (0–5)** – Diversity and density of surfaced node attributes.
- The `_score_query_effectiveness` method builds a rich, schema-aware prompt for the `PathJudge`. Crucially, it injects a specific **`evaluation_rubric`** that is bundled with the `goal` by the `GoalGenerator`. This forces the Judge to use the exact same criteria that the reasoning agent was trying to satisfy, solving the "goal/evaluation disconnect" problem.
- The prompt generation logic correctly describes the traversal path, even for paths that terminate immediately after the start node. It provides both a natural-language step-by-step summary and an ASCII-art graph representation to give the Judge full context.
  - The prompt instructs the LLM to return a single ```json block with the shape:
    `{ "reasoning": { "notes": "<string>" }, "score": <0–35> }`.
  - The raw LLM response is parsed, and the `score` and `reasoning` are stored for analysis.
- `PathJudge` is a thin, reusable wrapper over the chat-completions API, accepting an arbitrary `instructions` string.
- `runner.py` wires the verifier behind the `SIMULATION_ENABLE_VERIFIER` configuration flag and implements a two-stage evaluation process:
  - **Immediate Evaluation (Per-Request)**: The `SimulationEngine` now accepts an `on_request_completed` callback. The `runner` provides a function that is triggered the moment a request's traversal path is complete. This function immediately calls `BatchEvaluator` for that single request and prints a detailed `[Request X Verifier]` block for real-time feedback.
  - **Final Summary (Global)**: The `runner` also collects all individual evaluation results. At the very end of the simulation, it calls `BatchEvaluator.print_batch_summary()` one last time with the complete set of results. This prints a global summary, including aggregate statistics (average/min/max scores, grade distribution) and a breakdown of the top 3 and bottom 3 performing paths.
- The evaluator is schema-agnostic by construction:
  - For synthetic graphs, it highlights conventional business fields (`region`, `risk`, `status`, `category`) when present.
  - For real CSV graphs, it falls back to a generic `key=value` attribute summary per step, with automatic truncation for very wide schemas; no fields are hard-coded or assumed.

### Graph Schema and Goal Generation

The architecture cleanly separates graph structural knowledge and traversal objectives from the simulation engine:

#### GraphSchema Abstraction (`casts/core/interfaces.py`, `casts/core/schema.py`)

- `GraphSchema` ABC defines the contract for schema introspection: node types, edge labels, validation
- `InMemoryGraphSchema` provides a concrete implementation built from runtime node/edge data
- Schema instances are provided by `DataSource.get_schema()`, enabling each data source to expose its own structural constraints
- The LLM oracle uses schema information to constrain generated decisions to valid edge labels

#### GoalGenerator Interface (`casts/core/interfaces.py`, `casts/data/sources.py`)

- `GoalGenerator` ABC abstracts over traversal goal generation with `goal_texts`, `goal_weights`, and `select_goal()`
- Concrete implementations:
  - `SyntheticBusinessGraphGoalGenerator`: Intent-driven financial/business goals for synthetic graphs, explicitly phrased around multi-hop `friend`, `supplier`, `partner`, `investor`, `customer` relationships
  - `SocialGraphGoalGenerator`: Friend recommendations, community detection, influence paths
  - `GenericGraphGoalGenerator`: Fallback for unknown graph types
- Goal generators are provided by `DataSource.get_goal_generator()`, coupling goals to the graph domain
- `SimulationEngine` calls `graph.get_goal_generator().select_goal()` and never hardcodes goal texts or weights
- For the synthetic business graph, the goals encourage the LLM to:
  - explore communities via `friend` / `partner` multi-hop neighborhoods,
  - walk along `supplier` / `customer` / `investor` chains,
  - prefer repeated local traversal decisions over one-shot global optimization claims.

#### DataSource Integration (`casts/core/interfaces.py`, `casts/data/sources.py`)

- `DataSource` ABC requires implementations to provide both `get_schema()` and `get_goal_generator()`
- `SyntheticDataSource` generates a Zipf-distributed synthetic business graph with denser, type-aware relationships (e.g. Retail SME biased to `customer/supplier`, Logistics Partner biased to `partner/supplier`) and pairs it with `SyntheticBusinessGraphGoalGenerator`
- `RealDataSource` loads CSV datasets into an in-memory directed graph and uses a dedicated `RealBusinessGraphGoalGenerator` that turns the concrete entity and relation types (Person, Company, Account, Loan, `invest`, `guarantee`, `transfer`, etc.) into English, QA-style traversal goals tailored to risk, AML and audit workloads.
- When a `max_nodes` limit is configured, `RealDataSource` builds a `networkx` digraph, finds the largest weakly connected component, and then performs a BFS-style expansion from a random seed node inside that component to collect up to `max_nodes` nodes. This neighborhood-preserving sampling keeps the sampled subgraph structurally dense and avoids isolated nodes, which is crucial for multi-hop template learning.
- This design allows the same simulation engine to run on different graph domains by simply switching data sources, while each data source remains free to define its own schema snapshot, goal distribution, and sampling strategy.

#### RealDataSource, Connectivity Enhancement, and Subgraph Sampling

The `RealDataSource` class is responsible for loading graph data from CSV files and preparing it for simulation. Given that real-world datasets can be massive and suffer from poor connectivity (isolated nodes, fragmented components), `RealDataSource` implements a sophisticated multi-stage process to produce a high-quality, dense, and connected subgraph.

1. **Full Graph Loading**: It begins by loading all nodes and edges from the specified CSV files into an in-memory `networkx` `DiGraph`.

2. **Connectivity Enhancement**: Before any sampling occurs, it enhances the graph's connectivity by adding new, logically-derived edges:
    - **Owner Links (`_add_owner_links`)**: If two distinct owners (e.g., `Person` or `Company`) have accounts that transacted with each other, a `related_to` edge is added between the owners. This directly connects entities involved in financial flows.
    - **Shared Medium Links (`_add_shared_medium_links`)**: If multiple owners log in using the same device (`Medium`), bidirectional `shared_medium` edges are added between them, flagging a potential real-world connection.

3. **Connected Subgraph Sampling (`_sample_subgraph`)**: If a `max_nodes` limit is configured, the class avoids naive random sampling, which would destroy graph structure. Instead, it performs a neighborhood-preserving sampling strategy:
    - **Find Largest Component**: It first identifies the largest weakly connected component in the full graph, immediately discarding all isolated subgraphs.
    - **BFS Expansion**: It then selects a random seed node from within this largest component and performs a breadth-first search (BFS) style expansion, collecting nodes until the `max_nodes` limit is reached.
    - **Type-Aware Expansion**: The BFS is not standard; it prioritizes expanding to nodes of a type not yet seen in the sample. This ensures the subgraph has a diverse mix of entities (e.g., `Person`, `Company`, `Loan`) even with a small size limit.
    - **Final Filtering**: Finally, the master node and edge lists are filtered to contain only the nodes collected during the BFS expansion and the edges between them.

This process guarantees that the graph used by the `SimulationEngine` is a single, densely connected component, which is crucial for learning meaningful multi-hop traversal strategies and avoiding the "dead end" and "isolated island" problems observed in raw data.

#### Simulation Flow

- `runner.py` instantiates a `DataSource` (synthetic or real) via factory
- `SimulationEngine` receives the data source, then queries it for schema and goals at runtime
- The engine does not hardcode goal texts or weights; everything flows through the `GoalGenerator` interface
- This enables realistic experiments: business graphs use business goals, social graphs use social goals, etc.
- On the synthetic business graph, this leads to:
  - LLM-generated multi-hop templates such as `out('friend')`, `both('partner')`, `both('friend')`
  - observed hit rates around 60%+ in steady state, reflecting how CASTS learns and reuses navigation strategies over repeated workloads rather than computing globally optimal paths.

The decoupling achieves:

- **Reusability**: Same engine, different domains
- **Extensibility**: New graph types just need new `DataSource` + `GoalGenerator` implementations
- **Testability**: Schema and goals can be unit-tested independently
- **Mathematical fidelity**: Goals and schema constraints are explicit inputs to the LLM oracle, matching the $c = (s, p, g)$ model

## Mathematical Model Alignment

This section sketches, in a paper-style and at a high level, how the refactored CASTS architecture realizes the mathematical model described in `数学建模.md`. We focus on the mapping between (1) mathematical objects, (2) architectural modules, and (3) the behavior of the approximate decision function $\hat f_{\text{cache}}$.

### 1. Global Goal and Layered Decomposition

In the mathematical document, CASTS is defined around an expensive LLM decision function
$$
f : \mathcal{C} \to \mathcal{D}
$$
and a cheaper approximate function
$$
\hat f_{\text{cache}} : \mathcal{C} \to \mathcal{D} \cup \{\bot\}
$$
that must simultaneously satisfy three constraints:

1. **Correctness**: low conditional error when the cache decides;
2. **Efficiency**: $T_{\text{cache}}(c) \ll T_{LLM}(c)$;
3. **Coverage**: high probability of not falling back (high hit rate).

The refactored package layout mirrors this decomposition:

- `casts/core/` encodes the *mathematical state* and *local decision logic* (contexts, SKUs, strategy cache);
- `casts/services/` encapsulates *external oracles* (LLM and embedding) that implement $f$ and $e$ in the model;
- `casts/data/` and `casts/simulation/` provide the *workload and experimental harness* for theorems about hit rate, error rate, and latency under Zipf/long-tail assumptions;
- `casts/utils/` contains small, pure functions such as signatures and fingerprints that correspond to $s$, $\rho$ and related primitives.

In other words, the refactoring makes the split between "mathematical core" and "environmental services" explicit in the code structure.

### 2. Mapping of Mathematical Objects to Modules

We summarize the key correspondences between the mathematical model and the refactored modules.

#### 2.1 Context decomposition $c = (s, p, g)$

- In the model, each decision context is decomposed as $c = (s, p, g)$, where $s$ is the structural path signature, $p$ the local property state, and $g$ the query goal.
- In the architecture, `casts/core/models.py` defines a `Context` dataclass that explicitly carries:
  - `structural_signature`: Current traversal path as a string (realizing $s$). The system uses a **"Canonical Storage, Abstract Matching"** architecture:
    - **Storage**: SKUs always store signatures in **Level 2 (canonical)** format: `"V().out('friend').has('type','Person').out('supplier')"` - preserving all edge labels and filter parameters
    - **Matching**: At runtime, both the query signature $s$ and stored signature $s_{\text{sku}}$ are dynamically abstracted to the configured `SIGNATURE_LEVEL` before comparison:
      - **Level 0** (Abstract matching): `"V().out().filter().out()"` - only Step types
      - **Level 1** (Edge-aware matching, default): `"V().out('friend').filter().out('supplier')"` - preserves edge labels, abstracts filters
      - **Level 2** (Full path matching): `"V().out('friend').has('type','Person').out('supplier')"` - exact match
    - This decoupling ensures the knowledge base remains information-lossless while matching strategy is flexibly configurable
  - `properties`: Current node properties dictionary (realizing $p$)
  - `goal`: Natural language description of the traversal objective (realizing $g$)
- The `Context` class provides a `safe_properties` property that filters out identity fields (id, node_id, uuid, etc.) using `IDENTITY_KEYS`, ensuring only decision-relevant attributes are used.
- Property filtering is implemented directly in the `Context` class rather than in separate helpers, keeping the logic close to the data structure.

**Rationale for canonical storage with edge labels**:

The "Canonical Storage, Abstract Matching" architecture addresses critical design requirements:

- **Problem**: If signatures were stored in abstract form (Level 0), edge semantics would be permanently lost. Abstract signatures like `"V().out().out()"` cannot distinguish semantically different paths such as `friend→friend` vs `transfer→loan` vs `guarantee→guarantee`, leading to SKU collision and incorrect decision reuse in fraud detection scenarios.

- **Solution**: By storing all SKUs in Level 2 (canonical) format, the knowledge base preserves complete path semantics. The abstraction logic is moved to the matching phase in `StrategyCache._to_abstract_signature()`:
  - Signature space: Level 0 = $O(3^d)$, Level 1 = $O((3|E|)^d)$, Level 2 = $O((3|E| \cdot F)^d)$ where $|E|$ is edge types and $F$ is filter combinations
  - Hash collision reduction: Level 1 vs Level 0 reduces collisions by ~1000x for typical graphs ($|E|=10$, $d=3$)
  - Runtime flexibility: Matching strategy can be changed via configuration without regenerating SKUs

- **Trade-off**: Level 1 (default) balances precision (edge semantics) with generalization (abstract filters). Level 0 remains available for highly homogeneous graphs, while Level 2 enables zero-tolerance critical paths.

#### 2.2 Strategy Knowledge Units (SKUs) and knowledge base $\mathcal{K}$

The mathematical definition
$$
    ext{SKU} = (c_{\text{sku}}, d_{\text{template}}, \rho, v_{\text{proto}}, \eta, \sigma_{\text{logic}})
$$
with $c_{\text{sku}} = (s_{\text{sku}}, \Phi, g_{\text{sku}})$
is reflected as follows:

- `casts/core/models.py` defines a `StrategyKnowledgeUnit` dataclass whose fields correspond one-to-one with the tuple above:
  - `id`: Unique identifier for this SKU
  - `structural_signature`: $s_{\text{sku}}$ - structural pattern that must match exactly
  - `predicate`: $\Phi(p)$ - boolean function over properties
  - `goal_template`: $g_{\text{sku}}$ - goal pattern that must match exactly
  - `decision_template`: $d_{\text{template}}$ - traversal step template (e.g., "out('friend')")
  - `schema_fingerprint`: $\rho$ - schema version identifier
  - `property_vector`: $v_{\text{proto}}$ - embedding of properties at creation time
  - `confidence_score`: $\eta$ - dynamic confidence score (AIMD updated), default 1.0
  - `logic_complexity`: $\sigma_{\text{logic}}$ - intrinsic logic complexity measure, default 1
- The class provides a `context_template` property that returns $(s_{\text{sku}}, \Phi, g_{\text{sku}})$ as defined in the mathematical model
- `casts/core/services.py` holds the in-memory collection of SKUs (the knowledge base $\mathcal{K}$) as a `List[StrategyKnowledgeUnit]` inside the `StrategyCache` service

#### 2.3 Double-layer matching $\mathcal{C}_{\text{strict}}$, $\mathcal{C}_{\text{sim}}$, $\mathcal{C}_{\text{valid}}$

Mathematically, the candidate sets are defined as
$$
\mathcal{C}_{\text{strict}}(c) = \{\text{SKU} \in \mathcal{K} \mid s_{\text{sku}}=s,\ g_{\text{sku}}=g,\ \Phi(p),\ \eta\ge\eta_{\min},\ \rho=\rho_{\text{current}}\},
$$
$$
\mathcal{C}_{\text{sim}}(c) = \{\text{SKU} \in \mathcal{K} \mid s_{\text{sku}}=s,\ g_{\text{sku}}=g,\ \text{sim}(e(p), v_{\text{proto}})\ge\delta_{\text{sim}}(v_{\text{proto}}),\ \eta\ge\eta_{\text{tier2}}(\eta_{\min}),\ \rho=\rho_{\text{current}}\},
$$
$$
\mathcal{C}_{\text{valid}}(c) = \mathcal{C}_{\text{strict}}(c)\ \cup\ (\mathcal{C}_{\text{sim}}(c)\setminus\mathcal{C}_{\text{strict}}(c)).
$$

In the architecture, these constructions are realized by `StrategyCache` in `casts/core/services.py`:

- Structural signature matching $(s_{\text{sku}}=s)$ is implemented via `_signatures_match(runtime_sig, stored_sig)`, which dynamically abstracts both signatures to the configured `SIGNATURE_LEVEL` before comparison (see Section 2.1 for the canonical storage architecture);
- $\mathcal{C}_{\text{strict}}(c)$ is formed by iterating through all SKUs in the knowledge base and filtering by:
  1. Signature match via `_signatures_match()` (abstracts both $s$ and $s_{\text{sku}}$ to the same level)
  2. Exact goal match ($g_{\text{sku}}=g$)
  3. Predicate evaluation ($\Phi(p)$ returns True)
  4. Fingerprint equality ($\rho = \rho_{\text{current}}$)
  5. Confidence threshold ($\eta \ge \eta_{\min}$)
- if $\mathcal{C}_{\text{strict}}(c)$ is empty, `StrategyCache` delegates to `EmbeddingService` (in `casts/services/embedding.py`) to compute $e(p)$ and similarities to $v_{\text{proto}}$, and then applies the stricter Tier 2 constraints ($\delta_{\text{sim}}$, $\eta_{\text{tier2}}(\eta_{\min})$) to obtain $\mathcal{C}_{\text{sim}}(c)$;
- finally, the union $\mathcal{C}_{\text{valid}}(c)$ is implicitly constructed by taking Tier 1 results if available, otherwise Tier 2 results, exactly as in the theory.

#### 2.4 Embedding and similarity

- The embedding function $e(p)$ and similarity function $\text{sim}(\cdot, \cdot)$ in the model are implemented by `EmbeddingService` in `casts/services/embedding.py`.
- `EmbeddingService` is an OpenAI-compatible client that calls external embedding APIs (e.g., Alibaba Cloud DashScope).
- The service provides `embed_text()` and `embed_properties()` methods for generating vector embeddings.
- Similarity computation uses cosine similarity implemented in `casts/utils/helpers.py`.
- Embedding is only invoked on the property component $p$ of the context, while $s$ and $g$ are treated symbolically and matched exactly, reflecting the sensitivity analysis in the mathematical document.

#### 2.5 LLM oracle and SKU generation

- The expensive LLM decision function $f$ and the one-shot SKU generation process are implemented by `LLMOracle` in `casts/services/llm_oracle.py`.
- `LLMOracle` is an OpenAI-compatible client that calls external LLM APIs (e.g., Kimi, GPT).
- When $\hat f_{\text{cache}}(c) = \bot$, the system calls `LLMOracle` to obtain $f(c)$, to extract or confirm a decision template $d_{\text{template}}$, and to synthesize new SKUs (including $\Phi$, $\sigma_{\text{logic}}$ and initial $\eta$), which are then stored in `StrategyCache`.
- The LLM oracle uses the embedding service to generate property embeddings for new SKUs.
- A separate `PathJudge` service in `casts/services/path_judge.py` is used *only* for scoring complete traversal paths under a task-specific rubric (e.g., query effectiveness in the verifier). It is intentionally generic: callers construct the full prompt (rubric + context) and are responsible for parsing JSON output.

#### 2.6 Configuration management

- All configuration parameters are centralized in `casts/core/config.py` via the `DefaultConfiguration` class.
- Configuration includes: embedding service settings, LLM service settings, simulation parameters, and cache hyperparameters.
- The `Configuration` abstract interface in `casts/core/interfaces.py` defines the contract for configuration management.
- `runner.py` loads all configuration from `DefaultConfiguration` and passes it to components, eliminating hard-coded values.

### 3. Implementation of $\hat f_{\text{cache}}$ and Tier 1 / Tier 2

The mathematical behavior of the cache
$$
\hat f_{\text{cache}}(c) =
\begin{cases}
    ext{instantiate}(\text{SKU}^*_{\text{strict}}, c), & \mathcal{C}_{\text{strict}}(c)\neq\emptyset, \\
    ext{instantiate}(\text{SKU}^*_{\text{sim}}, c), & \mathcal{C}_{\text{strict}}(c)=\emptyset \land \mathcal{C}_{\text{sim}}(c)\neq\emptyset, \\
\bot, & \text{otherwise}
\end{cases}
$$
is realized as follows:

1. `StrategyCache` exposes a decision method (e.g. `decide(context)`), where `context` is the concrete instance of $c=(s,p,g)$.
2. Inside this method, the cache first constructs $\mathcal{C}_{\text{strict}}(c)$ using exact $(s,g)$ lookup, predicate evaluation $\Phi(p)$, fingerprint checks, and the baseline confidence threshold $\eta_{\min}$.
3. If $\mathcal{C}_{\text{strict}}(c)$ is non-empty, the SKU with maximal $\eta$ is selected as $\text{SKU}^*_{\text{strict}}$ and instantiated with the current $p$, yielding the cached decision.
4. If $\mathcal{C}_{\text{strict}}(c)$ is empty, the cache computes $e(p)$ via `EmbeddingService`, filters candidates by $\text{sim}(e(p), v_{\text{proto}}) \ge \delta_{\text{sim}}(v_{\text{proto}})$ and $\eta \ge \eta_{\text{tier2}}(\eta_{\min})$, and ranks them by $\eta$ to obtain $\text{SKU}^*_{\text{sim}}$.
5. If both stages yield no candidate, the method returns $\bot$, causing the caller to fall back to `LLMOracle`.

This control flow is structurally identical to the mathematical definition of Tier 1 (logic) and Tier 2 (similarity) in the modeling document.

### 4. Confidence $\eta$, fingerprint $\rho$ and similarity threshold $\delta_{\text{sim}}$

The mathematical analysis introduces three additional mechanisms: the dynamic confidence score $\eta$, the schema fingerprint $\rho$, and the similarity threshold $\delta_{\text{sim}}(v)$ that depends on $\eta$ and $\sigma_{\text{logic}}$.

- **Confidence $\eta$** is stored on each SKU in `casts/core/models.py` and updated in `StrategyCache` based on runtime feedback (successful or failed executions), following the additive-increase / multiplicative-decrease or EMA-style rules described in the theory.
- **Fingerprint $\rho$** is computed via helpers in `casts/utils/helpers.py` and attached to each SKU; it is checked at lookup time so that any schema change invalidates stale SKUs by exclusion rather than by silent corruption.
- **Thresholds $\eta_{\min}$ and $\eta_{\text{tier2}}(\eta_{\min})$** are encoded as follows: a minimum confidence field on `StrategyCache` (e.g. `min_confidence_threshold`), corresponding to the global baseline $\eta_{\min}$ used in Tier 1; and a helper `calculate_tier2_threshold(\eta_{\min}, \gamma)` plus a cache parameter `tier2_gamma`, realizing the derived Tier 2 bound $\eta_{\text{tier2}}(\eta_{\min}) = \gamma \cdot \eta_{\min}$.
- **Similarity threshold $\delta_{\text{sim}}(v)$** is implemented as a function that takes a SKU's $\eta$ and $\sigma_{\text{logic}}$ and returns a per-SKU cosine threshold, matching the intended behavior of
    $$
    \delta_{\text{sim}}(v) = 1 - \frac{\kappa}{\sigma_{\text{logic}}(v) \cdot (1 + \beta \log \eta(v))}
    $$
    up to engineering choices of constants and exact functional form.

Together, these mechanisms ensure that the qualitative properties proven in the mathematical document (correctness under a given $\epsilon$, efficiency, and high effective hit rate $h_{\text{eff}}$ under Zipf-like workloads) are reflected in the concrete system behavior of the refactored code.
