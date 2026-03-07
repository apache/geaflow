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

"""Configuration management for CASTS system.

Provides a clean abstraction over configuration sources (environment variables,
config files, etc.) to eliminate hard-coded values.
"""

import os
from typing import Literal, TypeVar, cast

from dotenv import load_dotenv

from core.interfaces import Configuration
from core.types import JsonDict

# Load environment variables from .env file
load_dotenv()


T = TypeVar("T")


class DefaultConfiguration(Configuration):
    """Default configuration with hardcoded values for CASTS.

    All configuration values are defined as class attributes for easy modification.
    This eliminates the need for .env files while keeping configuration centralized.
    """

    # ============================================
    # EMBEDDING SERVICE CONFIGURATION
    # ============================================
    EMBEDDING_ENDPOINT = os.environ.get("EMBEDDING_ENDPOINT", "").strip()
    EMBEDDING_APIKEY = os.environ.get("EMBEDDING_APIKEY", "").strip()
    EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "").strip()

    # ============================================
    # LLM SERVICE CONFIGURATION
    # ============================================
    LLM_ENDPOINT = os.environ.get("LLM_ENDPOINT", "").strip()
    LLM_APIKEY = os.environ.get("LLM_APIKEY", "").strip()
    LLM_MODEL = os.environ.get("LLM_MODEL", "").strip()

    # ============================================
    # SIMULATION CONFIGURATION
    # ============================================
    SIMULATION_GRAPH_SIZE = 40  # For synthetic data: the number of nodes in the generated graph.
    SIMULATION_NUM_EPOCHS = 5  # Number of simulation epochs to run.
    SIMULATION_MAX_DEPTH = 5  # Max traversal depth for a single path.
    SIMULATION_USE_REAL_DATA = (
        True  # If True, use real data from CSVs; otherwise, generate synthetic data.
    )
    SIMULATION_REAL_DATA_DIR = (
        "data/real_graph_data"  # Directory containing the real graph data CSV files.
    )
    SIMULATION_REAL_SUBGRAPH_SIZE = 200  # Max number of nodes to sample for the real data subgraph.
    SIMULATION_ENABLE_VERIFIER = True  # If True, enables the LLM-based path evaluator.
    SIMULATION_ENABLE_VISUALIZER = False  # If True, generates visualizations of simulation results.
    SIMULATION_VERBOSE_LOGGING = True  # If True, prints detailed step-by-step simulation logs.
    SIMULATION_MIN_STARTING_DEGREE = (
        2  # Minimum outgoing degree for starting nodes (Tier 2 fallback).
    )
    SIMULATION_MAX_RECOMMENDED_NODE_TYPES = (
        3  # Max node types LLM can recommend for starting nodes.
    )

    # ============================================
    # DATA CONFIGURATION
    # ============================================
    # Special-case mapping for edge data files that do not follow the standard naming convention.
    # Used for connectivity enhancement in RealDataSource.
    EDGE_FILENAME_MAPPING_SPECIAL_CASES = {
        "transfer": "AccountTransferAccount.csv",
        "own_person": "PersonOwnAccount.csv",
        "own_company": "CompanyOwnAccount.csv",
        "signin": "MediumSignInAccount.csv",
    }

    # ============================================
    # CACHE CONFIGURATION
    # Mathematical model alignment: see project documentation for formula derivation.
    # ============================================

    # Minimum confidence score for a Tier-1 (exact) match to be considered.
    CACHE_MIN_CONFIDENCE_THRESHOLD = 0.5

    # Multiplier for Tier-2 (similarity) confidence threshold.
    # Formula: tier2_threshold = TIER1_THRESHOLD * TIER2_GAMMA (gamma > 1)
    # Higher values require higher confidence for Tier-2 matching.
    CACHE_TIER2_GAMMA = 1.2

    # Kappa (kappa): Base threshold parameter.
    # Formula: delta_sim(v) = 1 - kappa / (sigma_logic(v) * (1 + beta * log(eta(v))))
    #
    # CRITICAL: Counter-intuitive behavior!
    # - Higher kappa -> LOWER threshold -> MORE permissive matching (easier to match)
    # - Lower kappa -> HIGHER threshold -> MORE strict matching (harder to match)
    #
    # This is because delta = 1 - kappa/(...):
    #   kappa increases -> kappa/(...) increases -> threshold decreases
    #
    # Prior reference model used kappa=0.01 which produces
    # very HIGH thresholds (~0.99), requiring near-perfect similarity.
    #
    # For early-stage exploration with suboptimal embeddings, use higher kappa values:
    #   kappa=0.25: threshold ~0.78-0.89 for typical SKUs (original problematic value)
    #   kappa=0.30: threshold ~0.73-0.86 for typical SKUs (more permissive)
    #   kappa=0.40: threshold ~0.64-0.82 for typical SKUs (very permissive)
    #
    # Current setting balances exploration and safety for similarity ~0.83
    CACHE_SIMILARITY_KAPPA = 0.30

    # Beta (beta): Frequency sensitivity parameter.
    # Controls how much a SKU's confidence score (eta) affects its similarity threshold.
    # Higher beta -> high-confidence (frequent) SKUs require stricter matching
    #   (threshold closer to 1).
    # Lower beta -> reduces the difference between high-frequency and low-frequency
    #   SKU thresholds.
    # Interpretation: beta adjusts frequency sensitivity.
    # Recommended range: 0.05-0.2
    # Using beta=0.05 for gentler frequency-based threshold adjustment.
    CACHE_SIMILARITY_BETA = 0.05
    # Fingerprint for the current graph schema. Changing this will invalidate all existing SKUs.
    CACHE_SCHEMA_FINGERPRINT = "schema_v1"

    # SIGNATURE CONFIGURATION
    # Signature abstraction level, used as a MATCHING STRATEGY at runtime.
    # SKUs are always stored in their canonical, most detailed (Level 2) format.
    #   0 = Abstract (out/in/both only)
    #   1 = Edge-aware (out('friend'))
    #   2 = Full path (including filters like has())
    SIGNATURE_LEVEL = 2

    # Optional: Whitelist of edge labels to track (None = track all).
    # Only applicable if SIGNATURE_LEVEL >= 1.
    SIGNATURE_EDGE_WHITELIST = None

    # ============================================
    # CYCLE DETECTION & PENALTY CONFIGURATION
    # ============================================
    # CYCLE_PENALTY modes: "NONE" (no validation), "PUNISH" (penalize but continue),
    # "STOP" (terminate path)
    CYCLE_PENALTY: Literal["NONE", "PUNISH", "STOP"] = "STOP"
    CYCLE_DETECTION_THRESHOLD = 0.7
    MIN_EXECUTION_CONFIDENCE = 0.1
    POSTCHECK_MIN_EVIDENCE = 3

    def get(self, key: str, default: T) -> T:
        """Get configuration value by key."""
        # Support legacy/alias key names used in the codebase.
        alias_map = {
            "EMBEDDING_MODEL_NAME": self.EMBEDDING_MODEL,
            "LLM_MODEL_NAME": self.LLM_MODEL,
        }
        if key in alias_map:
            return cast(T, alias_map[key])

        # Prefer direct attribute access to avoid duplicated defaults at call sites.
        return cast(T, getattr(self, key, default))

    def get_int(self, key: str, default: int = 0) -> int:
        """Get integer configuration value."""
        return int(self.get(key, default))

    def get_float(self, key: str, default: float = 0.0) -> float:
        """Get float configuration value."""
        return float(self.get(key, default))

    def get_bool(self, key: str, default: bool = False) -> bool:
        """Get boolean configuration value."""
        return bool(self.get(key, default))

    def get_str(self, key: str, default: str = "") -> str:
        """Get string configuration value."""
        return str(self.get(key, default))

    def get_embedding_config(self) -> dict[str, str]:
        """Get embedding service configuration."""
        missing = []
        if not self.EMBEDDING_ENDPOINT:
            missing.append("EMBEDDING_ENDPOINT")
        if not self.EMBEDDING_APIKEY:
            missing.append("EMBEDDING_APIKEY")
        if not self.EMBEDDING_MODEL:
            missing.append("EMBEDDING_MODEL")
        if missing:
            raise ValueError(
                "Missing required embedding configuration: "
                + ", ".join(missing)
                + ". Set them in the environment or a local .env file."
            )
        return {
            "endpoint": self.EMBEDDING_ENDPOINT,
            "api_key": self.EMBEDDING_APIKEY,
            "model": self.EMBEDDING_MODEL,
        }

    def get_llm_config(self) -> dict[str, str]:
        """Get LLM service configuration."""
        missing = []
        if not self.LLM_ENDPOINT:
            missing.append("LLM_ENDPOINT")
        if not self.LLM_APIKEY:
            missing.append("LLM_APIKEY")
        if not self.LLM_MODEL:
            missing.append("LLM_MODEL")
        if missing:
            raise ValueError(
                "Missing required LLM configuration: "
                + ", ".join(missing)
                + ". Set them in the environment or a local .env file."
            )
        return {
            "endpoint": self.LLM_ENDPOINT,
            "api_key": self.LLM_APIKEY,
            "model": self.LLM_MODEL,
        }

    def get_simulation_config(self) -> JsonDict:
        """Get simulation configuration."""
        return {
            "graph_size": self.SIMULATION_GRAPH_SIZE,
            "num_epochs": self.SIMULATION_NUM_EPOCHS,
            "max_depth": self.SIMULATION_MAX_DEPTH,
            "use_real_data": self.SIMULATION_USE_REAL_DATA,
            "real_data_dir": self.SIMULATION_REAL_DATA_DIR,
            "real_subgraph_size": self.SIMULATION_REAL_SUBGRAPH_SIZE,
            "enable_verifier": self.SIMULATION_ENABLE_VERIFIER,
            "enable_visualizer": self.SIMULATION_ENABLE_VISUALIZER,
        }

    def get_cache_config(self) -> JsonDict:
        """Get cache configuration."""
        return {
            "min_confidence_threshold": self.CACHE_MIN_CONFIDENCE_THRESHOLD,
            "tier2_gamma": self.CACHE_TIER2_GAMMA,
            "similarity_kappa": self.CACHE_SIMILARITY_KAPPA,
            "similarity_beta": self.CACHE_SIMILARITY_BETA,
            "schema_fingerprint": self.CACHE_SCHEMA_FINGERPRINT,
        }
