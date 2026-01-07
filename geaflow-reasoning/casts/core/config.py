"""Configuration management for CASTS system.

Provides a clean abstraction over configuration sources (environment variables,
config files, etc.) to eliminate hard-coded values.
"""

import os
from typing import Any, Dict

from dotenv import load_dotenv

from casts.core.interfaces import Configuration

# Load environment variables from .env file
load_dotenv()


class DefaultConfiguration(Configuration):
    """Default configuration with hardcoded values for CASTS.

    All configuration values are defined as class attributes for easy modification.
    This eliminates the need for .env files while keeping configuration centralized.
    """

    # ============================================
    # EMBEDDING SERVICE CONFIGURATION
    # ============================================
    EMBEDDING_ENDPOINT = os.environ.get("EMBEDDING_ENDPOINT", "")
    EMBEDDING_APIKEY = os.environ.get("EMBEDDING_APIKEY", "YOUR_EMBEDDING_API_KEY_HERE")
    EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "")

    # ============================================
    # LLM SERVICE CONFIGURATION
    # ============================================
    LLM_ENDPOINT = os.environ.get("LLM_ENDPOINT", "")
    LLM_APIKEY = os.environ.get("LLM_APIKEY", "YOUR_LLM_API_KEY_HERE")
    LLM_MODEL = os.environ.get("LLM_MODEL", "")

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
    # Mathematical model alignment: See 数学建模.md Section 4.6.2 for formula derivation
    # ============================================

    # Minimum confidence score for a Tier-1 (exact) match to be considered.
    CACHE_MIN_CONFIDENCE_THRESHOLD = 2.0

    # Multiplier for Tier-2 (similarity) confidence threshold.
    # Formula: tier2_threshold = TIER1_THRESHOLD * TIER2_GAMMA (where γ > 1)
    # Higher values require higher confidence for Tier-2 matching.
    CACHE_TIER2_GAMMA = 1.2

    # Kappa (κ): Base threshold parameter.
    # Formula: δ_sim(v) = 1 - κ / (σ_logic(v) · (1 + β · log(η(v))))
    #
    # CRITICAL: Counter-intuitive behavior!
    # - Higher κ → LOWER threshold → MORE permissive matching (easier to match)
    # - Lower κ → HIGHER threshold → MORE strict matching (harder to match)
    #
    # This is because δ = 1 - κ/(...):
    #   κ↑ → κ/(...)↑ → 1 - (large)↓ → threshold decreases
    #
    # Mathematical model (数学建模.md line 983-985) uses κ=0.01 which produces
    # very HIGH thresholds (~0.99), requiring near-perfect similarity.
    #
    # For early-stage exploration with suboptimal embeddings, use HIGHER κ values:
    #   κ=0.25: threshold ~0.78-0.89 for typical SKUs (original problematic value)
    #   κ=0.30: threshold ~0.73-0.86 for typical SKUs (more permissive)
    #   κ=0.40: threshold ~0.64-0.82 for typical SKUs (very permissive)
    #
    # Current setting balances exploration and safety for similarity ~0.83
    CACHE_SIMILARITY_KAPPA = 0.30

    # Beta (β): Frequency sensitivity parameter.
    # Controls how much a SKU's confidence score (η) affects its similarity threshold.
    # Higher beta → high-confidence (frequent) SKUs require stricter matching
    #   (threshold closer to 1).
    # Lower beta → reduces the difference between high-frequency and low-frequency
    #   SKU thresholds.
    # Interpretation: β adjusts "热度敏感性" (frequency sensitivity).
    # Recommended range: 0.05-0.2 (see 数学建模.md line 959, 983-985)
    # Using β=0.05 for gentler frequency-based threshold adjustment.
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

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key."""
        # Map key names to class attributes
        key_map = {
            "EMBEDDING_ENDPOINT": self.EMBEDDING_ENDPOINT,
            "EMBEDDING_APIKEY": self.EMBEDDING_APIKEY,
            "EMBEDDING_MODEL_NAME": self.EMBEDDING_MODEL,
            "LLM_ENDPOINT": self.LLM_ENDPOINT,
            "LLM_APIKEY": self.LLM_APIKEY,
            "LLM_MODEL_NAME": self.LLM_MODEL,
            "SIMULATION_GRAPH_SIZE": self.SIMULATION_GRAPH_SIZE,
            "SIMULATION_NUM_EPOCHS": self.SIMULATION_NUM_EPOCHS,
            "SIMULATION_MAX_DEPTH": self.SIMULATION_MAX_DEPTH,
            "SIMULATION_USE_REAL_DATA": self.SIMULATION_USE_REAL_DATA,
            "SIMULATION_REAL_DATA_DIR": self.SIMULATION_REAL_DATA_DIR,
            "SIMULATION_REAL_SUBGRAPH_SIZE": self.SIMULATION_REAL_SUBGRAPH_SIZE,
            "SIMULATION_ENABLE_VERIFIER": self.SIMULATION_ENABLE_VERIFIER,
            "SIMULATION_ENABLE_VISUALIZER": self.SIMULATION_ENABLE_VISUALIZER,
            "SIMULATION_VERBOSE_LOGGING": self.SIMULATION_VERBOSE_LOGGING,
            "CACHE_MIN_CONFIDENCE_THRESHOLD": self.CACHE_MIN_CONFIDENCE_THRESHOLD,
            "CACHE_TIER2_GAMMA": self.CACHE_TIER2_GAMMA,
            "CACHE_SIMILARITY_KAPPA": self.CACHE_SIMILARITY_KAPPA,
            "CACHE_SIMILARITY_BETA": self.CACHE_SIMILARITY_BETA,
            "CACHE_SCHEMA_FINGERPRINT": self.CACHE_SCHEMA_FINGERPRINT,
            "SIGNATURE_LEVEL": self.SIGNATURE_LEVEL,
        }
        return key_map.get(key, default)

    def get_int(self, key: str, default: int = 0) -> int:
        """Get integer configuration value."""
        # Map key names to class attributes
        key_map = {
            "SIMULATION_GRAPH_SIZE": self.SIMULATION_GRAPH_SIZE,
            "SIMULATION_NUM_EPOCHS": self.SIMULATION_NUM_EPOCHS,
            "SIMULATION_MAX_DEPTH": self.SIMULATION_MAX_DEPTH,
            "SIMULATION_REAL_SUBGRAPH_SIZE": self.SIMULATION_REAL_SUBGRAPH_SIZE,
            "SIMULATION_MIN_STARTING_DEGREE": self.SIMULATION_MIN_STARTING_DEGREE,
            "SIMULATION_MAX_RECOMMENDED_NODE_TYPES": self.SIMULATION_MAX_RECOMMENDED_NODE_TYPES,
            "SIGNATURE_LEVEL": self.SIGNATURE_LEVEL,
        }
        return key_map.get(key, default)

    def get_float(self, key: str, default: float = 0.0) -> float:
        """Get float configuration value."""
        # Map key names to class attributes
        key_map = {
            "CACHE_MIN_CONFIDENCE_THRESHOLD": self.CACHE_MIN_CONFIDENCE_THRESHOLD,
            "CACHE_TIER2_GAMMA": self.CACHE_TIER2_GAMMA,
            "CACHE_SIMILARITY_KAPPA": self.CACHE_SIMILARITY_KAPPA,
            "CACHE_SIMILARITY_BETA": self.CACHE_SIMILARITY_BETA,
        }
        return key_map.get(key, default)

    def get_bool(self, key: str, default: bool = False) -> bool:
        """Get boolean configuration value."""
        # Map key names to class attributes
        key_map = {
            "SIMULATION_USE_REAL_DATA": self.SIMULATION_USE_REAL_DATA,
            "SIMULATION_ENABLE_VERIFIER": self.SIMULATION_ENABLE_VERIFIER,
            "SIMULATION_ENABLE_VISUALIZER": self.SIMULATION_ENABLE_VISUALIZER,
            "SIMULATION_VERBOSE_LOGGING": self.SIMULATION_VERBOSE_LOGGING,
        }
        return key_map.get(key, default)

    def get_str(self, key: str, default: str = "") -> str:
        """Get string configuration value."""
        # Map key names to class attributes
        key_map = {
            "EMBEDDING_ENDPOINT": self.EMBEDDING_ENDPOINT,
            "EMBEDDING_APIKEY": self.EMBEDDING_APIKEY,
            "EMBEDDING_MODEL_NAME": self.EMBEDDING_MODEL,
            "LLM_ENDPOINT": self.LLM_ENDPOINT,
            "LLM_APIKEY": self.LLM_APIKEY,
            "LLM_MODEL_NAME": self.LLM_MODEL,
            "SIMULATION_REAL_DATA_DIR": self.SIMULATION_REAL_DATA_DIR,
            "CACHE_SCHEMA_FINGERPRINT": self.CACHE_SCHEMA_FINGERPRINT,
        }
        return key_map.get(key, default)

    def get_embedding_config(self) -> Dict[str, str]:
        """Get embedding service configuration."""
        return {
            "endpoint": self.EMBEDDING_ENDPOINT,
            "api_key": self.EMBEDDING_APIKEY,
            "model": self.EMBEDDING_MODEL,
        }

    def get_llm_config(self) -> Dict[str, str]:
        """Get LLM service configuration."""
        return {
            "endpoint": self.LLM_ENDPOINT,
            "api_key": self.LLM_APIKEY,
            "model": self.LLM_MODEL,
        }

    def get_simulation_config(self) -> Dict[str, Any]:
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

    def get_cache_config(self) -> Dict[str, Any]:
        """Get cache configuration."""
        return {
            "min_confidence_threshold": self.CACHE_MIN_CONFIDENCE_THRESHOLD,
            "tier2_gamma": self.CACHE_TIER2_GAMMA,
            "similarity_kappa": self.CACHE_SIMILARITY_KAPPA,
            "similarity_beta": self.CACHE_SIMILARITY_BETA,
            "schema_fingerprint": self.CACHE_SCHEMA_FINGERPRINT,
        }
