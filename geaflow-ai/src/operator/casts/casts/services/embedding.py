"""Embedding service for generating vector representations of graph properties."""

import hashlib
from typing import Any

import numpy as np
from openai import AsyncOpenAI

from casts.core.config import DefaultConfiguration
from casts.core.interfaces import Configuration
from casts.core.models import filter_decision_properties


class EmbeddingService:
    """OpenAI-compatible embedding API for generating property vectors."""
    
    DEFAULT_DIMENSION = 1024
    DEFAULT_MODEL = "text-embedding-v3"

    def __init__(self, config: Configuration):
        """Initialize embedding service with configuration.
        
        Args:
            config: Configuration object containing API settings
        """
        if isinstance(config, DefaultConfiguration):
            embedding_cfg = config.get_embedding_config()
            api_key = embedding_cfg["api_key"]
            endpoint = embedding_cfg["endpoint"]
            model = embedding_cfg["model"]
        else:
            # Fallback for other configuration types
            api_key = config.get_str("EMBEDDING_APIKEY")
            endpoint = config.get_str("EMBEDDING_ENDPOINT")
            model = config.get_str("EMBEDDING_MODEL_NAME")
        
        if not api_key or not endpoint:
            print("Warning: Embedding API credentials not configured, using deterministic fallback")
            self.client = None
        else:
            self.client = AsyncOpenAI(api_key=api_key, base_url=endpoint)
        
        self.model = model
        self.dimension = self.DEFAULT_DIMENSION

    async def embed_text(self, text: str) -> np.ndarray:
        """
        Generate embedding vector for a text string.
        
        Args:
            text: Input text to embed
            
        Returns:
            Normalized numpy array of embedding vector
        """
        # Use API if client is configured
        if self.client is not None:
            try:
                response = await self.client.embeddings.create(model=self.model, input=text)
                return np.array(response.data[0].embedding)
            except Exception as e:
                print(f"Embedding API error: {e}, falling back to deterministic generator")
        
        # Deterministic fallback for testing/offline scenarios
        seed = int(hashlib.sha256(text.encode()).hexdigest(), 16) % (2**32)
        rng = np.random.default_rng(seed)
        vector = rng.random(self.dimension)
        return vector / np.linalg.norm(vector)

    async def embed_properties(self, properties: dict[str, Any]) -> np.ndarray:
        """
        Generate embedding vector for a dictionary of properties.
        
        Args:
            properties: Property dictionary (identity fields will be filtered out)
            
        Returns:
            Normalized numpy array of embedding vector
        """
        # Use unified filtering logic to remove identity fields
        filtered = filter_decision_properties(properties)
        text = "|".join([f"{k}={v}" for k, v in sorted(filtered.items())])
        return await self.embed_text(text)
