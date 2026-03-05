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

"""Embedding service for generating vector representations of graph properties."""

import numpy as np
from openai import AsyncOpenAI

from core.config import DefaultConfiguration
from core.interfaces import Configuration
from core.models import filter_decision_properties
from core.types import JsonDict


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
            api_key = config.get_str("EMBEDDING_APIKEY")
            endpoint = config.get_str("EMBEDDING_ENDPOINT")
            model = config.get_str("EMBEDDING_MODEL")
            if not model:
                model = config.get_str("EMBEDDING_MODEL_NAME")

        missing = []
        if not endpoint:
            missing.append("EMBEDDING_ENDPOINT")
        if not api_key:
            missing.append("EMBEDDING_APIKEY")
        if not model:
            missing.append("EMBEDDING_MODEL_NAME")
        if missing:
            raise ValueError(
                "Missing required embedding configuration: " + ", ".join(missing)
            )

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
        if self.client is None:
            raise RuntimeError("Embedding client is not configured.")

        response = await self.client.embeddings.create(model=self.model, input=text)
        return np.array(response.data[0].embedding)

    async def embed_properties(self, properties: JsonDict) -> np.ndarray:
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
