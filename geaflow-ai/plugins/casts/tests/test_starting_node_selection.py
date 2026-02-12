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

"""Unit tests for starting node selection logic."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from core.config import DefaultConfiguration
from data.sources import SyntheticDataSource
from services.embedding import EmbeddingService
from services.llm_oracle import LLMOracle


@pytest.fixture
def mock_embedding_service():
    """Fixture for a mock embedding service."""
    return MagicMock(spec=EmbeddingService)


@pytest.fixture
def mock_config():
    """Fixture for a mock configuration."""
    return DefaultConfiguration()


@pytest.mark.asyncio
async def test_recommend_starting_node_types_basic(
    mock_embedding_service, mock_config
):
    """Test basic happy-path for recommending starting node types."""
    # Arrange
    oracle = LLMOracle(mock_embedding_service, mock_config)
    oracle.client = AsyncMock()

    # Mock the LLM response
    mock_response = MagicMock()
    mock_response.choices[0].message.content = '''```json
    ["Person", "Company"]
    ```'''
    oracle.client.chat.completions.create.return_value = mock_response

    goal = "Find risky investments between people and companies."
    available_types = {"Person", "Company", "Loan", "Account"}

    # Act
    recommended = await oracle.recommend_starting_node_types(
        goal, available_types
    )

    # Assert
    assert isinstance(recommended, list)
    assert len(recommended) == 2
    assert set(recommended) == {"Person", "Company"}
    oracle.client.chat.completions.create.assert_called_once()


@pytest.mark.asyncio
async def test_recommend_starting_node_types_malformed_json(
    mock_embedding_service, mock_config
):
    """Test robustness against malformed JSON from LLM."""
    # Arrange
    oracle = LLMOracle(mock_embedding_service, mock_config)
    oracle.client = AsyncMock()
    mock_response = MagicMock()
    mock_response.choices[0].message.content = '''```json
    ["Person", "Company",,]
    ```'''  # Extra comma
    oracle.client.chat.completions.create.return_value = mock_response

    # Act
    recommended = await oracle.recommend_starting_node_types(
        "test goal", {"Person", "Company"}
    )

    # Assert
    assert recommended == [] # Should fail gracefully


@pytest.mark.asyncio
async def test_recommend_starting_node_types_with_comments(
    mock_embedding_service, mock_config
):
    """Test that parse_jsons handles comments correctly."""
    # Arrange
    oracle = LLMOracle(mock_embedding_service, mock_config)
    oracle.client = AsyncMock()
    mock_response = MagicMock()
    mock_response.choices[0].message.content = '''```json
    // Top-level comment
    [
        "Person", // Person node type
        "Company"  // Company node type
    ]
    ```'''
    oracle.client.chat.completions.create.return_value = mock_response

    # Act
    recommended = await oracle.recommend_starting_node_types(
        "test goal", {"Person", "Company"}
    )

    # Assert
    assert set(recommended) == {"Person", "Company"}


@pytest.mark.asyncio
async def test_recommend_starting_node_types_filters_invalid_types(
    mock_embedding_service, mock_config
):
    """Test that LLM recommendations are filtered by available types."""
    # Arrange
    oracle = LLMOracle(mock_embedding_service, mock_config)
    oracle.client = AsyncMock()
    mock_response = MagicMock()
    mock_response.choices[0].message.content = '''```json
["Person", "Unicorn"]
```'''
    oracle.client.chat.completions.create.return_value = mock_response

    # Act
    recommended = await oracle.recommend_starting_node_types(
        "test goal", {"Person", "Company"}
    )

    # Assert
    assert recommended == ["Person"]


@pytest.fixture
def synthetic_data_source():
    """Fixture for a SyntheticDataSource with predictable structure."""
    source = SyntheticDataSource(size=10)
    # Override nodes and edges for predictable testing
    source._nodes = {
        "0": {"id": "0", "type": "Person"},
        "1": {"id": "1", "type": "Person"},
        "2": {"id": "2", "type": "Company"},
        "3": {"id": "3", "type": "Company"},
        "4": {"id": "4", "type": "Loan"}, # Degree 0
    }
    source._edges = {
        "0": [{"target": "1", "label": "friend"}, {"target": "2", "label": "invest"}], # Degree 2
        "1": [{"target": "3", "label": "invest"}], # Degree 1
        "2": [{"target": "0", "label": "customer"}, {"target": "3", "label": "partner"}], # Degree 2
        "3": [{"target": "1", "label": "customer"}], # Degree 1
    }
    return source


def test_get_starting_nodes_tier1(synthetic_data_source):
    """Test Tier 1 selection based on LLM recommendations."""
    # Act
    nodes = synthetic_data_source.get_starting_nodes(
        goal="", recommended_node_types=["Company"], count=2
    )
    # Assert
    assert len(nodes) == 2
    assert set(nodes) == {"2", "3"}


def test_get_starting_nodes_tier2(synthetic_data_source):
    """Test Tier 2 fallback based on min_degree."""
    # Act: Ask for a type that doesn't exist to force fallback
    nodes = synthetic_data_source.get_starting_nodes(
        goal="", recommended_node_types=["Unicorn"], count=2, min_degree=2
    )
    # Assert: Should get nodes with degree >= 2
    assert len(nodes) == 2
    assert set(nodes) == {"0", "2"}


def test_get_starting_nodes_tier3(synthetic_data_source):
    """Test Tier 3 fallback for any node with at least 1 edge."""
    # Act: Ask for more high-degree nodes than available
    nodes = synthetic_data_source.get_starting_nodes(
        goal="", recommended_node_types=["Unicorn"], count=4, min_degree=2
    )
    # Assert: Falls back to any node with degree >= 1
    assert len(nodes) == 4
    assert set(nodes) == {"0", "1", "2", "3"}


def test_get_starting_nodes_last_resort(synthetic_data_source):
    """Test final fallback to any node, even with degree 0."""
    # Act
    nodes = synthetic_data_source.get_starting_nodes(
        goal="", recommended_node_types=["Unicorn"], count=5, min_degree=3
    )
    # Assert
    assert len(nodes) == 5
    assert set(nodes) == {"0", "1", "2", "3", "4"}
