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

"""
GraphSAGE Transform Function for GeaFlow-Infer Framework.

This module implements the GraphSAGE (Graph Sample and Aggregate) algorithm
for generating node embeddings using PyTorch and the GeaFlow-Infer framework.

The implementation includes:
- GraphSAGETransFormFunction: Main transform function for model inference
- GraphSAGEModel: PyTorch model definition for GraphSAGE
- GraphSAGELayer: Single layer of GraphSAGE with different aggregators
- Aggregators: Mean, LSTM, and Pool aggregators for neighbor feature aggregation
"""

import abc
import os
from typing import List, Union, Dict, Any
import torch
import torch.nn as nn
import torch.nn.functional as F
import numpy as np


class TransFormFunction(abc.ABC):
    """
    Abstract base class for transform functions in GeaFlow-Infer.
    
    All user-defined transform functions must inherit from this class
    and implement the abstract methods.
    """
    def __init__(self, input_size):
        self.input_size = input_size

    @abc.abstractmethod
    def load_model(self, *args):
        """Load the model from file or initialize it."""
        pass

    @abc.abstractmethod
    def transform_pre(self, *args) -> Union[torch.Tensor, List[torch.Tensor]]:
        """
        Pre-process input data and perform model inference.
        
        Returns:
            Tuple of (result, vertex_id) where result is the model output
            and vertex_id is used for tracking.
        """
        pass

    @abc.abstractmethod
    def transform_post(self, *args):
        """
        Post-process model output.
        
        Args:
            *args: The result from transform_pre
            
        Returns:
            Final processed result to be sent back to Java
        """
        pass


class GraphSAGETransFormFunction(TransFormFunction):
    """
    GraphSAGE Transform Function for GeaFlow-Infer.
    
    This class implements the GraphSAGE algorithm for node embedding generation.
    It receives node features and neighbor features from Java, performs GraphSAGE
    aggregation, and returns the computed embeddings.
    
    Usage:
        The class is automatically instantiated by the GeaFlow-Infer framework.
        It expects:
        - args[0]: vertex_id (Object)
        - args[1]: vertex_features (List[Double>)
        - args[2]: neighbor_features_map (Map<Integer, List<List<Double>>>)
    """
    
    def __init__(self):
        super().__init__(input_size=3)  # vertexId, features, neighbor_features
        print("Initializing GraphSAGETransFormFunction")
        
        # Check for Metal support (MPS) on Mac
        if torch.backends.mps.is_available():
            self.device = torch.device("mps")
            print("Using Metal Performance Shaders (MPS) device")
        elif torch.cuda.is_available():
            self.device = torch.device("cuda")
            print("Using CUDA device")
        else:
            self.device = torch.device("cpu")
            print("Using CPU device")
        
        # Default model parameters (can be configured)
        # Note: input_dim should match the reduced feature dimension from Java side
        # Default is 64 (matching DEFAULT_REDUCED_DIMENSION in GraphSAGECompute)
        self.input_dim = 64  # Input feature dimension (reduced from full features)
        self.hidden_dim = 256  # Hidden layer dimension
        self.output_dim = 64  # Output embedding dimension
        self.num_layers = 2  # Number of GraphSAGE layers
        self.aggregator_type = 'mean'  # Aggregator type: 'mean', 'lstm', or 'pool'
        
        # Load model
        model_path = os.getcwd() + "/graphsage_model.pt"
        self.load_model(model_path)
    
    def load_model(self, model_path: str = None):
        """
        Load pre-trained GraphSAGE model or initialize a new one.
        
        Args:
            model_path: Path to the model file. If file doesn't exist,
                       a new model will be initialized.
        """
        try:
            if os.path.exists(model_path):
                print(f"Loading model from {model_path}")
                self.model = GraphSAGEModel(
                    input_dim=self.input_dim,
                    hidden_dim=self.hidden_dim,
                    output_dim=self.output_dim,
                    num_layers=self.num_layers,
                    aggregator_type=self.aggregator_type
                ).to(self.device)
                self.model.load_state_dict(torch.load(model_path, map_location=self.device))
                self.model.eval()
                print("Model loaded successfully")
            else:
                print(f"Model file not found at {model_path}, initializing new model")
                self.model = GraphSAGEModel(
                    input_dim=self.input_dim,
                    hidden_dim=self.hidden_dim,
                    output_dim=self.output_dim,
                    num_layers=self.num_layers,
                    aggregator_type=self.aggregator_type
                ).to(self.device)
                self.model.eval()
                print("New model initialized")
        except Exception as e:
            print(f"Error loading model: {e}")
            # Initialize a new model as fallback
            self.model = GraphSAGEModel(
                input_dim=self.input_dim,
                hidden_dim=self.hidden_dim,
                output_dim=self.output_dim,
                num_layers=self.num_layers,
                aggregator_type=self.aggregator_type
            ).to(self.device)
            self.model.eval()
            print("Fallback model initialized")
    
    def transform_pre(self, *args):
        """
        Pre-process input and perform GraphSAGE inference.
        
        Args:
            args[0]: vertex_id - The vertex ID
            args[1]: vertex_features - List of doubles representing vertex features
            args[2]: neighbor_features_map - Map from layer index to list of neighbor features
            
        Returns:
            Tuple of (embedding, vertex_id) where embedding is a list of doubles
        """
        try:
            vertex_id = args[0]
            vertex_features = args[1]
            neighbor_features_map = args[2]
            
            # Convert vertex features to tensor
            # Note: Features are already reduced by FeatureReducer in Java side
            if vertex_features is None or len(vertex_features) == 0:
                # Use zero features as default
                vertex_feature_tensor = torch.zeros(self.input_dim, dtype=torch.float32).to(self.device)
            else:
                # Features should already match input_dim (reduced by FeatureReducer)
                # But we still handle padding/truncation for safety
                feature_array = np.array(vertex_features, dtype=np.float32)
                if len(feature_array) < self.input_dim:
                    # Pad with zeros (shouldn't happen if reduction works correctly)
                    padded = np.pad(feature_array, (0, self.input_dim - len(feature_array)), 'constant')
                elif len(feature_array) > self.input_dim:
                    # Truncate (shouldn't happen if reduction works correctly)
                    padded = feature_array[:self.input_dim]
                else:
                    padded = feature_array
                vertex_feature_tensor = torch.tensor(padded, dtype=torch.float32).to(self.device)
            
            # Parse neighbor features
            neighbor_features_list = self._parse_neighbor_features(neighbor_features_map)
            
            # Perform GraphSAGE inference
            with torch.no_grad():
                embedding = self.model(vertex_feature_tensor, neighbor_features_list)
            
            # Convert to list for return
            embedding_list = embedding.cpu().numpy().tolist()
            
            return embedding_list, vertex_id
            
        except Exception as e:
            print(f"Error in transform_pre: {e}")
            import traceback
            traceback.print_exc()
            # Return zero embedding as fallback
            return [0.0] * self.output_dim, args[0] if len(args) > 0 else None
    
    def transform_post(self, *args):
        """
        Post-process the result from transform_pre.
        
        Args:
            args: The result tuple from transform_pre (embedding, vertex_id)
            
        Returns:
            The embedding as a list of doubles
        """
        if len(args) > 0:
            res = args[0]
            if isinstance(res, tuple) and len(res) > 0:
                return res[0]  # Return the embedding
            return res
        return None
    
    def _parse_neighbor_features(self, neighbor_features_map: Dict[int, List[List[float]]]) -> List[List[torch.Tensor]]:
        """
        Parse neighbor features from Java format to PyTorch tensors.
        
        Args:
            neighbor_features_map: Map from layer index to list of neighbor feature lists
            
        Returns:
            List of lists of tensors, one list per layer
        """
        neighbor_features_list = []
        
        for layer in range(self.num_layers):
            if layer in neighbor_features_map:
                layer_neighbors = neighbor_features_map[layer]
                neighbor_tensors = []
                
                for neighbor_features in layer_neighbors:
                    if neighbor_features is None or len(neighbor_features) == 0:
                        # Use zero features
                        neighbor_tensor = torch.zeros(self.input_dim, dtype=torch.float32).to(self.device)
                    else:
                        # Convert to tensor
                        # Note: Neighbor features are already reduced by FeatureReducer in Java side
                        feature_array = np.array(neighbor_features, dtype=np.float32)
                        if len(feature_array) < self.input_dim:
                            # Pad with zeros (shouldn't happen if reduction works correctly)
                            padded = np.pad(feature_array, (0, self.input_dim - len(feature_array)), 'constant')
                        elif len(feature_array) > self.input_dim:
                            # Truncate (shouldn't happen if reduction works correctly)
                            padded = feature_array[:self.input_dim]
                        else:
                            padded = feature_array
                        neighbor_tensor = torch.tensor(padded, dtype=torch.float32).to(self.device)
                    
                    neighbor_tensors.append(neighbor_tensor)
                
                neighbor_features_list.append(neighbor_tensors)
            else:
                # Empty layer
                neighbor_features_list.append([])
        
        return neighbor_features_list


class GraphSAGEModel(nn.Module):
    """
    GraphSAGE Model for node embedding generation.
    
    This model implements the GraphSAGE algorithm with configurable number of layers
    and aggregator types (mean, LSTM, or pool).
    """
    
    def __init__(self, input_dim: int, hidden_dim: int, output_dim: int,
                 num_layers: int = 2, aggregator_type: str = 'mean'):
        """
        Initialize GraphSAGE model.
        
        Args:
            input_dim: Input feature dimension
            hidden_dim: Hidden layer dimension
            output_dim: Output embedding dimension
            num_layers: Number of GraphSAGE layers
            aggregator_type: Type of aggregator ('mean', 'lstm', or 'pool')
        """
        super(GraphSAGEModel, self).__init__()
        self.num_layers = num_layers
        self.aggregator_type = aggregator_type
        
        # Create GraphSAGE layers
        self.layers = nn.ModuleList()
        for i in range(num_layers):
            in_dim = input_dim if i == 0 else hidden_dim
            out_dim = output_dim if i == num_layers - 1 else hidden_dim
            self.layers.append(GraphSAGELayer(in_dim, out_dim, aggregator_type))
    
    def forward(self, node_features: torch.Tensor,
                neighbor_features_list: List[List[torch.Tensor]]) -> torch.Tensor:
        """
        Forward pass through GraphSAGE model.
        
        Args:
            node_features: Tensor of shape [input_dim] for the current node
            neighbor_features_list: List of lists of tensors, one per layer
            
        Returns:
            Node embedding tensor of shape [output_dim]
        """
        # Start with the node features (1D tensor: [input_dim])
        h = node_features
        
        for i, layer in enumerate(self.layers):
            # Only use neighbor features from the neighbor_features_list for the first layer.
            # For subsequent layers, we don't use neighbor aggregation since the intermediate
            # features don't have corresponding neighbor representations.
            # This is a limitation of the single-node inference approach.
            if i == 0 and i < len(neighbor_features_list):
                neighbor_features = neighbor_features_list[i]
            else:
                neighbor_features = []
            
            # Pass 1D tensor to layer and get 1D output
            h = layer(h, neighbor_features)  # [in_dim] -> [out_dim]
        
        return h  # [output_dim]


class GraphSAGELayer(nn.Module):
    """
    Single GraphSAGE layer with neighbor aggregation.
    
    Implements one layer of GraphSAGE with configurable aggregator.
    """
    
    def __init__(self, in_dim: int, out_dim: int, aggregator_type: str = 'mean'):
        """
        Initialize GraphSAGE layer.
        
        Args:
            in_dim: Input feature dimension
            out_dim: Output feature dimension
            aggregator_type: Type of aggregator ('mean', 'lstm', or 'pool')
        """
        super(GraphSAGELayer, self).__init__()
        self.aggregator_type = aggregator_type
        
        if aggregator_type == 'mean':
            self.aggregator = MeanAggregator(in_dim, out_dim)
        elif aggregator_type == 'lstm':
            self.aggregator = LSTMAggregator(in_dim, out_dim)
        elif aggregator_type == 'pool':
            self.aggregator = PoolAggregator(in_dim, out_dim)
        else:
            raise ValueError(f"Unknown aggregator type: {aggregator_type}")
    
    def forward(self, node_feature: torch.Tensor,
                neighbor_features: List[torch.Tensor]) -> torch.Tensor:
        """
        Forward pass through GraphSAGE layer.
        
        Args:
            node_feature: Tensor of shape [in_dim] for the current node
            neighbor_features: List of tensors, each of shape [in_dim] for neighbors
            
        Returns:
            Aggregated feature tensor of shape [out_dim]
        """
        return self.aggregator(node_feature, neighbor_features)


class MeanAggregator(nn.Module):
    """
    Mean aggregator for GraphSAGE.
    
    Aggregates neighbor features by taking the mean, then concatenates
    with node features and applies a linear transformation.
    """
    
    def __init__(self, in_dim: int, out_dim: int):
        super(MeanAggregator, self).__init__()
        # When no neighbors, just use a linear layer on node features alone
        # When neighbors exist, concatenate and use larger linear layer
        self.in_dim = in_dim
        self.out_dim = out_dim
        self.linear_with_neighbors = nn.Linear(in_dim * 2, out_dim)
        self.linear_without_neighbors = nn.Linear(in_dim, out_dim)
    
    def forward(self, node_feature: torch.Tensor,
                neighbor_features: List[torch.Tensor]) -> torch.Tensor:
        """
        Aggregate neighbor features using mean.
        
        Args:
            node_feature: Tensor of shape [in_dim]
            neighbor_features: List of tensors, each of shape [in_dim]
            
        Returns:
            Aggregated feature tensor of shape [out_dim]
        """
        if len(neighbor_features) == 0:
            # No neighbors, just apply linear transformation to node features
            output = self.linear_without_neighbors(node_feature)
        else:
            # Stack neighbors and take mean
            neighbor_stack = torch.stack(neighbor_features, dim=0)  # [num_neighbors, in_dim]
            neighbor_mean = torch.mean(neighbor_stack, dim=0)  # [in_dim]
            
            # Concatenate node and aggregated neighbor features
            combined = torch.cat([node_feature, neighbor_mean], dim=0)  # [in_dim * 2]
            
            # Apply linear transformation
            output = self.linear_with_neighbors(combined)  # [out_dim]
        
        output = F.relu(output)
        return output


class LSTMAggregator(nn.Module):
    """
    LSTM aggregator for GraphSAGE.
    
    Uses an LSTM to aggregate neighbor features, which can capture
    more complex patterns than mean aggregation.
    """
    
    def __init__(self, in_dim: int, out_dim: int):
        super(LSTMAggregator, self).__init__()
        self.lstm = nn.LSTM(in_dim, out_dim // 2, batch_first=True, bidirectional=True)
        self.linear = nn.Linear(in_dim + out_dim, out_dim)
    
    def forward(self, node_feature: torch.Tensor,
                neighbor_features: List[torch.Tensor]) -> torch.Tensor:
        """
        Aggregate neighbor features using LSTM.
        
        Args:
            node_feature: Tensor of shape [in_dim]
            neighbor_features: List of tensors, each of shape [in_dim]
            
        Returns:
            Aggregated feature tensor of shape [out_dim]
        """
        if len(neighbor_features) == 0:
            # No neighbors, use zero vector
            neighbor_agg = torch.zeros(self.linear.out_features, device=node_feature.device)
        else:
            # Stack neighbors: [num_neighbors, in_dim]
            neighbor_stack = torch.stack(neighbor_features, dim=0).unsqueeze(0)  # [1, num_neighbors, in_dim]
            
            # Apply LSTM
            lstm_out, (hidden, _) = self.lstm(neighbor_stack)
            # Use the last hidden state
            neighbor_agg = hidden.view(-1)  # [out_dim]
        
        # Concatenate node and aggregated neighbor features
        combined = torch.cat([node_feature, neighbor_agg], dim=0)  # [in_dim + out_dim]
        
        # Apply linear transformation and activation
        output = self.linear(combined)  # [out_dim]
        output = F.relu(output)
        
        return output


class PoolAggregator(nn.Module):
    """
    Pool aggregator for GraphSAGE.
    
    Uses max pooling over neighbor features, then applies a neural network
    to transform the pooled features.
    """
    
    def __init__(self, in_dim: int, out_dim: int):
        super(PoolAggregator, self).__init__()
        self.in_dim = in_dim
        self.out_dim = out_dim
        self.pool_linear = nn.Linear(in_dim, in_dim)
        self.linear_with_neighbors = nn.Linear(in_dim * 2, out_dim)
        self.linear_without_neighbors = nn.Linear(in_dim, out_dim)
    
    def forward(self, node_feature: torch.Tensor,
                neighbor_features: List[torch.Tensor]) -> torch.Tensor:
        """
        Aggregate neighbor features using max pooling.
        
        Args:
            node_feature: Tensor of shape [in_dim]
            neighbor_features: List of tensors, each of shape [in_dim]
            
        Returns:
            Aggregated feature tensor of shape [out_dim]
        """
        if len(neighbor_features) == 0:
            # No neighbors, just apply linear transformation to node features
            output = self.linear_without_neighbors(node_feature)
        else:
            # Stack neighbors: [num_neighbors, in_dim]
            neighbor_stack = torch.stack(neighbor_features, dim=0)
            
            # Apply linear transformation to each neighbor
            neighbor_transformed = self.pool_linear(neighbor_stack)  # [num_neighbors, in_dim]
            neighbor_transformed = F.relu(neighbor_transformed)
            
            # Max pooling
            neighbor_pool, _ = torch.max(neighbor_transformed, dim=0)  # [in_dim]
            
            # Concatenate node and aggregated neighbor features
            combined = torch.cat([node_feature, neighbor_pool], dim=0)  # [in_dim * 2]
            
            # Apply linear transformation
            output = self.linear_with_neighbors(combined)  # [out_dim]
        
        output = F.relu(output)
        return output