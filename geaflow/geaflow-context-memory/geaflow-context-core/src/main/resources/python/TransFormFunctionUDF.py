#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TransFormFunction for Entity Memory Graph - GeaFlow-Infer Integration
"""

import sys
import os
import logging

# 添加当前目录到路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from entity_memory_graph import EntityMemoryGraph

logger = logging.getLogger(__name__)


class TransFormFunction:
    """
    GeaFlow-Infer TransformFunction for Entity Memory Graph
    
    This class bridges Java and Python, allowing Java code to call
    entity_memory_graph.py methods through GeaFlow's InferContext.
    """
    
    # GeaFlow-Infer required: input queue size
    input_size = 10000
    
    def __init__(self):
        """Initialize the entity memory graph instance"""
        self.graph = None
        logger.info("TransFormFunction initialized")
    
    def transform_pre(self, *inputs):
        """
        GeaFlow-Infer preprocessing: parse Java inputs
        
        Args:
            inputs: (method_name, *args) from Java InferContext.infer()
            
        Returns:
            Tuple of (method_name, args) for transform_post
        """
        if not inputs:
            raise ValueError("Empty inputs")
        
        method_name = inputs[0]
        args = inputs[1:] if len(inputs) > 1 else []
        
        logger.debug(f"transform_pre: method={method_name}, args={args}")
        return method_name, args
    
    def transform_post(self, pre_result):
        """
        GeaFlow-Infer postprocessing: execute method and return result
        
        Args:
            pre_result: (method_name, args) from transform_pre
            
        Returns:
            Result to be sent back to Java
        """
        method_name, args = pre_result
        
        try:
            if method_name == "init":
                # Initialize graph: init(base_decay, noise_threshold, max_edges_per_node, prune_interval)
                self.graph = EntityMemoryGraph(
                    base_decay=float(args[0]) if len(args) > 0 else 0.6,
                    noise_threshold=float(args[1]) if len(args) > 1 else 0.2,
                    max_edges_per_node=int(args[2]) if len(args) > 2 else 30,
                    prune_interval=int(args[3]) if len(args) > 3 else 1000
                )
                logger.info("Entity memory graph initialized")
                return True
            
            if self.graph is None:
                raise RuntimeError("Graph not initialized. Call 'init' first.")
            
            if method_name == "add":
                # Add entities: add(entity_ids_list)
                entity_ids = args[0] if args else []
                self.graph.add_entities(entity_ids)
                logger.debug(f"Added {len(entity_ids)} entities")
                return True
            
            elif method_name == "expand":
                # Expand entities: expand(seed_entity_ids, top_k)
                seed_ids = args[0] if len(args) > 0 else []
                top_k = int(args[1]) if len(args) > 1 else 20
                
                result = self.graph.expand_entities(seed_ids, top_k=top_k)
                
                # Convert to Java-compatible format: List<Object[]>
                # where Object[] = [entity_id, activation_strength]
                java_result = [[entity_id, float(strength)] for entity_id, strength in result]
                logger.debug(f"Expanded {len(seed_ids)} seeds to {len(java_result)} entities")
                return java_result
            
            elif method_name == "stats":
                # Get stats: stats()
                stats = self.graph.get_stats()
                # Convert to Java Map<String, Number>
                return stats
            
            elif method_name == "clear":
                # Clear graph: clear()
                self.graph.clear()
                logger.info("Graph cleared")
                return True
            
            else:
                raise ValueError(f"Unknown method: {method_name}")
        
        except Exception as e:
            logger.error(f"Error executing {method_name}: {e}", exc_info=True)
            raise
