/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geaflow.dsl.gremlin.parser;

import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of IGremlinParser for GeaFlow.
 */
public class GeaFlowGremlinParser implements IGremlinParser {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(GeaFlowGremlinParser.class);

    private final GraphTraversalSource g;

    public GeaFlowGremlinParser(Graph graph) {
        this.g = graph.traversal();
    }

    @Override
    public GremlinQuery parse(String gremlinScript) {
        if (gremlinScript == null || gremlinScript.trim().isEmpty()) {
            throw new IllegalArgumentException("Gremlin script cannot be null or empty");
        }
        
        try {
            // Parse the Gremlin script string into a traversal
            GraphTraversal traversal = (GraphTraversal) g.parse(gremlinScript);
            Bytecode bytecode = traversal.asAdmin().getBytecode();
            return new GremlinQuery(gremlinScript, bytecode, traversal);
        } catch (Exception e) {
            LOGGER.error("Failed to parse Gremlin script: {}", gremlinScript, e);
            throw new RuntimeException("Failed to parse Gremlin script: " + gremlinScript, e);
        }
    }

    @Override
    public GremlinQuery parse(Bytecode bytecode) {
        if (bytecode == null) {
            throw new IllegalArgumentException("Bytecode cannot be null");
        }
        
        try {
            // Create a traversal from the bytecode
            GraphTraversal traversal = GraphTraversalSource.from(g.getGraph(), bytecode);
            return new GremlinQuery(null, bytecode, traversal);
        } catch (Exception e) {
            LOGGER.error("Failed to create traversal from bytecode", e);
            throw new RuntimeException("Failed to create traversal from bytecode", e);
        }
    }
    
    /**
     * Get the GraphTraversalSource used by this parser.
     *
     * @return the GraphTraversalSource
     */
    public GraphTraversalSource getTraversalSource() {
        return g;
    }
}