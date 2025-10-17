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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.structure.Graph;
import javax.script.Bindings;
import javax.script.ScriptException;
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
            // Create a script engine for parsing Gremlin scripts
            GremlinGroovyScriptEngine scriptEngine = new GremlinGroovyScriptEngine();
            
            // Create bindings for the script engine
            Bindings bindings = scriptEngine.createBindings();
            bindings.put("g", g); // Add the GraphTraversalSource to the bindings
            
            // Parse and execute the script to get the traversal
            Object result = scriptEngine.eval(gremlinScript, bindings);
            
            // Extract the traversal from the result
            GraphTraversal traversal;
            if (result instanceof GraphTraversal) {
                traversal = (GraphTraversal) result;
            } else {
                // If the result is not a traversal, create a simple V() traversal as fallback
                traversal = g.V();
            }
            
            // Get the bytecode from the traversal
            Bytecode bytecode = traversal.asAdmin().getBytecode();
            
            return new GremlinQuery(gremlinScript, bytecode, traversal);
        } catch (ScriptException e) {
            LOGGER.error("Failed to parse Gremlin script: {}", gremlinScript, e);
            throw new RuntimeException("Failed to parse Gremlin script: " + gremlinScript, e);
        } catch (Exception e) {
            LOGGER.error("Unexpected error while parsing Gremlin script: {}", gremlinScript, e);
            throw new RuntimeException("Unexpected error while parsing Gremlin script: " + gremlinScript, e);
        }
    }

    @Override
    public GremlinQuery parse(Bytecode bytecode) {
        if (bytecode == null) {
            throw new IllegalArgumentException("Bytecode cannot be null");
        }
        
        try {
            // Create a traversal from the bytecode
            GraphTraversal traversal = g.from(bytecode);
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