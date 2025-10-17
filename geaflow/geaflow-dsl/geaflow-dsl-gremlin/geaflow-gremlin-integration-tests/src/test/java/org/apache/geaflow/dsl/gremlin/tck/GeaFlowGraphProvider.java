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

package org.apache.geaflow.dsl.gremlin.tck;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraphVariables;

import com.google.common.collect.Sets;

/**
 * Graph provider for GeaFlow Gremlin TCK tests.
 */
public class GeaFlowGraphProvider extends AbstractGraphProvider {
    
    private static final Set<Class> IMPLEMENTATION = Sets.newHashSet(
        TinkerGraphVariables.class
    );
    
    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, 
                                                   LoadGraphWith.GraphData loadGraphWith) {
        final Map<String, Object> config = new HashMap<>();
        config.put(Graph.GRAPH, TinkerGraph.class.getName());
        return config;
    }
    
    @Override
    public void clear(Graph graph, Configuration configuration) throws Exception {
        if (null != graph) {
            graph.close();
        }
    }
    
    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATION;
    }
    
    @Override
    public Graph openTestGraph(Configuration config) {
        return TinkerGraph.open(config);
    }
}