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

package org.apache.geaflow.dsl.gremlin.plan.optimizer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Registry for managing optimization rules.
 * This class provides a mechanism for registering and retrieving
 * optimization rules by name.
 */
public class OptimizationRuleRegistry {
    private Map<String, GremlinOptimizationRule> rules = new HashMap<>();
    
    /**
     * Register an optimization rule.
     * 
     * @param name the name of the rule
     * @param rule the optimization rule
     */
    public void registerRule(String name, GremlinOptimizationRule rule) {
        rules.put(name, rule);
    }
    
    /**
     * Unregister an optimization rule.
     * 
     * @param name the name of the rule to unregister
     */
    public void unregisterRule(String name) {
        rules.remove(name);
    }
    
    /**
     * Get all registered optimization rules.
     * 
     * @return collection of registered rules
     */
    public Collection<GremlinOptimizationRule> getRules() {
        return rules.values();
    }
    
    /**
     * Get an optimization rule by name.
     * 
     * @param name the name of the rule
     * @return the optimization rule, or null if not found
     */
    public GremlinOptimizationRule getRule(String name) {
        return rules.get(name);
    }
}