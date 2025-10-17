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

import org.apache.calcite.rel.RelNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Manager for coordinating Gremlin optimization rules.
 * This class manages and applies optimization rules in phases
 * to optimize Gremlin queries.
 */
public class GremlinOptimizerManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinOptimizerManager.class);
    
    private List<GremlinOptimizationRule> logicalRules;
    private List<GremlinOptimizationRule> physicalRules;
    
    public GremlinOptimizerManager() {
        this.logicalRules = new ArrayList<>();
        this.physicalRules = new ArrayList<>();
    }
    
    /**
     * Optimize a RelNode using the appropriate optimization rules.
     * 
     * @param relNode the RelNode to optimize
     * @param phase the optimization phase
     * @return the optimized RelNode
     */
    public RelNode optimize(RelNode relNode, OptimizationPhase phase) {
        LOGGER.info("Starting {} phase optimization", phase);
        
        // Apply the appropriate optimization rules based on the phase
        List<GremlinOptimizationRule> rules = 
            phase == OptimizationPhase.LOGICAL ? logicalRules : physicalRules;
        
        // Apply optimization rules
        RelNode optimized = applyOptimizationRules(relNode, rules);
        
        LOGGER.info("{} phase optimization completed", phase);
        return optimized;
    }
    
    /**
     * Apply optimization rules to a RelNode.
     * 
     * @param relNode the RelNode to optimize
     * @param rules the optimization rules to apply
     * @return the optimized RelNode
     */
    private RelNode applyOptimizationRules(RelNode relNode, List<GremlinOptimizationRule> rules) {
        // Sort rules by priority (highest first)
        List<GremlinOptimizationRule> sortedRules = new ArrayList<>(rules);
        sortedRules.sort(Comparator.comparingInt(GremlinOptimizationRule::getPriority).reversed());
        
        RelNode current = relNode;
        boolean changed = true;
        int iteration = 0;
        final int maxIterations = 10; // Maximum iterations to prevent infinite loops
        
        // Iteratively apply optimization rules until no more changes occur
        while (changed && iteration < maxIterations) {
            changed = false;
            iteration++;
            
            LOGGER.debug("Starting optimization iteration {}", iteration);
            
            for (GremlinOptimizationRule rule : sortedRules) {
                if (rule.matches(current)) {
                    RelNode optimized = rule.apply(current);
                    if (optimized != current) {
                        changed = true;
                        current = optimized;
                        LOGGER.debug("Applied optimization rule: {}", rule.getClass().getSimpleName());
                    }
                }
            }
            
            if (changed) {
                LOGGER.debug("Optimization iteration {} resulted in changes", iteration);
            }
        }
        
        if (iteration >= maxIterations) {
            LOGGER.warn("Optimization reached maximum iterations, there may be rule conflicts");
        }
        
        return current;
    }
    
    /**
     * Add a logical optimization rule.
     * 
     * @param rule the rule to add
     */
    public void addLogicalRule(GremlinOptimizationRule rule) {
        logicalRules.add(rule);
    }
    
    /**
     * Add a physical optimization rule.
     * 
     * @param rule the rule to add
     */
    public void addPhysicalRule(GremlinOptimizationRule rule) {
        physicalRules.add(rule);
    }
}