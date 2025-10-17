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

/**
 * Main optimizer for Gremlin queries that combines multiple optimization strategies.
 * This optimizer coordinates various optimization rules and applies them in phases
 * to improve query performance.
 */
public class GremlinQueryOptimizer {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinQueryOptimizer.class);
    
    private final OptimizationRuleRegistry ruleRegistry;
    private final GremlinOptimizerManager optimizerManager;
    
    public GremlinQueryOptimizer() {
        this.ruleRegistry = new OptimizationRuleRegistry();
        this.optimizerManager = new GremlinOptimizerManager();
        
        // Register default optimization rules
        registerDefaultRules();
    }
    
    /**
     * Main optimization entry point.
     * 
     * @param relNode the root RelNode to optimize
     * @return the optimized RelNode
     */
    public static RelNode optimize(RelNode relNode) {
        GremlinQueryOptimizer optimizer = new GremlinQueryOptimizer();
        return optimizer.optimizeInternal(relNode);
    }
    
    private RelNode optimizeInternal(RelNode relNode) {
        LOGGER.info("Starting Gremlin query optimization");
        
        // Logical optimization phase
        RelNode logicalOptimized = optimizeLogical(relNode);
        
        // Physical optimization phase
        RelNode physicalOptimized = optimizePhysical(logicalOptimized);
        
        LOGGER.info("Gremlin query optimization completed");
        return physicalOptimized;
    }
    
    private RelNode optimizeLogical(RelNode relNode) {
        // Apply logical optimization rules
        return optimizerManager.optimize(relNode, OptimizationPhase.LOGICAL);
    }
    
    private RelNode optimizePhysical(RelNode relNode) {
        // Apply physical optimization rules
        return optimizerManager.optimize(relNode, OptimizationPhase.PHYSICAL);
    }
    
    private void registerDefaultRules() {
        // Register default optimization rules
        ruleRegistry.registerRule("predicatePushdown", new GremlinPredicatePushdownRule());
        ruleRegistry.registerRule("batchMessage", new GremlinBatchMessageRule());
        ruleRegistry.registerRule("pathOptimization", new GremlinPathOptimizationRule());
        ruleRegistry.registerRule("joinOptimization", new GremlinJoinOptimizationRule());
        ruleRegistry.registerRule("aggregationPushdown", new GremlinAggregationPushdownRule());
    }
}