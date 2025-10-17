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

/**
 * Interface for Gremlin optimization rules.
 * Each optimization rule implements this interface to define
 * its matching and application logic.
 */
public interface GremlinOptimizationRule {
    /**
     * Check if this optimization rule can be applied to the given RelNode.
     * 
     * @param relNode the RelNode to check
     * @return true if the rule can be applied, false otherwise
     */
    boolean matches(RelNode relNode);
    
    /**
     * Apply the optimization rule to the given RelNode.
     * 
     * @param relNode the RelNode to optimize
     * @return the optimized RelNode
     */
    RelNode apply(RelNode relNode);
    
    /**
     * Get the priority of this optimization rule.
     * Higher priority rules are applied first.
     * 
     * @return the priority of this rule
     */
    int getPriority();
}