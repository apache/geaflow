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

import org.apache.calcite.rex.RexNode;
import java.util.List;

/**
 * Result of predicate analysis for pushdown optimization.
 * This class encapsulates the results of analyzing a predicate condition
 * to determine if it can be pushed down to the storage layer.
 */
public class PredicateAnalysisResult {
    
    private final boolean pushable;
    private final List<RexNode> pushablePredicates;
    private final List<RexNode> unpushablePredicates;
    
    public PredicateAnalysisResult(boolean pushable, List<RexNode> pushablePredicates, List<RexNode> unpushablePredicates) {
        this.pushable = pushable;
        this.pushablePredicates = pushablePredicates;
        this.unpushablePredicates = unpushablePredicates;
    }
    
    /**
     * Check if the predicate is pushable.
     * 
     * @return true if the predicate can be pushed down, false otherwise
     */
    public boolean isPushable() {
        return pushable;
    }
    
    /**
     * Get the pushable predicates.
     * 
     * @return list of pushable predicates
     */
    public List<RexNode> getPushablePredicates() {
        return pushablePredicates;
    }
    
    /**
     * Get the unpushable predicates.
     * 
     * @return list of unpushable predicates
     */
    public List<RexNode> getUnpushablePredicates() {
        return unpushablePredicates;
    }
}