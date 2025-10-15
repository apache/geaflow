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

package org.apache.geaflow.runtime.core.scheduler.statemachine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StateMachineManager<S, C> {

    private Map<S, List<ITransition<S, C>>> stateToTransitionMap = new HashMap<>();

    private static final ITransitionCondition ALWAYS = (s, c) -> true;

    public void addTransition(S source, S target) {
        this.addTransition(source, target, ALWAYS);
    }

    /**
     * Add transition from source state to target state.
     * The transition condition evaluation by added order.
     * For example, denote as source -> target : condition.
     * If s1 -> t1 : c1 is already added, then add s1 -> t2 : c2.
     * If (c1 matches) return t1
     * else if (c2 matches) return t2
     * else return null;
     */
    public void addTransition(S source, S target, ITransitionCondition<S, C> condition) {
        if (!stateToTransitionMap.containsKey(source)) {
            stateToTransitionMap.put(source, new ArrayList<>());
        }
        stateToTransitionMap.get(source).add((s, c) -> {
            if (condition.predicate(s, c)) {
                return target;
            } else {
                return null;
            }
        });
    }

    /**
     * Transition from source to certain target state by input context.
     */
    public S transition(S source, C context) {
        if (stateToTransitionMap.containsKey(source)) {
            List<ITransition<S, C>> transitions = stateToTransitionMap.get(source);
            for (ITransition<S, C> t : transitions) {
                S target = t.transition(source, context);
                if (target != null) {
                    return target;
                }
            }
        }
        return null;
    }
}
