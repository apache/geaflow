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

package org.apache.geaflow.dsl.runtime.engine;

import java.util.Map.Entry;
import java.util.Objects;
import org.apache.geaflow.dsl.runtime.traversal.message.KVTraversalAgg;

public class GeaFlowKVAlgorithmAggregateFunction extends GeaFlowKVTraversalAggregateFunction {

    private static final String ALGORITHM_ITERATION_PREFIX = "AlgorithmIteration-";

    public GeaFlowKVAlgorithmAggregateFunction(int parallelism) {
        super(parallelism);
    }

    public static KVTraversalAgg getAlgorithmAgg(long iteration) {
        return new KVTraversalAgg(ALGORITHM_ITERATION_PREFIX + iteration, 1);
    }

    @Override
    public IPartialGraphAggFunction<KVTraversalAgg<String, Integer>,
        KVTraversalAgg<String, Integer>, KVTraversalAgg<String, Integer>> getPartialAggregation() {
        return new IPartialGraphAggFunction<KVTraversalAgg<String, Integer>,
            KVTraversalAgg<String, Integer>, KVTraversalAgg<String, Integer>>() {

            private IPartialAggContext<KVTraversalAgg<String, Integer>> partialAggContext;

            @Override
            public KVTraversalAgg<String, Integer> create(
                IPartialAggContext<KVTraversalAgg<String, Integer>> partialAggContext) {
                this.partialAggContext = Objects.requireNonNull(partialAggContext);
                return KVTraversalAgg.empty();
            }


            @Override
            public KVTraversalAgg<String, Integer> aggregate(KVTraversalAgg<String, Integer> iterm,
                                                             KVTraversalAgg<String, Integer> result) {
                if (iterm == null) {
                    return result;
                } else if (result == null) {
                    return iterm;
                }
                for (Entry<String, Integer> entryObj : iterm.getMap().entrySet()) {
                    String key = entryObj.getKey();
                    if (result.getMap().containsKey(key)) {
                        result.getMap().put(key, result.getMap().get(key) + entryObj.getValue());
                    } else {
                        result.getMap().put(key, entryObj.getValue());
                    }
                }
                return result;
            }

            @Override
            public void finish(KVTraversalAgg<String, Integer> result) {
                assert partialAggContext != null;
                if (result != null) {
                    KVTraversalAgg<String, Integer> tmpResult = result.copy();
                    partialAggContext.collect(tmpResult);
                }
            }
        };
    }

    @Override
    public IGraphAggregateFunction<KVTraversalAgg<String, Integer>, KVTraversalAgg<String, Integer>,
        KVTraversalAgg<String, Integer>> getGlobalAggregation() {
        return new IGraphAggregateFunction<KVTraversalAgg<String, Integer>,
            KVTraversalAgg<String, Integer>, KVTraversalAgg<String, Integer>>() {

            private IGlobalGraphAggContext<KVTraversalAgg<String, Integer>> globalGraphAggContext;

            @Override
            public KVTraversalAgg<String, Integer> create(
                IGlobalGraphAggContext<KVTraversalAgg<String, Integer>> globalGraphAggContext) {
                this.globalGraphAggContext = Objects.requireNonNull(globalGraphAggContext);
                return KVTraversalAgg.empty();
            }

            @Override
            public KVTraversalAgg<String, Integer> aggregate(KVTraversalAgg<String, Integer> iterm,
                                                             KVTraversalAgg<String, Integer> result) {
                if (iterm == null) {
                    return result;
                } else if (result == null) {
                    return iterm;
                }
                for (Entry<String, Integer> entryObj : iterm.getMap().entrySet()) {
                    String key = entryObj.getKey();
                    if (result.getMap().containsKey(key)) {
                        result.getMap().put(key, result.getMap().get(key) + entryObj.getValue());
                    } else {
                        result.getMap().put(key, entryObj.getValue());
                    }
                }
                return result;
            }

            @Override
            public void finish(KVTraversalAgg<String, Integer> value) {
                assert globalGraphAggContext != null;
                long currentIteration = globalGraphAggContext.getIteration();
                String key = ALGORITHM_ITERATION_PREFIX + currentIteration;
                if (value == null || value.get(key) == null || value.get(key) == 0) {
                    globalGraphAggContext.terminate();
                }
            }
        };
    }
}
