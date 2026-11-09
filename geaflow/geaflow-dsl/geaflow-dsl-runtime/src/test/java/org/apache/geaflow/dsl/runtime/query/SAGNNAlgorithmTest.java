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

package org.apache.geaflow.dsl.runtime.query;

import org.testng.annotations.Test;

/**
 * GQL integration tests for the SA-GNN (Spatial Adaptive Graph Neural Network) algorithm.
 *
 * <p>These tests verify the end-to-end GQL execution pipeline for SA-GNN, covering:
 * <ul>
 *   <li>Basic CALL SAGNN(numSamples, numLayers) syntax</li>
 *   <li>Custom parameter variants</li>
 *   <li>Graph schema with spatial vertex features (64 dims: 62 semantic + 2 coordinates)</li>
 *   <li>Result projection via YIELD (vid, embedding)</li>
 * </ul>
 *
 * <p>The expected outputs contain only vertex IDs and names (not embedding values)
 * because SA-GNN embeddings are non-deterministic when PaddlePaddle is not available
 * (the algorithm falls back to zero-padded features).
 *
 * <p>To run these tests with full PaddlePaddle inference, set the following config:
 * <pre>
 *   geaflow.infer.env.enable=true
 *   geaflow.infer.framework.type=PADDLE
 *   geaflow.infer.env.use.system.python=true
 *   geaflow.infer.env.system.python.path=/path/to/python3
 *   geaflow.infer.env.user.transform.classname=SAGNNTransFormFunction
 * </pre>
 */
public class SAGNNAlgorithmTest {

    /**
     * Test basic SA-GNN with default parameters: numSamples=10, numLayers=2.
     *
     * <p>Verifies that the SAGNN algorithm completes graph traversal and produces
     * output rows for all 5 spatial POI vertices in the test graph.
     */
    @Test
    public void testSAGNN_001() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/sagnn_graph.sql")
            .withQueryPath("/query/gql_sagnn_001.sql")
            .execute()
            .checkSinkResult();
    }

    /**
     * Test SA-GNN with custom parameters: numSamples=5, numLayers=3.
     *
     * <p>Verifies that the algorithm works correctly with fewer neighbors
     * and more aggregation layers, which exercises deeper neighbourhood
     * expansion on the spatial POI graph.
     */
    @Test
    public void testSAGNN_002() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/sagnn_graph.sql")
            .withQueryPath("/query/gql_sagnn_002.sql")
            .execute()
            .checkSinkResult();
    }
}
