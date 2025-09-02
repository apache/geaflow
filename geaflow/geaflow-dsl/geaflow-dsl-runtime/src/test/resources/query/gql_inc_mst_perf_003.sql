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

/*
 * Incremental Minimum Spanning Tree algorithm大图性能Test
 * Test10K顶点图上的性能表现
 */
CREATE TABLE inc_mst_perf_large_result WITH (
    type='file',
    geaflow.dsl.file.path = '/tmp/geaflow/inc_mst_perf_large_result_003.txt'
);

USE GRAPH large_graph;
INSERT INTO inc_mst_perf_large_result
CALL IncMST(200, 0.001, 'mst_perf_large_edges') YIELD (srcId, targetId, weight) 
RETURN srcId, targetId, weight;

-- Verify results
SELECT * FROM inc_mst_perf_large_result;
