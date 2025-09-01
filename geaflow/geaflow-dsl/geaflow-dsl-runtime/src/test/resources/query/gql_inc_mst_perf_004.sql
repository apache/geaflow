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
 * 增量最小生成树算法增量更新性能测试
 * 测试动态图更新场景的性能表现
 */
CREATE SINK inc_mst_perf_incremental_result WITH (
    type='file',
    geaflow.dsl.file.path = '/tmp/geaflow/inc_mst_perf_incremental_result_004.txt'
);

USE GRAPH dynamic_graph;

-- 初始MST计算
INSERT INTO inc_mst_perf_incremental_result
CALL IncMST(50, 0.001, 'mst_perf_incremental_edges') ON GRAPH dynamic_graph 
RETURN srcId, targetId, weight;

-- 模拟边更新
INSERT INTO dynamic_graph.e_connects VALUES (3001, 3002, 0.2);
INSERT INTO dynamic_graph.e_connects VALUES (3002, 3003, 0.3);

-- 增量更新后的MST计算
INSERT INTO inc_mst_perf_incremental_result
CALL IncMST(50, 0.001, 'mst_perf_incremental_edges') ON GRAPH dynamic_graph 
RETURN srcId, targetId, weight;

-- 验证结果
SELECT * FROM inc_mst_perf_incremental_result;
