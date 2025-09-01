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
 * 增量最小生成树算法边删除测试
 * 测试动态删除边的场景
 */
CREATE SINK inc_mst_edge_del_result WITH (
    type='file',
    geaflow.dsl.file.path = '/tmp/geaflow/inc_mst_edge_del_result_005.txt'
);

USE GRAPH dynamic_graph;

-- 初始MST计算
INSERT INTO inc_mst_edge_del_result
CALL IncMST(50, 0.001, 'mst_edge_del_edges') ON GRAPH dynamic_graph 
RETURN srcId, targetId, weight;

-- 删除边
DELETE FROM dynamic_graph.e_connects WHERE srcId = 1001 AND targetId = 1002;

-- 重新计算MST
INSERT INTO inc_mst_edge_del_result
CALL IncMST(50, 0.001, 'mst_edge_del_edges') ON GRAPH dynamic_graph 
RETURN srcId, targetId, weight;

-- 验证结果
SELECT * FROM inc_mst_edge_del_result;
