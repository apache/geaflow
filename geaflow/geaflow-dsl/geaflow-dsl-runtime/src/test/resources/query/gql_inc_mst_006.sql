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
 * 增量最小生成树算法连通分量测试
 * 测试不连通图的MST计算
 */
CREATE SINK inc_mst_connected_result WITH (
    type='file',
    geaflow.dsl.file.path = '/tmp/geaflow/inc_mst_connected_result_006.txt'
);

USE GRAPH disconnected_graph;
INSERT INTO inc_mst_connected_result
CALL IncMST(100, 0.001, 'mst_connected_edges') ON GRAPH disconnected_graph 
RETURN srcId, targetId, weight;

-- 验证结果
SELECT * FROM inc_mst_connected_result;
