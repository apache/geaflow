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
 * 增量最小生成树算法复杂拓扑测试
 * 测试复杂图结构的MST计算
 */
CREATE SINK inc_mst_complex_result WITH (
    type='file',
    geaflow.dsl.file.path = '/tmp/geaflow/inc_mst_complex_result_010.txt'
);

USE GRAPH complex_graph;
INSERT INTO inc_mst_complex_result
CALL IncMST(150, 0.001, 'mst_complex_edges') ON GRAPH complex_graph 
RETURN srcId, targetId, weight;

-- 验证结果
SELECT * FROM inc_mst_complex_result;
