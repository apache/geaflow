/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the License.  You may obtain a copy of the License at
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
 * 增量K-Core算法边添加测试
 * 测试在动态图上添加边后的增量更新
 */
CREATE SINK inc_kcore_edge_add_result WITH (
    type='file',
    geaflow.dsl.file.path = '/tmp/geaflow/inc_kcore_edge_add_result_003.txt'
);

USE GRAPH dynamic_graph;

-- 初始K-Core计算
INSERT INTO inc_kcore_edge_add_result
CALL incremental_kcore(2) ON GRAPH dynamic_graph 
RETURN vid, core_value, degree, change_status
ORDER BY vid;

-- 添加新边
INSERT INTO dynamic_graph.edges VALUES (1001, 1002, 0.8, 1000);

-- 增量更新后的K-Core计算
INSERT INTO inc_kcore_edge_add_result
CALL incremental_kcore(2) ON GRAPH dynamic_graph 
RETURN vid, core_value, degree, change_status
ORDER BY vid;

-- 验证结果
SELECT * FROM inc_kcore_edge_add_result;
