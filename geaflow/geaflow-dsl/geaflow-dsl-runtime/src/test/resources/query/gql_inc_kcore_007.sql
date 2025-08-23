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
 * 增量K-Core算法性能测试
 * 在大图上测试算法性能
 */
CREATE SINK inc_kcore_performance_result WITH (
    type='file',
    geaflow.dsl.file.path = '/tmp/geaflow/inc_kcore_performance_result_007.txt'
);

USE GRAPH large_graph;

-- 执行性能测试
INSERT INTO inc_kcore_performance_result
CALL incremental_kcore(2, 100, 0.001) ON GRAPH large_graph 
RETURN vid, core_value, degree, change_status;

-- 统计结果
CREATE TABLE kcore_stats AS
SELECT 
    COUNT(*) as total_vertices,
    COUNT(CASE WHEN core_value >= 2 THEN 1 END) as kcore_vertices,
    AVG(degree) as avg_degree,
    MAX(degree) as max_degree,
    SUM(CASE WHEN change_status = 'CHANGED' THEN 1 ELSE 0 END) as changed_count
FROM inc_kcore_performance_result;

-- 输出统计信息
SELECT * FROM kcore_stats;
