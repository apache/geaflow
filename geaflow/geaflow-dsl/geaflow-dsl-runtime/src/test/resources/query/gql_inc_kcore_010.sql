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
 * 增量K-Core算法不连通组件测试
 * 测试在包含多个不连通组件的图上的算法表现
 */
CREATE SINK inc_kcore_disconnected_result WITH (
    type='file',
    geaflow.dsl.file.path = '/tmp/geaflow/inc_kcore_disconnected_result_010.txt'
);

USE GRAPH disconnected_graph;

-- 在包含不连通组件的图上执行K-Core算法
INSERT INTO inc_kcore_disconnected_result
CALL incremental_kcore(2, 50, 0.001) ON GRAPH disconnected_graph 
RETURN vid, core_value, degree, change_status
ORDER BY vid;

-- 验证结果
SELECT * FROM inc_kcore_disconnected_result;
