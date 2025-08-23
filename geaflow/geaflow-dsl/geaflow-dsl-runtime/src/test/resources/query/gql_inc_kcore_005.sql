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
 * 增量K-Core算法不同K值测试
 * 测试不同K值下的算法行为
 */
CREATE SINK inc_kcore_k_values_result WITH (
    type='file',
    geaflow.dsl.file.path = '/tmp/geaflow/inc_kcore_k_values_result_005.txt'
);

USE GRAPH modern;

-- 测试K=1
INSERT INTO inc_kcore_k_values_result
CALL incremental_kcore(1) ON GRAPH modern 
RETURN vid, core_value, degree, change_status, 1 as k_value;

-- 测试K=2
INSERT INTO inc_kcore_k_values_result
CALL incremental_kcore(2) ON GRAPH modern 
RETURN vid, core_value, degree, change_status, 2 as k_value;

-- 测试K=3
INSERT INTO inc_kcore_k_values_result
CALL incremental_kcore(3) ON GRAPH modern 
RETURN vid, core_value, degree, change_status, 3 as k_value;

-- 验证结果，按K值和顶点ID排序
SELECT * FROM inc_kcore_k_values_result
ORDER BY k_value, vid;
