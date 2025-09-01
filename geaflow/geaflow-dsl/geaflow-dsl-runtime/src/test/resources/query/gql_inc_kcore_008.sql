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
 * 增量K-Core算法自定义参数测试
 * 测试不同参数组合对算法结果的影响
 */
CREATE SINK inc_kcore_custom_params_result WITH (
    type='file',
    geaflow.dsl.file.path = '/tmp/geaflow/inc_kcore_custom_params_result_008.txt'
);

USE GRAPH modern;

-- 测试不同的K值
INSERT INTO inc_kcore_custom_params_result
CALL incremental_kcore(1, 50, 0.001) ON GRAPH modern 
RETURN vid, core_value, degree, change_status
ORDER BY vid;

-- 测试不同的最大迭代次数
INSERT INTO inc_kcore_custom_params_result
CALL incremental_kcore(2, 5, 0.001) ON GRAPH modern 
RETURN vid, core_value, degree, change_status
ORDER BY vid;

-- 测试不同的收敛阈值
INSERT INTO inc_kcore_custom_params_result
CALL incremental_kcore(3, 100, 0.01) ON GRAPH modern 
RETURN vid, core_value, degree, change_status
ORDER BY vid;

-- 验证结果
SELECT * FROM inc_kcore_custom_params_result;
