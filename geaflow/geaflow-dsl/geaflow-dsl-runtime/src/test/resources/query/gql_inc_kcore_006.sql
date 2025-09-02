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
 * Incremental K-Core algorithm convergence test
 * Test algorithm convergence under different iteration counts
 */
CREATE SINK inc_kcore_convergence_result WITH (
    type='file',
    geaflow.dsl.file.path = '/tmp/geaflow/inc_kcore_convergence_result_006.txt'
);

USE GRAPH modern;

-- Test convergence under different convergence thresholds
INSERT INTO inc_kcore_convergence_result
CALL incremental_kcore(2, 10, 0.1) ON GRAPH modern 
RETURN vid, core_value, degree, change_status
ORDER BY vid;

-- Use stricter convergence threshold
INSERT INTO inc_kcore_convergence_result
CALL incremental_kcore(2, 20, 0.001) ON GRAPH modern 
RETURN vid, core_value, degree, change_status
ORDER BY vid;

-- Verify results
SELECT * FROM inc_kcore_convergence_result;
