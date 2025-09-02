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
 * Incremental K-Core algorithm incremental update test
 * Test scenarios of dynamic edge addition and deletion
 */
CREATE SINK inc_kcore_incremental_result WITH (
    type='file',
    geaflow.dsl.file.path = '/tmp/geaflow/inc_kcore_incremental_result_002.txt'
);

USE GRAPH modern;

-- Initial K-Core calculation
INSERT INTO inc_kcore_incremental_result
CALL incremental_kcore(2, 50, 0.001) ON GRAPH modern 
RETURN vid, core_value, degree, change_status;

-- Simulate edge addition
INSERT INTO modern.knows VALUES (1001, 1002, 0.8);

-- K-Core calculation after incremental update
INSERT INTO inc_kcore_incremental_result
CALL incremental_kcore(2, 50, 0.001) ON GRAPH modern 
RETURN vid, core_value, degree, change_status;

-- Simulate edge deletion
DELETE FROM modern.knows WHERE weight < 0.5;

-- Update K-Core again
INSERT INTO inc_kcore_incremental_result
CALL incremental_kcore(2, 50, 0.001) ON GRAPH modern 
RETURN vid, core_value, degree, change_status;

-- Verify results
SELECT * FROM inc_kcore_incremental_result
ORDER BY vid, change_status;
