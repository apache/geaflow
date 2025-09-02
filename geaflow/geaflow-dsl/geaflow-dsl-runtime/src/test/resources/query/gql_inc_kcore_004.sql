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
 * Incremental K-Core algorithm edge deletion test
 * Test incremental update after deleting edges on dynamic graph
 */
CREATE SINK inc_kcore_edge_remove_result WITH (
    type='file',
    geaflow.dsl.file.path = '/tmp/geaflow/inc_kcore_edge_remove_result_004.txt'
);

USE GRAPH dynamic_graph;

-- Initial K-Core calculation
INSERT INTO inc_kcore_edge_remove_result
CALL incremental_kcore(2) ON GRAPH dynamic_graph 
RETURN vid, core_value, degree, change_status
ORDER BY vid;

-- Delete edges
DELETE FROM dynamic_graph.edges WHERE weight < 0.5;

-- K-Core calculation after incremental update
INSERT INTO inc_kcore_edge_remove_result
CALL incremental_kcore(2) ON GRAPH dynamic_graph 
RETURN vid, core_value, degree, change_status
ORDER BY vid;

-- Verify results
SELECT * FROM inc_kcore_edge_remove_result;
