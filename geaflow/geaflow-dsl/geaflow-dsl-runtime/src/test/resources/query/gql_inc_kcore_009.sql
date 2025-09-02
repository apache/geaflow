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
 * Incremental K-Core algorithm complex topology test
 * Test algorithm performance on complex graph structures
 */
CREATE SINK inc_kcore_complex_topology_result WITH (
    type='file',
    geaflow.dsl.file.path = '/tmp/geaflow/inc_kcore_complex_topology_result_009.txt'
);

USE GRAPH complex_graph;

-- Execute K-Core algorithm on complex topology graph
INSERT INTO inc_kcore_complex_topology_result
CALL incremental_kcore(3, 100, 0.001) ON GRAPH complex_graph 
RETURN vid, core_value, degree, change_status
ORDER BY vid;

-- Verify results
SELECT * FROM inc_kcore_complex_topology_result;
