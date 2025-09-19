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

-- 复杂图定义，用于测试增量K-Core算法在复杂拓扑上的表现

CREATE TABLE v_complex_nodes (
  id bigint,
  label varchar,
  weight double,
  type varchar
) WITH (
type='file',
geaflow.dsl.window.size = -1,
geaflow.dsl.file.path = 'resource:///data/complex_vertex.txt'
);

CREATE TABLE e_complex_edges (
  srcId bigint,
  targetId bigint,
  weight double,
  timestamp bigint,
  edgeType varchar
) WITH (
type='file',
geaflow.dsl.window.size = -1,
geaflow.dsl.file.path = 'resource:///data/complex_edge.txt'
);

CREATE GRAPH complex_graph (
Vertex nodes using v_complex_nodes WITH ID(id),
Edge edges using e_complex_edges WITH ID(srcId, targetId)
) WITH (
storeType='memory',
shardCount = 6
);
