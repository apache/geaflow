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

-- 大图定义，用于测试增量K-Core算法的性能

CREATE TABLE v_large_nodes (
  id bigint,
  label varchar,
  weight double
) WITH (
type='file',
geaflow.dsl.window.size = -1,
geaflow.dsl.file.path = 'resource:///data/large_vertex.txt'
);

CREATE TABLE e_large_edges (
  srcId bigint,
  targetId bigint,
  weight double,
  timestamp bigint
) WITH (
type='file',
geaflow.dsl.window.size = -1,
geaflow.dsl.file.path = 'resource:///data/large_edge.txt'
);

CREATE GRAPH large_graph (
Vertex nodes using v_large_nodes WITH ID(id),
Edge edges using e_large_edges WITH ID(srcId, targetId)
) WITH (
storeType='memory',
shardCount = 8
);
