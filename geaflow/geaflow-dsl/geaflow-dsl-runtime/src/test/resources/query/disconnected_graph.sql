/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

CREATE TABLE v_disconnected_node (
  name varchar,
  id bigint
) WITH (
	type='file',
	geaflow.dsl.window.size = -1,
	geaflow.dsl.file.path = 'resource:///data/disconnected_vertex.txt'
);

CREATE TABLE e_disconnected_edge (
  srcId bigint,
  targetId bigint,
  weight double
) WITH (
	type='file',
	geaflow.dsl.window.size = -1,
	geaflow.dsl.file.path = 'resource:///data/disconnected_edge.txt'
);

CREATE GRAPH disconnected_graph (
	Vertex node using v_disconnected_node WITH ID(id),
	Edge connects using e_disconnected_edge WITH ID(srcId, targetId)
) WITH (
	storeType='memory',
	shardCount = 2
);
