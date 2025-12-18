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

CREATE TABLE v_medium (
  id bigint,
  name varchar,
  attr varchar
) WITH (
	type='file',
	geaflow.dsl.window.size = -1,
	geaflow.dsl.file.path = 'resource:///data/medium_graph_vertex.txt'
);

CREATE TABLE e_medium (
  srcId bigint,
  targetId bigint
) WITH (
	type='file',
	geaflow.dsl.window.size = -1,
	geaflow.dsl.file.path = 'resource:///data/medium_graph_edge.txt'
);

CREATE GRAPH medium_graph (
	Vertex node using v_medium WITH ID(id),
	Edge link using e_medium WITH ID(srcId, targetId)
) WITH (
	storeType='memory',
	shardCount = 4
);

CREATE TABLE result_tb (
   vid int,
   coefficient double
) WITH (
      type='file',
      geaflow.dsl.file.path='${target}'
);

USE GRAPH medium_graph;

INSERT INTO result_tb
CALL cluster_coefficient(2) YIELD (vid, coefficient)
RETURN cast (vid as int), coefficient
;
