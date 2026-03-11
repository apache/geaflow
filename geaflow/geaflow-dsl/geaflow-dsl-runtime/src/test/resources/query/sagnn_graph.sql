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

-- Graph definition for SA-GNN (Spatial Adaptive Graph Neural Network) testing
-- Vertices represent spatial POIs (Points of Interest) with 64-dimensional features:
--   first 62 dims are semantic features, last 2 dims are [coord_x, coord_y]
-- Edges represent spatial proximity or user co-visit relationships

CREATE TABLE v_poi (
  id bigint,
  name varchar,
  features varchar  -- JSON string representing List<Double> features (64 dims)
) WITH (
	type='file',
	geaflow.dsl.window.size = -1,
	geaflow.dsl.column.separator = '|',
	geaflow.dsl.file.path = 'resource:///data/sagnn_vertex.txt'
);

CREATE TABLE e_relation (
  srcId bigint,
  targetId bigint,
  weight double
) WITH (
	type='file',
	geaflow.dsl.window.size = -1,
	geaflow.dsl.file.path = 'resource:///data/sagnn_edge.txt'
);

CREATE GRAPH sagnn_test (
	Vertex poi using v_poi WITH ID(id),
	Edge relation using e_relation WITH ID(srcId, targetId)
) WITH (
	storeType='memory',
	shardCount = 2
);

