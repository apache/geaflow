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

CREATE FUNCTION temporal_udga_probe AS
'org.apache.geaflow.ai.temporal.udga.TemporalUdgaProbe';

CREATE GRAPH temporal_probe_graph (
  Vertex entity (
    id bigint ID,
    name varchar
  ),
  Edge relates (
    src_id bigint SOURCE ID,
    target_id bigint DESTINATION ID
  )
) WITH (
  storeType = 'memory',
  shardCount = 1
);

CREATE TABLE temporal_probe_vertices (
  id bigint,
  name varchar
) WITH (
  type = 'file',
  geaflow.dsl.file.path = '${vertices}',
  geaflow.dsl.window.size = -1
);

CREATE TABLE temporal_probe_edges (
  src_id bigint,
  target_id bigint
) WITH (
  type = 'file',
  geaflow.dsl.file.path = '${edges}',
  geaflow.dsl.window.size = 1
);

INSERT INTO temporal_probe_graph.entity
SELECT id, name FROM temporal_probe_vertices;

INSERT INTO temporal_probe_graph.relates
SELECT src_id, target_id FROM temporal_probe_edges;

CREATE TABLE temporal_probe_results (
  vertex_id bigint,
  batch_count int,
  had_previous_value boolean,
  dynamic_edge_count int,
  received_message_count int
) WITH (
  type = 'file',
  geaflow.dsl.file.path = '${output}'
);

USE GRAPH temporal_probe_graph;

INSERT INTO temporal_probe_results
CALL temporal_udga_probe() YIELD (
  vertex_id,
  batch_count,
  had_previous_value,
  dynamic_edge_count,
  received_message_count
)
RETURN
  vertex_id,
  batch_count,
  had_previous_value,
  dynamic_edge_count,
  received_message_count;
