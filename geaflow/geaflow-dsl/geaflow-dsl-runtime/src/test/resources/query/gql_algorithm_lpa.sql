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

set geaflow.dsl.window.size = -1;
set geaflow.dsl.ignore.exception = true;

CREATE GRAPH IF NOT EXISTS test_lpa_graph (
  Vertex person (
    id varchar ID,
    name varchar
  ),
  Edge knows (
    srcId varchar SOURCE ID,
    targetId varchar DESTINATION ID,
    weight double
  )
) WITH (
  storeType='rocksdb',
  shardCount = 1
);

CREATE TABLE IF NOT EXISTS tbl_source (
  id varchar,
  name varchar
) WITH (
  type='file',
  geaflow.dsl.file.path = 'resource:///data/algo_test_vertex.txt'
);

CREATE TABLE IF NOT EXISTS tbl_edge_source (
  srcId varchar,
  targetId varchar,
  weight double
) WITH (
  type='file',
  geaflow.dsl.file.path = 'resource:///data/algo_test_edges.txt'
);

CREATE TABLE IF NOT EXISTS tbl_result (
  vid varchar,
  label varchar
) WITH (
  type='file',
  geaflow.dsl.file.path = '${target}'
);

USE GRAPH test_lpa_graph;

INSERT INTO test_lpa_graph.person(id, name)
SELECT id, name FROM tbl_source;

INSERT INTO test_lpa_graph.knows(srcId, targetId, weight)
SELECT srcId, targetId, weight FROM tbl_edge_source;

INSERT INTO tbl_result(vid, label)
CALL lpa(100, 'label') YIELD (id, label)
RETURN id, label
ORDER BY id
;
