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

CREATE GRAPH IF NOT EXISTS test_cc_graph (
  Vertex person (
    id bigint ID,
    name varchar
  ),
  Edge knows (
    srcId bigint SOURCE ID,
    targetId bigint DESTINATION ID,
    weight double
  )
) WITH (
  storeType='rocksdb',
  shardCount = 1
);

CREATE TABLE IF NOT EXISTS tbl_source (
  text varchar
) WITH (
  type='file',
  geaflow.dsl.file.path = 'resource:///data/algo_test_vertex.txt'
);

CREATE TABLE IF NOT EXISTS tbl_edge_source (
  text varchar
) WITH (
  type='file',
  geaflow.dsl.file.path = 'resource:///data/algo_test_edges.txt'
);

CREATE TABLE IF NOT EXISTS tbl_result (
  vid bigint,
  component bigint
) WITH (
  type='file',
  geaflow.dsl.file.path = '${target}'
);

USE GRAPH test_cc_graph;

INSERT INTO test_cc_graph.person(id, name)
SELECT
  cast(split_ex(text, ',', 0) as bigint) as id,
  split_ex(text, ',', 1) as name
FROM tbl_source;

INSERT INTO test_cc_graph.knows(srcId, targetId, weight)
SELECT
  cast(split_ex(text, ',', 0) as bigint) as srcId,
  cast(split_ex(text, ',', 1) as bigint) as targetId,
  cast(split_ex(text, ',', 2) as double) as weight
FROM tbl_edge_source;

INSERT INTO tbl_result(vid, component)
CALL cc(20, 'component') YIELD (id, component)
RETURN CAST(id as bigint), CAST(component as bigint)
ORDER BY id
;
