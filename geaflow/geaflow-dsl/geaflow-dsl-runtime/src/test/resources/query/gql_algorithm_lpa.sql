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

CREATE GRAPH IF NOT EXISTS g5 (
  Vertex v5 (
    vid varchar ID,
    vvalue int
  ),
  Edge e5 (
    srcId varchar SOURCE ID,
    targetId varchar DESTINATION ID
  )
) WITH (
  storeType='rocksdb',
  shardCount = 1
);

CREATE TABLE IF NOT EXISTS v_source (
    v_id varchar,
    v_value int,
    ts varchar,
    type varchar
) WITH (
  type='file',
  geaflow.dsl.file.path = 'resource:///data/lpa_vertex.txt'
);

CREATE TABLE IF NOT EXISTS e_source (
    src_id varchar,
    dst_id varchar
) WITH (
  type='file',
  geaflow.dsl.file.path = 'resource:///data/lpa_edges.txt'
);

CREATE TABLE IF NOT EXISTS tbl_result (
  v_id varchar,
  k_value varchar
) WITH (
  type='file',
  geaflow.dsl.file.path = '${target}'
);

USE GRAPH g5;

INSERT INTO g5.v5(vid, vvalue)
SELECT
v_id, v_value
FROM v_source;

INSERT INTO g5.e5(srcId, targetId)
SELECT
 src_id, dst_id
FROM e_source;

INSERT INTO tbl_result(v_id, k_value)
CALL lpa() YIELD (vid, kValue)
RETURN vid, kValue
;
