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

CREATE GRAPH modern (
	Vertex person (
	  id bigint ID,
	  name varchar,
	  age int
	),
	Vertex software (
	  id bigint ID,
	  name varchar,
	  lang varchar
	),
	Edge knows (
	  srcId bigint SOURCE ID,
	  targetId bigint DESTINATION ID,
	  weight double
	),
	Edge created (
	  srcId bigint SOURCE ID,
  	targetId bigint DESTINATION ID,
  	weight double
	)
) WITH (
	storeType='rocksdb',
	shardNum = 1
);

CREATE TABLE modern_vertex (
  id varchar,
  type varchar,
  name varchar,
  other varchar
) WITH (
  type='file',
  geaflow.dsl.file.path = 'resource:///data/modern_vertex.txt'
);

CREATE TABLE modern_edge (
  srcId bigint,
  targetId bigint,
  type varchar,
  weight double
) WITH (
  type='file',
  geaflow.dsl.file.path = 'resource:///data/modern_edge.txt'
);

INSERT INTO modern.person
SELECT cast(id as bigint), name, cast(other as int) as age
FROM modern_vertex WHERE type = 'person'
;

INSERT INTO modern.software
SELECT cast(id as bigint), name, cast(other as varchar) as lang
FROM modern_vertex WHERE type = 'software'
;

INSERT INTO modern.knows
SELECT srcId, targetId, weight
FROM modern_edge WHERE type = 'knows'
;

INSERT INTO modern.created
SELECT srcId, targetId, weight
FROM modern_edge WHERE type = 'created'
;

CREATE TABLE tbl_result (
  vid int,
	distance bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
CALL SSSP(1) YIELD (vid, distance)
RETURN cast (vid as int), distance
;
