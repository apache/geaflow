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

-- ISO-GQL Value Type Predicate Test 005: Variable type checking in complex queries
-- Tests type predicates with path variables and complex filters

CREATE TABLE tbl_result (
  source_id bigint,
  target_id bigint,
  path_length int,
  weight_sum double
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	src.id AS source_id,
	tgt.id AS target_id,
	1 AS path_length,
	CAST(e.weight AS DOUBLE) AS weight_sum
FROM (
  MATCH (src:person)-[e:knows]->(tgt:person)
  RETURN src, tgt, e
)
WHERE TYPED(src.id, 'INTEGER')
  AND TYPED(tgt.id, 'INTEGER')
  AND TYPED(e.weight, 'DOUBLE')

