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

-- ISO-GQL Value Type Predicate Test 004: Large Dataset Performance Test (100K scale)
-- Tests type predicates performance with large datasets
-- Simulates complex edge traversal with multiple type checks

CREATE TABLE tbl_result (
  source_id bigint,
  edge_weight double,
  target_id bigint,
  target_name varchar,
  match_count int
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	COUNT(*) AS match_count,
	0L AS source_id,
	0.0 AS edge_weight,
	0L AS target_id,
	'' AS target_name
FROM (
  MATCH (a:person)-[e:knows]->(b:person)-[e2:knows]->(c:person)
  RETURN a, e, b, e2, c
)
WHERE TYPED(a.id, 'INTEGER')
  AND TYPED(e.weight, 'DOUBLE')
  AND TYPED(b.id, 'INTEGER')
  AND TYPED(e2.weight, 'DOUBLE')
  AND NOT_TYPED(c.name, 'INTEGER')

