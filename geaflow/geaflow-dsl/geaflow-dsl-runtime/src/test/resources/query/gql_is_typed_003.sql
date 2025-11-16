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

-- ISO-GQL Value Type Predicate Test 003: Combined conditions with AND/OR
-- Tests type predicates combined with logical operators

CREATE TABLE tbl_result (
  person_id bigint,
  edge_weight double,
  person_name varchar
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a.id AS person_id,
	e.weight AS edge_weight,
	b.name AS person_name
FROM (
  MATCH (a:person)-[e:knows]->(b:person)
  RETURN a.id AS a_id, e, b
)
WHERE TYPED(a_id, 'INTEGER') 
  AND TYPED(weight, 'DOUBLE')
  AND NOT_TYPED(name, 'INTEGER')

