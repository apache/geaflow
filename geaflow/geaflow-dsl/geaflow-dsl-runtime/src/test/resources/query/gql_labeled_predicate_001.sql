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

-- Test Case 1: IS_LABELED / IS_NOT_LABELED predicate on vertices and edges.
-- For every matched (a)-[e]->(b) triple, evaluate the labeled predicate against
-- the known labels of the modern graph (person / software / knows / created).

CREATE TABLE tbl_result (
  a_id bigint,
  b_id bigint,
  a_is_person boolean,
  b_is_software boolean,
  a_is_not_software boolean,
  e_is_created boolean
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a.id,
	b.id,
	IS_LABELED(a, 'person') as a_is_person,
	IS_LABELED(b, 'software') as b_is_software,
	IS_NOT_LABELED(a, 'software') as a_is_not_software,
	IS_LABELED(e, 'created') as e_is_created
FROM (
  MATCH (a) -[e]-> (b)
  RETURN a, e, b
)
ORDER BY a.id, b.id
;
