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

-- ISO-GQL Value Type Predicate Test 001: Basic IS TYPED with Primitive Types
-- Tests basic type checking functionality for INTEGER and DOUBLE types

CREATE TABLE tbl_result (
  person_id bigint,
  name varchar,
  age int,
  weight double
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	p.id AS person_id,
	p.name AS name,
	p.age AS age,
	0.0 AS weight
FROM (
  MATCH (p:person)
  WHERE p.age IS TYPED INTEGER
  RETURN p
)
WHERE person_id > 0

