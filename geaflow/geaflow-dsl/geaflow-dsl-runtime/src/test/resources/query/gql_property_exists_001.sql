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

-- Test Case 1: Basic PROPERTY_EXISTS predicate on vertex properties
-- Tests that PROPERTY_EXISTS correctly identifies properties on vertices

CREATE TABLE tbl_result (
  id bigint,
  name varchar,
  has_name boolean,
  has_age boolean
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	v.id,
	v.name,
	PROPERTY_EXISTS(v, 'name') as has_name,
	PROPERTY_EXISTS(v, 'age') as has_age
FROM (
  MATCH (v:person)
  RETURN v
)
ORDER BY v.id
;
