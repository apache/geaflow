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

CREATE TABLE users (
	rt bigint,
	f1 bigint,
	f2 double,
	f3 varchar,
	f4 boolean,
	f5 varchar
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/users3.txt'
);

CREATE TABLE tbl_result (
	bit_and_value int,
	bit_or_value int,
	bool_and_value boolean
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO tbl_result
SELECT
  BIT_AND(cast(f1 as int)) AS bit_and_value,
  BIT_OR(cast(f1 as int)) AS bit_or_value,
  BOOL_AND(f4) AS bool_and_value
FROM users
