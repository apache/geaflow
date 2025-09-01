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

create function mycount as 'org.apache.geaflow.dsl.runtime.query.udf.MyCount';
create function concat as 'org.apache.geaflow.dsl.udf.table.string.Concat';

CREATE TABLE users (
	id long,
	name string,
	age double
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/users_double.txt'
);

CREATE TABLE tbl_result (
	p_long double,
	p_integer double,
	p_double double
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO tbl_result
SELECT
  PERCENTILE(o.id, 0.9) AS p_long,
  PERCENTILE(cast(o.id as int), 0.9) AS p_integer,
  PERCENTILE(o.age, 0.9) AS p_double
FROM users o
