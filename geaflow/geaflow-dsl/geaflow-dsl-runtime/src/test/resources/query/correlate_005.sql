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

CREATE function UDTF_SPLIT as 'org.apache.geaflow.dsl.udf.table.udtf.Split';

CREATE TABLE users (
	createTime bigint,
	productId bigint,
	orderId bigint,
	units bigint,
	user_name VARCHAR
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/users_correlate.txt'
);

CREATE TABLE tbl_result (
	f1 bigint,
	f2 bigint,
	f3 bigint,
	f4 bigint,
	name varchar
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO tbl_result
SELECT
	createTime,
	productId,
	orderId,
	units,
	t.name
FROM users, LATERAL table(UDTF_SPLIT(user_name, '|')) as t(name)
where name = '中国';
