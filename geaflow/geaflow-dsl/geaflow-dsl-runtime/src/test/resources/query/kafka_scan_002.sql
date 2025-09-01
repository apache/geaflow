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

CREATE TABLE kafka_source (
	id bigint,
	name varchar,
	age long
) WITH (
	type='kafka',
	geaflow.dsl.kafka.servers = 'localhost:9092',
	geaflow.dsl.kafka.topic = 'scan_002',
	geaflow.dsl.kafka.data.operation.timeout.seconds = 5,
	geaflow.dsl.time.window.size=10,
	geaflow.dsl.start.time='${stTime}'
);

CREATE TABLE tbl_result (
	id bigint,
	name varchar,
	age long
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO tbl_result
SELECT DISTINCT id, name, age
FROM kafka_source
ORDER BY id
LIMIT 5
;
