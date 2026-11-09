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

CREATE TABLE output_console (
    c1 varchar,
    c2 varchar,
    c3 varchar,
    c4 varchar,
    c5 varchar,
    c6 varchar,
    c7 varchar,
    c8 varchar
) WITH (
    type='file',
    geaflow.dsl.file.path='${target}'
);

INSERT INTO output_console
SELECT
    lpad('hi', 5, 'xy'),
    lpad('hello', 3, 'x'),
    lpad('hi', 0, 'x'),
    lpad('hi', 5, ''),
    lpad('股票', 4, '星'),
    lpad(cast(null as varchar), 5, 'x'),
    lpad('😀', 2, 'x'),
    lpad('hi', -1, 'x')
