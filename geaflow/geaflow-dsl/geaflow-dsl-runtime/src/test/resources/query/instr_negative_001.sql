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

CREATE TABLE output (
    last_occurrence bigint,
    second_occurrence bigint,
    bounded_search bigint,
    overlapping_occurrence bigint,
    out_of_range bigint
) WITH (
    type='file',
    geaflow.dsl.file.path='${target}'
);

INSERT INTO output
SELECT
    INSTR('abcabc', 'a', -1, 1),
    INSTR('abcabc', 'a', -1, 2),
    INSTR('abcabc', 'c', -3, 1),
    INSTR('aaaa', 'aa', -1, 2),
    INSTR('abcabc', 'a', -7, 1);
