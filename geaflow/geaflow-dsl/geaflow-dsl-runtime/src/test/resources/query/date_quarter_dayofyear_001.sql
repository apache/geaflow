-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- End-to-end SQL test for the QUARTER / DAY_OF_YEAR built-in date UDFs.
--
-- Unlike the unit tests in UDFDateTest which invoke the Java eval()
-- methods directly, this exercises the full SQL pipeline: function
-- registration in BuildInSqlFunctionTable, overload resolution, type
-- inference, and runtime invocation through the query engine.
--
-- Covers quarter boundaries (Q1-Q4), leap-year day_of_year values,
-- and year start/end boundaries.

set geaflow.dsl.column.separator = '|';

CREATE TABLE source (
    id bigint,
    date_str varchar
) WITH (
    type='file',
    geaflow.dsl.file.path = 'resource:///data/date_quarter_dayofyear.txt'
);

CREATE TABLE tbl_result (
    id bigint,
    quarter_v int,
    dayofyear_v int
) WITH (
    type='file',
    -- Use a relative forward-slash path instead of the ${target} placeholder
    -- to avoid Windows backslash-escaping issues (see math_sign_cbrt_trunc).
    geaflow.dsl.file.path='target/date_quarter_dayofyear_001'
);

INSERT INTO tbl_result
SELECT
    id,
    quarter(date_str) AS quarter_v,
    day_of_year(date_str) AS dayofyear_v
FROM source
ORDER BY id;
