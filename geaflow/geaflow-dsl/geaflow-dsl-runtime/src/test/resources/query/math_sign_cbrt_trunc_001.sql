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

-- End-to-end SQL test for the SIGN / CBRT / TRUNC built-in math UDFs.
--
-- Unlike the unit tests in MathUdfTest which invoke the Java eval()
-- methods directly, this exercises the full SQL pipeline: function
-- registration in BuildInSqlFunctionTable, overload resolution, type
-- inference, and runtime invocation through the query engine.
--
-- Type-inference behaviour captured by the expected output:
--   * sign(double)  -> Double  (sign_d column)
--   * sign(bigint)  -> Double  (sign_l column) - bigint is promoted to
--     double and matches sign(Double); the Long overload is not selected
--   * sign(int)     -> Double  (sign_i column) - same promotion
--   * trunc(bigint) -> Long    (trunc_l column) - Long overload selected
--   * trunc(int)    -> Integer (trunc_i column) - Integer overload selected
-- This difference between sign() and trunc() is the real Calcite overload
-- resolution behaviour, which only an end-to-end SQL test can surface.

set geaflow.dsl.column.separator = '|';

CREATE TABLE source (
    id bigint,
    d_val double,
    l_val bigint,
    i_val int
) WITH (
    type='file',
    geaflow.dsl.file.path = 'resource:///data/math_sign_cbrt_trunc.txt'
);

CREATE TABLE tbl_result (
    id bigint,
    -- SIGN overloads (Double, Long, Integer)
    sign_d double,
    sign_l bigint,
    sign_i int,
    -- CBRT (Double)
    cbrt_v double,
    -- TRUNC overloads: trunc(d, scale), trunc(d), trunc(long), trunc(int)
    trunc_d_scale double,
    trunc_d_only double,
    trunc_l bigint,
    trunc_i int
) WITH (
    type='file',
    -- Use a relative forward-slash path instead of the ${target} placeholder.
    -- QueryTester rewrites ${target} to an absolute path that on Windows
    -- contains backslashes; injected into a SQL string literal those
    -- backslashes get escaped (\t -> tab, \U/\s/\g dropped) and the sink
    -- path is corrupted. A relative path avoids the rewrite entirely and
    -- resolves to the same target/ directory checkSinkResult() reads.
    geaflow.dsl.file.path='target/math_sign_cbrt_trunc_001'
);

INSERT INTO tbl_result
SELECT
    id,
    sign(d_val) AS sign_d,
    sign(l_val) AS sign_l,
    sign(i_val) AS sign_i,
    cbrt(d_val) AS cbrt_v,
    trunc(d_val, 2) AS trunc_d_scale,
    trunc(d_val) AS trunc_d_only,
    trunc(l_val) AS trunc_l,
    trunc(i_val) AS trunc_i
FROM source
ORDER BY id;
