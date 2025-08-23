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

/*
 * 增量最小生成树算法基础测试
 * 在modern图上执行基础的IncMST算法
 */
CREATE SINK inc_mst_result WITH (
    type='file',
    geaflow.dsl.file.path = '/tmp/geaflow/inc_mst_result_001.txt'
);

USE GRAPH modern;
INSERT INTO inc_mst_result
CALL IncMST() ON GRAPH modern 
RETURN srcId, targetId, weight;

-- 验证结果
SELECT * FROM inc_mst_result;
