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

import {Request, Response} from 'express';
import {parse} from 'url';

// mock tableListDataSource
const genMasterInfo = () => {
  const metrics: API.ComponentInfo = {
    id: 0,
    name: "master",
    host: "localhost",
    pid: 123,
    agentPort: 8099
  };
  return metrics;
};

function getMasterInfo(req: Request, res: Response) {

  let result = {
    data: genMasterInfo(),
    success: true
  }
  return res.json(result);
}

export default {
  'GET /rest/master/info': getMasterInfo,
};
