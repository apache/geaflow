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
const genFlameGraphList = () => {
  const logList: API.FileInfo[] = [
    {
      path: "/tmp/flame-graph/flame-graph-1.html",
      createdTime: new Date().getTime(),
      size: 89128
    },
    {
      path: "/tmp/flame-graph/flame-graph-2.html",
      createdTime: new Date().getTime() - 1000,
      size: 214912122
    }
  ];
  return logList;
};

function getFlameGraphList(req: Request, res: Response) {

  let result = {
    data: genFlameGraphList(),
    success: true
  }
  return res.json(result);
}

export default {
  'GET /proxy/:agentUrl/rest/flame-graphs': getFlameGraphList,
};
