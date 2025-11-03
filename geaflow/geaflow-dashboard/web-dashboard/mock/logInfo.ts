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

// mock tableInfoDataSource
const genLogInfo = (req: Request) => {
  let params = req.query;
  let current = Number(params.pageNo);
  let size = Number(params.pageSize);
  let total = 102400;

  let logInfo: string = "";
  for (let i = (current - 1) * size; i < Math.min(total, (current + 1) * size); i++) {
    logInfo += "log " + i + "\t";
    if (i % 10 == 0) {
      logInfo += "\n"
    }
  }
  console.log(params);
  return {
    total: total,
    data: logInfo
  }
}

function getLogInfo(req: Request, res: Response) {

  let result = {
    data: genLogInfo(req),
    success: true
  }
  return res.json(result);
}

export default {
  'GET /proxy/:agentUrl/rest/logs/content': getLogInfo,
};
