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
export function genComponentList(current: number, pageSize: number, componentType: string) {
  const tableListDataSource: API.ComponentInfo[] = [];

  for (let i = 0; i < pageSize; i += 1) {
    const index = (current - 1) * 10 + i;
    tableListDataSource.push({
      id: index,
      name: componentType + "-" + index,
      host: "localhost",
      agentPort: 8089,
      pid: index,
      lastTimestamp: new Date().getTime() - (1000 * index),
      isActive: index % 5 != 0,
      metrics: {
        heapCommittedMB: 100,
        heapUsedMB: index,
        heapUsedRatio: index,
        totalMemoryMB: 1,
        fgcCount: 1,
        fgcTime: 1,
        gcTime: 1,
        gcCount: 1,
        avgLoad: 1,
        availCores: 100,
        processCpu: 100 - index,
        usedCores: 100 - index,
        activeThreads: 1
      }
    });
  }
  tableListDataSource.reverse();
  return tableListDataSource;
}

let tableListDataSource = genComponentList(1, 100, "driver");

function getDrivers(req: Request, res: Response, u: string) {
  const result = {
    data: tableListDataSource,
    success: true
  };

  return res.json(result);
}

export default {
  'GET /rest/drivers': getDrivers,
};
