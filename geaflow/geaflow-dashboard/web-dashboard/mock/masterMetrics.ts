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
const genMaterMetrics = () => {
  const metrics: API.ProcessMetrics = {
    heapCommittedMB: 1024,
    heapUsedMB: 1024,
    heapUsedRatio: 100,
    totalMemoryMB: 1024,
    fgcCount: 2,
    fgcTime: 3000,
    gcTime: 5000,
    gcCount: 5,
    avgLoad: 100,
    availCores: 12,
    processCpu: 20,
    usedCores: 10,
    activeThreads: 100,
  };
  return metrics;
};

function getProcessMetrics(req: Request, res: Response) {

  let result = {
    data: genMaterMetrics(),
    success: true
  }
  return res.json(result);
}

export default {
  'GET /rest/master/metrics': getProcessMetrics,
};
