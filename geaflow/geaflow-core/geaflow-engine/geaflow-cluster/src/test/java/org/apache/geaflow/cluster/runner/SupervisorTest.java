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

package org.apache.geaflow.cluster.runner;

import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.utils.SleepUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SupervisorTest {
    @Test
    public void test() {
        Configuration configuration = new Configuration();
        Map<String, String> envMap = new HashMap<>();
        String cmd = "sleep 30000";
        Supervisor supervisor = new Supervisor(cmd, configuration, false);
        supervisor.startWorker();
        // wait for process starts.
        while (!supervisor.isWorkerAlive()) {
            SleepUtils.sleepMilliSecond(500);
        }
        supervisor.stopWorker();
        // wait for process exits.
        SleepUtils.sleepSecond(1);
        Assert.assertFalse(supervisor.isWorkerAlive());
        supervisor.stop();
    }
}
