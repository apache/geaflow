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

package org.apache.geaflow.console.biz.shared.demo;

import java.util.HashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.geaflow.console.common.util.VelocityUtil;
import org.apache.geaflow.console.core.model.job.GeaflowJob;
import org.apache.geaflow.console.core.model.job.GeaflowProcessJob;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class SocketDemo extends DemoJob {

    private static final String DEMO_JOB_TEMPLATE = "template/demoJob.vm";

    public GeaflowJob build() {
        GeaflowProcessJob job = new GeaflowProcessJob();
        String code = VelocityUtil.applyResource(DEMO_JOB_TEMPLATE, new HashMap<>());
        job.setUserCode(code);
        job.setName("demoJob");
        return job;
    }
}
