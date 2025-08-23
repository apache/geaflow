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

package org.apache.geaflow.example.stream;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.utils.GsonUtil;
import org.apache.geaflow.common.utils.SleepUtils;
import org.apache.geaflow.env.Environment;
import org.apache.geaflow.env.EnvironmentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnBoundedStreamFoTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(UnBoundedStreamFoTest.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {
        Environment environment = EnvironmentFactory.onRayEnvironment(args);
        Configuration configuration = environment.getEnvironmentContext().getConfig();

        StreamWordCountPipeline pipeline = new StreamWordCountPipeline();
        Map<String, String> hbaseConfig = new HashMap<>();
        hbaseConfig.put("hbase.zookeeper.quorum", "hbase-v51-nn-10001.eu95sqa.tbsite"
            + ".net,hbase-v51-nn-10002.eu95sqa.tbsite.net,hbase-v51-nn-10003.eu95sqa.tbsite.net");
        hbaseConfig.put("zookeeper.znode.parent", "/hbase-eu95sqa-perf-test-ssd");
        hbaseConfig.put("hbase.zookeeper.property.clientPort", "2181");
        Map<String, String> config = new HashMap<>();
        config.put("geaflow.store.hbase.config.json", GsonUtil.toJson(hbaseConfig));

        configuration.putAll(config);
        pipeline.submit(environment);
        SleepUtils.sleepSecond(3);
        LOGGER.error("main finished");
    }
}
