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

package org.apache.geaflow.example.service;

import static org.apache.geaflow.file.FileConfigKeys.ROOT;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import java.util.List;
import org.apache.geaflow.analytics.service.config.AnalyticsServiceConfigKeys;
import org.apache.geaflow.analytics.service.query.QueryResults;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.common.encoder.RpcMessageEncoder;
import org.apache.geaflow.common.rpc.HostAndPort;
import org.apache.geaflow.common.serialize.SerializerFactory;
import org.apache.geaflow.common.utils.SleepUtils;
import org.apache.geaflow.env.EnvironmentFactory;
import org.apache.geaflow.metaserver.client.MetaServerQueryClient;
import org.apache.geaflow.metaserver.service.NamespaceType;
import org.apache.geaflow.rpc.proto.Analytics.QueryRequest;
import org.apache.geaflow.rpc.proto.Analytics.QueryResult;
import org.apache.geaflow.rpc.proto.AnalyticsServiceGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MultiQueryServiceTest extends BaseServiceTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiQueryServiceTest.class);

    @Test
    public void testQueryService() throws Exception {
        before();
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();
        configuration.put(ExecutionConfigKeys.DRIVER_NUM, "2");
        configuration.put(FrameworkConfigKeys.SERVICE_SHARE_ENABLE, Boolean.TRUE.toString());
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_QUERY, analyticsQuery);
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_QUERY_PARALLELISM, "2");
        configuration.put(ROOT, TEST_GRAPH_PATH);
        // Collection source must be set all window size in order to only execute one batch.
        configuration.put(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE, "-1");
        configuration.putAll(defaultConfig.getConfigMap());

        QueryService queryService = new QueryService();
        queryService.submit(environment);
        testQuery();
    }

    private void testQuery() {
        SleepUtils.sleepSecond(DEFAULT_WAITING_TIME);

        MetaServerQueryClient queryClient = new MetaServerQueryClient(defaultConfig);
        List<HostAndPort> serviceInfos = queryClient.queryAllServices(NamespaceType.DEFAULT);
        for (HostAndPort hostAndPort : serviceInfos) {
            LOGGER.info("host and port {}", hostAndPort);
            ManagedChannel channel = buildChannel(hostAndPort.getHost(), hostAndPort.getPort(), 3000);
            AnalyticsServiceGrpc.AnalyticsServiceBlockingStub stub = AnalyticsServiceGrpc.newBlockingStub(channel);

            ByteString.Output output = ByteString.newOutput();
            SerializerFactory.getKryoSerializer().serialize("request", output);
            QueryRequest request = QueryRequest.newBuilder()
                .setQuery(executeQuery)
                .setQueryConfig(output.toByteString())
                .build();
            QueryResult queryResult = stub.executeQuery(request);
            Assert.assertNotNull(queryResult);
            QueryResults result = RpcMessageEncoder.decode(queryResult.getQueryResult());
            Object defaultFormattedResult = result.getFormattedData();
            List<List<Object>> rawData = result.getRawData();
            Assert.assertEquals(1, rawData.size());
            Assert.assertEquals(3, rawData.get(0).size());
            Assert.assertEquals(rawData.get(0).get(0), 1100001L);
            Assert.assertEquals(rawData.get(0).get(1).toString(), "一");
            Assert.assertEquals(rawData.get(0).get(2).toString(), "王");
            Assert.assertEquals(defaultFormattedResult.toString(), "{\"viewResult\":{\"nodes\":[],\"edges\":[]},\"jsonResult\":[{\"firstName\":\"一\",\"lastName\":\"王\",\"id\":\"1100001\"}]}");
        }
        queryClient.close();
    }

}
