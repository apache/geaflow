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

import static org.apache.geaflow.analytics.service.client.utils.JDBCUtils.DRIVER_URL_START;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.CONTAINER_WORKER_NUM;
import static org.apache.geaflow.file.FileConfigKeys.ROOT;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.geaflow.analytics.service.client.AnalyticsClient;
import org.apache.geaflow.analytics.service.client.jdbc.AnalyticsDriver;
import org.apache.geaflow.analytics.service.client.jdbc.AnalyticsResultSet;
import org.apache.geaflow.analytics.service.config.AnalyticsServiceConfigKeys;
import org.apache.geaflow.analytics.service.query.QueryResults;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.utils.SleepUtils;
import org.apache.geaflow.env.EnvironmentFactory;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.testng.Assert;
import org.testng.annotations.Test;

public class QueryServiceTest extends BaseServiceTest {

    @Test
    public void testQueryService() {
        int port = 8093;
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_SERVICE_PORT, String.valueOf(port));
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_SERVICE_REGISTER_ENABLE, Boolean.FALSE.toString());
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_QUERY, analyticsQuery);
        configuration.put(ROOT, TEST_GRAPH_PATH);
        configuration.put(CONTAINER_WORKER_NUM, String.valueOf(4));
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_QUERY_PARALLELISM,
            String.valueOf(4));
        // Collection source must be set all window size in order to only execute one batch.
        configuration.put(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE, "-1");

        QueryService queryService = new QueryService();
        queryService.submit(environment);
        SleepUtils.sleepSecond(DEFAULT_WAITING_TIME);

        AnalyticsClient analyticsClient = AnalyticsClient.builder().withHost(HOST_NAME)
            .withPort(port).withRetryNum(3).build();

        QueryResults queryResults = analyticsClient.executeQuery(executeQuery);
        Assert.assertNotNull(queryResults);
        Object defaultFormattedResult = queryResults.getFormattedData();
        List<List<Object>> rawData = queryResults.getRawData();
        Assert.assertEquals(1, rawData.size());
        Assert.assertEquals(3, rawData.get(0).size());
        Assert.assertEquals(rawData.get(0).get(0), 1100001L);
        Assert.assertEquals(rawData.get(0).get(1).toString(), "一");
        Assert.assertEquals(rawData.get(0).get(2).toString(), "王");
        Assert.assertEquals(defaultFormattedResult.toString(),
            "{\"viewResult\":{\"nodes\":[],\"edges\":[]},\"jsonResult\":[{\"firstName\":\"一\","
                + "\"lastName\":\"王\",\"id\":\"1100001\"}]}");
        analyticsClient.shutdown();
    }

    @Test
    public void testJDBCResultSet() {
        int port = 8094;
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_SERVICE_PORT, String.valueOf(port));
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_SERVICE_REGISTER_ENABLE,
            Boolean.FALSE.toString());
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_QUERY, analyticsQuery);
        configuration.put(ROOT, TEST_GRAPH_PATH);
        configuration.put(CONTAINER_WORKER_NUM, String.valueOf(4));
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_QUERY_PARALLELISM,
            String.valueOf(4));
        // Collection source must be set all window size in order to only execute one batch.
        configuration.put(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE, "-1");

        QueryService queryService = new QueryService();
        queryService.submit(environment);
        SleepUtils.sleepSecond(DEFAULT_WAITING_TIME);
        String url = DRIVER_URL_START + HOST_NAME + ":" + port;
        Properties properties = new Properties();
        properties.put("user", "analytics_test");
        String testQuery = graphView + "MATCH (person:Person where id = 1100001)-[:isLocatedIn]->(city:City)\n"
            + "RETURN person, person.id, person.firstName, person.lastName";
        try {
            Class.forName(AnalyticsDriver.class.getCanonicalName());
            Connection connection = DriverManager.getConnection(url, properties);
            Statement statement = connection.createStatement();
            AnalyticsResultSet resultSet = (AnalyticsResultSet) statement.executeQuery(testQuery);
            Assert.assertNotNull(resultSet);
            Assert.assertTrue(resultSet.next());
            long personId = resultSet.getLong(2);
            Assert.assertEquals(1100001L, personId);
            Assert.assertEquals(resultSet.getLong("id"), personId);
            String personFirstName = resultSet.getString(3);
            Assert.assertEquals("一", personFirstName);
            Assert.assertEquals(resultSet.getString("firstName"), personFirstName);
            IVertex<Object, Map<Object, Object>> personVertexByLabel = resultSet.getVertex("person");
            IVertex<Object, Map<Object, Object>> personVertexByIndex = resultSet.getVertex(1);
            Assert.assertNotNull(personVertexByLabel);
            Assert.assertEquals(personVertexByLabel, personVertexByIndex);
            Assert.assertFalse(resultSet.next());
        } catch (Exception e) {
            throw new GeaflowRuntimeException(e);
        }
    }

}
