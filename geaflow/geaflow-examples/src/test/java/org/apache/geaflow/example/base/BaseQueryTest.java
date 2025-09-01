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

package org.apache.geaflow.example.base;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.cluster.system.ClusterMetaStore;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.connector.file.FileConstants;
import org.apache.geaflow.dsl.runtime.QueryClient;
import org.apache.geaflow.dsl.runtime.QueryContext;
import org.apache.geaflow.dsl.runtime.engine.GQLPipeLine;
import org.apache.geaflow.dsl.runtime.engine.GQLPipeLine.GQLPipelineHook;
import org.apache.geaflow.env.Environment;
import org.apache.geaflow.env.EnvironmentFactory;
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.geaflow.runtime.core.scheduler.resource.ScheduledWorkerManagerFactory;
import org.testng.Assert;

public class BaseQueryTest implements Serializable {

    private int testTimeWaitSeconds = 0;

    public static final String INIT_DDL = "/query/modern_graph.sql";
    public static final String DSL_STATE_REMOTE_PATH = "/tmp/dsl/";

    private String queryPath;

    private boolean compareWithOrder = false;

    private boolean withOutPrefix = false;

    private String graphDefinePath;

    private final Map<String, String> config = new HashMap<>();

    private BaseQueryTest() {
        try {
            initRemotePath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static BaseQueryTest build() {
        return new BaseQueryTest();
    }


    public BaseQueryTest withQueryPath(String queryPath) {
        this.queryPath = queryPath;
        return this;
    }

    public BaseQueryTest withTestTimeWaitSeconds(int testTimeWaitSeconds) {
        this.testTimeWaitSeconds = testTimeWaitSeconds;
        return this;
    }

    public BaseQueryTest compareWithOrder() {
        this.compareWithOrder = true;
        return this;
    }

    public BaseQueryTest withoutPrefix() {
        this.withOutPrefix = true;
        return this;
    }

    public BaseQueryTest withConfig(Map<String, String> config) {
        this.config.putAll(config);
        return this;
    }

    public BaseQueryTest withConfig(String key, Object value) {
        this.config.put(key, String.valueOf(value));
        return this;
    }

    public BaseQueryTest execute() throws Exception {
        if (queryPath == null) {
            throw new IllegalArgumentException("You should call withQueryPath() before execute().");
        }
        Map<String, String> config = new HashMap<>();
        config.put(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE.getKey(), String.valueOf(-1L));
        config.put(FileConfigKeys.ROOT.getKey(), DSL_STATE_REMOTE_PATH);
        config.put(DSLConfigKeys.GEAFLOW_DSL_QUERY_PATH.getKey(),
            withOutPrefix ? queryPath : FileConstants.PREFIX_JAVA_RESOURCE + queryPath);
        config.putAll(this.config);
        initResultDirectory();

        Environment environment = EnvironmentFactory.onLocalEnvironment();
        environment.getEnvironmentContext().withConfig(config);

        GQLPipeLine gqlPipeLine = new GQLPipeLine(environment, testTimeWaitSeconds);

        String graphDefinePath = null;
        if (this.graphDefinePath != null) {
            graphDefinePath = this.graphDefinePath;
        }
        gqlPipeLine.setPipelineHook(new TestGQLPipelineHook(graphDefinePath, queryPath));
        try {
            gqlPipeLine.execute();
        } finally {
            environment.shutdown();
            ClusterMetaStore.close();
            ScheduledWorkerManagerFactory.clear();
        }
        return this;
    }

    private void initResultDirectory() throws Exception {
        // delete target file path
        String targetPath = getTargetPath(queryPath);
        File targetFile = new File(targetPath);
        if (targetFile.exists()) {
            FileUtils.forceDelete(targetFile);
        }
    }

    private void initRemotePath() throws IOException {
        // delete state remote path
        File stateRemoteFile = new File(DSL_STATE_REMOTE_PATH);
        if (stateRemoteFile.exists()) {
            FileUtils.forceDelete(stateRemoteFile);
        }
    }

    public void checkSinkResult(String... path) throws Exception {
        String[] paths = queryPath.split("/");
        String lastPath = paths[paths.length - 1];
        String exceptPath = "/expect/" + lastPath.split("\\.")[0] + ".txt";
        String targetPath;
        if (path != null && path.length > 0) {
            targetPath = path[0];
        } else {
            targetPath = getTargetPath(queryPath);
        }
        String expectResult = IOUtils.resourceToString(exceptPath, Charset.defaultCharset()).trim();
        String actualResult = readFile(targetPath);
        compareResult(actualResult, expectResult);
    }

    private void compareResult(String actualResult, String expectResult) {
        if (compareWithOrder) {
            Assert.assertEquals(actualResult, expectResult);
        } else {
            String[] actualLines = actualResult.split("\n");
            Arrays.sort(actualLines);
            String[] expectLines = expectResult.split("\n");
            Arrays.sort(expectLines);

            String actualSort = StringUtils.join(actualLines, "\n");
            String expectSort = StringUtils.join(expectLines, "\n");
            if (!Objects.equals(actualSort, expectSort)) {
                Assert.assertEquals(actualResult, expectResult);
            }
        }
    }

    private String readFile(String path) throws IOException {
        File file = new File(path);
        if (file.isHidden()) {
            return "";
        }
        if (file.isFile()) {
            return IOUtils.toString(new File(path).toURI(), Charset.defaultCharset()).trim();
        }
        File[] files = file.listFiles();
        StringBuilder content = new StringBuilder();
        if (files != null) {
            for (File subFile : files) {
                String readText = readFile(subFile.getAbsolutePath());
                if (StringUtils.isBlank(readText)) {
                    continue;
                }
                if (content.length() > 0) {
                    content.append("\n");
                }
                content.append(readText);
            }
        }
        return content.toString().trim();
    }

    private static String getTargetPath(String queryPath) {
        assert queryPath != null;
        String[] paths = queryPath.split("/");
        String lastPath = paths[paths.length - 1];
        String targetPath = "target/" + lastPath.split("\\.")[0];
        String currentPath = new File(".").getAbsolutePath();
        targetPath = currentPath.substring(0, currentPath.length() - 1) + targetPath;
        return targetPath;
    }

    public BaseQueryTest withGraphDefine(String graphDefinePath) {
        this.graphDefinePath = Objects.requireNonNull(graphDefinePath);
        return this;
    }

    private static class TestGQLPipelineHook implements GQLPipelineHook {

        private final String graphDefinePath;

        private final String queryPath;

        public TestGQLPipelineHook(String graphDefinePath, String queryPath) {
            this.graphDefinePath = graphDefinePath;
            this.queryPath = queryPath;
        }

        @Override
        public String rewriteScript(String script, Configuration configuration) {
            return script.replace("${target}", getTargetPath(queryPath));
        }

        @Override
        public void beforeExecute(QueryClient queryClient, QueryContext queryContext) {
            if (graphDefinePath != null) {
                try {
                    String ddl = IOUtils.resourceToString(graphDefinePath, Charset.defaultCharset());
                    queryClient.executeQuery(ddl, queryContext);
                } catch (IOException e) {
                    throw new GeaFlowDSLException(e);
                }
            }
        }

        @Override
        public void afterExecute(QueryClient queryClient, QueryContext queryContext) {

        }
    }
}
