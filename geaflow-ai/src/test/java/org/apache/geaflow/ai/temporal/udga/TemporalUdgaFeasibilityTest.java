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

package org.apache.geaflow.ai.temporal.udga;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.geaflow.cluster.system.ClusterMetaStore;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.dsl.connector.file.FileConstants;
import org.apache.geaflow.dsl.runtime.QueryClient;
import org.apache.geaflow.dsl.runtime.QueryContext;
import org.apache.geaflow.dsl.runtime.engine.GQLPipeLine;
import org.apache.geaflow.dsl.runtime.engine.GQLPipeLine.GQLPipelineHook;
import org.apache.geaflow.env.Environment;
import org.apache.geaflow.env.EnvironmentFactory;
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.geaflow.runtime.core.scheduler.resource.ScheduledWorkerManagerFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TemporalUdgaFeasibilityTest {

    private static final String QUERY_RESOURCE =
        "/temporal/temporal_udga_feasibility.sql";

    @TempDir
    Path tempDirectory;

    @Test
    public void testRowUdgaCarriesStateAcrossTwoDynamicEdgeBatches()
        throws Exception {
        Path vertices = writeInput(
            "vertices.csv",
            "1,Alice",
            "2,Bob",
            "3,Carol");
        Path edges = writeInput(
            "edges.csv",
            "1,2",
            "2,3");
        Path output = tempDirectory.resolve("result");
        Environment environment = null;

        try {
            environment = EnvironmentFactory.onLocalEnvironment();
            environment.getEnvironmentContext().withConfig(
                localConfiguration());
            GQLPipeLine pipeline = new GQLPipeLine(environment, 0);
            pipeline.setPipelineHook(new PathReplacingHook(
                vertices,
                edges,
                output));

            pipeline.execute();

            List<ProbeResult> results = readResults(output);
            Set<Integer> observedBatchCounts = new HashSet<>();
            for (ProbeResult result : results) {
                observedBatchCounts.add(result.batchCount);
            }
            Assertions.assertEquals(
                new HashSet<>(Arrays.asList(1, 2)),
                observedBatchCounts);
            Assertions.assertTrue(results.stream().anyMatch(result ->
                result.vertexId == 2L
                    && result.batchCount == 2
                    && result.hadPreviousValue));
            Assertions.assertTrue(results.stream().anyMatch(result ->
                result.dynamicEdgeCount > 0));
            Assertions.assertTrue(results.stream().anyMatch(result ->
                result.receivedMessageCount > 0));
        } finally {
            if (environment != null) {
                environment.shutdown();
            }
            ClusterMetaStore.close();
            ScheduledWorkerManagerFactory.clear();
        }
    }

    private Path writeInput(String fileName, String... lines)
        throws IOException {
        Path path = tempDirectory.resolve(fileName);
        Files.write(path, Arrays.asList(lines), StandardCharsets.UTF_8);
        return path;
    }

    private Map<String, String> localConfiguration() {
        Map<String, String> config = new HashMap<>();
        config.put(
            DSLConfigKeys.GEAFLOW_DSL_QUERY_PATH.getKey(),
            FileConstants.PREFIX_JAVA_RESOURCE + QUERY_RESOURCE);
        config.put(
            ExecutionConfigKeys.JOB_APP_NAME.getKey(),
            "TemporalUdgaFeasibilityTest");
        config.put(
            ExecutionConfigKeys.JOB_WORK_PATH.getKey(),
            tempDirectory.resolve("work").toString());
        config.put(
            FileConfigKeys.ROOT.getKey(),
            tempDirectory.resolve("state").toString());
        return config;
    }

    private static List<ProbeResult> readResults(Path output)
        throws IOException {
        List<ProbeResult> results = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(output)) {
            for (Path path : (Iterable<Path>) paths
                .filter(Files::isRegularFile)::iterator) {
                for (String line : Files.readAllLines(
                    path,
                    StandardCharsets.UTF_8)) {
                    if (!line.trim().isEmpty()) {
                        results.add(ProbeResult.parse(line));
                    }
                }
            }
        }
        Assertions.assertFalse(results.isEmpty());
        return results;
    }

    private static String sqlPath(Path path) {
        return path.toAbsolutePath().toString().replace('\\', '/');
    }

    private static final class PathReplacingHook
        implements GQLPipelineHook, Serializable {

        private final String vertices;
        private final String edges;
        private final String output;

        private PathReplacingHook(
            Path vertices,
            Path edges,
            Path output) {
            this.vertices = sqlPath(vertices);
            this.edges = sqlPath(edges);
            this.output = sqlPath(output);
        }

        @Override
        public String rewriteScript(
            String script,
            Configuration configuration) {
            return script
                .replace("${vertices}", vertices)
                .replace("${edges}", edges)
                .replace("${output}", output);
        }

        @Override
        public void beforeExecute(
            QueryClient queryClient,
            QueryContext queryContext) {
        }

        @Override
        public void afterExecute(
            QueryClient queryClient,
            QueryContext queryContext) {
        }
    }

    private static final class ProbeResult {

        private final long vertexId;
        private final int batchCount;
        private final boolean hadPreviousValue;
        private final int dynamicEdgeCount;
        private final int receivedMessageCount;

        private ProbeResult(
            long vertexId,
            int batchCount,
            boolean hadPreviousValue,
            int dynamicEdgeCount,
            int receivedMessageCount) {
            this.vertexId = vertexId;
            this.batchCount = batchCount;
            this.hadPreviousValue = hadPreviousValue;
            this.dynamicEdgeCount = dynamicEdgeCount;
            this.receivedMessageCount = receivedMessageCount;
        }

        private static ProbeResult parse(String line) {
            String[] fields = line.split(",");
            Assertions.assertEquals(5, fields.length, line);
            return new ProbeResult(
                Long.parseLong(fields[0]),
                Integer.parseInt(fields[1]),
                Boolean.parseBoolean(fields[2]),
                Integer.parseInt(fields[3]),
                Integer.parseInt(fields[4]));
        }
    }
}
