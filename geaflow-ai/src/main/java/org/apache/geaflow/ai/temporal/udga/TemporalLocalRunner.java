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

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.geaflow.ai.temporal.codec.TemporalJsonCodec;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.cluster.system.ClusterMetaStore;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.dsl.connector.file.FileConstants;
import org.apache.geaflow.dsl.runtime.QueryClient;
import org.apache.geaflow.dsl.runtime.QueryContext;
import org.apache.geaflow.dsl.runtime.engine.GQLPipeLine;
import org.apache.geaflow.env.Environment;
import org.apache.geaflow.env.EnvironmentFactory;
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.geaflow.runtime.core.scheduler.resource.ScheduledWorkerManagerFactory;

/** Isolates each attempted transaction in its own Local Mode job and checkpoint directory. */
final class TemporalLocalRunner {

    private TemporalLocalRunner() {
    }

    static synchronized List<String> run(Path directory, Map<FactKey, FactKeyState> states,
                                         List<NormalizedMemoryEvent> events, int windowSize)
        throws Exception {
        Files.createDirectories(directory);
        List<String> facts = new ArrayList<>();
        for (FactKeyState state : states.values()) {
            facts.add(TemporalGraphSchema.factId(state.getKey()) + ","
                + TemporalGraphSchema.encode(new TemporalJsonCodec().factKeyToJson(
                    state.getKey()).toString()) + "," + state.encode());
        }
        List<String> vertices = new ArrayList<>();
        List<String> edges = new ArrayList<>();
        for (NormalizedMemoryEvent event : events) {
            String id = TemporalGraphSchema.eventId(event.getEventId());
            vertices.add(id + "," + TemporalGraphSchema.encode(
                new TemporalJsonCodec().eventToJson(event).toString()) + ",-");
            edges.add(id + "," + TemporalGraphSchema.factId(event.getFactKey()));
        }
        Files.write(directory.resolve("facts.csv"), facts, StandardCharsets.UTF_8);
        Files.write(directory.resolve("events.csv"), vertices, StandardCharsets.UTF_8);
        Files.write(directory.resolve("edges.csv"), edges, StandardCharsets.UTF_8);
        Environment environment = null;
        try {
            environment = EnvironmentFactory.onLocalEnvironment();
            Map<String, String> config = new HashMap<>();
            config.put(DSLConfigKeys.GEAFLOW_DSL_QUERY_PATH.getKey(),
                FileConstants.PREFIX_JAVA_RESOURCE + "/temporal/temporal_business.sql");
            config.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(), "TemporalBusinessUdga");
            config.put(ExecutionConfigKeys.JOB_WORK_PATH.getKey(), directory.resolve("work").toString());
            config.put(FileConfigKeys.ROOT.getKey(), directory.resolve("state").toString());
            environment.getEnvironmentContext().withConfig(config);
            GQLPipeLine pipeline = new GQLPipeLine(environment, 0);
            pipeline.setPipelineHook(new Inputs(directory, windowSize));
            pipeline.execute();
            List<String> rows = new ArrayList<>();
            try (Stream<Path> paths = Files.walk(directory.resolve("results"))) {
                for (Path path : (Iterable<Path>) paths.filter(Files::isRegularFile)::iterator) {
                    if (!path.getFileName().toString().startsWith(".")) {
                        rows.addAll(Files.readAllLines(path, StandardCharsets.UTF_8));
                    }
                }
            }
            return rows;
        } finally {
            try {
                if (environment != null) {
                    environment.shutdown();
                }
            } finally {
                ClusterMetaStore.close();
                ScheduledWorkerManagerFactory.clear();
            }
        }
    }

    private static final class Inputs implements GQLPipeLine.GQLPipelineHook, Serializable {

        private static final long serialVersionUID = 1L;
        private final String directory;
        private final int windowSize;

        private Inputs(Path directory, int windowSize) {
            this.directory = directory.toAbsolutePath().toString().replace('\\', '/');
            this.windowSize = windowSize;
        }

        @Override
        public String rewriteScript(String script, Configuration configuration) {
            return script.replace("${root}", directory)
                .replace("${windowSize}", Integer.toString(windowSize));
        }

        @Override
        public void beforeExecute(QueryClient client, QueryContext context) {
        }

        @Override
        public void afterExecute(QueryClient client, QueryContext context) {
        }
    }
}
