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

package org.apache.geaflow.dsl.connector.hudi;

import java.util.Objects;
import java.util.function.Supplier;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.hudi.common.engine.EngineProperty;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.util.Option;

/**
 * The {@link TaskContextSupplier} implementation for geaflow.
 */
public class GeaFlowTaskContextSupplier extends TaskContextSupplier {

    private final RuntimeContext context;

    public GeaFlowTaskContextSupplier(RuntimeContext context) {
        this.context = Objects.requireNonNull(context);
    }

    @Override
    public Supplier<Integer> getPartitionIdSupplier() {
        return () -> context.getTaskArgs().getTaskIndex();
    }

    @Override
    public Supplier<Integer> getStageIdSupplier() {
        return () -> context.getTaskArgs().getTaskIndex();
    }

    @Override
    public Supplier<Long> getAttemptIdSupplier() {
        return context::getPipelineId;
    }

    @Override
    public Option<String> getProperty(EngineProperty engineProperty) {
        return Option.empty();
    }
}
