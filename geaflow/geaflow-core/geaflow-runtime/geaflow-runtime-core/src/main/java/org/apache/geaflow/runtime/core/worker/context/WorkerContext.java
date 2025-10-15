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

package org.apache.geaflow.runtime.core.worker.context;

import org.apache.geaflow.cluster.task.ITaskContext;
import org.apache.geaflow.collector.ICollector;

public class WorkerContext extends AbstractWorkerContext {

    public WorkerContext(ITaskContext taskContext) {
        super(taskContext);
    }

    /**
     * Release worker resource.
     */
    @Override
    public void close() {
        for (ICollector<?> collector : collectors) {
            collector.close();
        }
        processor.close();
    }
}
