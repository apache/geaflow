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

package org.apache.geaflow.runtime.core.worker;

import java.util.Iterator;
import java.util.ServiceLoader;
import org.apache.geaflow.cluster.worker.IWorker;
import org.apache.geaflow.cluster.worker.WorkerType;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerFactory.class);

    public static IWorker createWorker(Configuration configuration) {
        WorkerType workerType = configuration.getBoolean(FrameworkConfigKeys.ASP_ENABLE)
            ? WorkerType.unaligned_compute : WorkerType.aligned_compute;

        ServiceLoader<IWorker> executorLoader = ServiceLoader.load(IWorker.class);
        Iterator<IWorker> executorIterable = executorLoader.iterator();
        while (executorIterable.hasNext()) {
            IWorker worker = executorIterable.next();
            if (worker.getWorkerType() == workerType) {
                return worker;
            }
        }
        LOGGER.error("NOT found IWorker implementation");
        throw new GeaflowRuntimeException(RuntimeErrors.INST.spiNotFoundError(IWorker.class.getSimpleName()));
    }
}
