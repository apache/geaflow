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

package org.apache.geaflow.cluster.fetcher;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import org.apache.geaflow.cluster.task.service.AbstractTaskService;
import org.apache.geaflow.common.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FetcherService extends AbstractTaskService<IFetchRequest, FetcherRunner> implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(FetcherService.class);

    private static final String FETCHER_FORMAT = "geaflow-fetcher-%d";

    private int slots;

    public FetcherService(int slots, Configuration configuration) {
        super(configuration, FETCHER_FORMAT);
        this.slots = slots;
    }

    @Override
    protected FetcherRunner[] buildTaskRunner() {
        Preconditions.checkArgument(slots > 0, "fetcher pool should be larger than 0");
        FetcherRunner[] fetcherRunners = new FetcherRunner[slots];
        for (int i = 0; i < slots; i++) {
            FetcherRunner runner = new FetcherRunner(configuration);
            fetcherRunners[i] = runner;
        }
        return fetcherRunners;
    }
}
