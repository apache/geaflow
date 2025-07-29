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

package org.apache.geaflow.analytics.service.client;

import java.io.Closeable;
import org.apache.geaflow.analytics.service.query.QueryResults;
import org.apache.geaflow.pipeline.service.ServiceType;

public interface IQueryRunner extends Closeable {

    /**
     * Init query runner.
     */
    void init(QueryRunnerContext handlerContext);

    /**
     * Execute query.
     */
    QueryResults executeQuery(String queryScript);

    /**
     * Get service type.
     */
    ServiceType getServiceType();

    /**
     * Cancel query.
     */
    QueryResults cancelQuery(long queryId);

    /**
     * Query runner is running.
     */
    boolean isRunning();

    /**
     * Query runner is aborted, when request fail.
     */
    boolean isAborted();

    /**
     * Query runner is error.
     */
    boolean isError();

    /**
     * Query runner is finished.
     */
    boolean isFinished();
}
