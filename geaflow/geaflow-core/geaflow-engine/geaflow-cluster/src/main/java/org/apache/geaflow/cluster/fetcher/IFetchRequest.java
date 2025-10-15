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

import java.io.Serializable;

public interface IFetchRequest extends Serializable {

    /**
     * Get the task id of this request.
     *
     * @return task id
     */
    int getTaskId();

    /**
     * Get the request type.
     *
     * @return request type
     */
    RequestType getRequestType();

    enum RequestType {

        /**
         * Init fetch request, setup fetch context and input slice meta.
         */
        INIT,
        /**
         * Fetch data of a window id.
         */
        FETCH,
        /**
         * Close the fetch task when data finish.
         */
        CLOSE

    }

}
