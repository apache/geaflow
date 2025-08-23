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

package org.apache.geaflow.store;

/**
 * IStateful store is stateful, which means it ensure data HA and can be recovered.
 */
public interface IStatefulStore extends IBaseStore {

    /**
     * make a snapshot and ensure data HA.
     */
    void archive(long checkpointId);

    /**
     * recover the store data.
     */
    void recovery(long checkpointId);

    /**
     * recover the latest store data.
     */
    long recoveryLatest();

    /**
     * trigger manual store data compaction.
     */
    void compact();

    /**
     * delete the store data.
     */
    void drop();
}
