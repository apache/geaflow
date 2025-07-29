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

package org.apache.geaflow.state;

import java.util.Collection;

/**
 * The interface describe the state supporting multi versioned graph modification.
 */
public interface MultiVersionedRevisableState<K, R> {

    /**
     * add state method with version and data.
     */
    void add(long version, R r);

    /**
     * update state method with version and data.
     */
    void update(long version, R r);

    /**
     * delete state method with version and data.
     */
    void delete(long version, R r);

    /**
     * delete state method with version and multi keys.
     */
    void delete(long version, K... ids);

    /**
     * delete state method with version and multi keys.
     */
    void delete(long version, Collection<K> ids);

}
