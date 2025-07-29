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
import java.util.List;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.state.query.QueryableAllGraphState;
import org.apache.geaflow.state.query.QueryableKeysGraphState;
import org.apache.geaflow.state.query.QueryableVersionGraphState;
import org.apache.geaflow.utils.keygroup.KeyGroup;

/**
 * The query interface for dynamic graph.
 */
public interface DynamicQueryableState<K, VV, EV, R> {

    /**
     * Returns the version list for some specific vertex id.
     */
    List<Long> getAllVersions(K id);

    /**
     * Returns the latest version for some specific vertex id.
     */
    long getLatestVersion(K id);

    /**
     * Returns the versioned-query interface for some specific id.
     */
    QueryableVersionGraphState<K, VV, EV, R> query(K id);

    /**
     * Returns the versioned-query interface for some specific id and versions.
     */
    QueryableVersionGraphState<K, VV, EV, R> query(K id, Collection<Long> versions);

    /**
     * Returns the full graph query interface for some specific version.
     */
    QueryableAllGraphState<K, VV, EV, R> query(long version);

    /**
     * Returns the full graph query interface for some specific version.
     */
    QueryableAllGraphState<K, VV, EV, R> query(long version, KeyGroup keyGroup);

    /**
     * Returns the point query interface for some specific version and ids.
     */
    QueryableKeysGraphState<K, VV, EV, R> query(long version, K... ids);

    /**
     * Returns the point query interface for some specific version and ids.
     */
    QueryableKeysGraphState<K, VV, EV, R> query(long version, List<K> ids);

    /**
     * Returns the graph id iterator.
     */
    CloseableIterator<K> idIterator();
}
