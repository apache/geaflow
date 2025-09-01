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

package org.apache.geaflow.shuffle.api.reader;

import java.io.Serializable;
import org.apache.geaflow.common.metric.ShuffleReadMetrics;
import org.apache.geaflow.shuffle.message.PipelineEvent;

public interface IShuffleReader extends Serializable {

    /**
     * Init shuffle reader with reader context.
     *
     * @param context reader context.
     */
    void init(IReaderContext context);

    /**
     * Fetch upstream shards.
     *
     * @param targetWindowId target window id
     */
    void fetch(long targetWindowId);

    /**
     * Returns true if the requested batches is not fetched completely.
     *
     * @return true/false.
     */
    boolean hasNext();

    /**
     * Returns the next batch.
     *
     * @return batch data or event.
     */
    PipelineEvent next();

    /**
     * Get read metrics.
     *
     * @return read metrics.
     */
    ShuffleReadMetrics getShuffleReadMetrics();

    /**
     * Close.
     */
    void close();

}
