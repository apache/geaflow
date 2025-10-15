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

package org.apache.geaflow.shuffle.pipeline.fetcher;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Optional;
import org.apache.geaflow.shuffle.pipeline.buffer.PipeFetcherBuffer;

/**
 * This class is an adaptation of Flink's org.apache.flink.runtime.io.network.partition.consumer.InputGate.
 */
public interface ShardFetcher {

    /**
     * Request the upstream slices and with specific batch id.
     *
     * @throws IOException io exception.
     */
    void requestSlices(long batchId) throws IOException;

    /**
     * Blocking call waiting for next {@link PipeFetcherBuffer}.
     *
     * @return {@code Optional.empty()} if {@link #isFinished()} returns true.
     */
    Optional<PipeFetcherBuffer> getNext() throws IOException, InterruptedException;

    /**
     * Poll the {@link PipeFetcherBuffer}.
     *
     * @return {@code Optional.empty()} if there is no data to return or if {@link #isFinished()}
     *     returns true.
     */
    Optional<PipeFetcherBuffer> pollNext() throws IOException, InterruptedException;

    /**
     * Check if data transfer is finished.
     */
    boolean isFinished();

    /**
     * Get the number of input channel.
     *
     * @return channel number.
     */
    int getNumberOfInputChannels();

    /**
     * Register fetcher listeners. Notify when fetcher has data.
     */
    void registerListener(ShardFetcherListener listener);

    /**
     * Close.
     */
    void close();

    class InputWithData<INPUT, DATA> {

        protected final INPUT input;
        protected final DATA data;
        protected final boolean moreAvailable;

        InputWithData(INPUT input, DATA data, boolean moreAvailable) {
            this.input = Preconditions.checkNotNull(input);
            this.data = Preconditions.checkNotNull(data);
            this.moreAvailable = moreAvailable;
        }
    }

}
