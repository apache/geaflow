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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.shuffle.pipeline.buffer.PipeFetcherBuffer;

/* This file is based on source code from the Flink Project (http://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Interface to fetch data from multiple {@link OneShardFetcher}.
 * This class is an adaptation of Flink's org.apache.flink.runtime.io.network.partition.consumer.UnionInputGate.
 */
public class MultiShardFetcher implements ShardFetcher, ShardFetcherListener {

    // The input fetchers to union.
    private final ShardFetcher[] shardFetchers;

    private final Set<ShardFetcher> inputFetchersWithRemainingData;

    private final ArrayDeque<ShardFetcher> inputFetchersWithData = new ArrayDeque<>();

    private final Set<ShardFetcher> enqueuedInputFetchersWithData = new HashSet<>();

    // Listeners to forward buffer notifications to.
    private final List<ShardFetcherListener> fetcherListeners = new ArrayList<>();

    // A mapping from input fetcher to (logical) channel index offset.
    // Valid channel indexes go from 0 (inclusive) to the total number of input channels (exclusive).
    private final Map<ShardFetcher, Integer> inputFetcherToIndexOffsetMap;

    private final int totalNumberOfInputChannels;
    private volatile boolean isReleased;

    public MultiShardFetcher(OneShardFetcher... inputFetchers) {
        this.shardFetchers = inputFetchers;
        Preconditions.checkArgument(inputFetchers.length > 1,
            "Union input fetcher should union at least two input fetchers.");

        if (Arrays.stream(inputFetchers).map(OneShardFetcher::getFetcherIndex).distinct().count()
            != inputFetchers.length) {
            throw new IllegalArgumentException(
                "Union of two input fetchers with the same index. Given indices: " + Arrays
                    .stream(inputFetchers).map(OneShardFetcher::getFetcherIndex)
                    .collect(Collectors.toList()));
        }

        this.inputFetcherToIndexOffsetMap = Maps.newHashMapWithExpectedSize(inputFetchers.length);
        this.inputFetchersWithRemainingData = Sets.newHashSetWithExpectedSize(inputFetchers.length);

        int currentNumberOfInputChannels = 0;

        for (OneShardFetcher fetcher : inputFetchers) {
            // The offset to use for buffer or event instances received from this input fetcher.
            inputFetcherToIndexOffsetMap.put(Preconditions.checkNotNull(fetcher), currentNumberOfInputChannels);
            inputFetchersWithRemainingData.add(fetcher);

            currentNumberOfInputChannels += fetcher.getNumberOfInputChannels();

            // Register the union fetcher as a listener for all single input fetchers
            fetcher.registerListener(this);
        }

        this.totalNumberOfInputChannels = currentNumberOfInputChannels;
        this.isReleased = false;
    }

    @Override
    public Optional<PipeFetcherBuffer> getNext() throws IOException, InterruptedException {
        return getNext(true);
    }

    @Override
    public Optional<PipeFetcherBuffer> pollNext() throws IOException, InterruptedException {
        return getNext(false);
    }

    private Optional<PipeFetcherBuffer> getNext(boolean blocking)
        throws IOException, InterruptedException {
        if (inputFetchersWithRemainingData.isEmpty()) {
            return Optional.empty();
        }

        Optional<InputWithData<ShardFetcher, PipeFetcherBuffer>> next = getNextInputData(blocking);
        if (!next.isPresent()) {
            return Optional.empty();
        }

        InputWithData<ShardFetcher, PipeFetcherBuffer> inputWithData = next.get();
        ShardFetcher shardFetcher = inputWithData.input;
        PipeFetcherBuffer resultBuffer = inputWithData.data;

        if (resultBuffer.moreAvailable()) {
            // This buffer or event was now removed from the non-empty fetchers queue
            // we re-add it in case it has more data, because in that case no "non-empty" notification
            // will come for that fetcher.
            queueFetcher(shardFetcher);
        }

        // Set the channel index to identify the input channel (across all unioned input fetchers).
        final int channelIndexOffset = inputFetcherToIndexOffsetMap.get(shardFetcher);

        resultBuffer.setChannelIndex(channelIndexOffset + resultBuffer.getChannelIndex());
        resultBuffer.setMoreAvailable(resultBuffer.moreAvailable() || inputWithData.moreAvailable);

        return Optional.of(inputWithData.data);
    }

    private Optional<InputWithData<ShardFetcher, PipeFetcherBuffer>> getNextInputData(
        boolean blocking) throws IOException, InterruptedException {

        ShardFetcher shardFetcher;
        boolean moreInputFetchersAvailable;

        while (true) {
            Optional<Tuple<ShardFetcher, Boolean>> fetcherOptional = getInputFetcher(blocking);
            if (!fetcherOptional.isPresent()) {
                return Optional.empty();
            }

            shardFetcher = fetcherOptional.get().f0;
            moreInputFetchersAvailable = fetcherOptional.get().f1;

            Optional<PipeFetcherBuffer> result = shardFetcher.pollNext();

            if (result.isPresent()) {
                return Optional.of(new InputWithData<>(shardFetcher, result.get(),
                    moreInputFetchersAvailable));
            }
        }
    }

    @Override
    public void requestSlices(long batchId) throws IOException {
        for (ShardFetcher fetcher : shardFetchers) {
            fetcher.requestSlices(batchId);
        }
    }

    @Override
    public void registerListener(ShardFetcherListener listener) {
        synchronized (fetcherListeners) {
            fetcherListeners.add(listener);
        }
    }

    @Override
    public void notifyAvailable(ShardFetcher shardFetcher) {
        queueFetcher(shardFetcher);
    }

    private void queueFetcher(ShardFetcher shardFetcher) {
        Preconditions.checkNotNull(shardFetcher);

        int availableInputFetchers;

        synchronized (inputFetchersWithData) {
            if (enqueuedInputFetchersWithData.contains(shardFetcher)) {
                return;
            }

            availableInputFetchers = inputFetchersWithData.size();

            inputFetchersWithData.add(shardFetcher);
            enqueuedInputFetchersWithData.add(shardFetcher);

            if (availableInputFetchers == 0) {
                inputFetchersWithData.notifyAll();
            }
        }

        if (availableInputFetchers == 0) {
            synchronized (fetcherListeners) {
                for (ShardFetcherListener listener : fetcherListeners) {
                    listener.notifyAvailable(this);
                }
            }
        }
    }

    private Optional<Tuple<ShardFetcher, Boolean>> getInputFetcher(boolean blocking)
        throws InterruptedException {
        synchronized (inputFetchersWithData) {
            while (inputFetchersWithData.size() == 0) {
                if (blocking) {
                    inputFetchersWithData.wait();
                } else {
                    return Optional.empty();
                }
            }

            ShardFetcher shardFetcher = inputFetchersWithData.remove();
            enqueuedInputFetchersWithData.remove(shardFetcher);
            boolean moreAvailable = !enqueuedInputFetchersWithData.isEmpty();

            return Optional.of(Tuple.of(shardFetcher, moreAvailable));
        }
    }

    /**
     * Returns the total number of input channels across all unioned input fetchers.
     */
    @Override
    public int getNumberOfInputChannels() {
        return totalNumberOfInputChannels;
    }

    @VisibleForTesting
    public ShardFetcher[] getShardFetchers() {
        return shardFetchers;
    }

    @Override
    public boolean isFinished() {
        for (ShardFetcher fetcher : shardFetchers) {
            if (!fetcher.isFinished()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void close() {
        if (!isReleased) {
            for (ShardFetcher fetcher : shardFetchers) {
                fetcher.close();
            }
            synchronized (inputFetchersWithData) {
                inputFetchersWithData.notifyAll();
            }
            isReleased = true;
        }
    }

}
