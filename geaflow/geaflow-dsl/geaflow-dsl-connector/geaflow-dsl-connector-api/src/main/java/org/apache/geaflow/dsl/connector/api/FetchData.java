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

package org.apache.geaflow.dsl.connector.api;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * The data fetched from the partition of {@link TableSource}.
 */
public class FetchData<T> implements Serializable {

    private final Iterator<T> dataIterator;

    private final int size;

    private final Offset nextOffset;

    private final boolean isFinish;

    private FetchData(Iterator<T> dataIterator, int size, Offset nextOffset, boolean isFinish) {
        this.dataIterator = Objects.requireNonNull(dataIterator);
        this.size = size;
        this.nextOffset = Objects.requireNonNull(nextOffset);
        this.isFinish = isFinish;
    }

    public static <T> FetchData<T> createStreamFetch(List<T> dataList, Offset nextOffset, boolean isFinish) {
        return new FetchData<>(dataList.listIterator(), dataList.size(), nextOffset, isFinish);
    }

    public static <T> FetchData<T> createBatchFetch(Iterator<T> dataIterator, Offset nextOffset) {
        return new FetchData<>(dataIterator, -1, nextOffset, true);
    }

    /**
     * Returns data list.
     */
    public Iterator<T> getDataIterator() {
        return dataIterator;
    }

    /**
     * Returns data size.
     */
    public int getDataSize() {
        return size;
    }

    /**
     * Returns the offset for next window.
     */
    public Offset getNextOffset() {
        return nextOffset;
    }

    public void seek(long seekPos) {
        long toSkip = seekPos;
        while (toSkip > 0) {
            if (!dataIterator.hasNext()) {
                throw new RuntimeException("skip to many rows " + seekPos);
            }
            dataIterator.next();
            toSkip --;
        }
    }

    /**
     * Returns true if the fetch has finished for the partition.
     */
    public boolean isFinish() {
        return isFinish;
    }
}
