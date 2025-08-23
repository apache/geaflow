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

package org.apache.geaflow.common.metric;

import java.io.Serializable;

public class ShuffleReadMetrics implements Serializable {

    private int fetchSlices;
    /**
     * total records of fetch response.
     */
    private long fetchRecords;
    private long decodeBytes;
    /**
     * time to write response.
     */
    private long fetchWaitMs;
    /**
     * time to decode response.
     */
    private long decodeMs;
    /**
     * cost from request sent to response back.
     */
    private long requestMs;

    public void merge(ShuffleReadMetrics readMetrics) {
        if (readMetrics != null) {
            fetchSlices += readMetrics.getFetchSlices();
            fetchRecords += readMetrics.getFetchRecords();
            decodeBytes += readMetrics.getDecodeBytes();
            fetchWaitMs += readMetrics.getFetchWaitMs();
            decodeMs += readMetrics.getDecodeMs();
        }
    }

    public int getFetchSlices() {
        return fetchSlices;
    }

    public void setFetchSlices(int fetchSlices) {
        this.fetchSlices = fetchSlices;
    }

    public long getFetchRecords() {
        return fetchRecords;
    }

    public void setFetchRecords(long fetchRecords) {
        this.fetchRecords = fetchRecords;
    }

    public long getDecodeBytes() {
        return decodeBytes;
    }

    public void setDecodeBytes(long decodeBytes) {
        this.decodeBytes = decodeBytes;
    }

    public void increaseDecodeBytes(long decodeBytes) {
        this.decodeBytes += decodeBytes;
    }


    public long getFetchWaitMs() {
        return fetchWaitMs;
    }

    public void setFetchWaitMs(long fetchWaitMs) {
        this.fetchWaitMs = fetchWaitMs;
    }

    public long getDecodeMs() {
        return decodeMs;
    }

    public void setDecodeMs(long decodeMs) {
        this.decodeMs = decodeMs;
    }

    public long getRequestMs() {
        return requestMs;
    }

    public void setRequestMs(long requestMs) {
        this.requestMs = requestMs;
    }

    public void updateDecodeBytes(long decodeBytes) {
        this.decodeBytes += decodeBytes;
    }

    public void updateFetchRecords(long records) {
        this.fetchRecords += records;
    }

    public void increaseDecodeMs(long decodeMs) {
        this.decodeMs += decodeMs;
    }

    public void incFetchWaitMs(long fetchWaitMs) {
        this.fetchWaitMs += fetchWaitMs;
    }

    @Override
    public String toString() {
        return "ReadMetrics{" + "fetchSlices=" + fetchSlices + ", fetchRecords=" + fetchRecords + ", decodeKB=" + decodeBytes / 1024 + ", fetchWaitMs=" + fetchWaitMs
            + ", decodeMs=" + decodeMs + '}';
    }
}
