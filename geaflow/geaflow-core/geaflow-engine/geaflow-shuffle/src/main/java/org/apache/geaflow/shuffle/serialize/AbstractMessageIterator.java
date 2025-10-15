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

package org.apache.geaflow.shuffle.serialize;

import java.io.InputStream;
import org.apache.commons.io.IOUtils;
import org.apache.geaflow.shuffle.pipeline.buffer.OutBuffer;

public abstract class AbstractMessageIterator<T> implements IMessageIterator<T> {

    private long recordNum;
    protected T currentValue;

    protected OutBuffer outBuffer;
    protected InputStream inputStream;

    public AbstractMessageIterator(OutBuffer outBuffer) {
        this.outBuffer = outBuffer;
        this.inputStream = outBuffer.getInputStream();
    }

    public AbstractMessageIterator(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    public OutBuffer getOutBuffer() {
        return this.outBuffer;
    }

    /**
     * Returns the next element in the iteration.
     *
     * @return the next element.
     */
    @Override
    public T next() {
        this.recordNum++;
        T result = this.currentValue;
        this.currentValue = null;
        return result;
    }

    @Override
    public long getSize() {
        return this.recordNum;
    }

    @Override
    public void close() {
        if (inputStream != null) {
            IOUtils.closeQuietly(inputStream);
            inputStream = null;
        }
        if (outBuffer != null) {
            outBuffer.release();
            outBuffer = null;
        }
    }

}
