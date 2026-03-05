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

package org.apache.geaflow.shuffle.network.protocol;

import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;

/* This file is based on source code from the Spark Project (http://spark.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This class is an adaptation of Spark's org.apache.spark.network.util.AbstractFileRegion.
 */
public abstract class AbstractFileRegion extends AbstractReferenceCounted implements FileRegion {

    protected int chunkSize = 64 * 1024 * 1024;

    protected long transferred;
    protected long contentSize;

    public AbstractFileRegion(long contentSize) {
        this.transferred = 0;
        this.contentSize = contentSize;
    }

    @Override
    public final long transfered() {
        return transferred();
    }

    @Override
    public long position() {
        return 0;
    }

    @Override
    public long transferred() {
        return transferred;
    }

    @Override
    public long count() {
        return contentSize;
    }

    @Override
    public AbstractFileRegion retain() {
        super.retain();
        return this;
    }

    @Override
    public AbstractFileRegion retain(int increment) {
        super.retain(increment);
        return this;
    }

    @Override
    public AbstractFileRegion touch() {
        super.touch();
        return this;
    }

    @Override
    public AbstractFileRegion touch(Object o) {
        return this;
    }

}
