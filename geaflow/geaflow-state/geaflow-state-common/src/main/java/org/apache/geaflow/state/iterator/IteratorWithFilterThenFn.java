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

package org.apache.geaflow.state.iterator;

import com.google.common.base.Preconditions;
import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.geaflow.common.iterator.CloseableIterator;

/**
 * This class accepts a iterator and makes its value filtered and transformed.
 */
public class IteratorWithFilterThenFn<T, R> extends BaseCloseableIterator<T, R> {

    private final Predicate<T> predicate;
    private final Function<T, R> function;
    private T nextValue;

    public IteratorWithFilterThenFn(CloseableIterator<T> iterator, Predicate<T> predicate, Function<T, R> function) {
        super(iterator);
        this.predicate = Preconditions.checkNotNull(predicate);
        this.function = function;
    }

    public IteratorWithFilterThenFn(Iterator<T> iterator, Predicate<T> predicate, Function<T, R> function) {
        super(iterator);
        this.predicate = Preconditions.checkNotNull(predicate);
        this.function = function;
    }

    @Override
    public boolean hasNext() {
        while (this.iterator.hasNext()) {
            nextValue = this.iterator.next();
            if (!predicate.test(nextValue)) {
                continue;
            }
            return true;
        }
        return false;
    }

    @Override
    public R next() {
        return function.apply(nextValue);
    }
}
