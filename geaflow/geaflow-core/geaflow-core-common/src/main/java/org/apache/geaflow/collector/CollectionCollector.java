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

package org.apache.geaflow.collector;

import java.util.List;
import org.apache.geaflow.shuffle.desc.OutputType;

public class CollectionCollector<T> extends AbstractCollector implements ICollector<T> {

    private List<ICollector<T>> collectors;

    public CollectionCollector(int id, List<ICollector<T>> collectors) {
        super(id);
        this.collectors = collectors;
    }


    @Override
    public void partition(T value) {
        for (ICollector<T> collector : collectors) {
            collector.partition(value);
        }
    }

    @Override
    public String getTag() {
        return "";
    }

    @Override
    public OutputType getType() {
        return OutputType.FORWARD;
    }

    @Override
    public void broadcast(T value) {

    }

    @Override
    public <KEY> void partition(KEY key, T value) {
        for (ICollector<T> collector : collectors) {
            collector.partition(key, value);
        }
    }
}
