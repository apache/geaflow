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

package org.apache.geaflow.pdata.stream.window;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.api.pdata.stream.PUnionStream;
import org.apache.geaflow.common.encoder.IEncoder;
import org.apache.geaflow.operator.impl.window.UnionOperator;
import org.apache.geaflow.pdata.stream.TransformType;

public class WindowUnionStream<T> extends WindowDataStream<T> implements PUnionStream<T> {

    private List<WindowDataStream<T>> unionWindowDataStreamList;

    public WindowUnionStream(WindowDataStream<T> stream, WindowDataStream<T> unionStream,
                             UnionOperator unionOperator) {
        super(stream, unionOperator);
        this.unionWindowDataStreamList = new ArrayList<>();
        this.addUnionDataStream(unionStream);
    }

    public void addUnionDataStream(WindowDataStream<T> unionStream) {
        this.unionWindowDataStreamList.add(unionStream);
    }

    public List<WindowDataStream<T>> getUnionWindowDataStreamList() {
        return unionWindowDataStreamList;
    }

    @Override
    public WindowUnionStream<T> withConfig(Map map) {
        setConfig(map);
        return this;
    }

    @Override
    public WindowUnionStream<T> withConfig(String key, String value) {
        setConfig(key, value);
        return this;
    }

    @Override
    public WindowUnionStream<T> withName(String name) {
        this.opArgs.setOpName(name);
        return this;
    }

    @Override
    public WindowUnionStream<T> withParallelism(int parallelism) {
        setParallelism(parallelism);
        return this;
    }

    @Override
    public TransformType getTransformType() {
        return TransformType.StreamUnion;
    }

    @Override
    public WindowUnionStream<T> withEncoder(IEncoder<T> encoder) {
        this.encoder = encoder;
        return this;
    }

}
