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

package org.apache.geaflow.api.pdata.stream.window;

import java.util.Map;
import org.apache.geaflow.api.pdata.PStreamSource;
import org.apache.geaflow.common.encoder.IEncoder;

public interface PWindowSource<T> extends PWindowStream<T>, PStreamSource<T> {

    @Override
    PWindowSource<T> withConfig(Map map);

    @Override
    PWindowSource<T> withConfig(String key, String value);

    @Override
    PWindowSource<T> withName(String name);

    @Override
    PWindowSource<T> withParallelism(int parallelism);

    @Override
    PWindowSource<T> withEncoder(IEncoder<T> encoder);

}
