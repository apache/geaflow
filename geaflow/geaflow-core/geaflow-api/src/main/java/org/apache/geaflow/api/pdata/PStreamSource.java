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

package org.apache.geaflow.api.pdata;

import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.geaflow.api.function.io.SourceFunction;
import org.apache.geaflow.api.pdata.base.PSource;
import org.apache.geaflow.api.pdata.stream.PStream;
import org.apache.geaflow.api.pdata.stream.window.PWindowSource;
import org.apache.geaflow.api.window.IWindow;
import org.apache.geaflow.common.encoder.IEncoder;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.pipeline.context.IPipelineContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface PStreamSource<T> extends PStream<T>, PSource {

    Logger LOGGER = LoggerFactory.getLogger(PStreamSource.class);

    static <T> PStreamSource<T> from(IPipelineContext pipelineContext,
                                     SourceFunction<T> sourceFunction,
                                     IWindow<T> window) {
        LOGGER.info("load PStreamSource SPI Implementation");
        ServiceLoader<PStreamSource> serviceLoader = ServiceLoader.load(PStreamSource.class);
        Iterator<PStreamSource> iterator = serviceLoader.iterator();
        boolean hasImpl = iterator.hasNext();
        if (hasImpl) {
            PStreamSource streamSource = iterator.next();
            return streamSource.build(pipelineContext, sourceFunction, window);
        } else {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.spiNotFoundError(PStreamSource.class.getSimpleName()));
        }
    }

    /**
     * Build source by window.
     *
     * @param pipelineContext
     * @param sourceFunction
     * @param window
     * @return
     */
    PStreamSource<T> build(IPipelineContext pipelineContext, SourceFunction<T> sourceFunction,
                           IWindow<T> window);

    PWindowSource<T> window(IWindow<T> window);

    @Override
    PStreamSource<T> withConfig(Map map);

    @Override
    PStreamSource<T> withConfig(String key, String value);

    @Override
    PStreamSource<T> withName(String name);

    @Override
    PStreamSource<T> withParallelism(int parallelism);

    @Override
    PStreamSource<T> withEncoder(IEncoder<T> encoder);

}
