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

package org.apache.geaflow.common.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;

public class FutureUtil {

    public static <T> List<T> wait(List<Future<T>> futureList) {
        return wait(futureList, 0);
    }

    public static <T> List<T> wait(List<Future<T>> futureList, int timeoutMs) {
        return wait(futureList, timeoutMs, TimeUnit.MILLISECONDS);
    }

    public static <T> List<T> wait(Collection<Future<T>> futureList, long timeout,
                                   TimeUnit timeUnit) {
        List<T> result = new ArrayList<>();
        for (Future<T> future : futureList) {
            try {
                if (timeout > 0) {
                    result.add(future.get(timeout, timeUnit));
                } else {
                    result.add(future.get());
                }
            } catch (Exception e) {
                throw new GeaflowRuntimeException(e);
            }
        }
        return result;
    }
}
