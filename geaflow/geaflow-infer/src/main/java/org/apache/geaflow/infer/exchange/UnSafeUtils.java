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

package org.apache.geaflow.infer.exchange;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

public class UnSafeUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(UnSafeUtils.class);

    private static final String THE_UNSAFE = "theUnsafe";
    public static final Unsafe UNSAFE;

    static {
        Unsafe instance;
        try {
            Field field = Unsafe.class.getDeclaredField(THE_UNSAFE);
            field.setAccessible(true);
            instance = (Unsafe) field.get(null);
        } catch (Exception e) {
            LOGGER.error("get unsafe field failed", e);
            instance = initDeclaredConstructor();
        }
        UNSAFE = instance;
    }

    private static Unsafe initDeclaredConstructor() {
        try {
            Constructor<Unsafe> c = Unsafe.class.getDeclaredConstructor();
            c.setAccessible(true);
            return c.newInstance();
        } catch (Exception e) {
            throw new GeaflowRuntimeException("init unsafe declared constructor failed", e);
        }
    }
}
