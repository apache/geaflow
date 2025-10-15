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

package org.apache.geaflow.infer.exchange.serialize;

import java.lang.reflect.Constructor;

public abstract class AnyClassConstructor implements IObjectConstructor {

    protected final Class<?> type;

    public AnyClassConstructor(Class<?> type) {
        this.type = type;
    }

    @Override
    public Object construct(Object[] args) {
        try {
            Class<?>[] paramTypes = new Class<?>[args.length];
            for (int i = 0; i < args.length; ++i) {
                paramTypes[i] = args[i].getClass();
            }
            Constructor<?> cons = type.getConstructor(paramTypes);
            initClassImpl(cons, args);
            return cons.newInstance(args);
        } catch (Exception e) {
            throw new PickleException("problem construction object: " + e);
        }
    }

    protected abstract Object initClassImpl(Constructor<?> cons, Object[] args) throws Exception;
}
