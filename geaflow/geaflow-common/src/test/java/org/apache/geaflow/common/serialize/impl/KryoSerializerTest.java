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

package org.apache.geaflow.common.serialize.impl;

import java.io.Serializable;
import org.testng.Assert;
import org.testng.annotations.Test;

public class KryoSerializerTest {

    @Test
    public void testSerializeLambda() {
        KryoSerializer kryoSerializer = new KryoSerializer();

        LambdaMsg msg = new LambdaMsg(e -> e + 1);
        byte[] serialize = kryoSerializer.serialize(msg);
        LambdaMsg deserialized = (LambdaMsg) kryoSerializer.deserialize(serialize);

        Assert.assertNotNull(deserialized.func);
        Assert.assertEquals(2, deserialized.getFunc().accept(1));

    }

    static class LambdaMsg<T> {

        Func func;

        public LambdaMsg(Func msg) {
            this.func = msg;
        }

        public Func getFunc() {
            return func;
        }
    }

    @FunctionalInterface
    interface Func extends Serializable {
        int accept(int input);
    }
}
