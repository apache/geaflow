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

package org.apache.geaflow.collection.array;


import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.geaflow.collection.PrimitiveType;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PrimitiveArrayFactoryTest {

    private Random random = new Random();

    private Object getRandomValue(Class clazz) {
        PrimitiveType type = PrimitiveType.getEnum(clazz.getSimpleName());
        switch (type) {
            case INT:
                return random.nextInt(10);
            case BYTE:
                return (byte) (random.nextInt(8));
            case DOUBLE:
                return random.nextDouble();
            case FLOAT:
                return random.nextFloat();
            case BOOLEAN:
                return random.nextBoolean();
            case SHORT:
                return (short) random.nextInt(1000);
            case LONG:
                return random.nextLong();
            case BYTE_ARRAY:
                byte[] bytes = new byte[5];
                random.nextBytes(bytes);
                return bytes;
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Test
    public void test() {
        List<Class> list = Arrays.asList(
            Integer.TYPE,
            Long.TYPE,
            Double.TYPE,
            Float.TYPE,
            Byte.TYPE,
            Short.TYPE,
            Boolean.TYPE,
            byte[].class
        );

        for (int i = 0; i < list.size(); i++) {
            PrimitiveArray array = PrimitiveArrayFactory.getCustomArray(list.get(i), 10);
            for (int j = 0; j < 10; j++) {
                Object obj = getRandomValue(list.get(i));
                array.set(j, obj);
                Assert.assertEquals(array.get(j), obj);
            }
            array.drop();
        }
    }

}
