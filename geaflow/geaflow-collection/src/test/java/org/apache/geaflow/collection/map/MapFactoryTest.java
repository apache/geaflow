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

package org.apache.geaflow.collection.map;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MapFactoryTest {

    @Test
    public void test() {
        List<Class> list = Arrays.asList(
            Integer.TYPE,
            Long.TYPE,
            Double.TYPE,
            Float.TYPE,
            Byte.TYPE,
            Short.TYPE,
            byte[].class
        );

        for (int i = 0; i < list.size() - 1; i++) {
            for (int j = 0; j < list.size(); j++) {
                Map map = MapFactory.buildMap(list.get(i), list.get(j));

                String key = list.get(i).getSimpleName();
                String value = list.get(j).getSimpleName();
                String mapClass = "OpenHashMap";
                if (value.equals("byte[]")) {
                    value = "ByteArray";
                    mapClass = "Map";
                }

                Assert.assertEquals(map.getClass().getSimpleName().toLowerCase(),
                    (key + "2" + value + mapClass).toLowerCase());
            }
        }
    }
}
