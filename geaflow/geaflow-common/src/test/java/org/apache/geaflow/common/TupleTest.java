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

package org.apache.geaflow.common;

import org.apache.geaflow.common.tuple.Triple;
import org.apache.geaflow.common.tuple.Tuple;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TupleTest {

    @Test
    public void testTuple() {
        Tuple<Integer, String> tuple = Tuple.of(1, "a");
        Assert.assertEquals(tuple.getF0().intValue(), 1);
        Assert.assertEquals(tuple.getF1(), "a");
    }

    @Test
    public void testTriple() {
        Triple<Integer, Integer, String> triple1 = Triple.of(1, 2, "a");
        Assert.assertEquals(triple1.getF0().intValue(), 1);
        Assert.assertEquals(triple1.getF1().intValue(), 2);
        Assert.assertEquals(triple1.getF2(), "a");
        Assert.assertEquals(triple1.toString(), "(1,2,a)");

        Triple<Integer, Integer, String> triple2 = Triple.of(1, 2, "a");
        Assert.assertEquals(triple2, triple1);
        Assert.assertEquals(triple2.hashCode(), triple1.hashCode());

        triple2.setF0(2);
        triple2.setF1(1);
        triple2.setF2("b");
        Assert.assertNotEquals(triple2, triple1);
    }
}
