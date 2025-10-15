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

package org.apache.geaflow.dsl.common.data;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import org.apache.geaflow.common.utils.ArrayUtil;
import org.testng.annotations.Test;

public class ArrayUtilTest {

    @Test
    public void testConcatArray() {
        Integer[] array1 = null;
        Integer[] array2 = null;
        Object[] concatArray1 = ArrayUtil.concatArray(array1, array2);
        assertNull(concatArray1);
        array1 = new Integer[0];
        Object[] concatArray2 = ArrayUtil.concatArray(array1, array2);
        assertEquals(concatArray2, array1);
        array2 = new Integer[0];
        Object[] concatArray3 = ArrayUtil.concatArray(array1, array2);
        assertEquals(concatArray3.length, 0);
    }
}
