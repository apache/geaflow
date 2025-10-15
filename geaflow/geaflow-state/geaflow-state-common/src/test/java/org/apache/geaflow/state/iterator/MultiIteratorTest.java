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

package org.apache.geaflow.state.iterator;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MultiIteratorTest {

    @Test
    public void test() {
        List<CloseableIterator<Integer>> list = new ArrayList<>();
        list.add(new IteratorWithFilter<>(Arrays.asList(1, 2, 3, 4, 5).iterator(),
            o -> o > 3));
        list.add(new IteratorWithFilter<>(Arrays.asList(1, 2, 3, 4, 5).iterator(),
            o -> o > 6));
        list.add(new IteratorWithFilter<>(Arrays.asList(1, 2, 3, 4, 5).iterator(),
            o -> o > 2));

        MultiIterator<Integer> it = new MultiIterator<>(list.iterator());
        List<Integer> res = Lists.newArrayList(it);
        Assert.assertEquals(res, Arrays.asList(4, 5, 3, 4, 5));
    }

}
