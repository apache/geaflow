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

package org.apache.geaflow.state.graph.encoder;

import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.common.config.keys.StateConfigKeys;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BytesEncoderTest {

    @Test
    public void test() {
        byte a = 0x3;
        IBytesEncoder encoder = new DefaultBytesEncoder();
        List<byte[]> list = new ArrayList<>();
        list.add("a".getBytes());
        list.add("b".getBytes());
        byte[] combined = encoder.combine(list);
        byte magic = encoder.parseMagicNumber(combined[combined.length - 1]);
        Assert.assertEquals(magic, encoder.getMyMagicNumber());

        List<byte[]> res = encoder.split(combined);
        Assert.assertEquals(res.get(0), list.get(0));
        Assert.assertEquals(res.get(1), list.get(1));

        combined = encoder.combine(list, StateConfigKeys.DELIMITER);
        magic = encoder.parseMagicNumber(combined[combined.length - 1]);
        Assert.assertEquals(magic, encoder.getMyMagicNumber());

        res = encoder.split(combined, StateConfigKeys.DELIMITER);
        Assert.assertEquals(res.get(0), list.get(0));
        Assert.assertEquals(res.get(1), list.get(1));
    }
}
