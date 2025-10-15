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

package org.apache.geaflow.operator.impl.graph.algo.vc.msgbox;

import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MsgBoxTest {

    @Test
    public void testCombinedMsgBox() {
        CombinedMsgBox<Integer, Integer> box = new CombinedMsgBox<>(Integer::sum);
        box.addInMessages(1, 1);
        box.addInMessages(1, 2);
        box.addInMessages(2, 2);
        Map<Integer, Integer> inBox = box.getInMessageBox();
        Assert.assertEquals(inBox.size(), 2);
        Assert.assertEquals((int) inBox.get(1), 3);
        Assert.assertEquals((int) inBox.get(2), 2);
        box.clearInBox();
        Assert.assertEquals(box.getInMessageBox().size(), 0);

        box.addOutMessage(0, 5);
        box.addOutMessage(1, 9);
        box.addOutMessage(2, 1);
        box.addOutMessage(2, 2);
        box.addOutMessage(2, 3);
        Map<Integer, Integer> outBox = box.getOutMessageBox();
        Assert.assertEquals(outBox.size(), 3);
        Assert.assertEquals((int) outBox.get(0), 5);
        Assert.assertEquals((int) outBox.get(1), 9);
        Assert.assertEquals((int) outBox.get(2), 6);
        box.clearOutBox();
        Assert.assertEquals(box.getOutMessageBox().size(), 0);
    }

}
