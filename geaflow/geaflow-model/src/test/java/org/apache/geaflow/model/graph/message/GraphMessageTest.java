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

package org.apache.geaflow.model.graph.message;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.encoder.Encoders;
import org.apache.geaflow.common.encoder.IEncoder;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.model.graph.message.encoder.GraphMessageEncoders;
import org.apache.geaflow.model.graph.message.encoder.ListGraphMessageEncoder;
import org.testng.Assert;
import org.testng.annotations.Test;

@SuppressWarnings("unchecked")
public class GraphMessageTest {

    @Test(expectedExceptions = {NoSuchElementException.class})
    public void testMessageException() {
        DefaultGraphMessage<Integer, Double> msg = new DefaultGraphMessage<>(1, 2.0);
        Assert.assertTrue(msg.hasNext());
        Assert.assertEquals(msg.next(), 2.0);
        Assert.assertFalse(msg.hasNext());
        msg.next();
    }

    @Test
    public void testDirectEmitMsg() {
        for (int i = 1; i <= 100; i++) {
            DefaultGraphMessage<Integer, Double> msg = new DefaultGraphMessage<>(i, i * 2.0);
            Assert.assertTrue(msg.hasNext());
            Assert.assertEquals(msg.next(), i * 2.0);
            Assert.assertFalse(msg.hasNext());
        }
    }

    @Test
    public void testListMessage() {
        for (int i = 1; i <= 100; i++) {
            List<Double> msgList = new ArrayList<>();
            for (int j = 0; j < 10; j++) {
                msgList.add(j * 2.0);
            }
            ListGraphMessage<Integer, Double> msg = new ListGraphMessage<>(i, msgList);
            for (int j = 0; j < 10; j++) {
                Assert.assertTrue(msg.hasNext());
                Assert.assertEquals(msg.next(), j * 2.0);
            }
            Assert.assertFalse(msg.hasNext());
        }
    }

    @Test
    public void testDefaultMessageEncoder() {
        IEncoder<IGraphMessage<Integer, Double>> msgEncoder =
            (IEncoder<IGraphMessage<Integer, Double>>) GraphMessageEncoders.build(Encoders.INTEGER, Encoders.DOUBLE);
        msgEncoder.init(new Configuration());

        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            for (int i = 1; i <= 100; i++) {
                for (int j = 0; j < 10; j++) {
                    IGraphMessage<Integer, Double> msg = new DefaultGraphMessage<>(i, j * 2.0);
                    msgEncoder.encode(msg, bos);
                }
            }

            byte[] arr = bos.toByteArray();
            ByteArrayInputStream bis = new ByteArrayInputStream(arr);

            int i = 1;
            int j = 0;
            while (bis.available() > 0) {
                IGraphMessage<Integer, Double> value = msgEncoder.decode(bis);
                int vertexId = value.getTargetVId();
                Assert.assertEquals(vertexId, i);
                while (value.hasNext()) {
                    Double next = value.next();
                    Assert.assertEquals(next, j * 2.0);
                    j++;
                }
                if (j == 10) {
                    i++;
                    j = 0;
                }
            }
        } catch (IOException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.undefinedError(e.getMessage()), e);
        }
    }

    @Test
    public void testListMessageEncoder() {
        IEncoder<ListGraphMessage<Integer, Double>> msgEncoder =
            new ListGraphMessageEncoder<>(Encoders.INTEGER, Encoders.DOUBLE);
        msgEncoder.init(new Configuration());

        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            for (int i = 1; i <= 100; i++) {
                List<Double> msgList = new ArrayList<>();
                for (int j = 0; j < 10; j++) {
                    msgList.add(j * 2.0);
                }
                ListGraphMessage<Integer, Double> msg = new ListGraphMessage<>(i, msgList);
                msgEncoder.encode(msg, bos);
            }

            byte[] arr = bos.toByteArray();
            ByteArrayInputStream bis = new ByteArrayInputStream(arr);

            int i = 1;
            while (bis.available() > 0) {
                ListGraphMessage<Integer, Double> value = msgEncoder.decode(bis);
                int vertexId = value.getTargetVId();
                Assert.assertEquals(vertexId, i);
                List<Double> messages = value.getMessages();
                for (int j = 0; j < 10; j++) {
                    Assert.assertEquals(messages.get(j), j * 2.0);
                }
                i++;
            }
        } catch (IOException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.undefinedError(e.getMessage()), e);
        }
    }

    @Test
    public void testNullEncoder() {
        IEncoder<? extends IGraphMessage<Object, Integer>> encoder1 =
            GraphMessageEncoders.build(null, Encoders.INTEGER);
        Assert.assertNull(encoder1);
        IEncoder<? extends IGraphMessage<Integer, Object>> encoder2 =
            GraphMessageEncoders.build(Encoders.INTEGER, null);
        Assert.assertNull(encoder2);
        IEncoder<? extends IGraphMessage<Integer, Object>> encoder3 =
            GraphMessageEncoders.build(null, null);
        Assert.assertNull(encoder3);
    }

    @Test(expectedExceptions = {GeaflowRuntimeException.class})
    public void testEncoderException() throws IOException {
        IEncoder<IGraphMessage<Integer, Double>> msgEncoder =
            (IEncoder<IGraphMessage<Integer, Double>>) GraphMessageEncoders.build(Encoders.INTEGER, Encoders.DOUBLE);
        msgEncoder.init(new Configuration());
        msgEncoder.encode(null, null);
    }

    @Test
    public void testDefaultMessageToString() {
        DefaultGraphMessage<Integer, Integer> msg = new DefaultGraphMessage<>(1, 1);
        Assert.assertEquals(msg.toString(), "DefaultGraphMessage{targetVId=1, message=1}");
    }

    @Test
    public void testListMsgToString() {
        ListGraphMessage<Integer, Integer> msg = new ListGraphMessage<>(1, Collections.singletonList(1));
        Assert.assertEquals(msg.toString(), "ListGraphMessage{targetVId=1, messages=[1]}");
    }

}
