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

package org.apache.geaflow.common.encoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.encoder.impl.CharacterEncoder;
import org.apache.geaflow.common.encoder.impl.DoubleEncoder;
import org.apache.geaflow.common.encoder.impl.EnumEncoder;
import org.apache.geaflow.common.encoder.impl.FloatEncoder;
import org.apache.geaflow.common.encoder.impl.GenericArrayEncoder;
import org.apache.geaflow.common.encoder.impl.IntegerEncoder;
import org.apache.geaflow.common.encoder.impl.LongEncoder;
import org.apache.geaflow.common.encoder.impl.ShortEncoder;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.tuple.Triple;
import org.apache.geaflow.common.tuple.Tuple;
import org.testng.Assert;
import org.testng.annotations.Test;

public class EncoderTest {

    @Test
    public void testBooleanEncoder() {
        IEncoder<Boolean> encoder = Encoders.BOOLEAN;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            encoder.encode(null, bos);
            encoder.encode(true, bos);
            encoder.encode(false, bos);
            byte[] arr = bos.toByteArray();
            ByteArrayInputStream bis = new ByteArrayInputStream(arr);
            Boolean d1 = encoder.decode(bis);
            Boolean d2 = encoder.decode(bis);
            Boolean d3 = encoder.decode(bis);
            Assert.assertNull(d1);
            Assert.assertEquals(d2, Boolean.TRUE);
            Assert.assertEquals(d3, Boolean.FALSE);
        } catch (Exception e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.undefinedError(e.getMessage()), e);
        }
    }

    @Test
    public void testBooleanArrEncoder() {
        IEncoder<boolean[]> encoder = Encoders.BOOLEAN_ARR;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        boolean[] booleans = {true, false};
        try {
            encoder.encode(null, bos);
            encoder.encode(booleans, bos);
            byte[] arr = bos.toByteArray();
            ByteArrayInputStream bis = new ByteArrayInputStream(arr);
            boolean[] d1 = encoder.decode(bis);
            boolean[] d2 = encoder.decode(bis);
            Assert.assertNull(d1);
            Assert.assertTrue(d2[0]);
            Assert.assertFalse(d2[1]);
        } catch (Exception e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.undefinedError(e.getMessage()), e);
        }
    }


    @Test
    public void testByteEncoder() {
        try {
            IEncoder<Byte> encoder = Encoders.BYTE;
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++) {
                encoder.encode((byte) i, bos);
            }

            byte[] arr = bos.toByteArray();
            ByteArrayInputStream bis = new ByteArrayInputStream(arr);

            int c = Byte.MIN_VALUE;
            while (bis.available() > 0) {
                byte res = encoder.decode(bis);
                Assert.assertEquals(res, c);
                c++;
            }
        } catch (IOException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.undefinedError(e.getMessage()), e);
        }
    }

    @Test
    public void testByteArrEncoder() {
        try {
            IEncoder<byte[]> encoder = Encoders.BYTE_ARR;
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            byte[] bytes = new byte[256];
            byte v = Byte.MIN_VALUE;
            for (int i = 0; i < 256; i++) {
                bytes[i] = v;
                v++;
            }
            encoder.encode(null, bos);
            encoder.encode(bytes, bos);

            byte[] arr = bos.toByteArray();
            ByteArrayInputStream bis = new ByteArrayInputStream(arr);

            byte[] bytes1 = encoder.decode(bis);
            Assert.assertNull(bytes1);

            byte[] bytes2 = encoder.decode(bis);
            v = Byte.MIN_VALUE;
            for (int i = 0; i < 256; i++) {
                Assert.assertEquals(bytes2[i], v);
                v++;
            }
        } catch (IOException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.undefinedError(e.getMessage()), e);
        }
    }

    @Test
    public void testShortEncoder() {
        try {
            doTestShortEncoder(ShortEncoder.INSTANCE, 1);
            doTestShortEncoder(ShortEncoder.INSTANCE, -1);
            doTestShortEncoder(Encoders.SHORT, 1);
            doTestShortEncoder(Encoders.SHORT, -1);
        } catch (IOException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.undefinedError(e.getMessage()), e);
        }
    }

    private static void doTestShortEncoder(IEncoder<Short> encoder, int flag) throws IOException {
        int offset = flag > 0 ? 1 : 0;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        for (int i = 0; i < 16; i++) {
            int v = (flag << i) - offset;
            encoder.encode((short) v, bos);
        }

        byte[] arr = bos.toByteArray();
        ByteArrayInputStream bis = new ByteArrayInputStream(arr);

        int c = 0;
        while (bis.available() > 0) {
            int v = (flag << c) - offset;
            short res = encoder.decode(bis);
            Assert.assertEquals(v, res);
            c++;
        }
    }

    @Test
    public void testShortArrEncoder() {
        try {
            int n = 16;
            IEncoder<short[]> encoder = Encoders.SHORT_ARR;
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            short[] shorts = new short[n];
            for (int i = 0; i < n; i++) {
                int v = (1 << i) - 1;
                shorts[i] = (short) v;
            }

            encoder.encode(null, bos);
            encoder.encode(shorts, bos);

            byte[] arr = bos.toByteArray();
            ByteArrayInputStream bis = new ByteArrayInputStream(arr);

            short[] shorts1 = encoder.decode(bis);
            Assert.assertNull(shorts1);

            short[] shorts2 = encoder.decode(bis);
            for (int i = 0; i < n; i++) {
                int v = (1 << i) - 1;
                Assert.assertEquals(shorts2[i], v);
            }
        } catch (IOException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.undefinedError(e.getMessage()), e);
        }
    }

    @Test
    public void testIntegerEncoder() {
        try {
            doTestIntegerEncoder(Encoders.INTEGER, 1);
            doTestIntegerEncoder(Encoders.INTEGER, -1);
            doTestIntegerEncoder(IntegerEncoder.INSTANCE, 1);
            doTestIntegerEncoder(IntegerEncoder.INSTANCE, -1);
        } catch (IOException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.undefinedError(e.getMessage()), e);
        }
    }

    private static void doTestIntegerEncoder(IEncoder<Integer> encoder, int flag) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        for (int i = 0; i < 32; i++) {
            int v = flag << i;
            encoder.encode(v, bos);
        }

        byte[] arr = bos.toByteArray();
        ByteArrayInputStream bis = new ByteArrayInputStream(arr);

        int c = 0;
        while (bis.available() > 0) {
            int v = flag << c;
            int res = encoder.decode(bis);
            Assert.assertEquals(v, res);
            c++;
        }
    }

    @Test
    public void testIntArrEncoder() {
        try {
            int n = 32;
            IEncoder<int[]> encoder = Encoders.INTEGER_ARR;
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            int[] ints = new int[n];
            for (int i = 0; i < n; i++) {
                int v = (1 << i) - 1;
                ints[i] = v;
            }

            encoder.encode(null, bos);
            encoder.encode(ints, bos);

            byte[] arr = bos.toByteArray();
            ByteArrayInputStream bis = new ByteArrayInputStream(arr);

            int[] ints1 = encoder.decode(bis);
            Assert.assertNull(ints1);

            int[] ints2 = encoder.decode(bis);
            for (int i = 0; i < n; i++) {
                int v = (1 << i) - 1;
                Assert.assertEquals(ints2[i], v);
            }
        } catch (IOException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.undefinedError(e.getMessage()), e);
        }
    }

    @Test
    public void testLongEncoder() {
        try {
            doTestLongEncoder(Encoders.LONG, 1);
            doTestLongEncoder(Encoders.LONG, -1);
            doTestLongEncoder(LongEncoder.INSTANCE, 1);
            doTestLongEncoder(LongEncoder.INSTANCE, -1);
        } catch (IOException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.undefinedError(e.getMessage()), e);
        }
    }

    private static void doTestLongEncoder(IEncoder<Long> encoder, long flag) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        for (int i = 0; i < 64; i++) {
            long v = flag << i;
            encoder.encode(v, bos);
        }

        byte[] arr = bos.toByteArray();
        ByteArrayInputStream bis = new ByteArrayInputStream(arr);

        int c = 0;
        while (bis.available() > 0) {
            long v = flag << c;
            long res = encoder.decode(bis);
            Assert.assertEquals(v, res);
            c++;
        }
    }

    @Test
    public void testLongArrEncoder() {
        try {
            int n = 64;
            IEncoder<long[]> encoder = Encoders.LONG_ARR;
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            long[] longs = new long[n];
            for (int i = 0; i < n; i++) {
                long v = (1L << i) - 1;
                longs[i] = v;
            }

            encoder.encode(null, bos);
            encoder.encode(longs, bos);

            byte[] arr = bos.toByteArray();
            ByteArrayInputStream bis = new ByteArrayInputStream(arr);

            long[] longs1 = encoder.decode(bis);
            Assert.assertNull(longs1);

            long[] longs2 = encoder.decode(bis);
            for (int i = 0; i < n; i++) {
                long v = (1L << i) - 1;
                Assert.assertEquals(longs2[i], v);
            }
        } catch (IOException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.undefinedError(e.getMessage()), e);
        }
    }

    @Test
    public void testFloatEncoder() {
        try {
            doTestFloatEncoder(Encoders.FLOAT, 1.25f);
            doTestFloatEncoder(Encoders.FLOAT, -1.25f);
            doTestFloatEncoder(FloatEncoder.INSTANCE, 1.25f);
            doTestFloatEncoder(FloatEncoder.INSTANCE, -1.25f);
        } catch (IOException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.undefinedError(e.getMessage()), e);
        }
    }

    private void doTestFloatEncoder(IEncoder<Float> encoder, float flag) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        for (int i = 0; i < 32; i++) {
            float v = flag * (1 << i);
            encoder.encode(v, bos);
        }

        byte[] arr = bos.toByteArray();
        ByteArrayInputStream bis = new ByteArrayInputStream(arr);

        int c = 0;
        while (bis.available() > 0) {
            float v = flag * (1 << c);
            float res = encoder.decode(bis);
            Assert.assertEquals(v, res);
            c++;
        }
    }

    @Test
    public void testFloatArrEncoder() {
        try {
            int n = 32;
            IEncoder<float[]> encoder = Encoders.FLOAT_ARR;
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            float[] floats = new float[n];
            for (int i = 0; i < n; i++) {
                float v = (1 << i) - 1;
                floats[i] = v;
            }

            encoder.encode(null, bos);
            encoder.encode(floats, bos);

            byte[] arr = bos.toByteArray();
            ByteArrayInputStream bis = new ByteArrayInputStream(arr);

            float[] floats1 = encoder.decode(bis);
            Assert.assertNull(floats1);

            float[] floats2 = encoder.decode(bis);
            for (int i = 0; i < n; i++) {
                float v = (1 << i) - 1;
                Assert.assertEquals(floats2[i], v);
            }
        } catch (IOException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.undefinedError(e.getMessage()), e);
        }
    }

    @Test
    public void testDoubleEncoder() {
        try {
            doTestDoubleEncoder(Encoders.DOUBLE, 1.25);
            doTestDoubleEncoder(Encoders.DOUBLE, -1.25);
            doTestDoubleEncoder(DoubleEncoder.INSTANCE, 1.25);
            doTestDoubleEncoder(DoubleEncoder.INSTANCE, -1.25);
        } catch (IOException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.undefinedError(e.getMessage()), e);
        }
    }

    private void doTestDoubleEncoder(IEncoder<Double> encoder, double flag) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        for (int i = 0; i < 64; i++) {
            double v = flag * (1 << i);
            encoder.encode(v, bos);
        }

        byte[] arr = bos.toByteArray();
        ByteArrayInputStream bis = new ByteArrayInputStream(arr);

        int c = 0;
        while (bis.available() > 0) {
            double v = flag * (1 << c);
            double res = encoder.decode(bis);
            Assert.assertEquals(v, res);
            c++;
        }
    }

    @Test
    public void testDoubleArrEncoder() {
        try {
            int n = 64;
            IEncoder<double[]> encoder = Encoders.DOUBLE_ARR;
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            double[] doubles = new double[n];
            for (int i = 0; i < n; i++) {
                double v = (1L << i) - 1;
                doubles[i] = v;
            }

            encoder.encode(null, bos);
            encoder.encode(doubles, bos);

            byte[] arr = bos.toByteArray();
            ByteArrayInputStream bis = new ByteArrayInputStream(arr);

            double[] doubles1 = encoder.decode(bis);
            Assert.assertNull(doubles1);

            double[] doubles2 = encoder.decode(bis);
            for (int i = 0; i < n; i++) {
                double v = (1L << i) - 1;
                Assert.assertEquals(doubles2[i], v);
            }
        } catch (IOException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.undefinedError(e.getMessage()), e);
        }
    }

    @Test
    public void testCharEncoder() {
        try {
            doTestCharEncoder(CharacterEncoder.INSTANCE);
            doTestCharEncoder(Encoders.CHARACTER);
        } catch (IOException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.undefinedError(e.getMessage()), e);
        }
    }

    private static void doTestCharEncoder(IEncoder<Character> encoder) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        for (int i = 0; i < 16; i++) {
            int v = 1 << i;
            encoder.encode((char) v, bos);
        }

        byte[] arr = bos.toByteArray();
        ByteArrayInputStream bis = new ByteArrayInputStream(arr);

        int c = 0;
        while (bis.available() > 0) {
            int v = 1 << c;
            char res = encoder.decode(bis);
            Assert.assertEquals(v, res);
            c++;
        }
    }

    @Test
    public void testCharArrEncoder() {
        try {
            int n = 16;
            IEncoder<char[]> encoder = Encoders.CHARACTER_ARR;
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            char[] chars = new char[n];
            for (int i = 0; i < n; i++) {
                int v = (1 << i) - 1;
                chars[i] = (char) v;
            }

            encoder.encode(null, bos);
            encoder.encode(chars, bos);

            byte[] arr = bos.toByteArray();
            ByteArrayInputStream bis = new ByteArrayInputStream(arr);

            char[] chars1 = encoder.decode(bis);
            Assert.assertNull(chars1);

            char[] chars2 = encoder.decode(bis);
            for (int i = 0; i < n; i++) {
                int v = (1 << i) - 1;
                Assert.assertEquals(chars2[i], v);
            }
        } catch (IOException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.undefinedError(e.getMessage()), e);
        }
    }

    @Test
    public void testStringEncoder() throws Exception {
        String[] strings = {
            "abc",
            "abc",
            null,
            null,
            null,
            "[ 007]",
            "!@#$%^&*()_=-",
            "abc",
            "abc",
            "abc",
            "让世界没有难用的图"
        };
        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        for (String s : strings) {
            Encoders.STRING.encode(s, bos);
        }

        byte[] arr = bos.toByteArray();
        ByteArrayInputStream bis = new ByteArrayInputStream(arr);

        int i = 0;
        while (bis.available() > 0) {
            String res = Encoders.STRING.decode(bis);
            if (i == 10) {
                Assert.assertEquals(strings[i], res, "" + strings[i].length() + " " + res.length());
            } else {
                Assert.assertEquals(strings[i], res);
            }
            i++;
        }
    }

    @Test
    public void testStringArrEncoder() throws Exception {
        String[] strings = {
            "abc",
            "abc",
            null,
            null,
            null,
            "[ 007]",
            "!@#$%^&*()_=-",
            "abc",
            "abc",
            "abc",
            "让世界没有难用的图"
        };
        IEncoder<String[]> encoder = new GenericArrayEncoder<>(Encoders.STRING, String[]::new);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        encoder.encode(null, bos);
        encoder.encode(strings, bos);

        byte[] arr = bos.toByteArray();
        ByteArrayInputStream bis = new ByteArrayInputStream(arr);
        String[] strings1 = encoder.decode(bis);
        Assert.assertNull(strings1);
        String[] strings2 = encoder.decode(bis);
        for (int i = 0; i < strings.length; i++) {
            Assert.assertEquals(strings[i], strings2[i], "not right " + i);
        }
    }

    @Test
    public void testEnumEncoder() throws Exception {
        IEncoder<TestEnum> encoder = new EnumEncoder<>(TestEnum.class);
        encoder.init(new Configuration());

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        encoder.encode(null, bos);
        encoder.encode(TestEnum.A, bos);
        encoder.encode(TestEnum.B, bos);
        encoder.encode(TestEnum.C, bos);
        encoder.encode(TestEnum.D, bos);
        encoder.encode(TestEnum.E, bos);

        byte[] arr = bos.toByteArray();
        ByteArrayInputStream bis = new ByteArrayInputStream(arr);

        TestEnum e0 = encoder.decode(bis);
        TestEnum e1 = encoder.decode(bis);
        TestEnum e2 = encoder.decode(bis);
        TestEnum e3 = encoder.decode(bis);
        TestEnum e4 = encoder.decode(bis);
        TestEnum e5 = encoder.decode(bis);
        Assert.assertNull(e0);
        Assert.assertEquals(e1, TestEnum.A);
        Assert.assertEquals(e2, TestEnum.B);
        Assert.assertEquals(e3, TestEnum.C);
        Assert.assertEquals(e4, TestEnum.D);
        Assert.assertEquals(e5, TestEnum.E);
    }

    public enum TestEnum {
        A, B, C, D, E
    }

    @Test
    public void testTupleEncoder() throws IOException {
        IEncoder<Tuple<Integer, Integer>> tupleEncoder = Encoders.tuple(Encoders.INTEGER, Encoders.INTEGER);
        Assert.assertNotNull(tupleEncoder);
        tupleEncoder.init(new Configuration());

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        for (int i = 0; i < 10; i++) {
            Tuple<Integer, Integer> data = Tuple.of(1, 1);
            tupleEncoder.encode(data, bos);
        }
        tupleEncoder.encode(null, bos);

        byte[] arr = bos.toByteArray();
        ByteArrayInputStream bis = new ByteArrayInputStream(arr);
        for (int i = 0; i < 10; i++) {
            Tuple<Integer, Integer> data = tupleEncoder.decode(bis);
            Assert.assertNotNull(data);
        }
        Assert.assertNull(tupleEncoder.decode(bis));
    }

    @Test
    public void testTripleEncoder() throws IOException {
        IEncoder<Triple<Integer, Integer, Integer>> tripleEncoder =
            Encoders.triple(Encoders.INTEGER, Encoders.INTEGER, Encoders.INTEGER);
        Assert.assertNotNull(tripleEncoder);
        tripleEncoder.init(new Configuration());

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        for (int i = 0; i < 10; i++) {
            Triple<Integer, Integer, Integer> data = Triple.of(1, 1, 1);
            tripleEncoder.encode(data, bos);
        }
        tripleEncoder.encode(null, bos);

        byte[] arr = bos.toByteArray();
        ByteArrayInputStream bis = new ByteArrayInputStream(arr);
        for (int i = 0; i < 10; i++) {
            Triple<Integer, Integer, Integer> data = tripleEncoder.decode(bis);
            Assert.assertNotNull(data);
        }
        Assert.assertNull(tripleEncoder.decode(bis));
    }

}
