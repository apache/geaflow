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
import java.util.Random;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.encoder.impl.EnumEncoder;
import org.apache.geaflow.common.encoder.impl.PojoEncoder;
import org.apache.geaflow.common.encoder.impl.TripleEncoder;
import org.apache.geaflow.common.encoder.impl.TupleEncoder;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.tuple.Triple;
import org.apache.geaflow.common.tuple.Tuple;
import org.testng.Assert;
import org.testng.annotations.Test;

public class EncoderResolverTest {

    @Test
    public void testResolveClass() {
        IEncoder<?> int1 = EncoderResolver.resolveClass(int.class);
        Assert.assertEquals(int1, Encoders.INTEGER);
        IEncoder<?> int2 = EncoderResolver.resolveClass(Integer.class);
        Assert.assertEquals(int2, Encoders.INTEGER);

        IEncoder<?> encoderObj = EncoderResolver.resolveClass(Object.class);
        Assert.assertNull(encoderObj);
        IEncoder<?> encoderCls = EncoderResolver.resolveClass(Class.class);
        Assert.assertNull(encoderCls);
        IEncoder<?> encoderIfc = EncoderResolver.resolveClass(ITest0.class);
        Assert.assertNull(encoderIfc);
    }

    @Test
    public void testResolveInterfaceAndImpl() {
        IEncoder<?> encoder0 = EncoderResolver.resolveFunction(ITest0.class, new CTest0());
        Assert.assertEquals(encoder0, Encoders.INTEGER);

        IEncoder<?> encoder1 = EncoderResolver.resolveFunction(ITest0.class, new CTest1());
        Assert.assertEquals(encoder1, Encoders.STRING);

        IEncoder<?> encoder2 = EncoderResolver.resolveFunction(ITest0.class, new CTest2());
        Assert.assertEquals(encoder2, Encoders.LONG);

        IEncoder<?> encoder3 = EncoderResolver.resolveFunction(ITest0.class, new CTest3());
        Assert.assertEquals(encoder3, Encoders.INTEGER);

        IEncoder<?> encoder4 = EncoderResolver.resolveFunction(ITest0.class, new CTest4());
        Assert.assertEquals(encoder4, Encoders.STRING);

        IEncoder<?> encoder5 = EncoderResolver.resolveFunction(ITest0.class, new CTest5());
        Assert.assertEquals(encoder5, Encoders.LONG);

        IEncoder<?> encoder6 = EncoderResolver.resolveFunction(ITest0.class, new CTest6<>());
        Assert.assertEquals(encoder6, Encoders.INTEGER);

        IEncoder<?> encoder7 = EncoderResolver.resolveFunction(ITest0.class, new CTest7());
        Assert.assertEquals(encoder7, Encoders.INTEGER);

        IEncoder<?> encoder8 = EncoderResolver.resolveFunction(ITest0.class, new CTest8());
        Assert.assertEquals(encoder8, Encoders.BOOLEAN);

        IEncoder<?> encoder9 = EncoderResolver.resolveFunction(ITest0.class, new CTest9<>());
        Assert.assertEquals(encoder9, Encoders.BOOLEAN);

        IEncoder<?> encoder10 = EncoderResolver.resolveFunction(ITest0.class, new CTest10<>());
        Assert.assertNull(encoder10);

        IEncoder<?> encoder11 = EncoderResolver.resolveFunction(ITest0.class, new CTest11<>());
        Assert.assertNull(encoder11);

        IEncoder<?> encoder12_0 = EncoderResolver.resolveFunction(ITest1.class, new CTest12(), 0);
        Assert.assertEquals(encoder12_0, Encoders.STRING);
        IEncoder<?> encoder12_1 = EncoderResolver.resolveFunction(ITest1.class, new CTest12(), 1);
        Assert.assertEquals(encoder12_1, Encoders.INTEGER);

        IEncoder<?> encoder13_0 = EncoderResolver.resolveFunction(ITest1.class, new CTest13<>(), 0);
        IEncoder<?> encoder13_1 = EncoderResolver.resolveFunction(ITest1.class, new CTest13<>(), 1);
        IEncoder<?> encoder13_2 = EncoderResolver.resolveFunction(ITest1.class, new CTest13<Integer>(), 0);
        IEncoder<?> encoder13_3 = EncoderResolver.resolveFunction(ITest1.class, new CTest13<Integer>(), 1);
        IEncoder<?> encoder13_4 = EncoderResolver.resolveFunction(ITest1.class, new CTest13<Integer>() {
        }, 0);
        Assert.assertNull(encoder13_0);
        Assert.assertEquals(encoder13_1, Encoders.LONG);
        Assert.assertNull(encoder13_2);
        Assert.assertEquals(encoder13_3, Encoders.LONG);
        Assert.assertEquals(encoder13_4, Encoders.INTEGER);

        IEncoder<?> encoder15 = EncoderResolver.resolveFunction(ITest1.class, new CTest15<Integer>(), 1);
        Assert.assertEquals(encoder15, Encoders.DOUBLE);
        IEncoder<?> encoder16_0 = EncoderResolver.resolveFunction(ITest1.class, new CTest16(), 0);
        IEncoder<?> encoder16_1 = EncoderResolver.resolveFunction(ITest1.class, new CTest16(), 1);
        Assert.assertEquals(encoder16_0, Encoders.INTEGER);
        Assert.assertEquals(encoder16_1, Encoders.FLOAT);
    }

    public interface ITest0<T> {
    }

    public static abstract class ACTest0 implements ITest0<String> {
    }

    public static abstract class ACTest1<T> implements ITest0<T> {
    }

    public static class CTest0 implements ITest0<Integer> {
    }

    public static class CTest1 extends ACTest0 {
    }

    public static class CTest2 extends ACTest1<Long> {
    }

    public static class CTest3 extends CTest0 {
    }

    public static class CTest4 extends CTest1 {
    }

    public static class CTest5 extends CTest2 {
    }

    public static abstract class ACTest2<T, R> implements ITest0<R> {
    }

    public static class CTest6<T> extends ACTest2<T, Integer> {
    }

    public static class CTest7 extends CTest6<String> {
    }

    public static class CTest8 extends ACTest2<String, Boolean> {
    }

    public static class CTest9<T> extends ACTest2<T, Boolean> {
    }

    public static class CTest10<T> extends ACTest2<Integer, T> {
    }

    public static abstract class ACTest3<T> implements ITest0<T> {
    }

    public static class CTest11<T> extends ACTest3<T> {
    }

    public interface ITest1<T, R> {
    }

    public static class CTest12 implements ITest1<String, Integer> {
    }

    public static class CTest13<T> implements ITest1<T, Long> {
    }

    public static abstract class ACTest4<T, R> implements ITest1<T, R> {
    }

    public static class CTest15<T> extends ACTest4<T, Double> {
    }

    public static class CTest16 extends ACTest4<Integer, Float> {
    }

    @Test
    public void testResolveTuple() {
        IEncoder<?> encoder17 = EncoderResolver.resolveFunction(ITest0.class, new CTest17());
        Assert.assertNotNull(encoder17);
        Assert.assertEquals(encoder17.getClass(), TupleEncoder.class);

        IEncoder<?> encoder18 = EncoderResolver.resolveFunction(ITest0.class, new CTest18());
        Assert.assertNotNull(encoder18);
        Assert.assertEquals(encoder18.getClass(), TupleEncoder.class);

        IEncoder<?> encoder19 = EncoderResolver.resolveFunction(ITest0.class, new CTest19());
        Assert.assertNotNull(encoder19);
        Assert.assertEquals(encoder19.getClass(), TupleEncoder.class);

        IEncoder<?> encoder20 = EncoderResolver.resolveFunction(ITest0.class, new CTest20());
        Assert.assertNotNull(encoder20);
        Assert.assertEquals(encoder20.getClass(), TupleEncoder.class);

        IEncoder<?> encoder21 = EncoderResolver.resolveFunction(ITest0.class, new CTest21());
        Assert.assertNotNull(encoder21);
        Assert.assertEquals(encoder21.getClass(), TupleEncoder.class);
    }

    @Test
    public void testResolveTriple() {
        IEncoder<?> encoder22 = EncoderResolver.resolveFunction(ITest0.class, new CTest22());
        Assert.assertNotNull(encoder22);
        Assert.assertEquals(encoder22.getClass(), TripleEncoder.class);
    }

    public static class CTest17 implements ITest0<Tuple<Integer, Long>> {
    }

    public static class CTest18 extends ACTest1<Tuple<Integer, Long>> {
    }

    public static class CTest19 extends ACTest1<Tuple<Integer, Tuple<String, Boolean>>> {
    }

    public static class MyTuple0 extends Tuple<String, Integer> {

        public MyTuple0(String s, Integer integer) {
            super(s, integer);
        }

    }

    public static class CTest20 extends ACTest1<MyTuple0> {
    }

    public static class MyTuple1<T0, T1> extends Tuple<T0, T1> {

        public MyTuple1(T0 t0, T1 t1) {
            super(t0, t1);
        }

    }

    public static class CTest21 extends ACTest1<MyTuple1<String, Integer>> {
    }

    public static class CTest22 extends ACTest1<Triple<String, Integer, Boolean>> {
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPojoEncoder0() throws Exception {
        Random random = new Random();
        random.setSeed(37);
        PojoEncoder<Pojo0> encoder = (PojoEncoder<Pojo0>) EncoderResolver.resolvePojo(Pojo0.class);
        encoder.init(new Configuration());

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        for (int i = 0; i < 100; i++) {
            int b = random.nextInt();
            String a = "abc" + b;
            long c = random.nextLong();
            encoder.encode(new Pojo0(a, b, c), bos);
        }

        byte[] arr = bos.toByteArray();
        ByteArrayInputStream bis = new ByteArrayInputStream(arr);

        random.setSeed(37);
        while (bis.available() > 0) {
            int b = random.nextInt();
            String a = "abc" + b;
            long c = random.nextLong();
            Pojo0 res = encoder.decode(bis);
            Assert.assertEquals(res.getA(), a);
            Assert.assertEquals((int) res.getB(), b);
            Assert.assertEquals(res.getC(), c);
        }

    }

    @Test(expectedExceptions = {GeaflowRuntimeException.class})
    public void testPojoEncoder1() {
        EncoderResolver.analysisPojo(Pojo1.class);
    }

    @Test(expectedExceptions = {GeaflowRuntimeException.class})
    public void testPojoEncoder2() {
        EncoderResolver.analysisPojo(Pojo2.class);
    }

    @Test(expectedExceptions = {GeaflowRuntimeException.class})
    public void testPojoEncoder3() {
        EncoderResolver.analysisPojo(Pojo3.class);
    }

    @Test(expectedExceptions = {GeaflowRuntimeException.class})
    public void testPojoEncoder4() {
        EncoderResolver.analysisPojo(Pojo4.class);
    }

    @Test(expectedExceptions = {GeaflowRuntimeException.class})
    public void testPojoEncoder5() {
        EncoderResolver.analysisPojo(Pojo5.class);
    }

    @Test(expectedExceptions = {GeaflowRuntimeException.class})
    public void testPojoEncoder6() {
        EncoderResolver.analysisPojo(Pojo6.class);
    }

    @Test(expectedExceptions = {GeaflowRuntimeException.class})
    public void testPojoEncoder7() {
        EncoderResolver.analysisPojo(Pojo7.class);
    }

    @Test(expectedExceptions = {GeaflowRuntimeException.class})
    public void testPojoEncoder8() {
        EncoderResolver.analysisPojo(Pojo8.class);
    }

    @Test(expectedExceptions = {GeaflowRuntimeException.class})
    public void testPojoEncoder9() {
        EncoderResolver.analysisPojo(Pojo9.class);
    }

    public static class Pojo0 {

        private String a;
        private Integer b;
        private long c;

        public Pojo0() {
        }

        public Pojo0(String a, Integer b, long c) {
            this.a = a;
            this.b = b;
            this.c = c;
        }

        public String getA() {
            return this.a;
        }

        public void setA(String a) {
            this.a = a;
        }

        public Integer getB() {
            return this.b;
        }

        public void setB(Integer b) {
            this.b = b;
        }

        public long getC() {
            return this.c;
        }

        public void setC(long c) {
            this.c = c;
        }

    }

    public static class Pojo1 {

        private Pojo1() {
        }

    }

    private static class Pojo2 {
    }

    public static abstract class Pojo3 {
    }

    public interface Pojo4 {
    }

    public static class Pojo5 extends Pojo1 {

        private int a;

        public int getA() {
            return this.a;
        }

        public void setA(int a) {
            this.a = a;
        }

    }

    public static class Pojo6 {

        private int a;

        public int getA() {
            return this.a;
        }

    }

    public static class Pojo7 {

        private int a;

        public void setA(int a) {
            this.a = a;
        }

    }

    public static class Pojo8 {
    }

    public static class Pojo9 {

        private int a;

        public Pojo9(int a) {
            this.a = a;
        }

        public int getA() {
            return this.a;
        }

        public void setA(int a) {
            this.a = a;
        }

    }

    @Test
    public void testEnumEncoder() {
        IEncoder<?> encoder = EncoderResolver.resolveClass(TestEnum.class);
        Assert.assertEquals(encoder.getClass(), EnumEncoder.class);
    }

    public enum TestEnum {
        A, B, C, D, E
    }

}
