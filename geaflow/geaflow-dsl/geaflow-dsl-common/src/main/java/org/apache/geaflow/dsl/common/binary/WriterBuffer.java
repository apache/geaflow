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

package org.apache.geaflow.dsl.common.binary;

import java.io.Serializable;
import org.apache.geaflow.common.binary.IBinaryObject;

public interface WriterBuffer extends Serializable {

    void initialize(int initSize);

    void grow(int size);

    void growTo(int targetSize);

    Object copyBuffer();

    int getCapacity();

    void writeByte(byte b);

    void writeInt(int v);

    void writeIntAlign(int v);

    void writeShort(short v);

    void writeShortAlign(short v);

    void writeLong(long v);

    void writeDouble(double v);

    void writeBytes(byte[] bytes);

    void writeBytes(IBinaryObject src, long srcOffset, long length);

    int getCursor();

    void setCursor(int cursor);

    void moveCursor(int cursor);

    int getExtendPoint();

    void moveToExtend();

    void setExtendPoint(int tailPoint);

    void reset();

    void setNullAt(long offset, int index);

    void release();

    boolean isReleased();
}
