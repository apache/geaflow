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

package org.apache.geaflow.common.type.primitive;

import com.google.common.primitives.Longs;
import java.sql.Date;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.Types;

public class DateType implements IType<Date> {

    public static final DateType INSTANCE = new DateType();

    @Override
    public String getName() {
        return Types.TYPE_NAME_DATE;
    }

    @Override
    public Class<Date> getTypeClass() {
        return Date.class;
    }

    @Override
    public byte[] serialize(Date obj) {
        return Longs.toByteArray(obj.getTime());
    }

    @Override
    public Date deserialize(byte[] bytes) {
        return new Date(Longs.fromByteArray(bytes));
    }

    @Override
    public int compare(Date x, Date y) {
        return Long.compare(x.getTime(), y.getTime());
    }

    @Override
    public boolean isPrimitive() {
        return true;
    }
}
