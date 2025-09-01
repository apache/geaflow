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

package org.apache.geaflow.dsl.connector.odps.utils;

import com.aliyun.odps.OdpsType;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.primitive.BinaryStringType;
import org.apache.geaflow.common.type.primitive.BooleanType;
import org.apache.geaflow.common.type.primitive.ByteType;
import org.apache.geaflow.common.type.primitive.DecimalType;
import org.apache.geaflow.common.type.primitive.DoubleType;
import org.apache.geaflow.common.type.primitive.FloatType;
import org.apache.geaflow.common.type.primitive.IntegerType;
import org.apache.geaflow.common.type.primitive.LongType;
import org.apache.geaflow.common.type.primitive.ShortType;
import org.apache.geaflow.common.type.primitive.StringType;
import org.apache.geaflow.common.type.primitive.TimestampType;
import org.apache.geaflow.dsl.common.types.ArrayType;
import org.apache.geaflow.dsl.common.types.VoidType;

public class OdpsConnectorUtils {

    public static boolean typeEquals(OdpsType odpsType, IType itype) {
        switch (odpsType) {
            case TINYINT:
            case SMALLINT:
                return itype instanceof LongType || itype instanceof IntegerType
                    || itype instanceof ShortType;
            case INT:
                return itype instanceof LongType || itype instanceof IntegerType;
            case BIGINT:
                return itype instanceof IntegerType;
            case FLOAT:
                return itype instanceof FloatType || itype instanceof DoubleType;
            case DOUBLE:
                return itype instanceof DoubleType;
            case BOOLEAN:
                return itype instanceof BooleanType;
            case CHAR:
            case STRING:
            case VARCHAR:
                return itype instanceof BinaryStringType || itype instanceof StringType;
            case BINARY:
                return itype instanceof ByteType || itype instanceof BinaryStringType;
            case DECIMAL:
                return itype instanceof DecimalType;
            case ARRAY:
                return itype instanceof ArrayType;
            case VOID:
                return itype instanceof VoidType;
            case DATETIME:
            case DATE:
                return itype instanceof TimestampType;
            case TIMESTAMP:
                return itype instanceof TimestampType || itype instanceof LongType;
            case MAP:
            case INTERVAL_DAY_TIME:
            case INTERVAL_YEAR_MONTH:
            case STRUCT:
            default:
                return false;
        }
    }
}
