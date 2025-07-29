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

package org.apache.geaflow.dsl.connector.hive.adapter;

import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.hive.common.HiveVersionAnnotation;

public class HiveVersionAdapters {

    public static final String HIVE_230 = "2.3.0";

    public static final String HIVE_231 = "2.3.1";

    public static final String HIVE_232 = "2.3.2";

    public static final String HIVE_233 = "2.3.3";

    public static final String HIVE_234 = "2.3.4";

    public static final String HIVE_235 = "2.3.5";

    public static final String HIVE_236 = "2.3.6";

    public static final String HIVE_237 = "2.3.7";

    public static final String HIVE_239 = "2.3.9";

    public static final String HIVE_300 = "3.0.0";

    public static final String HIVE_310 = "3.1.0";

    public static final String HIVE_311 = "3.1.1";

    public static final String HIVE_312 = "3.1.2";

    public static HiveVersionAdapter get() {
        Package myPackage = HiveVersionAnnotation.class.getPackage();
        HiveVersionAnnotation version = myPackage.getAnnotation(HiveVersionAnnotation.class);
        switch (version.version()) {
            case HIVE_230:
            case HIVE_231:
            case HIVE_232:
            case HIVE_233:
            case HIVE_234:
            case HIVE_235:
            case HIVE_236:
            case HIVE_237:
            case HIVE_239:
                return new Hive23Adapter(version.version());
            case HIVE_300:
            case HIVE_310:
            case HIVE_311:
            case HIVE_312:
                return new Hive3Adapter(version.version());
            default:
                throw new GeaFlowDSLException("Hive version: {} is not supported.", version.version());
        }
    }
}
