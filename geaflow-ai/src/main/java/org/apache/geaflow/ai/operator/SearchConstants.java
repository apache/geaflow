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

package org.apache.geaflow.ai.operator;

public class SearchConstants {

    /**
     * Non analyzed unique document key, holding {@code ModelUtils.getGraphEntityKey(entity)}.
     * Having an exact term per entity is what lets Lucene express updates and deletes in place
     * instead of forcing the whole index to be rebuilt.
     */
    public static String KEY = "key";
    public static String LABEL = "label";
    public static String ID = "id";
    public static String SRC = "src";
    public static String DST = "dst";
    public static String CONTENT = "content";
    public static String OPERATOR = "operator";
    public static String DELIMITER = "  ";

}
