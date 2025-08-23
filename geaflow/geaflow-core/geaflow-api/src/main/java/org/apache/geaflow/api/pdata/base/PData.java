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

package org.apache.geaflow.api.pdata.base;

import java.io.Serializable;
import java.util.Map;

public interface PData extends Serializable {

    /**
     * Returns id.
     */
    int getId();

    /**
     * Set config.
     */
    <R extends PData> R withConfig(Map<String, String> map);

    /**
     * Add key value pair config.
     */
    <R extends PData> R withConfig(String key, String value);

    /**
     * Set name.
     */
    <R extends PData> R withName(String name);

    /**
     * Set parallelism.
     */
    <R extends PData> R withParallelism(int parallelism);

}
