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

package org.apache.geaflow.file;

import java.util.ServiceLoader;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;

public class PersistentIOBuilder {

    public static IPersistentIO build(Configuration userConfig) {
        PersistentType type = PersistentType.valueOf(
            userConfig.getString(FileConfigKeys.PERSISTENT_TYPE).toUpperCase());
        ServiceLoader<IPersistentIO> serviceLoader = ServiceLoader.load(IPersistentIO.class);
        for (IPersistentIO persistentIO : serviceLoader) {
            if (persistentIO.getPersistentType() == type) {
                persistentIO.init(userConfig);
                return persistentIO;
            }
        }

        throw new GeaflowRuntimeException(RuntimeErrors.INST.spiNotFoundError(type.toString()));
    }

}
