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

package org.apache.geaflow.dsl.connector.socket;

import org.apache.geaflow.common.config.ConfigKey;
import org.apache.geaflow.common.config.ConfigKeys;

public class SocketConfigKeys {

    public static final ConfigKey GEAFLOW_DSL_SOCKET_HOST = ConfigKeys
        .key("geaflow.dsl.socket.host")
        .noDefaultValue()
        .description("The host which socket links to.");

    public static final ConfigKey GEAFLOW_DSL_SOCKET_PORT = ConfigKeys
        .key("geaflow.dsl.socket.port")
        .noDefaultValue()
        .description("The port which socket links to.");
}
