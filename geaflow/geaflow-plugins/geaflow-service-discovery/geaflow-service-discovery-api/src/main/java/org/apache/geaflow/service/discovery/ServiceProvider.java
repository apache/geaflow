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

package org.apache.geaflow.service.discovery;

public interface ServiceProvider extends ServiceConsumer {

    /**
     * Watch the specified path for updated events.
     * If the node already exists, the method returns true. If the node does not exist, the method returns false.
     */
    boolean watchAndCheckExists(String path);

    /**
     * Delete the specified path.
     */
    void delete(String path);

    /**
     * Creates the specified node with the specified data and watches it.
     */
    boolean createAndWatch(String path, byte[] data);

    /**
     * Update the specified node with the specified data.
     */
    boolean update(String path, byte[] data);

}
