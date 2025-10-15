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

package org.apache.geaflow.example.fo;

import org.apache.geaflow.cluster.container.ContainerContext;
import org.apache.geaflow.cluster.driver.DriverContext;
import org.apache.geaflow.common.config.Configuration;

public class MockContext {

    static class MockContainerContext extends ContainerContext {

        private boolean isRecover;

        public MockContainerContext(int index, Configuration config, boolean isRecover) {
            super(index, config);
            this.isRecover = isRecover;
        }

        public boolean isRecover() {
            return isRecover;
        }
    }

    static class MockDriverContext extends DriverContext {

        private boolean isRecover;

        public MockDriverContext(int index, Configuration config, boolean isRecover) {
            super(index, 0, config);
            this.isRecover = isRecover;
        }

        public boolean isRecover() {
            return isRecover;
        }

    }
}
