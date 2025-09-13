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

package org.apache.geaflow.utils.keygroup;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.Objects;

public class KeyGroup implements Serializable {

    private final int startKeyGroup;
    private final int endKeyGroup;

    /**
     * Defines the range [startKeyGroup, endKeyGroup].
     *
     * @param startKeyGroup start of the range (inclusive)
     * @param endKeyGroup   end of the range (inclusive)
     */
    public KeyGroup(int startKeyGroup, int endKeyGroup) {
        Preconditions.checkArgument(startKeyGroup >= 0);
        Preconditions.checkArgument(startKeyGroup <= endKeyGroup);
        this.startKeyGroup = startKeyGroup;
        this.endKeyGroup = endKeyGroup;
        Preconditions.checkArgument(getNumberOfKeyGroups() >= 0, "Potential overflow detected.");
    }

    /**
     * Get the number of key-groups in the range.
     */
    public int getNumberOfKeyGroups() {
        return 1 + endKeyGroup - startKeyGroup;
    }

    public int getStartKeyGroup() {
        return startKeyGroup;
    }

    public int getEndKeyGroup() {
        return endKeyGroup;
    }

    public boolean contains(KeyGroup other) {
        return this.startKeyGroup <= other.startKeyGroup && this.endKeyGroup >= other.endKeyGroup;
    }

    public boolean contains(int keyGroupId) {
        return this.startKeyGroup <= keyGroupId && this.endKeyGroup >= keyGroupId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(startKeyGroup, endKeyGroup);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof KeyGroup)) {
            return false;
        }
        KeyGroup keyGroup = (KeyGroup) obj;
        return this.startKeyGroup == keyGroup.getStartKeyGroup() && this.endKeyGroup == keyGroup
            .getEndKeyGroup();
    }

    @Override
    public String toString() {
        return "KeyGroup{" + "startKeyGroup=" + startKeyGroup + ", endKeyGroup=" + endKeyGroup
            + '}';
    }
}
