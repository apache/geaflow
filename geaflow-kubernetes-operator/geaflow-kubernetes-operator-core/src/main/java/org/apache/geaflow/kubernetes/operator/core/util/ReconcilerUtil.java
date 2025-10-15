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

package org.apache.geaflow.kubernetes.operator.core.util;

import io.fabric8.kubernetes.client.CustomResource;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.apache.geaflow.kubernetes.operator.core.model.customresource.AbstractJobSpec;
import org.apache.geaflow.kubernetes.operator.core.model.customresource.AbstractJobStatus;
import org.apache.geaflow.kubernetes.operator.core.model.job.JobState;

public class ReconcilerUtil {

    /**
     * build custom update-control object from the status change of CR.
     *
     * @param previous the previous CR before this update.
     * @param current  the current CR after update.
     * @return custom update-control object
     */
    public static <SPEC extends AbstractJobSpec, STATUS extends AbstractJobStatus,
        CR extends CustomResource<SPEC, STATUS>> UpdateControl<CR> buildUpdateControl(
        CR previous, CR current) {
        UpdateControl<CR> updateControl = UpdateControl.noUpdate();
        long rescheduleAfter = getRescheduleAfterMs(previous, current);
        if (rescheduleAfter > -1) {
            updateControl.rescheduleAfter(rescheduleAfter);
        }
        return updateControl;
    }

    /**
     * get reschedule-after mill-seconds from the status of previous and current CR.
     * -1 means do not reschedule.
     *
     * @param previous the previous CR before this update.
     * @param current  the current CR after update.
     * @return reschedule-after mill-seconds
     */
    public static <SPEC extends AbstractJobSpec, STATUS extends AbstractJobStatus,
        CR extends CustomResource<SPEC, STATUS>> long getRescheduleAfterMs(
        CR previous, CR current) {
        if (current.getStatus().getState() == JobState.REDEPLOYING) {
            return 1000;
        }
        return -1;
    }

}
