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

package org.apache.geaflow.state.strategy.accessor;

import org.apache.geaflow.state.action.ActionType;
import org.apache.geaflow.state.action.EmptyAction;
import org.apache.geaflow.state.action.IAction;
import org.apache.geaflow.state.action.archive.ArchiveAction;
import org.apache.geaflow.state.action.close.CloseAction;
import org.apache.geaflow.state.action.compact.CompactAction;
import org.apache.geaflow.state.action.drop.DropAction;
import org.apache.geaflow.state.action.finish.FinishAction;
import org.apache.geaflow.state.action.load.LoadAction;
import org.apache.geaflow.state.action.recovery.RecoveryAction;

public class ActionBuilder {

    public static IAction build(ActionType actionType) {
        switch (actionType) {
            case ARCHIVE:
                return new ArchiveAction();
            case RECOVER:
                return new RecoveryAction();
            case FINISH:
                return new FinishAction();
            case CLOSE:
                return new CloseAction();
            case DROP:
                return new DropAction();
            case COMPACT:
                return new CompactAction();
            case LOAD:
                return new LoadAction();
            default:
                return new EmptyAction(actionType);
        }
    }
}
