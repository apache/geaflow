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

package org.apache.geaflow.dsl.runtime.traversal.data;

import java.util.List;
import java.util.function.Function;
import org.apache.geaflow.dsl.common.data.Path;
import org.apache.geaflow.dsl.common.data.StepRecord;
import org.apache.geaflow.dsl.runtime.traversal.path.ITreePath;
import org.apache.geaflow.dsl.runtime.traversal.path.ITreePath.PathFilterFunction;
import org.apache.geaflow.dsl.runtime.traversal.path.ITreePath.PathMapFunction;

public interface StepRecordWithPath extends StepRecord {

    ITreePath getPathById(Object vertexId);

    Iterable<ITreePath> getPaths();

    Iterable<Object> getVertexIds();

    StepRecordWithPath filter(PathFilterFunction function, int[] refPathIndices);

    StepRecordWithPath mapPath(PathMapFunction<Path> function, int[] refPathIndices);

    StepRecordWithPath mapTreePath(Function<ITreePath, ITreePath> function);

    <O> List<O> map(PathMapFunction<O> function, int[] refPathIndices);

    StepRecordWithPath subPathSet(int[] pathIndices);

    boolean isPathEmpty();
}
