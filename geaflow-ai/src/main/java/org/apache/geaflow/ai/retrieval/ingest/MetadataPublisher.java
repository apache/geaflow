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

package org.apache.geaflow.ai.retrieval.ingest;

import java.io.IOException;
import java.util.List;
import org.apache.geaflow.ai.retrieval.index.IndexArtifact;
import org.apache.geaflow.ai.retrieval.metadata.ImportMetadata;

/** Owns import state changes and atomic publication of completed artifacts. */
public interface MetadataPublisher {

    void begin(IngestionContext context) throws IOException;

    ImportMetadata publish(IngestionContext context, GraphArtifact graph,
                           List<IndexArtifact> indexes) throws IOException;

    void fail(IngestionContext context, Exception failure) throws IOException;
}
