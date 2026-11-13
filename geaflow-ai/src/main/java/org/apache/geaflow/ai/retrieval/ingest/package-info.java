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

/**
 * Storage-neutral ingestion extension points.
 *
 * <p>Loaders own the source records they return. The pipeline owns graph and index artifacts
 * created during an attempt and closes them after publication or failure. Implementations must
 * write versioned temporary output and expose it only through their artifact metadata; the
 * metadata publisher is the sole component allowed to publish a ready version.</p>
 */
package org.apache.geaflow.ai.retrieval.ingest;
