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

package org.apache.geaflow.context.nlp.linker;

import java.util.List;
import org.apache.geaflow.context.nlp.entity.Entity;

/**
 * Interface for entity linking and disambiguation.
 * Links extracted entities to canonical forms and merges duplicates.
 */
public interface EntityLinker {

  /**
   * Initialize the linker.
   *
   * @throws Exception if initialization fails
   */
  void initialize() throws Exception;

  /**
   * Link entities to canonical forms and merge duplicates.
   *
   * @param entities The extracted entities
   * @return A list of linked and deduplicated entities
   * @throws Exception if linking fails
   */
  List<Entity> linkEntities(List<Entity> entities) throws Exception;

  /**
   * Get the similarity score between two entities.
   *
   * @param entity1 The first entity
   * @param entity2 The second entity
   * @return The similarity score (0.0 to 1.0)
   */
  double getEntitySimilarity(Entity entity1, Entity entity2);

  /**
   * Close the linker and release resources.
   *
   * @throws Exception if closing fails
   */
  void close() throws Exception;
}
