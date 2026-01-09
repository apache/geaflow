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

package org.apache.geaflow.context.nlp.entity;

import java.io.Serializable;

/**
 * Represents a relation between two entities.
 */
public class Relation implements Serializable {

  private static final long serialVersionUID = 1L;

  private String id;
  private String sourceId;
  private String targetId;
  private String relationType;
  private String relationName; // e.g., "prefers", "works_for"
  private double confidence; // 0.0 to 1.0
  private String source; // Which model extracted this
  private String description; // Additional information

  /**
   * Default constructor.
   */
  public Relation() {
  }

  /**
   * Constructor with basic fields.
   *
   * @param sourceId The source entity ID
   * @param relationType The relation type
   * @param targetId The target entity ID
   */
  public Relation(String sourceId, String relationType, String targetId) {
    this.sourceId = sourceId;
    this.targetId = targetId;
    this.relationType = relationType;
    this.relationName = relationType;
    this.confidence = 1.0;
  }

  /**
   * Constructor with all fields.
   *
   * @param id The relation ID
   * @param sourceId The source entity ID
   * @param targetId The target entity ID
   * @param relationType The relation type
   * @param relationName The relation name
   * @param confidence The confidence score
   * @param source The source model
   * @param description Additional description
   */
  public Relation(String id, String sourceId, String targetId, String relationType,
      String relationName, double confidence, String source, String description) {
    this.id = id;
    this.sourceId = sourceId;
    this.targetId = targetId;
    this.relationType = relationType;
    this.relationName = relationName;
    this.confidence = confidence;
    this.source = source;
    this.description = description;
  }

  // Getters and setters

  /**
   * Gets the relation ID.
   *
   * @return The relation ID
   */
  public String getId() {
    return id;
  }

  /**
   * Sets the relation ID.
   *
   * @param id The relation ID to set
   */
  public void setId(String id) {
    this.id = id;
  }

  /**
   * Gets the source entity ID.
   *
   * @return The source entity ID
   */
  public String getSourceId() {
    return sourceId;
  }

  /**
   * Sets the source entity ID.
   *
   * @param sourceId The source entity ID to set
   */
  public void setSourceId(String sourceId) {
    this.sourceId = sourceId;
  }

  /**
   * Gets the target entity ID.
   *
   * @return The target entity ID
   */
  public String getTargetId() {
    return targetId;
  }

  /**
   * Sets the target entity ID.
   *
   * @param targetId The target entity ID to set
   */
  public void setTargetId(String targetId) {
    this.targetId = targetId;
  }

  /**
   * Gets the relation type.
   *
   * @return The relation type
   */
  public String getRelationType() {
    return relationType;
  }

  /**
   * Sets the relation type.
   *
   * @param relationType The relation type to set
   */
  public void setRelationType(String relationType) {
    this.relationType = relationType;
  }

  /**
   * Gets the relation name.
   *
   * @return The relation name
   */
  public String getRelationName() {
    return relationName;
  }

  /**
   * Sets the relation name.
   *
   * @param relationName The relation name to set
   */
  public void setRelationName(String relationName) {
    this.relationName = relationName;
  }

  /**
   * Gets the confidence score.
   *
   * @return The confidence score (0.0 to 1.0)
   */
  public double getConfidence() {
    return confidence;
  }

  /**
   * Sets the confidence score.
   *
   * @param confidence The confidence score to set
   */
  public void setConfidence(double confidence) {
    this.confidence = confidence;
  }

  /**
   * Gets the source model.
   *
   * @return The source model name
   */
  public String getSource() {
    return source;
  }

  /**
   * Sets the source model.
   *
   * @param source The source model name to set
   */
  public void setSource(String source) {
    this.source = source;
  }

  /**
   * Gets the description.
   *
   * @return The description
   */
  public String getDescription() {
    return description;
  }

  /**
   * Sets the description.
   *
   * @param description The description to set
   */
  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public String toString() {
    return "Relation{"
        + "id='" + id + '\''
        + ", sourceId='" + sourceId + '\''
        + ", targetId='" + targetId + '\''
        + ", relationType='" + relationType + '\''
        + ", relationName='" + relationName + '\''
        + ", confidence=" + confidence
        + ", source='" + source + '\''
        + ", description='" + description + '\''
        + '}';
  }
}
