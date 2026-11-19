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
 * Represents an entity extracted from text.
 */
public class Entity implements Serializable {

  private static final long serialVersionUID = 1L;

  private String id;
  private String text;
  private String type; // Person, Organization, Location, etc.
  private int startOffset; // Character position in text
  private int endOffset; // Character position in text
  private double confidence; // 0.0 to 1.0
  private String source; // Which model extracted this

  /**
   * Default constructor.
   */
  public Entity() {
  }

  /**
   * Constructor with basic fields.
   *
   * @param text The entity text
   * @param type The entity type
   */
  public Entity(String text, String type) {
    this.text = text;
    this.type = type;
    this.confidence = 1.0;
  }

  /**
   * Constructor with all fields.
   *
   * @param id The entity ID
   * @param text The entity text
   * @param type The entity type
   * @param startOffset The start offset in text
   * @param endOffset The end offset in text
   * @param confidence The confidence score
   * @param source The source model
   */
  public Entity(String id, String text, String type, int startOffset, int endOffset,
      double confidence, String source) {
    this.id = id;
    this.text = text;
    this.type = type;
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.confidence = confidence;
    this.source = source;
  }

  // Getters and setters

  /**
   * Gets the entity ID.
   *
   * @return The entity ID
   */
  public String getId() {
    return id;
  }

  /**
   * Sets the entity ID.
   *
   * @param id The entity ID to set
   */
  public void setId(String id) {
    this.id = id;
  }

  /**
   * Gets the entity text.
   *
   * @return The entity text
   */
  public String getText() {
    return text;
  }

  /**
   * Sets the entity text.
   *
   * @param text The entity text to set
   */
  public void setText(String text) {
    this.text = text;
  }

  /**
   * Gets the entity type.
   *
   * @return The entity type
   */
  public String getType() {
    return type;
  }

  /**
   * Sets the entity type.
   *
   * @param type The entity type to set
   */
  public void setType(String type) {
    this.type = type;
  }

  /**
   * Gets the start offset.
   *
   * @return The start offset in text
   */
  public int getStartOffset() {
    return startOffset;
  }

  /**
   * Sets the start offset.
   *
   * @param startOffset The start offset to set
   */
  public void setStartOffset(int startOffset) {
    this.startOffset = startOffset;
  }

  /**
   * Gets the end offset.
   *
   * @return The end offset in text
   */
  public int getEndOffset() {
    return endOffset;
  }

  /**
   * Sets the end offset.
   *
   * @param endOffset The end offset to set
   */
  public void setEndOffset(int endOffset) {
    this.endOffset = endOffset;
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

  @Override
  public String toString() {
    return "Entity{"
        + "id='" + id + '\''
        + ", text='" + text + '\''
        + ", type='" + type + '\''
        + ", startOffset=" + startOffset
        + ", endOffset=" + endOffset
        + ", confidence=" + confidence
        + ", source='" + source + '\''
        + '}';
  }
}
