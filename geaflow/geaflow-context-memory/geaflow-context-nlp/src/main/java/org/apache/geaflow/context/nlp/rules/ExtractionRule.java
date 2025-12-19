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

package org.apache.geaflow.context.nlp.rules;

import java.util.regex.Pattern;

/**
 * Represents an extraction rule with pattern and metadata.
 */
public class ExtractionRule {

  private String type;
  private Pattern pattern;
  private double confidence;
  private int priority;

  public ExtractionRule(String type, String patternString, double confidence) {
    this(type, patternString, confidence, 0);
  }

  public ExtractionRule(String type, String patternString, double confidence, int priority) {
    this.type = type;
    this.pattern = Pattern.compile(patternString);
    this.confidence = confidence;
    this.priority = priority;
  }

  public String getType() {
    return type;
  }

  public Pattern getPattern() {
    return pattern;
  }

  public double getConfidence() {
    return confidence;
  }

  public int getPriority() {
    return priority;
  }

  public void setConfidence(double confidence) {
    this.confidence = confidence;
  }

  public void setPriority(int priority) {
    this.priority = priority;
  }
}
