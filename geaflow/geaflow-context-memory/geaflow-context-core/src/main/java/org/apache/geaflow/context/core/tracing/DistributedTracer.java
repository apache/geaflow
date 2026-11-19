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

package org.apache.geaflow.context.core.tracing;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Distributed tracing for Context Memory operations.
 * Supports trace context propagation and structured logging.
 */
public class DistributedTracer {

  private static final Logger LOGGER = LoggerFactory.getLogger(DistributedTracer.class);

  private static final ThreadLocal<TraceContext> traceContextHolder = new ThreadLocal<>();

  /**
   * Trace context holding span information.
   */
  public static class TraceContext {

    private final String traceId;
    private final String spanId;
    private final String parentSpanId;
    private final long startTime;
    private final Map<String, String> tags;

    public TraceContext(String parentSpanId) {
      this.traceId = generateTraceId();
      this.spanId = generateSpanId();
      this.parentSpanId = parentSpanId;
      this.startTime = System.currentTimeMillis();
      this.tags = new HashMap<>();
    }

    public String getTraceId() {
      return traceId;
    }

    public String getSpanId() {
      return spanId;
    }

    public String getParentSpanId() {
      return parentSpanId;
    }

    public long getStartTime() {
      return startTime;
    }

    public long getDurationMs() {
      return System.currentTimeMillis() - startTime;
    }

    public Map<String, String> getTags() {
      return tags;
    }

    public void setTag(String key, String value) {
      tags.put(key, value);
    }

    @Override
    public String toString() {
      return String.format(
          "TraceContext{traceId=%s, spanId=%s, parentSpan=%s, duration=%d ms}",
          traceId, spanId, parentSpanId, getDurationMs());
    }
  }

  /**
   * Start a new trace span.
   *

   * @param operationName Name of the operation
   * @return The trace context
   */
  public static TraceContext startSpan(String operationName) {
    TraceContext context = new TraceContext(null);
    traceContextHolder.set(context);

    // Set MDC for structured logging
    MDC.put("traceId", context.traceId);
    MDC.put("spanId", context.spanId);

    LOGGER.info("Started span: {} [traceId={}]", operationName, context.traceId);
    return context;
  }

  /**
   * Start a child span within current trace.
   *

   * @param operationName Name of the operation
   * @return The child trace context
   */
  public static TraceContext startChildSpan(String operationName) {
    TraceContext parentContext = traceContextHolder.get();
    if (parentContext == null) {
      return startSpan(operationName);
    }

    TraceContext childContext = new TraceContext(parentContext.spanId);
    traceContextHolder.set(childContext);

    MDC.put("traceId", childContext.traceId);
    MDC.put("spanId", childContext.spanId);

    LOGGER.debug("Started child span: {} [traceId={}, parentSpan={}]",
        operationName, childContext.traceId, childContext.parentSpanId);
    return childContext;
  }

  /**
   * Get current trace context.
   *

   * @return Current trace context or null
   */
  public static TraceContext getCurrentContext() {
    return traceContextHolder.get();
  }

  /**
   * End current span and return to parent.
   */
  public static void endSpan() {
    TraceContext context = traceContextHolder.get();
    if (context != null) {
      LOGGER.info("Ended span: {} [duration={}ms]", context.spanId, context.getDurationMs());
      traceContextHolder.remove();
      MDC.clear();
    }
  }

  /**
   * Record an event in the current span.
   *

   * @param eventName Event name
   * @param attributes Event attributes
   */
  public static void recordEvent(String eventName, Map<String, String> attributes) {
    TraceContext context = traceContextHolder.get();
    if (context != null) {
      LOGGER.debug("Event recorded: {} in span {}", eventName, context.spanId);
      if (attributes != null) {
        attributes.forEach((k, v) -> context.setTag("event." + k, v));
      }
    }
  }

  /**
   * Generate unique trace ID.
   *

   * @return Trace ID
   */
  private static String generateTraceId() {
    return UUID.randomUUID().toString();
  }

  /**
   * Generate unique span ID.
   *

   * @return Span ID
   */
  private static String generateSpanId() {
    return UUID.randomUUID().toString().substring(0, 16);
  }
}
