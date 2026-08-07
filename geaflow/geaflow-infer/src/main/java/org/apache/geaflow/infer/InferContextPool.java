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

package org.apache.geaflow.infer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.geaflow.common.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread-safe pool for managing InferContext instances across the application.
 *
 * <p>This class manages the lifecycle of InferContext to avoid repeated expensive
 * initialization in both test and production scenarios. It caches InferContext instances
 * keyed by configuration hash to support multiple configurations.
 *
 * <p>Key features:
 * <ul>
 *   <li>Configuration-based pooling: Supports multiple InferContext instances for different configs</li>
 *   <li>Lazy initialization: InferContext is created on first access</li>
 *   <li>Thread-safe: Uses ReentrantReadWriteLock for concurrent access</li>
 *   <li>Clean shutdown: Properly closes all resources on demand</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>
 *   Configuration config = new Configuration();
 *   config.put(FrameworkConfigKeys.INFER_ENV_ENABLE.getKey(), "true");
 *   // ... more config
 *
 *   InferContext context = InferContextPool.getOrCreate(config);
 *   Object result = context.infer(inputs);
 *
 *   // Clean up when done (optional - graceful shutdown)
 *   InferContextPool.closeAll();
 * </pre>
 */
public class InferContextPool {

    private static final Logger LOGGER = LoggerFactory.getLogger(InferContextPool.class);

    // Pool of InferContext instances, keyed by configuration hash
    private static final ConcurrentHashMap<String, InferContext<?>> contextPool =
        new ConcurrentHashMap<>();

    private static final ReentrantReadWriteLock poolLock = new ReentrantReadWriteLock();

    /**
     * Gets or creates a cached InferContext instance based on configuration.
     *
     * <p>This method ensures thread-safe lazy initialization. Calls with the same
     * configuration hash will return the same InferContext instance, avoiding expensive
     * re-initialization.
     *
     * @param config The configuration for InferContext
     * @return A cached or newly created InferContext instance
     * @throws RuntimeException if InferContext creation fails
     */
    @SuppressWarnings("unchecked")
    public static <OUT> InferContext<OUT> getOrCreate(Configuration config) {
        String configKey = generateConfigKey(config);

        // Try read lock first (most common case: already initialized)
        poolLock.readLock().lock();
        try {
            InferContext<?> existing = contextPool.get(configKey);
            if (existing != null) {
                LOGGER.debug("Returning cached InferContext instance for key: {}", configKey);
                return (InferContext<OUT>) existing;
            }
        } finally {
            poolLock.readLock().unlock();
        }

        // Upgrade to write lock for initialization
        poolLock.writeLock().lock();
        try {
            // Double-check after acquiring write lock
            InferContext<?> existing = contextPool.get(configKey);
            if (existing != null) {
                LOGGER.debug("Returning cached InferContext instance (after lock upgrade): {}", configKey);
                return (InferContext<OUT>) existing;
            }

            // Initialize new instance
            LOGGER.info("Creating new InferContext instance for config key: {}", configKey);
            long startTime = System.currentTimeMillis();

            try {
                InferContext<?> newContext = new InferContext<>(config);
                contextPool.put(configKey, newContext);
                long elapsedTime = System.currentTimeMillis() - startTime;
                LOGGER.info("InferContext created successfully in {}ms for key: {}", elapsedTime, configKey);
                return (InferContext<OUT>) newContext;
            } catch (Exception e) {
                LOGGER.error("Failed to create InferContext for key: {}", configKey, e);
                throw new RuntimeException("InferContext initialization failed: " + e.getMessage(), e);
            }
        } finally {
            poolLock.writeLock().unlock();
        }
    }

    /**
     * Gets the cached InferContext instance for the given config without creating a new one.
     *
     * @param config The configuration to lookup
     * @return The cached instance, or null if not yet initialized
     */
    @SuppressWarnings("unchecked")
    public static <OUT> InferContext<OUT> getInstance(Configuration config) {
        String configKey = generateConfigKey(config);
        poolLock.readLock().lock();
        try {
            return (InferContext<OUT>) contextPool.get(configKey);
        } finally {
            poolLock.readLock().unlock();
        }
    }

    /**
     * Checks if an InferContext instance is cached for the given config.
     *
     * @param config The configuration to check
     * @return true if an instance is cached, false otherwise
     */
    public static boolean isInitialized(Configuration config) {
        String configKey = generateConfigKey(config);
        poolLock.readLock().lock();
        try {
            return contextPool.containsKey(configKey);
        } finally {
            poolLock.readLock().unlock();
        }
    }

    /**
     * Closes a specific InferContext instance if cached.
     *
     * @param config The configuration of the instance to close
     */
    public static void close(Configuration config) {
        String configKey = generateConfigKey(config);
        poolLock.writeLock().lock();
        try {
            InferContext<?> context = contextPool.remove(configKey);
            if (context != null) {
                try {
                    LOGGER.info("Closing InferContext instance for key: {}", configKey);
                    context.close();
                } catch (Exception e) {
                    LOGGER.error("Error closing InferContext for key: {}", configKey, e);
                }
            }
        } finally {
            poolLock.writeLock().unlock();
        }
    }

    /**
     * Closes all cached InferContext instances and clears the pool.
     *
     * <p>This should be called during application shutdown or when completely resetting
     * the inference environment to properly clean up all resources.
     */
    public static void closeAll() {
        poolLock.writeLock().lock();
        try {
            for (String key : contextPool.keySet()) {
                InferContext<?> context = contextPool.remove(key);
                if (context != null) {
                    try {
                        LOGGER.info("Closing InferContext instance for key: {}", key);
                        context.close();
                    } catch (Exception e) {
                        LOGGER.error("Error closing InferContext for key: {}", key, e);
                    }
                }
            }
            LOGGER.info("All InferContext instances closed and pool cleared");
        } finally {
            poolLock.writeLock().unlock();
        }
    }

    /**
     * Clears all cached instances without closing them.
     *
     * <p>Useful for testing scenarios where you want to force fresh context creation.
     * Note: This does NOT close the instances. Call closeAll() first if cleanup is needed.
     */
    public static void clear() {
        poolLock.writeLock().lock();
        try {
            LOGGER.info("Clearing InferContextPool without closing {} instances", contextPool.size());
            contextPool.clear();
        } finally {
            poolLock.writeLock().unlock();
        }
    }

    /**
     * Gets pool statistics for monitoring and debugging.
     *
     * @return A descriptive string with pool status
     */
    public static String getStatus() {
        poolLock.readLock().lock();
        try {
            return String.format("InferContextPool{size=%d, instances=%s}",
                contextPool.size(), contextPool.keySet());
        } finally {
            poolLock.readLock().unlock();
        }
    }

    /**
     * Generates a cache key from configuration.
     *
     * <p>Uses a hash-based approach to create unique keys for different configurations.
     * This allows supporting multiple InferContext instances with different settings.
     *
     * @param config The configuration
     * @return A unique key for this configuration
     */
    private static String generateConfigKey(Configuration config) {
        // Use configuration hash code as the key
        // In production, this could be enhanced with explicit key parameters
        return "infer_" + Integer.toHexString(config.hashCode());
    }
}
