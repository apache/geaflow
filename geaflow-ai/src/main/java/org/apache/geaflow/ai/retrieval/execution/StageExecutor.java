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

package org.apache.geaflow.ai.retrieval.execution;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;
import org.apache.geaflow.ai.retrieval.api.model.ExecutionMode;

/** Executes independent retrieval stages with isolated results and deterministic collection order. */
public final class StageExecutor {
    private static final long CANCELLATION_GRACE_NANOS = TimeUnit.MILLISECONDS.toNanos(100L);

    public <T> Map<String, StageResult<T>> execute(List<Stage<T>> stages, ExecutionMode mode,
                                                    ExecutorService executor) {
        return execute(stages, mode, executor, 0L, System::nanoTime);
    }

    /** Executes stages against a shared absolute deadline and cancels unfinished parallel work. */
    public <T> Map<String, StageResult<T>> execute(List<Stage<T>> stages, ExecutionMode mode,
                                                    ExecutorService executor, long deadlineNanos,
                                                    LongSupplier clock) {
        Objects.requireNonNull(stages, "stages");
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(clock, "clock");
        if (mode == ExecutionMode.PARALLEL && executor == null) {
            throw new IllegalArgumentException("parallel execution requires an executor");
        }
        List<Stage<T>> ordered = new ArrayList<>(stages);
        ordered.sort(Comparator.comparing(Stage::getName));
        Set<String> names = new HashSet<>();
        for (Stage<T> stage : ordered) {
            if (!names.add(stage.getName())) {
                throw new IllegalArgumentException("stage names must be unique: " + stage.getName());
            }
        }
        Map<String, StageResult<T>> results = new LinkedHashMap<>();
        if (mode == ExecutionMode.PARALLEL) {
            executeParallel(ordered, executor, results, deadlineNanos, clock);
        } else {
            for (Stage<T> stage : ordered) {
                if (deadlineReached(deadlineNanos, clock)) {
                    results.put(stage.getName(), timeoutFailure(stage.getName()));
                    continue;
                }
                results.put(stage.getName(), run(stage));
            }
        }
        return results;
    }

    public <T> Map<String, StageResult<T>> executeSequential(List<Stage<T>> stages) {
        return execute(stages, ExecutionMode.SEQUENTIAL, null);
    }

    private static <T> void executeParallel(List<Stage<T>> stages, ExecutorService executor,
                                            Map<String, StageResult<T>> results, long deadlineNanos,
                                            LongSupplier clock) {
        Map<String, Future<StageResult<T>>> futures = new LinkedHashMap<>();
        Map<String, StageTask<T>> tasks = new LinkedHashMap<>();
        Map<String, StageResult<T>> preflightResults = new LinkedHashMap<>();
        for (Stage<T> stage : stages) {
            StageTask<T> task = new StageTask<>(stage);
            tasks.put(stage.getName(), task);
            if (deadlineReached(deadlineNanos, clock)) {
                task.cancel();
                preflightResults.put(stage.getName(), timeoutFailure(stage.getName()));
            } else {
                futures.put(stage.getName(), executor.submit(task));
            }
        }
        Map<String, StageResult<T>> collected = new LinkedHashMap<>(preflightResults);
        boolean deadlineReached = false;
        boolean interrupted = false;
        for (Map.Entry<String, Future<StageResult<T>>> entry : futures.entrySet()) {
            String name = entry.getKey();
            Future<StageResult<T>> future = entry.getValue();
            if (deadlineReached && !future.isDone()) {
                cancel(tasks.get(name), future);
                collected.put(name, timeoutFailure(name));
                continue;
            }
            try {
                if (deadlineNanos == 0L) {
                    collected.put(name, future.get());
                } else {
                    long remaining = deadlineNanos - clock.getAsLong();
                    if (remaining <= 0L) {
                        deadlineReached = true;
                        if (!future.isDone()) {
                            cancel(tasks.get(name), future);
                            collected.put(name, timeoutFailure(name));
                            continue;
                        }
                        collected.put(name, future.get());
                    } else {
                        collected.put(name, future.get(remaining, TimeUnit.NANOSECONDS));
                    }
                }
            } catch (InterruptedException error) {
                interrupted = true;
                for (Map.Entry<String, Future<StageResult<T>>> pending : futures.entrySet()) {
                    cancel(tasks.get(pending.getKey()), pending.getValue());
                    collected.putIfAbsent(pending.getKey(), StageResult.failure(pending.getKey(), error));
                }
                break;
            } catch (TimeoutException error) {
                deadlineReached = true;
                cancel(tasks.get(name), future);
                collected.put(name, StageResult.failure(name, error));
            } catch (ExecutionException error) {
                Throwable cause = error.getCause() == null ? error : error.getCause();
                collected.put(name, StageResult.failure(name, cause));
            } catch (CancellationException error) {
                collected.put(name, StageResult.failure(name, error));
            }
        }
        if (deadlineReached || interrupted) {
            for (StageTask<T> task : tasks.values()) {
                interrupted |= task.awaitCompletion(CANCELLATION_GRACE_NANOS);
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        for (Stage<T> stage : stages) {
            StageResult<T> result = collected.get(stage.getName());
            results.put(stage.getName(), result == null
                ? timeoutFailure(stage.getName()) : result);
        }
    }

    private static <T> void cancel(StageTask<T> task, Future<StageResult<T>> future) {
        task.cancel();
        future.cancel(true);
    }

    private static <T> StageResult<T> timeoutFailure(String name) {
        return StageResult.failure(name, new TimeoutException("stage deadline exceeded"));
    }

    private static boolean deadlineReached(long deadlineNanos, LongSupplier clock) {
        return deadlineNanos != 0L && clock.getAsLong() - deadlineNanos >= 0L;
    }

    private static <T> StageResult<T> run(Stage<T> stage) {
        long started = System.nanoTime();
        try {
            return StageResult.success(stage.getName(), stage.getCallable().call(),
                System.nanoTime() - started);
        } catch (Throwable error) {
            return StageResult.failure(stage.getName(), error, System.nanoTime() - started);
        }
    }

    private static final class StageTask<T> implements Callable<StageResult<T>> {
        private final Stage<T> stage;
        private boolean cancelled;
        private boolean started;
        private boolean completed;

        private StageTask(Stage<T> stage) {
            this.stage = stage;
        }

        @Override
        public StageResult<T> call() {
            synchronized (this) {
                if (cancelled) {
                    completed = true;
                    notifyAll();
                    return StageResult.failure(stage.getName(), new CancellationException());
                }
                started = true;
            }
            try {
                return run(stage);
            } finally {
                synchronized (this) {
                    completed = true;
                    notifyAll();
                }
            }
        }

        private synchronized void cancel() {
            cancelled = true;
            if (!started) {
                completed = true;
                notifyAll();
            }
        }

        private boolean awaitCompletion(long timeoutNanos) {
            boolean interrupted = false;
            long end = System.nanoTime() + timeoutNanos;
            synchronized (this) {
                while (!completed) {
                    long remaining = end - System.nanoTime();
                    if (remaining <= 0L) {
                        break;
                    }
                    try {
                        TimeUnit.NANOSECONDS.timedWait(this, remaining);
                    } catch (InterruptedException error) {
                        interrupted = true;
                    }
                }
            }
            return interrupted;
        }
    }

    /** Immutable stage definition. */
    public static final class Stage<T> {
        private final String name;
        private final Callable<T> callable;

        public Stage(String name, Callable<T> callable) {
            if (name == null || name.trim().isEmpty() || callable == null) {
                throw new IllegalArgumentException("stage name and callable are required");
            }
            this.name = name;
            this.callable = callable;
        }

        public String getName() {
            return name;
        }

        private Callable<T> getCallable() {
            return callable;
        }
    }

    /** Isolated stage outcome; one failed stage does not overwrite another stage's result. */
    public static final class StageResult<T> {
        private final String name;
        private final T value;
        private final Throwable error;
        private final long elapsedNanos;

        private StageResult(String name, T value, Throwable error, long elapsedNanos) {
            this.name = name;
            this.value = value;
            this.error = error;
            this.elapsedNanos = elapsedNanos;
        }

        private static <T> StageResult<T> success(String name, T value, long elapsedNanos) {
            return new StageResult<>(name, value, null, elapsedNanos);
        }

        private static <T> StageResult<T> failure(String name, Throwable error) {
            return failure(name, error, 0L);
        }

        private static <T> StageResult<T> failure(String name, Throwable error, long elapsedNanos) {
            return new StageResult<>(name, null, error, elapsedNanos);
        }

        public String getName() {
            return name;
        }

        public T getValue() {
            return value;
        }

        public Throwable getError() {
            return error;
        }

        public long getElapsedNanos() {
            return elapsedNanos;
        }

        public boolean isSuccess() {
            return error == null;
        }
    }
}
