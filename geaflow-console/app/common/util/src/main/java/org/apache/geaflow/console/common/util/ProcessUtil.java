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

package org.apache.geaflow.console.common.util;

import com.alibaba.fastjson.JSON;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteResultHandler;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.common.util.exception.GeaflowLogException;
import org.springframework.util.StreamUtils;

@Slf4j
public class ProcessUtil {

    public static String execute(String cmd) {
        return execute(cmd, -1);
    }

    public static String execute(String cmd, long timeout) {
        return execCommand(cmd, false, timeout);
    }

    public static int executeAsync(String cmd) {
        return executeAsync(cmd, -1);
    }

    public static int executeAsync(String cmd, long timeout) {
        String pid = execCommand(cmd, true, timeout);
        return Integer.parseInt(pid);
    }

    public static List<String[]> search(String command, String include, String exclude) {
        String output = execute(command);
        String[] lines = StringUtils.split(output, "\n");

        List<String[]> results = new ArrayList<>();
        if (lines == null) {
            return results;
        }

        for (String line : lines) {
            if (include != null && !line.contains(include)) {
                continue;
            }

            if (exclude != null && line.contains(exclude)) {
                continue;
            }

            results.add(StringUtils.split(line, " \t"));
        }

        return results;
    }

    public static List<String> searchPids(String keyword) {
        List<String[]> results = search("ps -a -x -o pid,command", keyword, null);
        return results.stream().map(v -> v[0]).collect(Collectors.toList());
    }

    public static boolean existPid(int pid) {
        try {
            List<String[]> results = search(Fmt.as("ps -p {}", pid), null, "PID");
            if (results.size() > 1) {
                throw new GeaflowException("To much process found of pid {}", pid);
            }

            for (String[] result : results) {
                return result[0].equals(String.valueOf(pid));
            }

            return false;

        } catch (Exception e) {
            log.error("Process {} not found, {}", pid, e.getMessage());
            return false;
        }
    }

    public static void killPid(int pid) {
        execute(Fmt.as("kill -9 {}", pid));
    }

    public static int execAsyncCommand(CommandLine command, long waitTime, String logFile, String finishFile) {
        ProcessExecutor executor = new ProcessExecutor();

        try {
            FileOutputStream outputStream = new FileOutputStream(logFile);
            executor.setStreamHandler(new PumpStreamHandler(outputStream, outputStream));
            AsyncExecuteResultHandler handler = new AsyncExecuteResultHandler(command.toString(), outputStream,
                outputStream);
            handler.setFinishFile(finishFile);

            // execute command
            executor.execute(command, handler);

            // wait async process start at least 1s
            Thread.sleep(Math.max(waitTime, 1000));
            return executor.getPid();

        } catch (Exception e) {
            throw new GeaflowLogException("Execute command `{}` failed", command.toString(), e);
        }
    }

    private static String execCommand(String cmd, boolean async, long timeout) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ByteArrayOutputStream errorStream = new ByteArrayOutputStream();

        CommandLine command = CommandLine.parse(cmd);
        ProcessExecutor executor = new ProcessExecutor();

        executor.setStreamHandler(new PumpStreamHandler(outputStream, errorStream));
        executor.setWatchdog(new ExecuteWatchdog(async ? -1 : timeout));

        try {
            if (async) {
                executor.execute(command, new AsyncExecuteResultHandler(cmd, outputStream, errorStream));

                // wait async process start at least 1s
                Thread.sleep(Math.max(timeout, 1000));
                return String.valueOf(executor.getPid());
            }

            DefaultExecuteResultHandler handler = new DefaultExecuteResultHandler();
            executor.execute(command, handler);

            // wait sync process start at least 1s, at most 10s
            handler.waitFor(Math.min(Math.max(timeout, 1000), 10000));

            if (!handler.hasResult()) {
                throw new GeaflowException("Command has no result");
            }

            ExecuteException exception = handler.getException();
            if (exception != null) {
                throw exception;
            }

            int exitValue = handler.getExitValue();
            if (exitValue != 0) {
                throw new GeaflowException("Command exit({}) failed", exitValue);
            }

            log.info("Execute command `{}` success", cmd);
            return StreamUtils.copyToString(outputStream, StandardCharsets.UTF_8).trim();

        } catch (Exception e) {
            String error = StreamUtils.copyToString(errorStream, StandardCharsets.UTF_8);
            throw new GeaflowException("Execute command `{}` failed, error={}", cmd, JSON.toJSONString(error), e);
        }
    }

    private static class ProcessExecutor extends DefaultExecutor {

        @Getter
        private int pid;

        @Override
        protected Process launch(CommandLine command, Map<String, String> env, File dir) throws IOException {
            Process process = super.launch(command, env, dir);

            try {
                Field field = process.getClass().getDeclaredField("pid");
                field.setAccessible(true);
                this.pid = (int) field.get(process);
                log.debug("Start process `{}` with pid {}", command.toString(), pid);

            } catch (Exception e) {
                throw new GeaflowException("Get process pid failed", e);
            }

            return process;
        }
    }

    private static class AsyncExecuteResultHandler implements ExecuteResultHandler {

        private final String command;

        private final OutputStream outputStream;

        private final OutputStream errorStream;

        @Setter
        private String finishFile;

        public AsyncExecuteResultHandler(String command, OutputStream outputStream, OutputStream errorStream) {
            this.command = command;
            this.outputStream = outputStream;
            this.errorStream = errorStream;
        }

        @Override
        public void onProcessComplete(int exitValue) {
            if (exitValue != 0) {
                if (errorStream instanceof ByteArrayOutputStream) {
                    String error = StreamUtils.copyToString((ByteArrayOutputStream) errorStream,
                        StandardCharsets.UTF_8);
                    log.error("Execute async command `{}` exit({}) failed, error={}", command, exitValue,
                        JSON.toJSONString(error));

                } else {
                    String msg = Fmt.as("Execute async command `{}` exit({}) failed\n", command, exitValue);
                    writeMessage(errorStream, msg);
                }

            } else {
                if (outputStream instanceof ByteArrayOutputStream) {
                    String output = StreamUtils.copyToString((ByteArrayOutputStream) outputStream,
                        StandardCharsets.UTF_8);
                    log.info("Execute async command `{}` success, output={}", command, JSON.toJSONString(output));

                } else {
                    String msg = Fmt.as("Execute async command `{}` success\n", command);
                    writeMessage(outputStream, msg);
                }

                if (finishFile != null) {
                    FileUtil.touch(finishFile);
                }
            }
        }

        @Override
        public void onProcessFailed(ExecuteException e) {
            if (errorStream instanceof ByteArrayOutputStream) {
                String error = StreamUtils.copyToString((ByteArrayOutputStream) errorStream, StandardCharsets.UTF_8);
                log.info("Execute async command `{}` failed, error={}", command, JSON.toJSONString(error), e);

            } else {
                String msg = Fmt.as("Execute async command `{}` failed\n{}\n", command,
                    ExceptionUtils.getStackTrace(e));
                writeMessage(errorStream, msg);
            }
        }

        private void writeMessage(OutputStream stream, String message) {
            try {
                stream.write(message.getBytes());
                stream.flush();

            } catch (Exception e) {
                log.error("Write message '{}' to stream failed", message, e);
            }
        }
    }

}
