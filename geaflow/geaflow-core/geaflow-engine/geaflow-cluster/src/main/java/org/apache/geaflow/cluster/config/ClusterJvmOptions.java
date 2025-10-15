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

package org.apache.geaflow.cluster.config;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public class ClusterJvmOptions {

    private static final String XMX_OPTION = "-Xmx";
    private static final String XMS_OPTION = "-Xms";
    private static final String XMN_OPTION = "-Xmn";
    private static final String XSS_OPTION = "-Xss";
    private static final String MAX_DIRECT_MEM_SIZE = "MaxDirectMemorySize";
    private static final String CLUSTER_WORKER_JVM_ARG_PATTERN =
        "(-XX:\\+(HeapDumpOnOutOfMemoryError|CrashOnOutOfMemoryError|UseG1GC))"
            + "|(-XX:MaxDirectMemorySize=(\\d+)(m|g|M|G))"
            + "|(-XX:(HeapDumpPath|ErrorFile)=(.+))"
            + "|(-(Xmx|Xms|Xmn|Xss)(\\d+)(k|m|g|M|G))"
            + "|(-Dray\\.logging\\.max\\.log\\.file\\.num=(\\d)+)"
            + "|(-Dray\\.logging\\.level=(INFO|DEBUG|WARN|ERROR))"
            + "|(-Dray\\.logging\\.max\\.log\\.file\\.size=(\\d)+(MB|GB))"
            + "|(-Dray\\.task\\.return_task_exception=(false|true))";

    private int xmsMB = 0;
    private int xmnMB = 0;
    private int maxHeapMB;
    private int maxDirectMB;
    private final List<String> jvmOptions = new ArrayList<>();
    private final List<String> extraOptions = new ArrayList<>();

    public int getXmsMB() {
        return xmsMB;
    }

    public int getXmnMB() {
        return xmnMB;
    }

    public int getMaxHeapMB() {
        return maxHeapMB;
    }

    public void setMaxHeapMB(int maxHeapMB) {
        this.maxHeapMB = maxHeapMB;
    }

    public int getMaxDirectMB() {
        return maxDirectMB;
    }

    public List<String> getJvmOptions() {
        return jvmOptions;
    }

    public List<String> getExtraOptions() {
        return extraOptions;
    }

    public static ClusterJvmOptions build(List<String> options) {
        return parseJvmOptions(options.iterator());
    }

    public static ClusterJvmOptions build(String jvmArgs) {
        if (StringUtils.isBlank(jvmArgs)) {
            return null;
        }

        String[] args = jvmArgs.trim().split("\\s*,\\s*");
        Iterator<String> iterator = Arrays.stream(args).iterator();

        return parseJvmOptions(iterator);
    }

    private static ClusterJvmOptions parseJvmOptions(Iterator<String> args) {
        String jvmArgPattern = CLUSTER_WORKER_JVM_ARG_PATTERN;
        ClusterJvmOptions jvmOptions = new ClusterJvmOptions();

        while (args.hasNext()) {
            String jvmArg = args.next();
            if (StringUtils.isBlank(jvmArg)) {
                continue;
            }

            if (!jvmArg.matches(jvmArgPattern)) {
                throw new IllegalArgumentException(
                    String.format("jvm arg %s not match the pattern %s", jvmArg, jvmArgPattern));
            }

            if (jvmArg.startsWith("-XX:")) {
                jvmOptions.parseNonStableOption(jvmArg);
            } else if (jvmArg.startsWith("-X")) {
                jvmOptions.parseNonStandardOption(jvmArg);
            } else if (jvmArg.startsWith("-D")) {
                jvmOptions.parseSystemOption(jvmArg);
            } else {
                throw new RuntimeException("not support jvm option " + jvmArg + " yet");
            }
        }

        return jvmOptions;
    }

    /**
     * parse -X options.
     *
     * @param jvmArg
     */
    private void parseNonStandardOption(String jvmArg) {
        if (jvmArg.startsWith(XMX_OPTION)) {
            this.maxHeapMB = parseMemoryOptionToMB(jvmArg, XMX_OPTION);
        } else if (jvmArg.startsWith(XMS_OPTION)) {
            this.xmsMB = parseMemoryOptionToMB(jvmArg, XMS_OPTION);
        } else if (jvmArg.startsWith(XMN_OPTION)) {
            this.xmnMB = parseMemoryOptionToMB(jvmArg, XMN_OPTION);
        } else if (jvmArg.startsWith(XSS_OPTION)) {
            //this.xss = parseMemoryOptionToMB(jvmArg, XSS_OPTION);
            this.extraOptions.add(jvmArg);
        } else {
            throw new RuntimeException("not support -X option " + jvmArg + " yet");
        }
    }

    /**
     * parse -D options.
     *
     * @param jvmArg
     */
    private void parseSystemOption(String jvmArg) {
        String option = jvmArg.substring(2);
        if (StringUtils.isBlank(option)) {
            throw new IllegalArgumentException("invalid jvm option " + jvmArg);
        }

        this.jvmOptions.add(jvmArg);
    }

    /**
     * parse -XX: options.
     *
     * @param jvmArg
     */
    private void parseNonStableOption(String jvmArg) {
        String option = jvmArg.substring(4);
        if (option.startsWith(MAX_DIRECT_MEM_SIZE)) {
            Preconditions.checkArgument(option.length() > MAX_DIRECT_MEM_SIZE.length() + 1,
                "the jvm option %s is too short", jvmArg);

            String size = option.substring(MAX_DIRECT_MEM_SIZE.length() + 1);
            this.maxDirectMB = convertMemoryToMB(size);
        } else {
            this.extraOptions.add(jvmArg);
        }
        this.jvmOptions.add(jvmArg);
    }

    /**
     * parse -Xmx, -Xms, -Xmn, -Xss option to MB value.
     *
     * @param memArg
     * @return
     */
    private int parseMemoryOptionToMB(String memArg, String prefix) {
        Preconditions.checkArgument(memArg.length() > prefix.length(),
            "invalid memory argument %s for option %s", memArg, prefix);
        String memory = memArg.substring(prefix.length());
        int size = convertMemoryToMB(memory);
        Preconditions.checkArgument(size > 0,
            "memory size should greater than 0m while current is %s", size);
        this.jvmOptions.add(prefix + size + "m");
        return size;
    }

    private int convertMemoryToMB(String memory) {
        int size;
        if (memory.endsWith("G") || memory.endsWith("g")
            || memory.endsWith("M") || memory.endsWith("m")
            || memory.endsWith("K") || memory.endsWith("k")) {
            char memoryUnit = memory.charAt(memory.length() - 1);
            int memorySize = Integer.valueOf(memory.substring(0, memory.length() - 1));
            switch (memoryUnit) {
                case 'M':
                case 'm':
                    size = memorySize;
                    break;
                case 'G':
                case 'g':
                    size = memorySize * 1024;
                    break;
                case 'K':
                case 'k':
                    size = memorySize / 1024;
                    break;
                default:
                    throw new IllegalArgumentException("invalid memory size " + memory);
            }
        } else {
            int memorySize = Integer.parseInt(memory);
            size = memorySize / 1024 / 1024;
        }
        Preconditions.checkArgument(size > 0,
            "memory size should greater than 0m while current is %s", size);
        return size;
    }

    @Override
    public String toString() {
        return "ClusterJvmOptions{" + "xmsMB=" + xmsMB + ", xmnMB=" + xmnMB + ", maxHeapMB="
            + maxHeapMB + ", maxDirectMB=" + maxDirectMB + ", jvmOptions=" + jvmOptions
            + ", extraOptions=" + extraOptions + '}';
    }

}
