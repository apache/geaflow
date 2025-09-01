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

package org.apache.geaflow.common.utils;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertThrows;

import java.io.IOException;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ProcessUtilTest {

    @Mock
    private Runtime runtime;

    @Mock
    private Process process;

    @BeforeMethod
    public void setUp() throws IOException {
        MockitoAnnotations.initMocks(this);
        when(runtime.exec(anyString())).thenReturn(process);
    }

    @Test
    public void execute_CommandThrowsIOException_ExceptionHandled() throws IOException, InterruptedException {
        String cmd = "some command";
        when(runtime.exec(anyString())).thenThrow(new IOException("IO error"));

        assertThrows(GeaflowRuntimeException.class, () -> ProcessUtil.execute(cmd));
    }

    @Test
    public void execute_CommandThrowsInterruptedException_ExceptionHandled() throws IOException, InterruptedException {
        String cmd = "some command";
        when(runtime.exec(anyString())).thenReturn(process);
        doThrow(new InterruptedException("Interrupted")).when(process).waitFor();

        assertThrows(GeaflowRuntimeException.class, () -> ProcessUtil.execute(cmd));
    }
}
