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

package org.apache.geaflow.common.errorcode;

import java.util.Set;

public interface RuntimeErrorCode {

    // Common module.
    // RUN-00xxxxxx.
    @ErrorFactory.ErrCode(codeId = "RUN-00000001",
        cause = "Undefined error: {0}",
        details = "",
        action = "Please check your code or dsl, and contact admin.")
    String undefinedError(String info);

    @ErrorFactory.ErrCode(codeId = "RUN-00000002",
        cause = "Run error: {0}",
        details = "",
        action = "Please check your code, or contact admin.")
    String runError(String info);

    @ErrorFactory.ErrCode(codeId = "RUN-00000003",
        cause = "SystemInternalError - not find ''{0}'' SPI implement",
        details = "",
        action = "Please contact admin.")
    String spiNotFoundError(String className);

    @ErrorFactory.ErrCode(codeId = "RUN-00000004",
        cause = "Key conflicts error: userKey={0}, systemKeys={1}",
        details = "",
        action = "Please check your config or contact admin.")
    String keyConflictsError(Set<String> userArgsKeys, Set<String> systemArgsKeys);

    @ErrorFactory.ErrCode(codeId = "RUN-00000005",
        cause = "Operator {0} type not support error.",
        details = "",
        action = "Please check your code, or contact admin.")
    String operatorTypeNotSupportError(String opType);

    @ErrorFactory.ErrCode(codeId = "RUN-00000006",
        cause = "unsupported operation",
        details = "",
        action = "Please check your config, or contact admin.")
    String unsupportedError();

    @ErrorFactory.ErrCode(codeId = "RUN-00000007",
        cause = "SystemInternalError - not support ''{0}'' event type",
        details = "",
        action = "Please contact admin.")
    String requestTypeNotSupportError(String requestType);

    @ErrorFactory.ErrCode(codeId = "RUN-00000008",
        cause = "Config key not found: {0}",
        details = "",
        action = "Please check your config or contact admin.")
    String configKeyNotFound(String key);

    // Plan module.
    // RUN-01xxxxxx.
    @ErrorFactory.ErrCode(codeId = "RUN-01000001",
        cause = "SystemInternalError - the previous vertex of vertex '{0}' is null",
        details = "",
        action = "Please check your code or dsl, and contact admin.")
    String previousVertexIsNullError(String info);

    @ErrorFactory.ErrCode(codeId = "RUN-01000002",
        cause = "Logical error - action is empty, missing necessary sink or get action",
        details = "",
        action = "Please contact admin for help.")
    String actionIsEmptyError();

    @ErrorFactory.ErrCode(codeId = "RUN-01000003",
        cause = "SystemInternalError - not support stream type ''{0}''",
        details = "",
        action = "Please contact admin for help.")
    String streamTypeNotSupportError(String streamType);


    // Execution module.
    // RUN-02xxxxxx.
    @ErrorFactory.ErrCode(codeId = "RUN-02000001",
        cause = "shuffle serialize error: {0}",
        details = "",
        action = "Check the cause, and contact admin for help if necessary.")
    String shuffleSerializeError(String info);

    @ErrorFactory.ErrCode(codeId = "RUN-02000002",
        cause = "shuffle deserialize error: {0}",
        details = "",
        action = "Check the cause, and contact admin for help if necessary.")
    String shuffleDeserializeError(String info);


    // state module.
    // RUN-03xxxxxx.

    /**
     * State common.
     *
     * @param info info
     * @return msg
     */
    @ErrorFactory.ErrCode(codeId = "RUN-03000001",
        cause = "StateCommonError, error message: {0}",
        details = "",
        action = "Please contact admin for help.")
    String stateCommonError(String info);

    /**
     * State RocksDB.
     *
     * @param info info
     * @return msg
     */
    @ErrorFactory.ErrCode(codeId = "RUN-03000002",
        cause = "StateRocksDbError, error message: {0}",
        details = "",
        action = "Please contact admin for help.")
    String stateRocksDbError(String info);


    // framework module.
    // RUN-04xxxxxx.

    /**
     * Resource.
     *
     * @param info info
     * @return msg
     */
    @ErrorFactory.ErrCode(codeId = "RUN-04000001",
        cause = "ResourceError, error message: {0}",
        details = "",
        action = "Please contact admin for help.")
    String resourceError(String info);

    /**
     * Type system.
     *
     * @param info info
     * @return msg
     */
    @ErrorFactory.ErrCode(codeId = "RUN-04000002",
        cause = "Type system error, error message: {0}",
        details = "",
        action = "Please contact admin for help.")
    String typeSysError(String info);

    @ErrorFactory.ErrCode(codeId = "RUN-04000003",
        cause = "IllegalRequireNumError, error message: {0}",
        details = "",
        action = "Please contact admin for help.")
    String resourceIllegalRequireNumError(String info);

    @ErrorFactory.ErrCode(codeId = "RUN-04000004",
        cause = "ResourceRecoveringError, error message: {0}",
        details = "",
        action = "Please contact admin for help.")
    String resourceRecoveringError(String info);

    @ErrorFactory.ErrCode(codeId = "RUN-04000005",
        cause = "ResourceNotReadyError, error message: {0}",
        details = "",
        action = "Please contact admin for help.")
    String resourceNotReadyError(String info);

    @ErrorFactory.ErrCode(codeId = "RUN-04000006",
        cause = "analyticsClientError, error message: {0}",
        details = "",
        action = "Please contact admin for help.")
    String analyticsClientError(String info);

    // DSL module.
    // RUN-05xxxxxx.

    /**
     * DSL runtime.
     *
     * @param info info
     * @return msg
     */
    @ErrorFactory.ErrCode(codeId = "RUN-05000001",
        cause = "DslRuntimeError, error message: {0}",
        details = "",
        action = "Please contact admin for help.")
    String dslRuntimeError(String info);

    /**
     * DSL parser.
     *
     * @param info info
     * @return msg
     */
    @ErrorFactory.ErrCode(codeId = "RUN-05000002",
        cause = "DslParserError, error message: {0}",
        details = "",
        action = "Please contact admin for help.")
    String dslParserError(String info);

}
