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

package org.apache.geaflow.dsl.gremlin.parser;

import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;

/**
 * Interface for parsing Gremlin queries.
 */
public interface IGremlinParser {

    /**
     * Parse a Gremlin query string into a GremlinQuery object.
     *
     * @param gremlinScript the Gremlin query string
     * @return the parsed GremlinQuery object
     */
    GremlinQuery parse(String gremlinScript);

    /**
     * Parse a Gremlin Bytecode into a GremlinQuery object.
     *
     * @param bytecode the Gremlin Bytecode
     * @return the parsed GremlinQuery object
     */
    GremlinQuery parse(Bytecode bytecode);
}