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

package org.apache.geaflow.api.graph.function.vc;

/**
 * Interface for graph aggregate function.
 *
 * @param <ITERM>   The type of aggregate input iterm.
 * @param <PAGG>    The type of partial aggregate iterm.
 * @param <PRESULT> The type of partial aggregate result.
 * @param <GAGG>    The type of global aggregate iterm.
 * @param <RESULT>  The type of global aggregate result.
 */
public interface VertexCentricAggregateFunction<ITERM, PAGG, PRESULT, GAGG, RESULT> {

    IPartialGraphAggFunction<ITERM, PAGG, PRESULT> getPartialAggregation();

    IGraphAggregateFunction<PRESULT, GAGG, RESULT> getGlobalAggregation();

    interface IPartialAggContext<VALUE> {

        long getIteration();

        void collect(VALUE result);

    }

    interface IPartialGraphAggFunction<ITERM, PAGG, PRESULT> {

        PAGG create(IPartialAggContext<PRESULT> partialAggContext);

        PRESULT aggregate(ITERM iterm, PAGG result);

        void finish(PRESULT result);
    }

    interface IGlobalGraphAggContext<RESULT> {

        long getIteration();

        void broadcast(RESULT result);

        void terminate();

    }

    interface IGraphAggregateFunction<ITERM, GAGG, RESULT> {

        GAGG create(IGlobalGraphAggContext<RESULT> globalGraphAggContext);

        RESULT aggregate(ITERM iterm, GAGG agg);

        void finish(RESULT result);
    }

}
