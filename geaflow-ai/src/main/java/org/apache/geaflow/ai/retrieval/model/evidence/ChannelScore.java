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

package org.apache.geaflow.ai.retrieval.model.evidence;

import com.google.gson.annotations.SerializedName;
import java.util.Objects;
import org.apache.geaflow.ai.retrieval.validation.ModelValidation;
import org.apache.geaflow.ai.retrieval.validation.RetrievalModelValidationException;

/** Score and ranking metadata emitted by one retrieval channel. */
public final class ChannelScore {

    @SerializedName("channel")
    private final String channel;
    @SerializedName("rawScore")
    private final double rawScore;
    @SerializedName("normalizedScore")
    private final Double normalizedScore;
    @SerializedName("rank")
    private final int rank;

    public ChannelScore(String channel, double rawScore, Double normalizedScore, int rank) {
        this.channel = ModelValidation.required(channel, "channel");
        this.rawScore = ModelValidation.finite(rawScore, "rawScore");
        this.normalizedScore = ModelValidation.optionalScore(normalizedScore, "normalizedScore");
        if (rank < 1) {
            throw new RetrievalModelValidationException("rank must be at least 1");
        }
        this.rank = rank;
    }

    public String getChannel() {
        return channel;
    }

    public double getRawScore() {
        return rawScore;
    }

    public Double getNormalizedScore() {
        return normalizedScore;
    }

    public int getRank() {
        return rank;
    }

    public boolean sameIdentityAs(ChannelScore other) {
        return equals(other);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof ChannelScore)) {
            return false;
        }
        ChannelScore that = (ChannelScore) object;
        return Double.compare(rawScore, that.rawScore) == 0 && rank == that.rank
            && Objects.equals(channel, that.channel)
            && Objects.equals(normalizedScore, that.normalizedScore);
    }

    @Override
    public int hashCode() {
        return Objects.hash(channel, rawScore, normalizedScore, rank);
    }
}
