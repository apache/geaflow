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

package org.apache.geaflow.ai.common.model;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.ai.common.config.Constants;
import org.apache.geaflow.ai.graph.GraphEdge;
import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.graph.GraphVertex;

public class ModelUtils {

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    public static List<String> splitLongText(int maxChunkSize, String... textList) {
        List<String> chunks = new ArrayList<>();
        for (String text : textList) {
            for (int i = 0; i < text.length(); i += maxChunkSize) {
                int end = Math.min(i + maxChunkSize, text.length());
                chunks.add(text.substring(i, end));
            }
        }
        return chunks;
    }

    /**
     * A fingerprint of the text that was handed to the embedding model, so that a stored vector can
     * later be told apart from one produced by a different text. The entity key alone cannot do
     * this: it is derived from the id and the label, and stays the same when the value changes.
     *
     * <p>The chunk count and a separator are folded in, so that the same characters split
     * differently do not fingerprint alike.
     *
     * @param chunks the texts embedded for one entity, in the order they were sent
     * @return the fingerprint as lower case hexadecimal
     */
    public static String getEmbeddedTextFingerprint(List<String> chunks) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required to fingerprint embedded text", e);
        }
        digest.update(Integer.toString(chunks.size()).getBytes(StandardCharsets.UTF_8));
        for (String chunk : chunks) {
            digest.update((byte) 0);
            digest.update(chunk.getBytes(StandardCharsets.UTF_8));
        }
        byte[] bytes = digest.digest();
        StringBuilder hex = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            hex.append(HEX[(b >> 4) & 0xF]).append(HEX[b & 0xF]);
        }
        return hex.toString();
    }

    public static String getGraphEntityKey(GraphEntity entity) {
        if (entity instanceof GraphVertex) {
            return Constants.PREFIX_V + ((GraphVertex) entity).getVertex().getId() + entity.getLabel();
        } else if (entity instanceof GraphEdge) {
            return Constants.PREFIX_E + ((GraphEdge) entity).getEdge().getSrcId()
                    + entity.getLabel() + ((GraphEdge) entity).getEdge().getDstId();
        }
        return "";
    }
}
