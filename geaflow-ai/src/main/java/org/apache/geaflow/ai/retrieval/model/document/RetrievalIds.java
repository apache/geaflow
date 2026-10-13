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

package org.apache.geaflow.ai.retrieval.model.document;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.TreeSet;
import org.apache.geaflow.ai.retrieval.validation.ModelValidation;

/** Deterministic identifiers for source documents, chunks, and chunk evidence. */
public final class RetrievalIds {

    private RetrievalIds() {
    }

    public static String sourceDocumentId(String dataset, String datasetVersion, String split,
                                          String sourceUri, String sourceHash) {
        String identity = join(ModelValidation.required(dataset, "dataset"),
            ModelValidation.required(datasetVersion, "datasetVersion"),
            ModelValidation.required(split, "split"), normalizeSourceUri(sourceUri),
            ModelValidation.required(sourceHash, "sourceHash"));
        return "doc-" + digest(identity);
    }

    public static String chunkId(String documentId, String policyVersion, int chunkIndex,
                                 int startOffset, int endOffset, String text) {
        ModelValidation.nonNegative(chunkIndex, "chunkIndex");
        ModelValidation.nonNegative(startOffset, "startOffset");
        ModelValidation.nonNegative(endOffset, "endOffset");
        if (endOffset < startOffset) {
            throw new IllegalArgumentException("endOffset must not be before startOffset");
        }
        return "chunk-" + digest(join(ModelValidation.required(documentId, "documentId"),
            ModelValidation.required(policyVersion, "policyVersion"), String.valueOf(chunkIndex),
            String.valueOf(startOffset), String.valueOf(endOffset),
            ModelValidation.required(text, "text")));
    }

    public static String chunkEvidenceId(String documentId, String chunkId) {
        String document = ModelValidation.required(documentId, "documentId");
        String chunk = ModelValidation.required(chunkId, "chunkId");
        return document.length() + ":" + document + chunk;
    }

    /** Returns a stable source hash for an entity or edge backed by one or more documents. */
    public static String aggregateSourceHash(List<String> sourceHashes) {
        if (sourceHashes == null || sourceHashes.isEmpty()) {
            throw new IllegalArgumentException("sourceHashes must not be empty");
        }
        TreeSet<String> ordered = new TreeSet<>();
        for (String sourceHash : sourceHashes) {
            ordered.add(ModelValidation.required(sourceHash, "sourceHash"));
        }
        if (ordered.size() == 1) {
            return ordered.first();
        }
        List<String> values = new ArrayList<>(ordered);
        return "sha256:aggregate:" + digest(join(values.toArray(new String[0])));
    }

    /** Normalizes URI scheme/host case, default ports, and dot segments for identity only. */
    public static String normalizeSourceUri(String sourceUri) {
        if (sourceUri == null) {
            return null;
        }
        try {
            URI uri = new URI(ModelValidation.required(sourceUri, "sourceUri")).normalize();
            String scheme = uri.getScheme();
            String host = uri.getHost();
            if (scheme == null) {
                return normalizeEscapes(uri.toASCIIString());
            }
            scheme = scheme.toLowerCase(Locale.ROOT);
            if (host == null) {
                String rawSchemeSpecificPart = uri.getRawSchemeSpecificPart();
                if ("file".equals(scheme) && rawSchemeSpecificPart.startsWith("/")) {
                    if (!rawSchemeSpecificPart.startsWith("//")) {
                        rawSchemeSpecificPart = "//" + rawSchemeSpecificPart;
                    }
                }
                StringBuilder normalized = new StringBuilder(scheme).append(':')
                    .append(normalizeEscapes(rawSchemeSpecificPart));
                if (uri.getRawFragment() != null) {
                    normalized.append('#').append(normalizeEscapes(uri.getRawFragment()));
                }
                return new URI(normalized.toString()).toASCIIString();
            }
            host = host.toLowerCase(Locale.ROOT);
            int port = uri.getPort();
            if (("http".equals(scheme) && port == 80)
                || ("https".equals(scheme) && port == 443)) {
                port = -1;
            }
            StringBuilder normalized = new StringBuilder(scheme).append("://");
            if (uri.getRawUserInfo() != null) {
                normalized.append(normalizeEscapes(uri.getRawUserInfo())).append('@');
            }
            normalized.append(host);
            if (port >= 0) {
                normalized.append(':').append(port);
            }
            appendRawComponent(normalized, uri.getRawPath());
            if (uri.getRawQuery() != null) {
                normalized.append('?').append(normalizeEscapes(uri.getRawQuery()));
            }
            if (uri.getRawFragment() != null) {
                normalized.append('#').append(normalizeEscapes(uri.getRawFragment()));
            }
            return new URI(normalized.toString()).toASCIIString();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("invalid sourceUri", e);
        }
    }

    private static void appendRawComponent(StringBuilder target, String value) {
        if (value != null) {
            target.append(normalizeEscapes(value));
        }
    }

    private static String normalizeEscapes(String value) {
        StringBuilder normalized = new StringBuilder(value.length());
        for (int index = 0; index < value.length(); index++) {
            char current = value.charAt(index);
            if (current == '%' && index + 2 < value.length()) {
                normalized.append('%')
                    .append(Character.toUpperCase(value.charAt(index + 1)))
                    .append(Character.toUpperCase(value.charAt(index + 2)));
                index += 2;
            } else {
                normalized.append(current);
            }
        }
        return normalized.toString();
    }

    private static String join(String... fields) {
        StringBuilder result = new StringBuilder();
        for (String field : fields) {
            String value = Objects.requireNonNull(field, "identity field");
            result.append(value.length()).append(':').append(value);
        }
        return result.toString();
    }

    private static String digest(String value) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256")
                .digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(bytes.length * 2);
            for (byte current : bytes) {
                result.append(String.format("%02x", current & 0xff));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }
}
