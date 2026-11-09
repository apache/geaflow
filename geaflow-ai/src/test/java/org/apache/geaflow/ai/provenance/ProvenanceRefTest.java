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

package org.apache.geaflow.ai.provenance;

import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ProvenanceRefTest {

    private static final Gson GSON = new Gson();

    @Test
    public void testValidProvenancePassesValidation() {
        SourceSpan span = new SourceSpan("chunk-000012", 48, 132);
        ProvenanceRef provenance = new ProvenanceRef("doc-001", "chunk-000012", span,
            "fake-extractor-1.0.0", "extraction-schema-1.0.0");
        Assertions.assertDoesNotThrow(() -> ProvenanceRef.validate(provenance));
        Assertions.assertEquals("doc-001", provenance.getDocumentId());
        Assertions.assertEquals("chunk-000012", provenance.getChunkId());
        Assertions.assertEquals(span, provenance.getSourceSpan());
    }

    @Test
    public void testProvenanceWithoutSpanIsValid() {
        ProvenanceRef provenance = new ProvenanceRef("doc-001", "chunk-000013", null,
            "fake-extractor-1.0.0", "extraction-schema-1.0.0");
        Assertions.assertDoesNotThrow(() -> provenance.validate());
        Assertions.assertNull(provenance.getSourceSpan());
    }

    @Test
    public void testMissingProvenanceFailsValidation() {
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> ProvenanceRef.validate(null));
    }

    @Test
    public void testMissingRequiredFieldsFailValidation() {
        SourceSpan span = new SourceSpan("chunk-1", 0, 10);
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> new ProvenanceRef(null, "chunk-1", span, "e-1", "s-1").validate());
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> new ProvenanceRef("doc-1", " ", span, "e-1", "s-1").validate());
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> new ProvenanceRef("doc-1", "chunk-1", span, null, "s-1").validate());
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> new ProvenanceRef("doc-1", "chunk-1", span, "e-1", null).validate());
    }

    @Test
    public void testSpanFromOtherChunkFails() {
        SourceSpan span = new SourceSpan("chunk-other", 0, 10);
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> new ProvenanceRef("doc-1", "chunk-1", span, "e-1", "s-1").validate());
    }

    @Test
    public void testJsonExampleRoundTrip() throws IOException {
        ProvenanceRef provenance = readJson("/provenance/fact-provenance.json",
            ProvenanceRef.class);
        Assertions.assertDoesNotThrow(() -> provenance.validate());
        ProvenanceRef expected = new ProvenanceRef("doc-20260822-001", "chunk-000012",
            new SourceSpan("chunk-000012", 48, 132),
            "fake-extractor-1.0.0", "extraction-schema-1.0.0");
        Assertions.assertEquals(expected, provenance);
        Assertions.assertEquals(expected, GSON.fromJson(GSON.toJson(provenance), ProvenanceRef.class));
    }

    @Test
    public void testJsonExampleWithoutSpanRoundTrip() throws IOException {
        ProvenanceRef provenance = readJson("/provenance/fact-provenance-without-span.json",
            ProvenanceRef.class);
        Assertions.assertDoesNotThrow(() -> provenance.validate());
        Assertions.assertNull(provenance.getSourceSpan());
        Assertions.assertEquals(provenance,
            GSON.fromJson(GSON.toJson(provenance), ProvenanceRef.class));
    }

    @Test
    public void testSourceSpanJsonExampleRoundTrip() throws IOException {
        SourceSpan span = readJson("/provenance/source-span.json", SourceSpan.class);
        Assertions.assertDoesNotThrow(() -> span.validate());
        Assertions.assertEquals(new SourceSpan("chunk-000012", 48, 132), span);
    }

    @Test
    public void testProvenanceDoesNotCarryRawText() {
        String rawDocumentText = "Alice works at Acme Corp in Shanghai.";
        SourceSpan span = new SourceSpan("chunk-000012", 0, rawDocumentText.length());
        ProvenanceRef provenance = new ProvenanceRef("doc-001", "chunk-000012", span,
            "fake-extractor-1.0.0", "extraction-schema-1.0.0");
        String json = GSON.toJson(provenance);
        Assertions.assertFalse(json.contains(rawDocumentText),
            "provenance must not embed raw document text");
    }

    private static <T> T readJson(String resource, Class<T> type) throws IOException {
        try (InputStream stream = ProvenanceRefTest.class.getResourceAsStream(resource)) {
            Assertions.assertNotNull(stream, "missing test resource " + resource);
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(stream, StandardCharsets.UTF_8));
            return GSON.fromJson(reader.lines().collect(Collectors.joining("\n")), type);
        }
    }
}
