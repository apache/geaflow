/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package org.apache.geaflow.ai.retrieval.answer;

import java.util.List;
import org.apache.geaflow.ai.retrieval.model.evidence.Evidence;

/** Replaceable answer synthesis boundary; implementations must cite supplied evidence only. */
public interface AnswerSynthesizer {
    AnswerResponse synthesize(AnswerRequest request, List<Evidence> evidence);
}
