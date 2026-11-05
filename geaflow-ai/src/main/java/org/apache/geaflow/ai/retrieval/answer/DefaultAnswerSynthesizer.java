/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package org.apache.geaflow.ai.retrieval.answer;

import java.util.List;
import org.apache.geaflow.ai.retrieval.execution.TokenEstimator;
import org.apache.geaflow.ai.retrieval.model.evidence.Evidence;

/** Offline default synthesizer backed by the deterministic extractive consumer. */
public final class DefaultAnswerSynthesizer implements AnswerSynthesizer {
    private final ExtractiveAnswerConsumer consumer;

    public DefaultAnswerSynthesizer(TokenEstimator estimator) {
        consumer = new ExtractiveAnswerConsumer(estimator);
    }

    @Override
    public AnswerResponse synthesize(AnswerRequest request, List<Evidence> evidence) {
        return consumer.consume(request, evidence);
    }
}
