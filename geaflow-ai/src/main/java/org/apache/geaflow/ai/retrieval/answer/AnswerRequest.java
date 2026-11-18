/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package org.apache.geaflow.ai.retrieval.answer;

/** Request for deterministic extractive answer consumption. */
public final class AnswerRequest {
    private final String question;
    private final int tokenBudget;
    private final String requestId;
    private final String graphVersion;
    private final String indexVersion;
    private final String graphName;
    private final String mode;

    public AnswerRequest(String question, int tokenBudget) {
        this(question, tokenBudget, null, null, null, null, null);
    }

    /**
     * Creates a request that also carries the retrieval identity it was built from.
     *
     * @param requestId retrieval request id, echoed so an answer can be traced back to its search
     * @param graphVersion graph version the evidence came from
     * @param indexVersion index version the evidence came from
     */
    public AnswerRequest(String question, int tokenBudget, String requestId, String graphVersion,
                         String indexVersion) {
        this(question, tokenBudget, requestId, graphVersion, indexVersion, null, null);
    }

    /**
     * Creates a request that also names the graph to query.
     *
     * @param graphName graph to search; when absent the server falls back to its configured graph
     */
    public AnswerRequest(String question, int tokenBudget, String requestId, String graphVersion,
                         String indexVersion, String graphName) {
        this(question, tokenBudget, requestId, graphVersion, indexVersion, graphName, null);
    }

    /**
     * Creates a request that also selects the retrieval mode.
     *
     * @param mode retrieval mode to run before answering; defaults to the server's configured mode
     */
    public AnswerRequest(String question, int tokenBudget, String requestId, String graphVersion,
                         String indexVersion, String graphName, String mode) {
        if (question == null || question.trim().isEmpty() || tokenBudget < 1) {
            throw new IllegalArgumentException("question and positive tokenBudget are required");
        }
        this.question = question;
        this.tokenBudget = tokenBudget;
        this.requestId = requestId;
        this.graphVersion = graphVersion;
        this.indexVersion = indexVersion;
        this.graphName = graphName;
        this.mode = mode;
    }

    public String getQuestion() {
        return question;
    }

    public int getTokenBudget() {
        return tokenBudget;
    }

    public String getRequestId() {
        return requestId;
    }

    public String getGraphVersion() {
        return graphVersion;
    }

    public String getIndexVersion() {
        return indexVersion;
    }

    public String getGraphName() {
        return graphName;
    }

    public String getMode() {
        return mode;
    }
}
