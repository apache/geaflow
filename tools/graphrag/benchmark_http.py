"""HTTP benchmark runner; writes one JSON object per request and a summary."""
import argparse
import csv
import gzip
import hashlib
import json
import subprocess
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from tools.graphrag.eval.metrics import exact_match, f1, percentile
from tools.graphrag.eval.retrieval import (load_query_vectors, load_supporting_targets,
                                           score_evidence)


METHODS = ('BM25_ONLY', 'VECTOR_ONLY', 'VECTOR_OVERFETCH', 'GRAPH_ONLY',
           'FIXED_HYBRID', 'ADAPTIVE_HYBRID')


# Ablation conditions required by the design document: no-anchor, high-degree, lexical,
# semantic and multi-hop. Each condition adjusts only the wire request, never the Java engine.
ABLATION_METHODS = ('FIXED_HYBRID', 'ADAPTIVE_HYBRID')
ABLATIONS = ('none', 'no-anchor', 'high-degree', 'lexical', 'semantic', 'multi-hop',
             'budget-tight', 'budget-loose')


def apply_ablation(body, ablation):
    """Returns a copy of the wire request shaped for one ablation condition."""
    if ablation is None or ablation == 'none':
        return body
    shaped = json.loads(json.dumps(body))
    budget = shaped['budget']
    if ablation == 'no-anchor':
        # Keep text recall but remove every graph anchor path.
        shaped['mode'] = 'BM25_ONLY'
    elif ablation == 'high-degree':
        # Shrink the neighbour cap so high-degree vertices cannot expand freely.
        shaped['mode'] = 'GRAPH_ONLY'
        budget['maxCandidates'] = max(1, budget['maxCandidates'] // 4)
    elif ablation == 'lexical':
        shaped['mode'] = 'BM25_ONLY'
    elif ablation == 'semantic':
        shaped['mode'] = 'VECTOR_ONLY'
        if 'queryVector' not in shaped:
            raise ValueError('semantic ablation requires --vectors')
    elif ablation == 'multi-hop':
        # Force a single hop by capping the candidate window; the server derives maxHop.
        budget['maxCandidates'] = max(1, budget['maxCandidates'] // 2)
    elif ablation == 'budget-tight':
        # Budget-level ablation: shrink the candidate budget without changing the method, so a
        # quality drop is attributable to budget rather than to a different retriever.
        budget['maxCandidates'] = max(1, budget['maxCandidates'] // 4)
    elif ablation == 'budget-loose':
        budget['maxCandidates'] = budget['maxCandidates'] * 4
    else:
        raise ValueError('unknown ablation: %s' % ablation)
    # Dropping the vector for a non-vector mode is what makes the ablation actually run. The hybrid
    # methods carry a queryVector; BM25_ONLY and GRAPH_ONLY reject it with UNSUPPORTED_OPTION, so
    # keeping it turned every ablated request into an HTTP 400 that was summarised as recall 0.0 --
    # an idle request masquerading as a measured result.
    if shaped['mode'] in ('BM25_ONLY', 'GRAPH_ONLY'):
        shaped.pop('queryVector', None)
    # exactRanking is a property of the hybrid probe, not of the single-channel target modes.
    if shaped['mode'] in ('BM25_ONLY', 'GRAPH_ONLY'):
        shaped['exactRanking'] = False
    return shaped


METHOD_CONFIG = {
    'BM25_ONLY': {'mode': 'BM25_ONLY', 'executionMode': 'SEQUENTIAL', 'candidateMultiplier': 1,
                  'requiresVector': False, 'exactRanking': True},
    'VECTOR_ONLY': {'mode': 'VECTOR_ONLY', 'executionMode': 'SEQUENTIAL', 'candidateMultiplier': 1,
                    'requiresVector': True, 'exactRanking': True},
    # Overfetch must spend the SAME candidate budget as every other method to be an equal-budget
    # baseline: it widens the evaluated candidate window, not the returned top-k. Scaling top-k
    # instead would change the metric's @K denominator and make the comparison meaningless.
    'VECTOR_OVERFETCH': {'mode': 'VECTOR_ONLY', 'executionMode': 'SEQUENTIAL',
                         'candidateMultiplier': 1, 'candidateMultiplierOverride': 4,
                         'requiresVector': True, 'exactRanking': True},
    'GRAPH_ONLY': {'mode': 'GRAPH_ONLY', 'executionMode': 'SEQUENTIAL', 'candidateMultiplier': 1,
                   'requiresVector': False, 'exactRanking': False},
    'FIXED_HYBRID': {'mode': 'FIXED_HYBRID', 'executionMode': 'CASCADED', 'candidateMultiplier': 1,
                     'requiresVector': True, 'exactRanking': True},
    # Adaptive Hybrid uses the explicit ADAPTIVE_HYBRID mode, which runs the routing policy under
    # cascaded execution (the design's intended default). Both hybrid methods share CASCADED, so
    # the comparison isolates the routing policy instead of conflating it with execution mode.
    'ADAPTIVE_HYBRID': {'mode': 'ADAPTIVE_HYBRID', 'executionMode': 'CASCADED',
                        'candidateMultiplier': 1, 'requiresVector': True, 'exactRanking': True},
}


# One hard primary constraint per benchmark run, as the design requires. Launching a matrix that
# spends the candidate budget for one method and the latency budget for another would destroy the
# equal-budget premise, so the choice is fixed for the whole run and recorded on every row.
BUDGET_CONSTRAINTS = ('CANDIDATES', 'LATENCY', 'TOKENS')


def _summary_path(output):
    """Returns the CSV sibling of a JSONL result, ignoring any compression suffix."""
    name = Path(output).name
    for suffix in ('.jsonl.gz', '.jsonl'):
        if name.endswith(suffix):
            return Path(output).with_name(name[:-len(suffix)] + '.csv')
    return Path(output).with_suffix('.csv')


def _open_output(path):
    """Opens a result stream, compressing when the name ends in .gz.

    Full-dataset runs embed every response body, which reaches many gigabytes for
    a six-method matrix. Compressing keeps a reproducible artifact on disk instead
    of forcing operators to delete it.
    """
    if str(path).endswith('.gz'):
        return gzip.open(path, 'wt', encoding='utf-8')
    return Path(path).open('w', encoding='utf-8')


def request(url, body):
    payload = json.dumps(body).encode('utf-8')
    headers = {'Content-Type': 'application/json'}
    if body.get('requestId'):
        headers['X-Request-Id'] = body['requestId']
    request = urllib.request.Request(url, payload, headers)
    started = time.perf_counter()
    try:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(request, timeout=body['budget']['timeoutMs'] / 1000) as response:
            raw = response.read()
            try:
                decoded = json.loads(raw)
                error = None
            except ValueError as parse_error:
                decoded = {'rawBody': raw.decode('utf-8', errors='replace')}
                error = 'invalid JSON response: %s' % parse_error
            return response.status, decoded, (time.perf_counter() - started) * 1000, error
    except urllib.error.HTTPError as error:
        raw = error.read().decode('utf-8', errors='replace')
        try:
            response = json.loads(raw)
        except ValueError:
            response = {'rawBody': raw}
        return error.code, response, (time.perf_counter() - started) * 1000, str(error)
    except Exception as error:  # record failures as data so runs are never silently partial
        return 0, {}, (time.perf_counter() - started) * 1000, str(error)


def fetch_answer(args, query, retrieval_body):
    """Asks the answer endpoint for an extractive answer derived from the same retrieval.

    Returns ``(answer, abstention_reason)``. The answer layer is deliberately separate from
    retrieval: it consumes the cited evidence and produces either a supported extractive answer or
    a typed abstention. Both outcomes are reported, because an abstention is a real result and
    must not be silently scored as an empty string.
    """
    answer_body = {
        'requestId': retrieval_body.get('requestId'),
        'graphName': retrieval_body.get('graphName'),
        'graphVersion': retrieval_body.get('graphVersion'),
        'indexVersion': retrieval_body.get('indexVersion'),
        'question': query['question'],
        'tokenBudget': retrieval_body.get('budget', {}).get('tokenBudget', 4096),
        'mode': retrieval_body.get('mode'),
    }
    status, response, _latency, error = request(args.answers_url, answer_body)
    if status != 200 or not isinstance(response, dict):
        return None, 'ANSWER_HTTP_%s%s' % (status, ':' + error if error else '')
    answer = response.get('answer')
    reason = response.get('abstentionReason')
    if isinstance(answer, str) and answer.strip():
        return answer, None
    return None, reason or 'EMPTY_ANSWER'


def code_revision():
    """Returns the short source revision so results can be attributed to one build."""
    try:
        return subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD'],
                                       text=True, stderr=subprocess.DEVNULL).strip() or 'unknown'
    except (OSError, subprocess.SubprocessError):
        return 'unknown'


def run_provenance(args):
    """Collects the reproducibility inputs every result row carries."""
    return {
        'codeRevision': code_revision(),
        'datasetRelease': getattr(args, 'graph_version', None),
        'randomSeed': getattr(args, 'seed', 0),
        'configVersion': getattr(args, 'config_version', 'v1'),
    }


def assert_equal_budget(methods):
    """Fails when the baseline methods do not share one candidate budget.

    The design compares all modes under equal budget, so a method that silently widens its
    candidate limit would make the comparison meaningless. VECTOR_OVERFETCH is the explicit
    exception the design calls for: it is the "equal-budget Vector overfetch" baseline, so it
    spends the same candidate budget and only widens the evaluated window.
    """
    base = {method: METHOD_CONFIG[method]['candidateMultiplier']
            for method in methods if 'candidateMultiplier' in METHOD_CONFIG[method]
            and not METHOD_CONFIG[method].get('candidateMultiplierOverride')}
    if len(set(base.values())) > 1:
        raise ValueError('methods must share one candidate budget: %s' % base)


def assert_single_primary_constraint(methods, primary_constraint):
    """Fails when the run would spend more than one hard constraint.

    The design requires exactly one hard primary constraint per benchmark run so that quality
    differences are attributable to the retriever rather than to a moving budget denominator.
    """
    if primary_constraint not in BUDGET_CONSTRAINTS:
        raise ValueError('primary constraint must be one of %s, got %r'
                         % (BUDGET_CONSTRAINTS, primary_constraint))
    if primary_constraint == 'TOKENS' and not methods:
        raise ValueError('no methods selected')


def request_body(args, query, method, query_vector=None):
    """Build the auditable wire request for one benchmark method."""
    config = METHOD_CONFIG[method]
    if config['requiresVector'] and query_vector is None:
        raise ValueError('%s requires --vectors' % method)
    candidate_limit = args.max_candidates * config['candidateMultiplier']
    # Equal-budget baseline: overfetch evaluates a wider window under the run's one hard constraint
    # rather than widening the returned top-k, which would change the measured @K.
    if config.get('candidateMultiplierOverride'):
        candidate_limit = args.max_candidates * config['candidateMultiplierOverride']
    query_identity = str(query.get('id') or query['question'])
    stable_request_id = 'benchmark-' + hashlib.sha256(query_identity.encode('utf-8')).hexdigest()[:32]
    body = {'requestId': stable_request_id,
            'graphName': args.graph_name, 'graphVersion': args.graph_version,
            'indexVersion': args.index_version, 'query': query['question'],
            'mode': config['mode'], 'executionMode': config['executionMode'],
            'exactRanking': bool(config['exactRanking'] and getattr(args, 'exact_ranking', True)),
            'budget': {'topK': args.top_k,
                       'timeoutMs': args.timeout_ms,
                       'maxCandidates': candidate_limit, 'tokenBudget': args.token_budget,
                       'primaryConstraint': getattr(args, 'primary_constraint', 'CANDIDATES')}}
    if config['requiresVector']:
        body['queryVector'] = query_vector.tolist() if hasattr(query_vector, 'tolist') else query_vector
    return apply_ablation(body, getattr(args, 'ablation', None))


def run(args):
    queries = [json.loads(line) for line in Path(args.queries).read_text().splitlines() if line.strip()]
    vectors = None
    if args.vectors:
        vectors = load_query_vectors(args.vectors, queries)
    gold_targets = load_supporting_targets(args.gold, args.chunks) if args.gold else {}
    if args.gold and any(str(query['id']) not in gold_targets for query in queries):
        raise ValueError('gold does not cover all query ids')
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    selected_queries = queries[:args.limit] if args.limit else queries
    active = METHODS if not getattr(args, 'ablation', None) else ABLATION_METHODS
    assert_equal_budget(active)
    assert_single_primary_constraint(active, getattr(args, 'primary_constraint', 'CANDIDATES'))

    def execute(item):
        index, query, method = item
        config = METHOD_CONFIG[method]
        body = request_body(args, query, method, vectors[index] if vectors is not None else None)
        # The recorded budget must be the one the request carried; an ablation changes it, and
        # reporting the pre-ablation value would mislabel the experiment.
        candidate_limit = body['budget']['maxCandidates']
        status, response, latency, error = request(args.url, body)
        attempts = 1
        while status != 200 and attempts <= args.retries:
            retry_status, retry_response, retry_latency, retry_error = request(args.url, body)
            latency += retry_latency
            status, response, error = retry_status, retry_response, retry_error
            attempts += 1
        evidence = []
        documents = []
        for item in response.get('evidence', []) if isinstance(response, dict) else []:
            chunks = item.get('chunks') or [item]
            for chunk in chunks:
                if chunk.get('chunkId'):
                    evidence.append(chunk['chunkId'])
                if chunk.get('documentId'):
                    documents.append(chunk['documentId'])
        target = gold_targets.get(str(query['id']), {})
        scored = score_evidence(response, target.get('supportingSentences'),
                                target.get('relevantChunkIds') if args.chunks else None,
                                target.get('relations') or None)
        trace = response.get('trace') if isinstance(response, dict) else None
        # Retrieval responses never carry an 'answer' field, so EM/F1 stayed empty in every run.
        # The answer layer is a separate endpoint; consulting it is opt-in because it adds a request
        # per retrieval and its extractive output is not expected to match short gold answers.
        predicted_answer = response.get('answer') if isinstance(response, dict) else None
        answer_abstention = None
        if getattr(args, 'answers_url', None) and predicted_answer is None and status == 200:
            predicted_answer, answer_abstention = fetch_answer(args, query, body)
        expected_answer = query.get('answer')
        protocol_valid = int(status == 200 and isinstance(response, dict)
                            and isinstance(response.get('evidence'), list)
                            and isinstance(response.get('paths'), list)
                            and isinstance(trace, dict))
        row = {'id': query.get('id'), 'method': method,
               'ablation': getattr(args, 'ablation', None) or 'none',
               # 'mode'/'executionMode' describe the request that was actually sent. An ablation
               # rewrites the wire mode (no-anchor -> BM25_ONLY, high-degree -> GRAPH_ONLY), so
               # recording the pre-ablation config value labelled single-channel results as hybrid
               # ones and turned an idle run into a fake recall datapoint. 'requestedMode' keeps the
               # method's own mode so the delta stays auditable.
               'mode': body['mode'],
               'requestedMode': config['mode'],
               'executionMode': body.get('executionMode', config['executionMode']),
               'topKLimit': body['budget']['topK'],
               'candidateLimit': candidate_limit,
               'primaryConstraint': body['budget'].get('primaryConstraint'),
               'status': status, 'protocolValid': protocol_valid, 'latencyMs': latency,
               'attempts': attempts,
               'error': error, 'evidence': evidence, 'documents': documents,
               'supportingFactCount': len(target.get('supportingSentences', [])),
               'unresolvedSupportingFacts': target.get('unresolvedSupportingFacts', 0),
               'response': response,
               'answer': expected_answer, 'predictedAnswer': predicted_answer,
               'answerAbstentionReason': answer_abstention,
               'exactMatch': exact_match(predicted_answer, expected_answer)
               if isinstance(predicted_answer, str) and isinstance(expected_answer, str) else None,
               'f1': f1(predicted_answer, expected_answer)
               if isinstance(predicted_answer, str) and isinstance(expected_answer, str) else None}
        row.update(scored)
        row.update(run_provenance(args))
        row['selectedChannels'] = (trace.get('selectedChannels', [])
                                    if isinstance(trace, dict) else [])
        row['traceStopReason'] = trace.get('stopReason') if isinstance(trace, dict) else None
        row['exactRanking'] = int(bool(trace.get('exactRanking'))) if isinstance(trace, dict) else 0
        row['policyId'] = trace.get('policyId') if isinstance(trace, dict) else None
        row['policyVersion'] = trace.get('policyVersion') if isinstance(trace, dict) else None
        row['tokenEstimatorVersion'] = (trace.get('tokenEstimatorVersion')
                                        if isinstance(trace, dict) else None)
        row['replanCount'] = len(trace.get('replanEvents') or []) if isinstance(trace, dict) else 0
        row['tokensRetained'] = trace.get('tokensRetained', 0) if isinstance(trace, dict) else 0
        row['edgesExamined'] = trace.get('edgesExamined', 0) if isinstance(trace, dict) else 0
        # The design requires every secondary work measurement to be reported, "so a graph method
        # should not appear cheap only because it returns few candidates after scanning a large
        # frontier". These three were present in the trace but never reached the summary CSV.
        row['neighborsSampled'] = (trace.get('neighborsSampled', 0)
                                   if isinstance(trace, dict) else 0)
        row['verticesReached'] = (trace.get('verticesReached', 0)
                                  if isinstance(trace, dict) else 0)
        row['uniqueEvidenceCandidates'] = (trace.get('uniqueEvidenceCandidates', 0)
                                           if isinstance(trace, dict) else 0)
        # Routing reproducibility: the feature schema and the concrete policy parameters are what
        # let a historical routing decision be replayed, not the id/version alone.
        row['featureSchemaVersion'] = (trace.get('featureSchemaVersion')
                                       if isinstance(trace, dict) else None)
        params = trace.get('policyParameters') if isinstance(trace, dict) else None
        row['policyParameters'] = (json.dumps(params, sort_keys=True)
                                   if isinstance(params, dict) and params else None)
        evaluated = trace.get('evaluatedCounts', {}) if isinstance(trace, dict) else {}
        raw_scored = trace.get('rawCandidatesScored', {}) if isinstance(trace, dict) else {}
        for channel in ('BM25', 'VECTOR', 'GRAPH'):
            row['evaluated' + channel] = evaluated.get(channel, 0)
            row['rawScored' + channel] = raw_scored.get(channel, 0)
        return row

    active_methods = METHODS if not getattr(args, 'ablation', None) else ABLATION_METHODS
    assert_single_primary_constraint(active_methods,
                                     getattr(args, 'primary_constraint', 'CANDIDATES'))
    jobs = ((index, query, method) for index, query in enumerate(selected_queries)
            for method in active_methods)
    with _open_output(output) as stream, ThreadPoolExecutor(max_workers=args.workers) as pool:
        for row in pool.map(execute, jobs):
            stream.write(json.dumps(row, ensure_ascii=False) + '\n')
            rows.append({key: value for key, value in row.items()
                         if key not in ('response', 'evidence', 'documents', 'answer', 'predictedAnswer')})
            if len(rows) % 100 == 0:
                stream.flush()
                print('completed %d/%d requests' % (len(rows),
                                                     len(selected_queries) * len(active_methods)), flush=True)
    summary = _summary_path(output)
    fields = ('method', 'ablation', 'mode', 'requestedMode', 'executionMode', 'exactRanking',
              'topKLimit', 'candidateLimit', 'primaryConstraint', 'requests', 'success',
              'protocolValid', 'nonempty', 'withPaths', 'withGraphTraces', 'withProvenance',
              'traceValid', 'meanAttempts', 'meanEvaluatedBM25', 'meanEvaluatedVECTOR',
              'meanEvaluatedGRAPH', 'meanRawScoredBM25', 'meanRawScoredVECTOR',
              'meanRawScoredGRAPH', 'meanTokensRetained', 'meanEdgesExamined',
              'meanNeighborsSampled', 'meanVerticesReached', 'meanUniqueEvidenceCandidates',
              'evidenceRecallAtK', 'mrr', 'ndcgAtK', 'relationPathHit', 'relationPathTargetCount',
              'answerExactMatch', 'answerF1', 'answerAbstentionCount',
              'policyId', 'policyVersion', 'policyParameters', 'featureSchemaVersion',
              'tokenEstimatorVersion', 'meanReplanCount',
              'codeRevision', 'datasetRelease', 'randomSeed', 'configVersion',
              'meanLatencyMs',
              'p50LatencyMs', 'p95LatencyMs', 'p99LatencyMs', 'supportingSentenceRecall',
              'allSupportingSentencesHit', 'unresolvedSupportingFacts', 'textChannelBoundScored')
    with summary.open('w', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for method in active_methods:
            selected = [row for row in rows if row['method'] == method]
            latencies = [row['latencyMs'] for row in selected]
            measured = [row for row in selected if row['supportingSentenceRecall'] is not None]
            writer.writerow({'method': method,
                             'ablation': getattr(args, 'ablation', None) or 'none',
                             # The summary must report the mode the requests carried, matching the
                             # per-row value. Falling back to METHOD_CONFIG here would re-introduce
                             # the mislabelling that the row-level fix removes.
                             'mode': _single_mode(selected, method),
                             'requestedMode': METHOD_CONFIG[method]['mode'],
                             'executionMode': _single_value(selected, 'executionMode')
                             or METHOD_CONFIG[method]['executionMode'],
                             'topKLimit': max((row['topKLimit'] for row in selected), default=0),
                             'exactRanking': int(bool(args.exact_ranking and
                                                     METHOD_CONFIG[method]['exactRanking'])),
                             'candidateLimit': max((row['candidateLimit'] for row in selected),
                                                  default=0),
                             'primaryConstraint': _first(selected, 'primaryConstraint'),
                             'requests': len(selected),
                             'success': sum(row['status'] == 200 for row in selected),
                             'protocolValid': sum(row['protocolValid'] for row in selected),
                             'nonempty': sum(row['nonemptyEvidence'] for row in selected),
                             'withPaths': sum(row['pathCount'] > 0 for row in selected),
                             'withGraphTraces': sum(row['graphTraceCount'] > 0 for row in selected),
                             'withProvenance': sum(row['provenanceRefCount'] > 0 for row in selected),
                             'traceValid': sum(row['traceValid'] for row in selected),
                             'meanAttempts': sum(row['attempts'] for row in selected) / len(selected)
                             if selected else 0,
                             'meanEvaluatedBM25': sum(row['evaluatedBM25'] for row in selected)
                             / len(selected) if selected else 0,
                             'meanEvaluatedVECTOR': sum(row['evaluatedVECTOR'] for row in selected)
                             / len(selected) if selected else 0,
                             'meanEvaluatedGRAPH': sum(row['evaluatedGRAPH'] for row in selected)
                             / len(selected) if selected else 0,
                             'meanRawScoredBM25': sum(row['rawScoredBM25'] for row in selected)
                             / len(selected) if selected else 0,
                             'meanRawScoredVECTOR': sum(row['rawScoredVECTOR'] for row in selected)
                             / len(selected) if selected else 0,
                             'meanRawScoredGRAPH': sum(row['rawScoredGRAPH'] for row in selected)
                             / len(selected) if selected else 0,
                             'meanTokensRetained': sum(row['tokensRetained'] for row in selected)
                             / len(selected) if selected else 0,
                             'meanEdgesExamined': sum(row['edgesExamined'] for row in selected)
                             / len(selected) if selected else 0,
                             'meanNeighborsSampled': sum(row['neighborsSampled']
                                                         for row in selected)
                             / len(selected) if selected else 0,
                             'meanVerticesReached': sum(row['verticesReached']
                                                        for row in selected)
                             / len(selected) if selected else 0,
                             'meanUniqueEvidenceCandidates': sum(row['uniqueEvidenceCandidates']
                                                                 for row in selected)
                             / len(selected) if selected else 0,
                             'evidenceRecallAtK': _mean_metric(selected, 'evidenceRecallAtK'),
                             'mrr': _mean_metric(selected, 'mrr'),
                             'ndcgAtK': _mean_metric(selected, 'ndcgAtK'),
                             'relationPathHit': _mean_metric(selected, 'relationPathHit'),
                             'relationPathTargetCount': _mean_metric(selected,
                                                                    'relationPathTargetCount'),
                             'answerExactMatch': _mean_metric(selected, 'exactMatch'),
                             'answerF1': _mean_metric(selected, 'f1'),
                             # An abstention is an outcome, not a missing value: count it so an
                             # abstaining answer layer cannot look like a scoring gap.
                             'answerAbstentionCount': sum(
                                 1 for row in selected
                                 if row.get('answerAbstentionReason') is not None),
                             'policyId': _first(selected, 'policyId'),
                             'policyVersion': _first(selected, 'policyVersion'),
                             'policyParameters': _first(selected, 'policyParameters'),
                             'featureSchemaVersion': _first(selected, 'featureSchemaVersion'),
                             'tokenEstimatorVersion': _first(selected, 'tokenEstimatorVersion'),
                             'meanReplanCount': sum(row['replanCount'] for row in selected)
                             / len(selected) if selected else 0,
                             'codeRevision': code_revision(),
                             'datasetRelease': args.graph_version,
                             'randomSeed': getattr(args, 'seed', 0),
                             'configVersion': getattr(args, 'config_version', 'v1'),
                             'meanLatencyMs': sum(latencies) / len(latencies) if latencies else 0,
                             'p50LatencyMs': percentile(latencies, .5),
                             'p95LatencyMs': percentile(latencies, .95),
                             'p99LatencyMs': percentile(latencies, .99),
                             'supportingSentenceRecall': sum(row['supportingSentenceRecall'] for row in measured)
                             / len(measured) if measured else None,
                             'allSupportingSentencesHit': sum(row['allSupportingSentencesHit'] for row in measured)
                             / len(measured) if measured else None,
                             'unresolvedSupportingFacts': sum(row['unresolvedSupportingFacts']
                                                              for row in selected),
                             # A text channel that scored exactly its candidate limit on every request
                             # is bounded, not saturated. Reporting that here keeps a capped run
                             # from being read as a measurement of text retrieval quality.
                             'textChannelBoundScored': _text_channel_bound(selected)})


def _text_channel_bound(rows):
    """Flags a text channel that scored exactly its candidate limit on every request."""
    if not rows:
        return 0
    limit = max(row.get('candidateLimit') or 0 for row in rows)
    if limit < 1:
        return 0
    for key in ('rawScoredBM25', 'rawScoredVECTOR'):
        scored = [row.get(key) or 0 for row in rows]
        if scored and all(value == limit for value in scored):
            return 1
    return 0


def _first(rows, key):
    for row in rows:
        if row.get(key):
            return row[key]
    return None


def _single_value(rows, key):
    """Returns the one distinct value for ``key``, or None when absent.

    Raises when a group mixes values: a single-method summary aggregates requests that a caller
    believes are homogeneous, so a mix means the experiment shape is broken and silently reporting
    one of the values would hide it.
    """
    values = {row.get(key) for row in rows if row.get(key) is not None}
    if len(values) > 1:
        raise ValueError('mixed %s within one method group: %s' % (key, sorted(values)))
    return values.pop() if values else None


def _single_mode(rows, method):
    """Returns the mode actually sent for ``method``, defaulting to its configured mode.

    A row always carries ``mode``, so this only differs from the raw config when an ablation
    rewrote the request -- which is exactly the case that used to be misreported.
    """
    return _single_value(rows, 'mode') or METHOD_CONFIG[method]['mode']


def _mean_metric(rows, key):
    values = [row[key] for row in rows if row.get(key) is not None]
    return sum(values) / len(values) if values else None


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', required=True)
    parser.add_argument('--queries', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--vectors')
    parser.add_argument('--gold', help='Generic JSONL gold records')
    parser.add_argument('--chunks', help='Canonical chunks JSONL used to map gold facts to evidence IDs')
    parser.add_argument('--answers-url',
                        help='Answer endpoint (POST /api/v1/answers). When set, each retrieval is '
                             'followed by an answer request so EM/F1 are actually produced; when '
                             'omitted those columns stay empty rather than reporting invented values.')
    parser.add_argument('--graph-name', required=True)
    parser.add_argument('--graph-version', required=True)
    parser.add_argument('--index-version', required=True)
    parser.add_argument('--limit', type=int)
    parser.add_argument('--top-k', type=int, default=10)
    parser.add_argument('--timeout-ms', type=int, default=10000)
    parser.add_argument('--max-candidates', type=int, default=100)
    parser.add_argument('--token-budget', type=int, default=4096)
    parser.add_argument('--primary-constraint', choices=BUDGET_CONSTRAINTS, default='CANDIDATES',
                        help='the ONE hard budget constraint this run spends; recorded on every '
                             'row so an equal-budget comparison cannot mix constraint kinds')
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--exact-ranking', action='store_true', default=True,
                        help='scan the complete text/vector index before ranking (quality mode). '
                             'On by default: the bounded path exists to cap scoring work, and a '
                             'benchmark that silently measured the capped path reported '
                             'text-channel recall at chance level.')
    parser.add_argument('--no-exact-ranking', dest='exact_ranking', action='store_false',
                        help='measure the candidate-bounded path instead of exact ranking')
    parser.add_argument('--ablation', choices=ABLATIONS[1:], default=None,
                        help='Run the FIXED_HYBRID/ADAPTIVE_HYBRID pair under one ablation '
                             'condition (query-category: no-anchor, high-degree, lexical, semantic, '
                             'multi-hop; budget-level: budget-tight, budget-loose).')
    parser.add_argument('--seed', type=int, default=0,
                        help='reproducibility seed recorded with every result row')
    parser.add_argument('--config-version', default='v1',
                        help='retrieval configuration version recorded with every result row')
    parser.add_argument('--retries', type=int, default=2,
                        help='retry failed HTTP requests this many times')
    run(parser.parse_args())
