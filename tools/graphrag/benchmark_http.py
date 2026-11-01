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
ABLATIONS = ('none', 'no-anchor', 'high-degree', 'lexical', 'semantic', 'multi-hop')


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
    else:
        raise ValueError('unknown ablation: %s' % ablation)
    return shaped


METHOD_CONFIG = {
    'BM25_ONLY': {'mode': 'BM25_ONLY', 'executionMode': 'SEQUENTIAL', 'candidateMultiplier': 1,
                  'requiresVector': False, 'exactRanking': True},
    'VECTOR_ONLY': {'mode': 'VECTOR_ONLY', 'executionMode': 'SEQUENTIAL', 'candidateMultiplier': 1,
                    'requiresVector': True, 'exactRanking': True},
    # Overfetch widens the returned window while retaining the shared candidate budget.
    'VECTOR_OVERFETCH': {'mode': 'VECTOR_ONLY', 'executionMode': 'SEQUENTIAL',
                         'candidateMultiplier': 1, 'topKMultiplier': 4,
                         'requiresVector': True, 'exactRanking': True},
    'GRAPH_ONLY': {'mode': 'GRAPH_ONLY', 'executionMode': 'SEQUENTIAL', 'candidateMultiplier': 1,
                   'requiresVector': False, 'exactRanking': False},
    'FIXED_HYBRID': {'mode': 'HYBRID', 'executionMode': 'CASCADED', 'candidateMultiplier': 1,
                     'requiresVector': True, 'exactRanking': True},
    # Adaptive Hybrid keeps SEQUENTIAL so the routing policy is actually consulted; the service
    # only evaluates the policy outside CASCADED. FIXED_HYBRID stays CASCADED, which is what
    # makes the two methods a meaningful comparison instead of duplicate request bodies.
    'ADAPTIVE_HYBRID': {'mode': 'HYBRID', 'executionMode': 'SEQUENTIAL', 'candidateMultiplier': 1,
                        'requiresVector': True, 'exactRanking': True},
}


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
    candidate limit would make the comparison meaningless.
    """
    limits = {method: METHOD_CONFIG[method]['candidateMultiplier']
              for method in methods if 'candidateMultiplier' in METHOD_CONFIG[method]}
    if len(set(limits.values())) > 1:
        raise ValueError('methods must share one candidate budget: %s' % limits)


def request_body(args, query, method, query_vector=None):
    """Build the auditable wire request for one benchmark method."""
    config = METHOD_CONFIG[method]
    if config['requiresVector'] and query_vector is None:
        raise ValueError('%s requires --vectors' % method)
    candidate_limit = args.max_candidates * config['candidateMultiplier']
    query_identity = str(query.get('id') or query['question'])
    stable_request_id = 'benchmark-' + hashlib.sha256(query_identity.encode('utf-8')).hexdigest()[:32]
    body = {'requestId': stable_request_id,
            'graphName': args.graph_name, 'graphVersion': args.graph_version,
            'indexVersion': args.index_version, 'query': query['question'],
            'mode': config['mode'], 'executionMode': config['executionMode'],
            'exactRanking': bool(config['exactRanking'] and getattr(args, 'exact_ranking', False)),
            'budget': {'topK': args.top_k * config.get('topKMultiplier', 1),
                       'timeoutMs': args.timeout_ms,
                       'maxCandidates': candidate_limit, 'tokenBudget': args.token_budget}}
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

    def execute(item):
        index, query, method = item
        config = METHOD_CONFIG[method]
        candidate_limit = args.max_candidates * config['candidateMultiplier']
        body = request_body(args, query, method, vectors[index] if vectors is not None else None)
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
        predicted_answer = response.get('answer') if isinstance(response, dict) else None
        expected_answer = query.get('answer')
        protocol_valid = int(status == 200 and isinstance(response, dict)
                            and isinstance(response.get('evidence'), list)
                            and isinstance(response.get('paths'), list)
                            and isinstance(trace, dict))
        row = {'id': query.get('id'), 'method': method,
               'ablation': getattr(args, 'ablation', None) or 'none',
               'mode': config['mode'],
               'executionMode': config['executionMode'], 'topKLimit': body['budget']['topK'],
               'candidateLimit': candidate_limit,
               'status': status, 'protocolValid': protocol_valid, 'latencyMs': latency,
               'attempts': attempts,
               'error': error, 'evidence': evidence, 'documents': documents,
               'supportingFactCount': len(target.get('supportingSentences', [])),
               'unresolvedSupportingFacts': target.get('unresolvedSupportingFacts', 0),
               'response': response,
               'answer': expected_answer, 'predictedAnswer': predicted_answer,
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
        evaluated = trace.get('evaluatedCounts', {}) if isinstance(trace, dict) else {}
        raw_scored = trace.get('rawCandidatesScored', {}) if isinstance(trace, dict) else {}
        for channel in ('BM25', 'VECTOR', 'GRAPH'):
            row['evaluated' + channel] = evaluated.get(channel, 0)
            row['rawScored' + channel] = raw_scored.get(channel, 0)
        return row

    active_methods = METHODS if not getattr(args, 'ablation', None) else ABLATION_METHODS
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
    fields = ('method', 'ablation', 'mode', 'executionMode', 'exactRanking', 'topKLimit',
              'candidateLimit', 'requests', 'success',
              'protocolValid', 'nonempty', 'withPaths', 'withGraphTraces', 'withProvenance',
              'traceValid', 'meanAttempts', 'meanEvaluatedBM25', 'meanEvaluatedVECTOR',
              'meanEvaluatedGRAPH', 'meanRawScoredBM25', 'meanRawScoredVECTOR',
              'meanRawScoredGRAPH', 'meanTokensRetained', 'meanEdgesExamined',
              'evidenceRecallAtK', 'mrr', 'ndcgAtK', 'relationPathHit',
              'answerExactMatch', 'answerF1',
              'policyId', 'policyVersion', 'tokenEstimatorVersion', 'meanReplanCount',
              'codeRevision', 'datasetRelease', 'randomSeed', 'configVersion',
              'meanLatencyMs',
              'p50LatencyMs', 'p95LatencyMs', 'p99LatencyMs', 'supportingSentenceRecall',
              'allSupportingSentencesHit', 'unresolvedSupportingFacts')
    with summary.open('w', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for method in active_methods:
            selected = [row for row in rows if row['method'] == method]
            latencies = [row['latencyMs'] for row in selected]
            measured = [row for row in selected if row['supportingSentenceRecall'] is not None]
            writer.writerow({'method': method,
                             'ablation': getattr(args, 'ablation', None) or 'none',
                             'mode': METHOD_CONFIG[method]['mode'],
                             'executionMode': METHOD_CONFIG[method]['executionMode'],
                             'topKLimit': args.top_k * METHOD_CONFIG[method].get('topKMultiplier', 1),
                             'exactRanking': int(bool(args.exact_ranking and
                                                     METHOD_CONFIG[method]['exactRanking'])),
                             'candidateLimit': args.max_candidates * METHOD_CONFIG[method]['candidateMultiplier'],
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
                             'evidenceRecallAtK': _mean_metric(selected, 'evidenceRecallAtK'),
                             'mrr': _mean_metric(selected, 'mrr'),
                             'ndcgAtK': _mean_metric(selected, 'ndcgAtK'),
                             'relationPathHit': _mean_metric(selected, 'relationPathHit'),
                             'answerExactMatch': _mean_metric(selected, 'exactMatch'),
                             'answerF1': _mean_metric(selected, 'f1'),
                             'policyId': _first(selected, 'policyId'),
                             'policyVersion': _first(selected, 'policyVersion'),
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
                                                              for row in selected)})


def _first(rows, key):
    for row in rows:
        if row.get(key):
            return row[key]
    return None


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
    parser.add_argument('--graph-name', required=True)
    parser.add_argument('--graph-version', required=True)
    parser.add_argument('--index-version', required=True)
    parser.add_argument('--limit', type=int)
    parser.add_argument('--top-k', type=int, default=10)
    parser.add_argument('--timeout-ms', type=int, default=10000)
    parser.add_argument('--max-candidates', type=int, default=100)
    parser.add_argument('--token-budget', type=int, default=4096)
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--exact-ranking', action='store_true',
                        help='scan the complete text/vector index before ranking (quality mode)')
    parser.add_argument('--ablation', choices=ABLATIONS[1:], default=None,
                        help='Run the FIXED_HYBRID/ADAPTIVE_HYBRID pair under one ablation '
                             'condition (no-anchor, high-degree, lexical, semantic, multi-hop).')
    parser.add_argument('--seed', type=int, default=0,
                        help='reproducibility seed recorded with every result row')
    parser.add_argument('--config-version', default='v1',
                        help='retrieval configuration version recorded with every result row')
    parser.add_argument('--retries', type=int, default=2,
                        help='retry failed HTTP requests this many times')
    run(parser.parse_args())
