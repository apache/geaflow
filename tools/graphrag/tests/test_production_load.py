from argparse import Namespace
import gzip
import json

from tools.graphrag.production_load import (METHODS, _evaluate_slo, _failure_drills,
                                            _select_target_queries, _write_report, request_body,
                                            schedule_offsets, summarize_phase, validate_response)


def test_open_loop_schedule_uses_fixed_arrival_offsets():
    assert schedule_offsets(2, 2.1) == [0.0, 0.5, 1.0, 1.5, 2.0]
    assert schedule_offsets(5, 2) == [index / 5 for index in range(10)]
    assert schedule_offsets(0, 2) == []


def test_request_matrix_is_bounded_and_tracks_overfetch_separately():
    assert len(METHODS) == 6
    common = dict(graph_name='g', graph_version='gv', index_version='iv', query='question',
                  query_vector=[1.0, 0.0], top_k=5, timeout_ms=1000, max_candidates=50,
                  token_budget=100, primary_constraint='CANDIDATES', request_id='run:1')

    fixed = request_body(method='FIXED_HYBRID', **common)
    adaptive = request_body(method='ADAPTIVE_HYBRID', **common)
    parallel = request_body(method='PARALLEL_HYBRID', **common)
    overfetch = request_body(method='VECTOR_OVERFETCH', **common)

    assert fixed['executionMode'] == 'CASCADED'
    assert adaptive['executionMode'] == 'SEQUENTIAL'
    assert parallel['executionMode'] == 'PARALLEL'
    assert overfetch['budget']['maxCandidates'] == 200
    assert all(body['exactRanking'] is False for body in (fixed, adaptive, parallel, overfetch))
    assert all(body['budget']['primaryConstraint'] == 'CANDIDATES'
               for body in (fixed, adaptive, parallel, overfetch))


def test_response_validation_checks_trace_versions_and_hard_budget():
    request = request_body(
        graph_name='g', graph_version='gv', index_version='iv', query='q', method='ADAPTIVE_HYBRID',
        query_vector=[1.0, 0.0], top_k=5, timeout_ms=1000, max_candidates=50,
        token_budget=100, primary_constraint='TOKENS', request_id='run:1')
    response = {
        'requestId': 'run:1', 'graphName': 'g', 'graphVersion': 'gv',
        'evidence': [], 'paths': [], 'sources': [], 'degradedChannels': [],
        'effectiveBudget': {'tokenBudget': 100},
        'trace': {'traceVersion': 'v1', 'requestId': 'run:1', 'selectedMode': 'HYBRID',
                  'indexVersion': 'iv', 'tokensRetained': 100, 'elapsedNanos': 1000000,
                  'totalCandidatesEvaluated': 50},
    }
    assert validate_response(200, response, request) == []

    response['trace']['tokensRetained'] = 101
    assert 'token_budget_exceeded' in validate_response(200, response, request)
    assert 'request_id_mismatch' in validate_response(
        200, {**response, 'requestId': 'wrong'}, request)


def test_typed_retrieval_timeout_is_a_valid_error_envelope():
    request = request_body(
        graph_name='g', graph_version='gv', index_version='iv', query='q', method='BM25_ONLY',
        query_vector=None, top_k=5, timeout_ms=1000, max_candidates=50, token_budget=100,
        primary_constraint='CANDIDATES', request_id='run:1')

    assert validate_response(504, {'requestId': 'run:1', 'code': 'RETRIEVAL_TIMEOUT'}, request) == []
    assert validate_response(504, {'requestId': 'wrong', 'code': 'RETRIEVAL_TIMEOUT'}, request)


def test_failure_drills_require_stale_graph_version_rejection():
    class Worker:
        def post(self, body, scheduled_at):
            code = 'INDEX_NOT_READY' if body['graphVersion'].startswith('stale_') else 'INVALID_REQUEST'
            return {'status': 400 if code == 'INVALID_REQUEST' else 503,
                    'response': {'code': code}}

    args = Namespace(graph_name='g', graph_version='gv', index_version='iv', top_k=5,
                     timeout_ms=1000, max_candidates=50, token_budget=100)
    drills = _failure_drills(Worker(), args, 'run')

    assert all(drill['passed'] for drill in drills)
    assert next(drill for drill in drills if drill['drill'] == 'stale_version')['expectedCode'] \
        == 'INDEX_NOT_READY'


def test_failure_drill_does_not_accept_a_successful_stale_version():
    class Worker:
        def post(self, body, scheduled_at):
            return {'status': 200, 'response': {'code': 'OK'}}

    args = Namespace(graph_name='g', graph_version='gv', index_version='iv', top_k=5,
                     timeout_ms=1000, max_candidates=50, token_budget=100)

    assert not next(drill for drill in _failure_drills(Worker(), args, 'run')
                    if drill['drill'] == 'stale_version')['passed']


def test_target_query_selection_reads_appended_gzip_members(tmp_path):
    trace = tmp_path / 'results.jsonl.gz'
    first = {'queryId': 'q1', 'method': 'GRAPH_ONLY', 'status': 200,
             'response': {'trace': {'anchorsConsidered': 0, 'edgesExamined': 2}}}
    second = {'queryId': 'q2', 'method': 'GRAPH_ONLY', 'status': 200,
              'response': {'trace': {'anchorsConsidered': 1, 'edgesExamined': 20}}}
    with gzip.open(trace, 'wt', encoding='utf-8') as stream:
        stream.write(json.dumps(first) + '\n')
    with gzip.open(trace, 'at', encoding='utf-8') as stream:
        stream.write(json.dumps(second) + '\n')

    selected = _select_target_queries(trace, [
        {'id': 'q1', 'question': 'first', 'supporting_facts': []},
        {'id': 'q2', 'question': 'second question', 'supporting_facts': ['a', 'b']},
    ])

    assert selected['no_anchor']['id'] == 'q1'
    assert selected['high_graph_work']['id'] == 'q2'
    assert selected['multi_hop']['id'] == 'q2'


def test_report_surfaces_transport_and_protocol_failures(tmp_path):
    report = tmp_path / 'report.md'
    manifest = {'dataset': '2wiki', 'datasetRelease': 'official', 'split': 'dev',
                'graphVersion': 'v1'}
    summary = {'method': 'VECTOR_ONLY', 'phase': 'steady_50_qps', 'targetQps': 50,
               'issued': 10, 'scheduled': 10, 'success': 9, 'transportFailure': 1,
               'unexpected5xx': 0, 'protocolInvalid': 1, 'clientRejected': 0,
               'typedTimeout': 0, 'p95LatencyMs': 5, 'p99LatencyMs': 7}
    acceptance = {'passed': False, 'failures': ['VECTOR_ONLY/steady_50_qps']}

    _write_report(report, manifest, [summary], {}, [], acceptance)

    text = report.read_text(encoding='utf-8')
    assert 'Transport failures' in text
    assert 'Protocol invalid' in text
    assert '| VECTOR_ONLY / steady_50_qps | 50 | 10 / 10 | 9 | 1 | 0 | 1 |' in text


def test_phase_summary_distinguishes_target_misses_from_server_failures():
    rows = [
        {'status': 200, 'protocolValid': 1, 'latencyMs': 10.0, 'queueDelayMs': 1.0},
        {'status': 504, 'protocolValid': 0, 'latencyMs': 1000.0, 'queueDelayMs': 3.0,
         'errorCode': 'RETRIEVAL_TIMEOUT'},
        {'status': -1, 'protocolValid': 0, 'latencyMs': None, 'queueDelayMs': None,
         'errorCode': 'MAX_INFLIGHT'},
    ]
    summary = summarize_phase(rows, target_qps=20, scheduled=3)
    assert summary['scheduled'] == 3
    assert summary['issued'] == 2
    assert summary['success'] == 1
    assert summary['typedTimeout'] == 1
    assert summary['clientRejected'] == 1
    assert summary['p99LatencyMs'] == 990.1


def test_slo_is_unconfigured_without_thresholds():
    summary = {'method': 'VECTOR_ONLY', 'phase': 'steady', 'p95LatencyMs': 900.0,
               'p99LatencyMs': 1500.0}
    slo = _evaluate_slo([summary], None, None)
    assert slo['latencySloConfigured'] is False
    assert slo['sloViolations'] == []


def test_slo_breach_is_reported_per_phase():
    summary = {'method': 'VECTOR_ONLY', 'phase': 'steady_50_qps', 'p95LatencyMs': 900.0,
               'p99LatencyMs': 1500.0}
    slo = _evaluate_slo([summary], 200.0, 500.0)
    assert slo['latencySloConfigured'] is True
    assert any('p95' in v for v in slo['sloViolations'])
    assert any('p99' in v for v in slo['sloViolations'])


def test_slo_pass_reports_no_violations():
    summary = {'method': 'VECTOR_ONLY', 'phase': 'steady_50_qps', 'p95LatencyMs': 120.0,
               'p99LatencyMs': 300.0}
    slo = _evaluate_slo([summary], 200.0, 500.0)
    assert slo['latencySloConfigured'] is True
    assert slo['sloViolations'] == []


def _worker_with_stub(monkeypatch, outcomes):
    """Returns an HttpWorker whose transport plays back `outcomes` in order."""
    from tools.graphrag.production_load import HttpWorker

    worker = HttpWorker('http://127.0.0.1:1')
    calls = []

    def fake_send(body, payload, row):
        outcome = outcomes[len(calls)]
        calls.append(outcome)
        if isinstance(outcome, BaseException):
            row['error'] = '%s: %s' % (type(outcome).__name__, outcome)
            raise outcome
        row.update(status=200, response={'ok': True})

    monkeypatch.setattr(worker, '_send', fake_send)
    monkeypatch.setattr(worker, '_close', lambda: None)
    body = {'requestId': 'r1', 'budget': {'timeoutMs': 1000}}
    return worker, body, calls


def test_stale_pooled_connection_is_retried_once(monkeypatch):
    """A keep-alive socket the server already closed must not be counted as a failed request.

    The server's idle timeout closes pooled connections. Without a reconnect, the client reports
    RemoteDisconnected and the run fails acceptance on transport failures even though every
    request actually succeeded -- which is what happened on the first SLO run.
    """
    import http.client

    worker, body, calls = _worker_with_stub(
        monkeypatch, [http.client.RemoteDisconnected('Remote end closed connection'), None])

    row = worker.post(body, 0.0)

    assert len(calls) == 2, 'a stale connection must be retried exactly once'
    assert row['status'] == 200
    assert row.get('staleConnectionRetried') is True
    assert 'error' not in row, 'a successful retry must not keep the transport error'
    assert row['completed'] is True


def test_retried_request_still_fails_when_the_reconnect_also_fails(monkeypatch):
    """A genuine transport failure must be reported per request, never raised.

    Raising would abort the entire load run instead of counting one failed request.
    """
    import http.client

    worker, body, calls = _worker_with_stub(
        monkeypatch, [http.client.RemoteDisconnected('closed'), http.client.RemoteDisconnected('closed')])

    row = worker.post(body, 0.0)

    assert len(calls) == 2
    assert row['status'] == 0
    assert 'after reconnect' in row['error']
    assert row['completed'] is True


def test_non_stale_transport_error_is_not_retried(monkeypatch):
    """Only a stale pooled connection is retried; other transport errors surface immediately."""
    import http.client

    worker, body, calls = _worker_with_stub(
        monkeypatch, [http.client.BadStatusLine('garbage')])

    try:
        worker.post(body, 0.0)
    except http.client.BadStatusLine:
        pass
    else:
        raise AssertionError('a non-stale transport error must propagate')
    assert len(calls) == 1
