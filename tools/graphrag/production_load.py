"""Open-loop HTTP load testing for the versioned GraphRAG retrieval service."""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import http.client
import json
import math
import os
import platform
import random
import re
import socket
import subprocess
import sys
import threading
import time
import urllib.parse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from tools.graphrag.eval.metrics import percentile
from tools.graphrag.eval.retrieval import load_query_vectors, load_supporting_sentences, score_evidence


METHODS = ('BM25_ONLY', 'VECTOR_ONLY', 'VECTOR_OVERFETCH', 'GRAPH_ONLY',
           'FIXED_HYBRID', 'ADAPTIVE_HYBRID')

METHOD_CONFIG = {
    'BM25_ONLY': {'mode': 'BM25_ONLY', 'executionMode': 'SEQUENTIAL', 'multiplier': 1,
                  'requiresVector': False},
    'VECTOR_ONLY': {'mode': 'VECTOR_ONLY', 'executionMode': 'SEQUENTIAL', 'multiplier': 1,
                    'requiresVector': True},
    'VECTOR_OVERFETCH': {'mode': 'VECTOR_ONLY', 'executionMode': 'SEQUENTIAL', 'multiplier': 4,
                         'requiresVector': True},
    'GRAPH_ONLY': {'mode': 'GRAPH_ONLY', 'executionMode': 'SEQUENTIAL', 'multiplier': 1,
                   'requiresVector': False},
    'FIXED_HYBRID': {'mode': 'HYBRID', 'executionMode': 'CASCADED', 'multiplier': 1,
                     'requiresVector': True},
    'ADAPTIVE_HYBRID': {'mode': 'HYBRID', 'executionMode': 'SEQUENTIAL', 'multiplier': 1,
                        'requiresVector': True},
    'PARALLEL_HYBRID': {'mode': 'HYBRID', 'executionMode': 'PARALLEL', 'multiplier': 1,
                        'requiresVector': True},
}


def schedule_offsets(target_qps: float, duration_seconds: float) -> List[float]:
    """Return fixed open-loop arrival offsets; an arrival is never shifted by service latency."""
    if target_qps <= 0 or duration_seconds <= 0:
        return []
    count = int(math.ceil(target_qps * duration_seconds - 1e-12))
    return [index / target_qps for index in range(count)]


def request_body(graph_name: str, graph_version: str, index_version: str, query: str,
                 method: str, query_vector: Optional[Sequence[float]], top_k: int,
                 timeout_ms: int, max_candidates: int, token_budget: int,
                 primary_constraint: str, request_id: str) -> Dict[str, Any]:
    """Build one bounded, version-pinned request for a benchmark lane."""
    if method not in METHOD_CONFIG:
        raise ValueError('unknown method: %s' % method)
    config = METHOD_CONFIG[method]
    if config['requiresVector'] and query_vector is None:
        raise ValueError('%s requires a query vector' % method)
    if primary_constraint not in ('CANDIDATES', 'LATENCY', 'TOKENS'):
        raise ValueError('unknown primary constraint: %s' % primary_constraint)
    return {
        'requestId': request_id,
        'graphName': graph_name,
        'graphVersion': graph_version,
        'indexVersion': index_version,
        'query': query,
        'mode': config['mode'],
        'executionMode': config['executionMode'],
        'exactRanking': False,
        'budget': {
            'topK': top_k,
            'timeoutMs': timeout_ms,
            'maxCandidates': max_candidates * config['multiplier'],
            'tokenBudget': token_budget,
            'primaryConstraint': primary_constraint,
        },
        **({'queryVector': list(query_vector)} if config['requiresVector'] else {}),
    }


def validate_response(status: int, response: Any, request: Dict[str, Any]) -> List[str]:
    """Validate the v1 retrieval envelope and the request's hard primary budget."""
    failures = []
    if (status == 504 and isinstance(response, dict)
            and response.get('code') == 'RETRIEVAL_TIMEOUT'
            and response.get('requestId') == request.get('requestId')):
        return []
    if status != 200:
        return ['http_status_%s' % status]
    if not isinstance(response, dict):
        return ['response_not_object']
    for name in ('evidence', 'paths', 'sources', 'degradedChannels'):
        if not isinstance(response.get(name), list):
            failures.append('invalid_%s' % name)
    trace = response.get('trace')
    if not isinstance(trace, dict):
        return failures + ['trace_missing']
    if trace.get('traceVersion') != 'v1':
        failures.append('trace_version_invalid')
    if response.get('requestId') != request.get('requestId') or trace.get('requestId') != request.get('requestId'):
        failures.append('request_id_mismatch')
    if response.get('graphVersion') != request.get('graphVersion'):
        failures.append('graph_version_mismatch')
    if trace.get('indexVersion') != request.get('indexVersion'):
        failures.append('index_version_mismatch')
    if not isinstance(response.get('effectiveBudget'), dict):
        failures.append('effective_budget_missing')
    budget = request['budget']
    if budget['primaryConstraint'] == 'CANDIDATES':
        if trace.get('totalCandidatesEvaluated', 0) > budget['maxCandidates']:
            failures.append('candidate_budget_exceeded')
    elif budget['primaryConstraint'] == 'TOKENS':
        if trace.get('tokensRetained', 0) > budget['tokenBudget']:
            failures.append('token_budget_exceeded')
    elif trace.get('elapsedNanos', 0) > budget['timeoutMs'] * 1_000_000:
        failures.append('latency_budget_exceeded')
    return failures


def summarize_phase(rows: Sequence[Dict[str, Any]], target_qps: float,
                    scheduled: int) -> Dict[str, Any]:
    """Summarize client outcomes without conflating server errors and driver saturation."""
    issued = [row for row in rows if row.get('status') != -1]
    latencies = [row['latencyMs'] for row in issued if row.get('latencyMs') is not None]
    return {
        'targetQps': target_qps,
        'scheduled': scheduled,
        'issued': len(issued),
        'completed': len([row for row in issued if row.get('completed')]),
        'success': sum(row.get('status') == 200 for row in issued),
        'typedTimeout': sum(row.get('errorCode') == 'RETRIEVAL_TIMEOUT' for row in issued),
        'server5xx': sum(500 <= (row.get('status') or 0) < 600 for row in issued),
        'unexpected5xx': sum(500 <= (row.get('status') or 0) < 600
                             and not (row.get('status') == 504
                                      and row.get('errorCode') == 'RETRIEVAL_TIMEOUT')
                             for row in issued),
        'transportFailure': sum(row.get('status') == 0 for row in issued),
        'clientRejected': sum(row.get('status') == -1 for row in rows),
        'protocolValid': sum(row.get('protocolValid') == 1 for row in rows),
        'protocolInvalid': sum(row.get('protocolValid') != 1 for row in rows),
        'meanLatencyMs': sum(latencies) / len(latencies) if latencies else 0.0,
        'p50LatencyMs': percentile(latencies, 0.50),
        'p95LatencyMs': percentile(latencies, 0.95),
        'p99LatencyMs': percentile(latencies, 0.99),
        'maxLatencyMs': max(latencies) if latencies else 0.0,
        'meanQueueDelayMs': _mean(row.get('queueDelayMs') for row in issued),
    }


def _mean(values: Iterable[Optional[float]]) -> float:
    selected = [value for value in values if value is not None]
    return sum(selected) / len(selected) if selected else 0.0


class HttpWorker:
    """Thread-local persistent HTTP connections; failed requests are never retried."""

    def __init__(self, url: str):
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme != 'http' or not parsed.hostname:
            raise ValueError('load target must be an http:// URL')
        self.host = parsed.hostname
        self.port = parsed.port or 80
        self.path = (parsed.path.rstrip('/') or '') + '/api/v1/retrievals'
        if parsed.query:
            self.path += '?' + parsed.query
        self.local = threading.local()

    def post(self, body: Dict[str, Any], scheduled_at: float) -> Dict[str, Any]:
        payload = json.dumps(body, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
        started = time.monotonic()
        row = {'requestId': body['requestId'], 'scheduledAtMonotonic': scheduled_at,
               'startedAtMonotonic': started, 'queueDelayMs': max(0.0, started - scheduled_at) * 1000.0}
        connection = getattr(self.local, 'connection', None)
        if connection is None:
            connection = http.client.HTTPConnection(
                self.host, self.port, timeout=body['budget']['timeoutMs'] / 1000.0 + 2.0)
            self.local.connection = connection
        elif connection.sock is not None:
            connection.sock.settimeout(body['budget']['timeoutMs'] / 1000.0 + 2.0)
        try:
            connection.request('POST', self.path, payload, {
                'Content-Type': 'application/json; charset=utf-8',
                'X-Request-Id': body['requestId'],
            })
            response = connection.getresponse()
            raw = response.read()
            status = response.status
            try:
                decoded = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                decoded = {'rawBody': raw.decode('utf-8', errors='replace')}
                row['error'] = 'invalid_json: %s' % error
            row.update(status=status, response=decoded)
            if isinstance(decoded, dict):
                row['errorCode'] = decoded.get('code')
        except (OSError, http.client.HTTPException, socket.timeout) as error:
            row.update(status=0, response={}, error='%s: %s' % (type(error).__name__, error))
            self._close()
        row['finishedAtMonotonic'] = time.monotonic()
        row['latencyMs'] = (row['finishedAtMonotonic'] - started) * 1000.0
        row['completed'] = True
        return row

    def _close(self) -> None:
        connection = getattr(self.local, 'connection', None)
        if connection is not None:
            connection.close()
            self.local.connection = None


def _http_json(url: str, path: str, timeout: float = 3.0) -> Tuple[int, Any]:
    worker = HttpWorker(url)
    connection = http.client.HTTPConnection(worker.host, worker.port, timeout=timeout)
    try:
        connection.request('GET', path)
        response = connection.getresponse()
        raw = response.read()
        try:
            body = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError):
            body = raw.decode('utf-8', errors='replace')
        return response.status, body
    finally:
        connection.close()


def _read_proc(pid: int) -> Dict[str, Any]:
    stat = Path('/proc/%d/stat' % pid).read_text(encoding='utf-8')
    fields = stat[stat.rfind(')') + 2:].split()
    ticks = int(fields[11]) + int(fields[12])
    resident_bytes = int(fields[21]) * os.sysconf('SC_PAGE_SIZE')
    return {'cpuTicks': ticks, 'rssBytes': resident_bytes}


def _host_cpu() -> Tuple[int, int]:
    fields = Path('/proc/stat').read_text(encoding='ascii').splitlines()[0].split()[1:]
    values = [int(value) for value in fields]
    return sum(values), values[3] + values[4]


def _jstat(pid: int) -> Dict[str, Any]:
    try:
        result = subprocess.run(['jstat', '-gc', str(pid)], capture_output=True, text=True,
                                timeout=2.0, check=False)
        lines = result.stdout.strip().splitlines()
        if result.returncode != 0 or len(lines) < 2:
            return {}
        names = lines[0].split()
        values = lines[1].split()
        stats = dict(zip(names, (float(value) for value in values)))
        used_kb = sum(stats.get(name, 0.0) for name in ('S0U', 'S1U', 'EU', 'OU'))
        return {'heapUsedMB': used_kb / 1024.0, 'oldUsedKB': stats.get('OU', 0.0),
                'youngGcCount': stats.get('YGC', 0), 'youngGcTimeSec': stats.get('YGCT', 0),
                'fullGcCount': stats.get('FGC', 0), 'fullGcTimeSec': stats.get('FGCT', 0)}
    except (OSError, ValueError, subprocess.TimeoutExpired):
        return {}


class ResourceSampler(threading.Thread):
    """Sample Linux host/process counters and JVM GC/heap statistics during a run."""

    FIELDS = ('timestampUtc', 'elapsedSeconds', 'hostCpuPercent', 'hostMemAvailableMB',
              'processCpuPercent', 'processRssMB', 'heapUsedMB', 'oldUsedKB',
              'youngGcCount', 'youngGcTimeSec', 'fullGcCount', 'fullGcTimeSec')

    def __init__(self, pid: int, interval: float):
        super().__init__(daemon=True)
        self.pid = pid
        self.interval = interval
        self.stop_event = threading.Event()
        self.rows: List[Dict[str, Any]] = []
        self.error: Optional[str] = None

    def stop(self) -> None:
        self.stop_event.set()

    def run(self) -> None:
        started = time.monotonic()
        previous_proc = None
        previous_host = None
        previous_at = None
        last_jstat = 0.0
        jvm = {}
        while not self.stop_event.is_set():
            now = time.monotonic()
            try:
                process = _read_proc(self.pid)
                host = _host_cpu()
                meminfo = Path('/proc/meminfo').read_text(encoding='ascii')
                available_kb = int(next(line.split()[1] for line in meminfo.splitlines()
                                        if line.startswith('MemAvailable:')))
                elapsed = max(1e-9, now - previous_at) if previous_at is not None else None
                process_cpu = None
                host_cpu = None
                if elapsed and previous_proc and previous_host:
                    ticks = os.sysconf('SC_CLK_TCK')
                    process_cpu = 100.0 * (process['cpuTicks'] - previous_proc['cpuTicks']) / ticks / elapsed
                    host_total = host[0] - previous_host[0]
                    host_idle = host[1] - previous_host[1]
                    host_cpu = 100.0 * (host_total - host_idle) / host_total if host_total else 0.0
                if now - last_jstat >= 5.0:
                    jvm = _jstat(self.pid)
                    last_jstat = now
                self.rows.append({
                    'timestampUtc': datetime.now(timezone.utc).isoformat(),
                    'elapsedSeconds': now - started,
                    'hostCpuPercent': host_cpu,
                    'hostMemAvailableMB': available_kb / 1024.0,
                    'processCpuPercent': process_cpu,
                    'processRssMB': process['rssBytes'] / (1024.0 * 1024.0),
                    **jvm,
                })
                previous_proc, previous_host, previous_at = process, host, now
            except (OSError, StopIteration, ValueError) as error:
                self.error = '%s: %s' % (type(error).__name__, error)
                break
            self.stop_event.wait(self.interval)


def _drain(futures: Dict[Any, Dict[str, Any]], stream: Any,
           phase_rows: List[Dict[str, Any]], phase: str, method: str) -> None:
    done = [future for future in futures if future.done()]
    for future in done:
        metadata = futures.pop(future)
        row = future.result()
        request = metadata['body']
        protocol_errors = validate_response(row['status'], row.get('response'), request)
        row.update(method=method, phase=phase, queryId=metadata['queryId'],
                   targetQps=metadata['targetQps'], primaryConstraint=request['budget']['primaryConstraint'],
                   maxCandidates=request['budget']['maxCandidates'], timeoutMs=request['budget']['timeoutMs'],
                   tokenBudget=request['budget']['tokenBudget'], protocolValid=int(not protocol_errors),
                   protocolErrors=protocol_errors)
        row.pop('startedAtMonotonic', None)
        row.pop('finishedAtMonotonic', None)
        row.pop('scheduledAtMonotonic', None)
        stream.write(json.dumps(row, ensure_ascii=False, separators=(',', ':')) + '\n')
        phase_rows.append({key: value for key, value in row.items() if key != 'response'})


def _run_phase(pool: ThreadPoolExecutor, worker: HttpWorker, queries: Sequence[Dict[str, Any]],
               vectors: Dict[str, Sequence[float]], run_id: str, method: str, phase: str,
               qps: float, duration: float, max_inflight: int, request_stream: Any,
               top_k: int, timeout_ms: int, max_candidates: int, token_budget: int,
               primary_constraint: str, graph_name: str, graph_version: str,
               index_version: str, sequence_start: int) -> Tuple[Dict[str, Any], int]:
    offsets = schedule_offsets(qps, duration)
    started = time.monotonic()
    futures: Dict[Any, Dict[str, Any]] = {}
    rows: List[Dict[str, Any]] = []
    sequence = sequence_start
    query_cursor = sequence_start
    for offset in offsets:
        target = started + offset
        while time.monotonic() < target:
            _drain(futures, request_stream, rows, phase, method)
            time.sleep(min(0.005, max(0.0, target - time.monotonic())))
        _drain(futures, request_stream, rows, phase, method)
        query = queries[query_cursor % len(queries)]
        query_cursor += 1
        request_id = '%s:%s:%s:%d' % (run_id, method, phase, sequence)
        sequence += 1
        method_config = method
        body = request_body(
            graph_name, graph_version, index_version, query['question'], method_config,
            vectors.get(str(query['id'])), top_k, timeout_ms, max_candidates, token_budget,
            primary_constraint, request_id)
        if len(futures) >= max_inflight:
            rejected = {'requestId': request_id, 'method': method, 'phase': phase,
                        'queryId': str(query['id']), 'targetQps': qps, 'status': -1,
                        'completed': False, 'latencyMs': None, 'queueDelayMs': None,
                        'errorCode': 'MAX_INFLIGHT', 'protocolValid': 0,
                        'protocolErrors': ['client_max_inflight']}
            request_stream.write(json.dumps(rejected, ensure_ascii=False, separators=(',', ':')) + '\n')
            rows.append(rejected)
            continue
        future = pool.submit(worker.post, body, target)
        futures[future] = {'body': body, 'queryId': str(query['id']), 'targetQps': qps}
    while futures:
        done, _ = wait(tuple(futures), timeout=1.0, return_when=FIRST_COMPLETED)
        if done:
            _drain(futures, request_stream, rows, phase, method)
    return summarize_phase(rows, qps, len(offsets)), sequence


def _load_inputs(query_path: Path, vector_path: Path) -> Tuple[List[Dict[str, Any]], Dict[str, Sequence[float]]]:
    queries = [json.loads(line) for line in query_path.read_text(encoding='utf-8').splitlines()
               if line.strip()]
    if not queries:
        raise ValueError('query file is empty')
    if len({str(query['id']) for query in queries}) != len(queries):
        raise ValueError('query ids must be unique')
    vectors = load_query_vectors(vector_path, queries)
    return queries, {str(query['id']): vectors[index].tolist() for index, query in enumerate(queries)}


def _select_target_queries(path: Path, queries: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    by_id = {str(query['id']): query for query in queries}
    no_anchor = None
    high_work = None
    high_edges = -1
    with gzip.open(path, 'rt', encoding='utf-8') as stream:
        for line in stream:
            row = json.loads(line)
            if row.get('method') != 'GRAPH_ONLY' or row.get('status') != 200:
                continue
            trace = (row.get('response') or {}).get('trace') or {}
            query = by_id.get(str(row.get('queryId')))
            if query is None:
                continue
            if trace.get('anchorsConsidered', 0) == 0 and no_anchor is None:
                no_anchor = query
            edges = int(trace.get('edgesExamined', 0))
            if edges > high_edges:
                high_work, high_edges = query, edges
    if no_anchor is None:
        no_anchor = {'id': 'synthetic_no_anchor',
                     'question': 'zxqv qzxv no-such-entity 927451', 'supporting_facts': []}
    long_query = max(queries, key=lambda query: len(query['question']))
    multi_hop = next((query for query in queries if len(query.get('supporting_facts', [])) >= 2),
                     queries[0])
    return {'no_anchor': no_anchor, 'high_graph_work': high_work or queries[0],
            'long_query': long_query, 'multi_hop': multi_hop}


def _failure_drills(worker: HttpWorker, args: argparse.Namespace, run_id: str) -> List[Dict[str, Any]]:
    base = request_body(
        args.graph_name, args.graph_version, args.index_version, 'Confucius', 'BM25_ONLY', None,
        args.top_k, args.timeout_ms, args.max_candidates, args.token_budget, 'CANDIDATES',
        '%s:drill:invalid_budget' % run_id)
    drills = [
        ('empty_query', {**base, 'requestId': '%s:drill:empty_query' % run_id, 'query': ''},
         'INVALID_REQUEST'),
        ('invalid_budget', {**base, 'budget': {**base['budget'], 'maxCandidates': 0}},
         'INVALID_REQUEST'),
        ('stale_version', {**base, 'requestId': '%s:drill:stale_version' % run_id,
                           'graphVersion': 'stale_%s' % args.graph_version}, 'INDEX_NOT_READY'),
    ]
    results = []
    for name, body, expected in drills:
        row = worker.post(body, time.monotonic())
        response = row.get('response') if isinstance(row.get('response'), dict) else {}
        row['drill'] = name
        row['expectedCode'] = expected
        row['passed'] = int(expected is None or response.get('code') == expected)
        results.append(row)
    return results


def _check_ready(base_url: str, graph_name: str, graph_version: str) -> Dict[str, Any]:
    status, body = _http_json(base_url, '/ready')
    if status != 200 or not isinstance(body, dict) or body.get('status') != 'READY':
        raise RuntimeError('service is not ready: status=%s body=%s' % (status, body))
    if body.get('graphName') != graph_name or body.get('graphVersion') != graph_version:
        raise RuntimeError('service readiness version mismatch: %s' % body)
    return body


def _monitor_service(base_url: str, stop_event: threading.Event, rows: List[Dict[str, Any]],
                     interval: float) -> None:
    started = time.monotonic()
    while not stop_event.is_set():
        row: Dict[str, Any] = {'timestampUtc': datetime.now(timezone.utc).isoformat(),
                               'elapsedSeconds': time.monotonic() - started}
        for name, path in (('ready', '/ready'), ('retrievalMetrics', '/metrics/retrieval')):
            try:
                status, body = _http_json(base_url, path)
                row[name] = {'status': status, 'body': body}
            except (OSError, RuntimeError, ValueError) as error:
                row[name] = {'error': '%s: %s' % (type(error).__name__, error)}
        rows.append(row)
        stop_event.wait(interval)


def _git_metadata(root: Path) -> Dict[str, Any]:
    try:
        revision = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=root, check=True,
                                  capture_output=True, text=True, timeout=3.0).stdout.strip()
        dirty = bool(subprocess.run(['git', 'status', '--porcelain'], cwd=root, check=True,
                                    capture_output=True, text=True, timeout=3.0).stdout.strip())
        return {'revision': revision, 'dirty': dirty}
    except (OSError, subprocess.SubprocessError):
        return {'revision': 'unknown', 'dirty': None}


def _bundle_metadata(root: Path, graph_name: str, graph_version: str) -> Dict[str, Any]:
    ready_path = root / 'READY.json'
    ready_bytes = ready_path.read_bytes()
    ready = json.loads(ready_bytes)
    if ready.get('state') != 'READY' or ready.get('version') != graph_version:
        raise RuntimeError('bundle READY metadata does not match the requested version')
    if graph_name not in (ready.get('graph_name'), ready.get('dataset')):
        raise RuntimeError('bundle READY metadata does not match the graph name')
    return {'readySha256': hashlib.sha256(ready_bytes).hexdigest(),
            'artifactChecksums': ready.get('artifact_checksums', {}),
            'documents': ready.get('documents'), 'chunks': ready.get('chunks'),
            'vertices': ready.get('vertices'), 'edges': ready.get('edges'),
            'vectorSource': ready.get('vector_source'), 'vectorVersion': ready.get('vector_version')}


def _java_version(pid: int) -> Optional[str]:
    try:
        result = subprocess.run(['jcmd', str(pid), 'VM.version'], capture_output=True,
                                text=True, timeout=3.0, check=False)
        return result.stdout.strip() if result.returncode == 0 else None
    except (OSError, subprocess.SubprocessError):
        return None


def _write_csv(path: Path, rows: Sequence[Dict[str, Any]], fields: Sequence[str]) -> None:
    with path.open('w', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)


def _summarize_quality(path: Path, gold_path: Optional[Path]) -> Dict[str, Any]:
    if gold_path is None:
        return {}
    gold = load_supporting_sentences(gold_path)
    by_method: Dict[str, List[float]] = {}
    with gzip.open(path, 'rt', encoding='utf-8') as stream:
        for line in stream:
            row = json.loads(line)
            response = row.get('response')
            target = gold.get(str(row.get('queryId')))
            if row.get('status') != 200 or target is None:
                continue
            score = score_evidence(response, target)['supportingSentenceRecall']
            by_method.setdefault(row['method'], []).append(score)
    return {method: {'supportingSentenceRecall': sum(values) / len(values),
                     'queries': len(values)}
            for method, values in by_method.items() if values}


def _write_report(path: Path, manifest: Dict[str, Any], summaries: Sequence[Dict[str, Any]],
                  quality: Dict[str, Any], resource_rows: Sequence[Dict[str, Any]],
                  acceptance: Dict[str, Any]) -> None:
    lines = [
        '# GraphRAG production load run',
        '',
        'Dataset: `%s/%s/%s`, artifact version `%s`.' % (
            manifest['dataset'], manifest['datasetRelease'], manifest['split'], manifest['graphVersion']),
        'HTTP mode: open-loop fixed QPS; retries: 0; exact ranking: disabled.',
        'Acceptance: **%s**; failures: `%s`.' % (
            'PASS' if acceptance['passed'] else 'FAIL',
            ', '.join(acceptance['failures']) or 'none'),
        '',
        '| Method / phase | Target QPS | Issued / scheduled | Success | Transport failures | Unexpected 5xx | '
        'Protocol invalid | Client rejected | Typed timeout | P95 ms | P99 ms |',
        '| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |',
    ]
    for row in summaries:
        lines.append('| %s / %s | %s | %d / %d | %d | %d | %d | %d | %d | %d | %.2f | %.2f |' % (
            row['method'], row['phase'], row['targetQps'], row['issued'], row['scheduled'],
            row['success'], row['transportFailure'], row['unexpected5xx'], row['protocolInvalid'],
            row['clientRejected'], row['typedTimeout'], row['p95LatencyMs'], row['p99LatencyMs']))
    lines.extend(['', '## Resource samples', '', 'Samples collected: %d.' % len(resource_rows)])
    if resource_rows:
        process_rss = [row['processRssMB'] for row in resource_rows]
        lines.append('Server RSS ranged from %.1f MB to %.1f MB.' % (min(process_rss), max(process_rss)))
        host_cpu = [row['hostCpuPercent'] for row in resource_rows if row.get('hostCpuPercent') is not None]
        if host_cpu:
            lines.append('Host CPU ranged from %.1f%% to %.1f%%.' % (min(host_cpu), max(host_cpu)))
    if quality:
        lines.extend(['', '## Retrieval quality diagnostic', '',
                      'Supporting sentence recall is diagnostic only and is not an acceptance gate.'])
        for method, value in sorted(quality.items()):
            lines.append('- `%s`: %.4f over %d successful responses.' % (
                method, value['supportingSentenceRecall'], value['queries']))
        lines.append('Graph evidence may include corpus co-occurrence edges; interpret recall in that context.')
    lines.extend(['', 'No fixed P95/P99 service SLO was configured; latency values are a local capacity baseline.',
                  ''])
    path.write_text('\n'.join(lines), encoding='utf-8')


def run(args: argparse.Namespace) -> Dict[str, Any]:
    root = Path(args.repo_root).resolve()
    queries, vectors = _load_inputs(Path(args.queries), Path(args.vectors))
    random.Random(args.seed).shuffle(queries)
    bundle = _bundle_metadata(Path(args.bundle_root), args.graph_name, args.graph_version)
    ready = _check_ready(args.url, args.graph_name, args.graph_version)
    run_id = args.run_id or datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    if not re.fullmatch(r'[A-Za-z0-9._:]{1,80}', run_id):
        raise ValueError('run id must contain only letters, digits, dot, underscore or colon')
    output_root = Path(args.output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    output = output_root / run_id
    output.mkdir(exist_ok=False)
    started_at = datetime.now(timezone.utc).isoformat()
    request_path = output / 'request-results.jsonl.gz'
    sampler = ResourceSampler(args.server_pid, args.sample_interval)
    service_rows: List[Dict[str, Any]] = []
    service_stop = threading.Event()
    monitor = threading.Thread(target=_monitor_service,
                               args=(args.url, service_stop, service_rows, args.metrics_interval),
                               daemon=True)
    sampler.start()
    monitor.start()
    summaries: List[Dict[str, Any]] = []
    try:
        worker = HttpWorker(args.url)
        with gzip.open(request_path, 'wt', encoding='utf-8') as request_stream:
            with ThreadPoolExecutor(max_workers=args.max_inflight) as pool:
                for method in METHODS:
                    sequence = 0
                    warm, sequence = _run_phase(
                        pool, worker, queries, vectors, run_id, method, 'warmup', args.warmup_qps,
                        args.warmup_seconds, args.max_inflight, request_stream, args.top_k,
                        args.timeout_ms, args.max_candidates, args.token_budget, 'CANDIDATES',
                        args.graph_name, args.graph_version, args.index_version, sequence)
                    summaries.append({'method': method, 'phase': 'warmup', **warm})
                    for qps in args.qps:
                        phase = 'steady_%g_qps' % qps
                        summary, sequence = _run_phase(
                            pool, worker, queries, vectors, run_id, method, phase, qps,
                            args.stage_seconds, args.max_inflight, request_stream, args.top_k,
                            args.timeout_ms, args.max_candidates, args.token_budget, 'CANDIDATES',
                            args.graph_name, args.graph_version, args.index_version, sequence)
                        summaries.append({'method': method, 'phase': phase, **summary})
                if args.soak_seconds > 0:
                    summary, _ = _run_phase(
                        pool, worker, queries, vectors, run_id, 'ADAPTIVE_HYBRID', 'soak_50_qps',
                        args.soak_qps, args.soak_seconds, args.max_inflight, request_stream,
                        args.top_k, args.timeout_ms, args.max_candidates, args.token_budget,
                        'CANDIDATES', args.graph_name, args.graph_version, args.index_version, 0)
                    summaries.append({'method': 'ADAPTIVE_HYBRID', 'phase': 'soak-50-qps', **summary})

        targeted = _select_target_queries(request_path, queries)
        targeted_cases = [
            ('parallel_hybrid', 'PARALLEL_HYBRID', queries, 'CANDIDATES',
             args.timeout_ms, args.max_candidates, args.token_budget),
            ('latency_budget', 'ADAPTIVE_HYBRID', queries, 'LATENCY',
             args.latency_budget_ms, args.max_candidates, args.token_budget),
            ('token_budget', 'ADAPTIVE_HYBRID', queries, 'TOKENS',
             args.timeout_ms, args.max_candidates, args.target_token_budget),
            ('no_anchor', 'GRAPH_ONLY', [targeted['no_anchor']], 'CANDIDATES',
             args.timeout_ms, args.max_candidates, args.token_budget),
            ('high_graph_work', 'GRAPH_ONLY', [targeted['high_graph_work']], 'CANDIDATES',
             args.timeout_ms, args.max_candidates, args.token_budget),
            ('long_query', 'ADAPTIVE_HYBRID', [targeted['long_query']], 'CANDIDATES',
             args.timeout_ms, args.max_candidates, args.token_budget),
            ('multi_hop', 'ADAPTIVE_HYBRID', [targeted['multi_hop']], 'CANDIDATES',
             args.timeout_ms, args.max_candidates, args.token_budget),
        ]
        with gzip.open(request_path, 'at', encoding='utf-8') as request_stream:
            with ThreadPoolExecutor(max_workers=args.max_inflight) as pool:
                for phase, method, case_queries, constraint, timeout, candidates, tokens in targeted_cases:
                    summary, _ = _run_phase(
                        pool, worker, case_queries, vectors, run_id, method, phase,
                        args.targeted_qps, args.targeted_seconds, args.max_inflight,
                        request_stream, args.top_k, timeout, candidates, tokens, constraint,
                        args.graph_name, args.graph_version, args.index_version, 0)
                    summaries.append({'method': method, 'phase': phase, **summary})
                failure_drills = _failure_drills(worker, args, run_id)
    finally:
        service_stop.set()
        sampler.stop()
        monitor.join(timeout=5.0)
        sampler.join(timeout=5.0)
    _check_ready(args.url, args.graph_name, args.graph_version)
    metrics_status, metrics_snapshot = _http_json(args.url, '/metrics/retrieval')
    manifest = {
        'runId': run_id,
        'startedAtUtc': started_at,
        'dataset': args.dataset,
        'datasetRelease': args.dataset_release,
        'split': args.split,
        'queryCount': len(queries),
        'graphName': args.graph_name,
        'graphVersion': args.graph_version,
        'indexVersion': args.index_version,
        'bundle': bundle,
        'readyResponse': ready,
        'url': args.url,
        'seed': args.seed,
        'methods': list(METHODS),
        'profile': {'warmupQps': args.warmup_qps, 'warmupSeconds': args.warmup_seconds,
                    'steadyQps': args.qps, 'stageSeconds': args.stage_seconds,
                    'soakQps': args.soak_qps, 'soakSeconds': args.soak_seconds,
                    'targetedQps': args.targeted_qps, 'targetedSeconds': args.targeted_seconds,
                    'maxInflight': args.max_inflight},
        'targetQueries': {name: {'id': query['id'], 'question': query['question']}
                          for name, query in targeted.items()},
        'requestDefaults': {'topK': args.top_k, 'timeoutMs': args.timeout_ms,
                            'maxCandidates': args.max_candidates, 'tokenBudget': args.token_budget,
                            'exactRanking': False, 'retries': 0},
        'environment': {'platform': platform.platform(), 'python': sys.version,
                        'cpuCount': os.cpu_count(), 'serverPid': args.server_pid,
                        'javaVersion': _java_version(args.server_pid)},
        'git': _git_metadata(root),
        'metricsSnapshotStatus': metrics_status,
        'metricsSnapshot': metrics_snapshot,
        'resourceSamplerError': sampler.error,
        'serviceSamples': len(service_rows),
    }
    quality = _summarize_quality(request_path, Path(args.gold) if args.gold else None)
    acceptance_failures = []
    for summary in summaries:
        if summary['clientRejected'] or summary['protocolInvalid'] or summary['unexpected5xx'] \
                or summary['transportFailure'] or summary['completed'] + summary['clientRejected'] \
                != summary['scheduled']:
            acceptance_failures.append('%s/%s' % (summary['method'], summary['phase']))
    if any(not drill['passed'] for drill in failure_drills):
        acceptance_failures.append('failure_drills')
    acceptance = {'passed': not acceptance_failures, 'failures': acceptance_failures,
                  'latencySloConfigured': False,
                  'typedRetrievalTimeoutsAreReportedSeparately': True}
    _write_csv(output / 'summary.csv', summaries,
               ('method', 'phase', 'targetQps', 'scheduled', 'issued', 'completed', 'success',
                'typedTimeout', 'server5xx', 'unexpected5xx', 'transportFailure',
                'clientRejected', 'protocolValid', 'protocolInvalid',
                'meanLatencyMs', 'p50LatencyMs', 'p95LatencyMs', 'p99LatencyMs', 'maxLatencyMs',
                'meanQueueDelayMs'))
    _write_csv(output / 'resource-samples.csv', sampler.rows, ResourceSampler.FIELDS)
    (output / 'failure-drills.json').write_text(
        json.dumps(failure_drills, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    (output / 'service-metrics.jsonl').write_text(
        ''.join(json.dumps(row, ensure_ascii=False) + '\n' for row in service_rows), encoding='utf-8')
    (output / 'manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + '\n',
                                          encoding='utf-8')
    (output / 'acceptance.json').write_text(json.dumps(acceptance, indent=2) + '\n',
                                           encoding='utf-8')
    _write_report(output / 'report.md', manifest, summaries, quality, sampler.rows, acceptance)
    return {'outputDir': str(output), 'summaries': summaries, 'quality': quality,
            'resourceSamples': len(sampler.rows), 'serviceSamples': len(service_rows),
            'acceptance': acceptance}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', required=True)
    parser.add_argument('--queries', required=True)
    parser.add_argument('--vectors', required=True)
    parser.add_argument('--output-dir', required=True)
    parser.add_argument('--bundle-root', required=True)
    parser.add_argument('--gold')
    parser.add_argument('--repo-root', default=str(Path(__file__).resolve().parents[2]))
    parser.add_argument('--graph-name', required=True)
    parser.add_argument('--graph-version', required=True)
    parser.add_argument('--index-version', required=True)
    parser.add_argument('--server-pid', type=int, required=True)
    parser.add_argument('--dataset', required=True)
    parser.add_argument('--dataset-release', required=True)
    parser.add_argument('--split', required=True)
    parser.add_argument('--run-id')
    parser.add_argument('--seed', type=int, default=0)
    parser.add_argument('--qps', type=float, nargs='+', default=[5.0, 20.0, 50.0])
    parser.add_argument('--warmup-qps', type=float, default=5.0)
    parser.add_argument('--warmup-seconds', type=float, default=30.0)
    parser.add_argument('--stage-seconds', type=float, default=120.0)
    parser.add_argument('--soak-qps', type=float, default=50.0)
    parser.add_argument('--soak-seconds', type=float, default=600.0)
    parser.add_argument('--targeted-qps', type=float, default=20.0)
    parser.add_argument('--targeted-seconds', type=float, default=60.0)
    parser.add_argument('--max-inflight', type=int, default=512)
    parser.add_argument('--sample-interval', type=float, default=1.0)
    parser.add_argument('--metrics-interval', type=float, default=5.0)
    parser.add_argument('--top-k', type=int, default=10)
    parser.add_argument('--timeout-ms', type=int, default=10000)
    parser.add_argument('--max-candidates', type=int, default=50)
    parser.add_argument('--token-budget', type=int, default=4096)
    parser.add_argument('--latency-budget-ms', type=int, default=250)
    parser.add_argument('--target-token-budget', type=int, default=128)
    args = parser.parse_args()
    result = run(args)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
