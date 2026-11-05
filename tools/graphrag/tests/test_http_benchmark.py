import gzip
import json
import zipfile
from pathlib import Path

import numpy as np
import pytest

from types import SimpleNamespace

from tools.graphrag.benchmark_http import (METHODS, METHOD_CONFIG, _open_output, _summary_path,
                                            apply_ablation, assert_equal_budget,
                                            assert_single_primary_constraint, request_body,
                                            run_provenance)
from tools.graphrag.eval.retrieval import (load_query_vectors, load_supporting_facts,
                                            load_supporting_sentences, load_supporting_targets,
                                            materialize_hotpot_queries, score_evidence,
                                            supporting_fact_audit)


def test_query_vectors_align_by_id(tmp_path):
    path = tmp_path / 'vectors.npz'
    np.savez(path, ids=['b', 'a'], vectors=[[0., 1.], [1., 0.]])
    assert load_query_vectors(path, [{'id': 'a'}, {'id': 'b'}]).tolist() == [[1., 0.], [0., 1.]]
    with pytest.raises(ValueError):
        load_query_vectors(path, [{'id': 'missing'}])
    with pytest.raises(ValueError):
        load_query_vectors(path, [{'id': 'a'}, {'id': 'a'}])


def test_gold_is_resolved_from_context_not_title(tmp_path):
    path = tmp_path / 'source.zip'
    record = {'_id': 'q', 'context': [['Title', ['First.', 'Second.']]],
              'supporting_facts': [['Title', 1]]}
    with zipfile.ZipFile(path, 'w') as archive:
        archive.writestr('__MACOSX/._dev.json', b'apple metadata')
        archive.writestr('data/dev.json', json.dumps([record]))
    assert load_supporting_sentences(path)['q'] == ['Second.']
    assert load_supporting_facts(path)['q'] == [('Title', 1, 'Second.')]


def test_hotpot_json_materializes_queries_and_audits_bad_fact_without_hiding_it(tmp_path):
    source = tmp_path / 'hotpot.json'
    output = tmp_path / 'queries.jsonl'
    records = [{'_id': 'q', 'question': 'Question?', 'answer': 'answer',
                'context': [['Title', ['First.', 'Second.']]],
                'supporting_facts': [['Title', 1], ['Title', 9]]}]
    source.write_text(json.dumps(records), encoding='utf-8')

    assert materialize_hotpot_queries(source, output) == 1
    query = json.loads(output.read_text(encoding='utf-8').splitlines()[0])
    assert query['id'] == 'q'
    assert load_supporting_sentences(source)['q'] == ['Second.']
    assert load_supporting_facts(source)['q'] == [('Title', 1, 'Second.')]
    audit = supporting_fact_audit(source)
    assert audit['supportingFacts'] == 2
    assert audit['resolved'] == 1
    assert audit['unresolved'] == [{'queryId': 'q', 'title': 'Title', 'sentenceIndex': 9,
                                    'availableSentences': 2}]


def test_sentence_coverage_deduplicates_chunks_and_requires_full_sentence():
    response = {'evidence': [{'chunks': [{'chunkId': 'a', 'text': 'First sentence. Partial'},
                                         {'chunkId': 'a', 'text': 'First sentence. Partial'}],
                              'graphTraces': [{'path': {
                                  'vertices': [{'vertexId': 'v1'}, {'vertexId': 'v2'}],
                                  'edges': [{'edgeId': 'e', 'label': 'mentions'}],
                                  'sampled': False}}],
                              'sources': [{'sourceUri': 'source'}]}]}
    metrics = score_evidence(response, ['First sentence.', 'Partial sentence.'])
    assert metrics['supportingSentenceRecall'] == .5
    assert metrics['allSupportingSentencesHit'] == 0
    assert metrics['nonemptyEvidence'] == 1
    assert metrics['pathCount'] == 1
    assert score_evidence({'evidence': []}, ['First sentence.'])['supportingSentenceRecall'] == 0


def test_item_level_chunk_is_counted():
    response = {'evidence': [{'chunkId': 'a', 'text': 'First sentence.'}]}
    assert score_evidence(response, ['First sentence.'])['supportingSentenceRecall'] == 1


def test_canonical_trace_paths_and_provenance_are_counted():
    path = {'vertices': [{'vertexId': 'a'}, {'vertexId': 'b'}],
            'edges': [{'edgeId': 'edge-1', 'label': 'related'}], 'sampled': False}
    response = {'evidence': [{'chunks': [{'chunkId': 'a', 'text': 'Fact.',
                                          'sources': [{'documentId': 'd'}]}],
                              'graphTraces': [{'path': path}, {'path': path}]}],
                'trace': {'traceVersion': 'v1', 'selectedMode': 'GRAPH_ONLY'}}
    metrics = score_evidence(response, ['Fact.'])
    assert metrics['pathCount'] == 1
    assert metrics['nestedPathCount'] == 1
    assert metrics['topLevelPathCount'] == 0
    assert metrics['provenanceRefCount'] == 1
    assert metrics['traceValid'] == 1


def test_invalid_wire_response_cannot_claim_evidence():
    metrics = score_evidence(None, ['Fact.'])
    assert metrics['nonemptyEvidence'] == 0
    assert metrics['pathCount'] == 0
    assert metrics['traceValid'] == 0


def test_six_benchmark_methods_have_distinct_request_semantics():
    assert len(METHODS) == 6
    assert METHOD_CONFIG['FIXED_HYBRID']['executionMode'] == 'CASCADED'
    assert METHOD_CONFIG['ADAPTIVE_HYBRID']['executionMode'] == 'CASCADED'
    # The two hybrid methods differ by mode, not by execution mode: ADAPTIVE_HYBRID selects the
    # routing policy, while both run the design's default cascaded execution.
    assert METHOD_CONFIG['ADAPTIVE_HYBRID']['mode'] == 'ADAPTIVE_HYBRID'
    assert METHOD_CONFIG['FIXED_HYBRID']['mode'] == 'FIXED_HYBRID'
    assert METHOD_CONFIG['ADAPTIVE_HYBRID']['mode'] != METHOD_CONFIG['FIXED_HYBRID']['mode']
    assert METHOD_CONFIG['VECTOR_OVERFETCH']['mode'] == 'VECTOR_ONLY'
    # Equal-budget overfetch: the widened window must not change the measured @K, so top-k stays
    # fixed and only the evaluated candidate budget widens.
    assert METHOD_CONFIG['VECTOR_OVERFETCH']['candidateMultiplier'] == 1
    assert METHOD_CONFIG['VECTOR_OVERFETCH']['candidateMultiplierOverride'] > 1
    assert 'topKMultiplier' not in METHOD_CONFIG['VECTOR_OVERFETCH']
    args = SimpleNamespace(graph_name='g', graph_version='gv', index_version='iv',
                           top_k=5, timeout_ms=1000, max_candidates=10, token_budget=100,
                           exact_ranking=True, primary_constraint='CANDIDATES')
    query = {'question': 'q'}
    fixed = request_body(args, query, 'FIXED_HYBRID', [1.0, 0.0])
    adaptive = request_body(args, query, 'ADAPTIVE_HYBRID', [1.0, 0.0])
    overfetch = request_body(args, query, 'VECTOR_OVERFETCH', [1.0, 0.0])
    assert fixed['executionMode'] == 'CASCADED'
    assert adaptive['executionMode'] == fixed['executionMode']
    assert adaptive['mode'] != fixed['mode']
    assert fixed['requestId'] == adaptive['requestId']
    assert fixed['exactRanking'] is True
    # The measured @K denominator is identical across every method.
    assert overfetch['budget']['topK'] == fixed['budget']['topK']
    # Overfetch spends a wider evaluated window under the same single hard constraint.
    assert overfetch['budget']['maxCandidates'] > fixed['budget']['maxCandidates']
    # Every method declares exactly one, identical, hard primary constraint.
    constraints = {fixed['budget']['primaryConstraint'], adaptive['budget']['primaryConstraint'],
                   overfetch['budget']['primaryConstraint']}
    assert constraints == {'CANDIDATES'}


def test_ablation_shapes_only_the_wire_request():
    base = SimpleNamespace(graph_name='g', graph_version='gv', index_version='iv',
                           top_k=5, timeout_ms=1000, max_candidates=100, token_budget=100,
                           exact_ranking=True, ablation=None)
    query = {'question': 'q'}
    reference = request_body(base, query, 'ADAPTIVE_HYBRID', [1.0, 0.0])

    for name in ('no-anchor', 'high-degree', 'lexical', 'semantic', 'multi-hop'):
        shaped_args = SimpleNamespace(**{**vars(base), 'ablation': name})
        shaped = request_body(shaped_args, query, 'ADAPTIVE_HYBRID', [1.0, 0.0])
        assert shaped['requestId'] == reference['requestId'], name
        assert shaped['graphName'] == reference['graphName'], name
        assert shaped['budget']['topK'] == reference['budget']['topK'], name
        # The reference request itself must not be mutated by shaping.
        assert reference['mode'] == 'ADAPTIVE_HYBRID'
        assert reference['budget']['maxCandidates'] == 100

    no_anchor = request_body(SimpleNamespace(**{**vars(base), 'ablation': 'no-anchor'}),
                             query, 'ADAPTIVE_HYBRID', [1.0, 0.0])
    lexical = request_body(SimpleNamespace(**{**vars(base), 'ablation': 'lexical'}),
                           query, 'ADAPTIVE_HYBRID', [1.0, 0.0])
    semantic = request_body(SimpleNamespace(**{**vars(base), 'ablation': 'semantic'}),
                            query, 'ADAPTIVE_HYBRID', [1.0, 0.0])
    high_degree = request_body(SimpleNamespace(**{**vars(base), 'ablation': 'high-degree'}),
                               query, 'ADAPTIVE_HYBRID', [1.0, 0.0])
    multi_hop = request_body(SimpleNamespace(**{**vars(base), 'ablation': 'multi-hop'}),
                             query, 'ADAPTIVE_HYBRID', [1.0, 0.0])
    assert no_anchor['mode'] == 'BM25_ONLY'
    assert lexical['mode'] == 'BM25_ONLY'
    assert semantic['mode'] == 'VECTOR_ONLY'
    assert high_degree['mode'] == 'GRAPH_ONLY'
    assert high_degree['budget']['maxCandidates'] < reference['budget']['maxCandidates']
    assert multi_hop['budget']['maxCandidates'] < reference['budget']['maxCandidates']


def test_ablation_requires_vectors_and_rejects_unknown_names():
    args = SimpleNamespace(graph_name='g', graph_version='gv', index_version='iv',
                           top_k=5, timeout_ms=1000, max_candidates=10, token_budget=100,
                           exact_ranking=True, ablation='semantic')
    try:
        request_body(args, {'question': 'q'}, 'ADAPTIVE_HYBRID', None)
    except ValueError as error:
        assert 'vectors' in str(error)
    else:
        raise AssertionError('semantic ablation without vectors must fail')

    try:
        apply_ablation({'budget': {}}, 'unknown')
    except ValueError as error:
        assert 'unknown ablation' in str(error)
    else:
        raise AssertionError('unknown ablation must fail')

    # 'none' is an explicit no-op alias for the default run.
    body = {'budget': {'maxCandidates': 10}}
    assert apply_ablation(body, 'none') == body


def test_supporting_facts_map_to_ranked_chunk_metrics(tmp_path):
    gold_path = tmp_path / 'gold.json'
    gold_path.write_text(json.dumps([{
        '_id': 'q', 'context': [['Title', ['A distractor.', 'A supporting fact.']]],
        'supporting_facts': [['Title', 1]],
    }]), encoding='utf-8')
    chunks_path = tmp_path / 'chunks.jsonl'
    chunks_path.write_text('\n'.join([
        json.dumps({'document_id': 'q', 'chunk_id': 'c1', 'text': 'A distractor.'}),
        json.dumps({'document_id': 'q', 'chunk_id': 'c2', 'text': 'A supporting fact.'}),
    ]) + '\n', encoding='utf-8')

    target = load_supporting_targets(gold_path, chunks_path)['q']
    scored = score_evidence({'evidence': [
        {'chunks': [{'chunkId': 'c1', 'text': 'A distractor.'}]},
        {'chunks': [{'chunkId': 'c2', 'text': 'A supporting fact.'}]},
    ]}, target['supportingSentences'], target['relevantChunkIds'])

    assert target['relevantChunkIds'] == ['c2']
    assert target['unresolvedSupportingFacts'] == 0
    assert scored['evidenceRecallAtK'] == 1.0
    assert scored['mrr'] == 0.5
    assert 0 < scored['ndcgAtK'] < 1


def test_relation_path_labels_are_scored_against_two_wiki_gold(tmp_path):
    gold_path = tmp_path / 'gold.json'
    gold_path.write_text(json.dumps([{
        '_id': 'q', 'evidences': [['Film', 'director', 'Person'],
                                 ['Person', 'mother', 'Parent']],
    }]), encoding='utf-8')
    targets = load_supporting_targets(gold_path)['q']['relations']
    response = {'evidence': [{'graphTraces': [{'path': {'edges': [
        {'label': 'director'}, {'label': 'mother'}]}}]}]}

    assert targets == ['director', 'mother']
    assert score_evidence(response, relation_targets=targets)['relationPathHit'] == 1


def test_run_provenance_records_reproducibility_inputs():
    args = SimpleNamespace(graph_version='gv-1', seed=7, config_version='v2')
    provenance = run_provenance(args)
    assert provenance['datasetRelease'] == 'gv-1'
    assert provenance['randomSeed'] == 7
    assert provenance['configVersion'] == 'v2'
    assert provenance['codeRevision']


def test_primary_constraint_is_single_and_recorded_on_every_request():
    args = SimpleNamespace(graph_name='g', graph_version='gv', index_version='iv',
                           top_k=5, timeout_ms=1000, max_candidates=10, token_budget=100,
                           exact_ranking=False, primary_constraint='TOKENS')
    query = {'question': 'q'}
    for method in METHODS:
        body = request_body(args, query, method, [1.0, 0.0])
        assert body['budget']['primaryConstraint'] == 'TOKENS', method
    assert_single_primary_constraint(METHODS, 'TOKENS')
    try:
        assert_single_primary_constraint(METHODS, 'WALLCLOCK')
    except ValueError as error:
        assert 'primary constraint' in str(error)
    else:
        raise AssertionError('an unsupported primary constraint must be rejected')


def test_equal_budget_rejects_divergent_candidate_multipliers():
    assert_equal_budget(METHODS)
    original = METHOD_CONFIG['GRAPH_ONLY']['candidateMultiplier']
    METHOD_CONFIG['GRAPH_ONLY']['candidateMultiplier'] = 4
    try:
        try:
            assert_equal_budget(METHODS)
        except ValueError as error:
            assert 'candidate budget' in str(error)
        else:
            raise AssertionError('a widened candidate budget must be rejected')
    finally:
        METHOD_CONFIG['GRAPH_ONLY']['candidateMultiplier'] = original


def test_output_stream_is_gzip_when_suffix_says_so(tmp_path):
    target = tmp_path / 'rows.jsonl.gz'
    with _open_output(target) as stream:
        stream.write(json.dumps({'a': 1}) + '\n')
    with gzip.open(target, 'rt', encoding='utf-8') as stream:
        assert json.loads(stream.readline()) == {'a': 1}


def test_output_stream_is_plain_text_otherwise(tmp_path):
    target = tmp_path / 'rows.jsonl'
    with _open_output(target) as stream:
        stream.write('{"a": 1}\n')
    assert target.read_text(encoding='utf-8') == '{"a": 1}\n'


def test_summary_path_drops_compressed_suffix():
    assert _summary_path(Path('run/six.jsonl')).name == 'six.csv'
    assert _summary_path(Path('run/six.jsonl.gz')).name == 'six.csv'


def test_budget_ablations_scale_the_wire_budget_only():
    base = SimpleNamespace(graph_name='g', graph_version='gv', index_version='iv',
                           top_k=10, timeout_ms=5000, max_candidates=100, token_budget=4000,
                           exact_ranking=True, ablation=None)
    query = {'question': 'q'}
    reference = request_body(base, query, 'ADAPTIVE_HYBRID', [1.0, 0.0])
    assert reference['budget']['maxCandidates'] == 100

    for name, expected in (('budget-tight', 25), ('budget-loose', 400)):
        shaped = apply_ablation(dict(reference, budget=dict(reference['budget'])), name)
        assert shaped['budget']['maxCandidates'] == expected, name
        # A budget ablation must change cost only, never the retrieval method under test.
        assert shaped['mode'] == reference['mode'], name
        assert shaped['executionMode'] == reference['executionMode'], name


def test_reported_budget_reflects_the_ablated_request():
    """A budget ablation must be visible in the recorded budget, not masked by the CLI default."""
    base = SimpleNamespace(graph_name='g', graph_version='gv', index_version='iv',
                           top_k=10, timeout_ms=5000, max_candidates=100, token_budget=4000,
                           exact_ranking=True, ablation='budget-tight')
    body = request_body(base, {'question': 'q'}, 'ADAPTIVE_HYBRID', [1.0, 0.0])
    assert body['budget']['maxCandidates'] == 25
    assert base.max_candidates == 100, 'the CLI default must stay untouched'


def test_gold_supports_hotpotqa_parquet(tmp_path):
    """HotpotQA gold ships as parquet; loading it must not require a JSON conversion step."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from tools.graphrag.eval.retrieval import load_supporting_targets

    # Mirror the columnar shape Hugging Face ships for HotpotQA.
    schema = pa.schema([
        ('id', pa.string()), ('question', pa.string()), ('answer', pa.string()),
        ('supporting_facts', pa.struct([('title', pa.list_(pa.string())),
                                        ('sent_id', pa.list_(pa.int32()))])),
        ('context', pa.struct([('title', pa.list_(pa.string())),
                               ('sentences', pa.list_(pa.list_(pa.string())))])),
    ])
    table = pa.table({
        'id': ['q1'],
        'question': ['Who wrote it?'],
        'answer': ['Alice'],
        'supporting_facts': [{'title': ['Article A'], 'sent_id': [0]}],
        'context': [{'title': ['Article A'],
                     'sentences': [['Alice wrote it.', 'Other sentence.']]}],
    }, schema=schema)
    path = tmp_path / 'dev.parquet'
    pq.write_table(table, path)

    targets = load_supporting_targets(path)

    assert 'q1' in targets
    assert targets['q1']['supportingSentences'] == ['Alice wrote it.']


def test_summary_fields_carry_every_required_secondary_metric():
    """The design requires all secondary work measurements in the result.

    A graph method must not look cheap merely because it returns few candidates after scanning a
    large frontier, so neighbour/vertex/unique-candidate counts and the routing reproducibility
    fields must reach the summary CSV rather than staying buried in the per-request trace.
    """
    import inspect
    from tools.graphrag import benchmark_http

    source = inspect.getsource(benchmark_http.run)
    for field in ('meanNeighborsSampled', 'meanVerticesReached',
                  'meanUniqueEvidenceCandidates', 'policyParameters',
                  'featureSchemaVersion'):
        assert field in source, '%s must be written to the summary CSV' % field


def test_row_extracts_secondary_work_from_the_trace():
    """The per-row extractor must read the trace keys the Java service actually emits."""
    import inspect
    from tools.graphrag import benchmark_http

    source = inspect.getsource(benchmark_http.run)
    for key in ('neighborsSampled', 'verticesReached', 'uniqueEvidenceCandidates',
                'featureSchemaVersion', 'policyParameters'):
        assert "trace.get('%s'" % key in source, \
            'row extractor must read trace.%s' % key


def test_policy_parameters_are_serialised_deterministically():
    """Policy parameters must round-trip as stable JSON so two runs can be compared textually."""
    params = {'highAverageIdf': '1.5', 'minimumAnchorCount': '1', 'baseNeighborCap': '16'}
    encoded = json.dumps(params, sort_keys=True)
    assert encoded == ('{"baseNeighborCap": "16", "highAverageIdf": "1.5", '
                       '"minimumAnchorCount": "1"}')


def test_summary_fields_carry_every_required_secondary_metric_duplicate_guard():
    """Guard: the five secondary/reproducibility columns must exist exactly once in the header."""
    import inspect
    from tools.graphrag import benchmark_http

    source = inspect.getsource(benchmark_http.run)
    fields_block = source[source.index('fields = ('):source.index('with summary.open')]
    for field in ('meanNeighborsSampled', 'meanVerticesReached',
                  'meanUniqueEvidenceCandidates', 'policyParameters',
                  'featureSchemaVersion'):
        assert fields_block.count("'%s'" % field) == 1, \
            '%s must appear exactly once in the CSV header' % field


def _fake_response():
    """A wire response shaped like /api/v1/retrievals so scoring accepts it."""
    return {'evidence': [{'chunks': [{'chunkId': 'c1', 'documentId': 'd1'}]}],
            'paths': [], 'trace': {'traceVersion': 'v1', 'selectedChannels': ['BM25']}}


def _run_with_fake_transport(tmp_path, ablation, method='ADAPTIVE_HYBRID'):
    """Runs the real benchmark loop against a recording fake transport."""
    from tools.graphrag import benchmark_http

    queries = tmp_path / 'queries.jsonl'
    queries.write_text(json.dumps({'id': 'q1', 'question': 'q', 'answer': 'a'}) + '\n')
    vectors = tmp_path / 'vectors.npz'
    np.savez(vectors, ids=['q1'], vectors=[[1.0, 0.0]])
    output = tmp_path / ('out-%s.jsonl' % (ablation or 'none'))
    sent = []

    def fake_request(url, body):
        sent.append(body)
        return 200, _fake_response(), 1.0, None

    args = SimpleNamespace(
        url='http://example.test/api/v1/retrievals', queries=str(queries),
        output=str(output), vectors=str(vectors), gold=None, chunks=None, graph_name='g',
        graph_version='gv', index_version='iv', limit=None, top_k=10, timeout_ms=1000,
        max_candidates=100, token_budget=4096, primary_constraint='CANDIDATES',
        workers=1, exact_ranking=False, ablation=ablation, seed=0, config_version='v1',
        retries=0)
    original = benchmark_http.request
    benchmark_http.request = fake_request
    try:
        benchmark_http.run(args)
    finally:
        benchmark_http.request = original

    rows = [json.loads(line) for line in output.read_text().splitlines() if line.strip()]
    summary_path = benchmark_http._summary_path(output)
    import csv as csv_module
    with summary_path.open(encoding='utf-8') as stream:
        summary = list(csv_module.DictReader(stream))
    return rows, summary, sent


@pytest.mark.parametrize('ablation,expected_mode', [
    ('no-anchor', 'BM25_ONLY'),
    ('lexical', 'BM25_ONLY'),
    ('high-degree', 'GRAPH_ONLY'),
])
def test_ablation_rows_record_the_mode_actually_sent(tmp_path, ablation, expected_mode):
    """The CSV must label an ablated request with the mode the server received.

    Regression: the runner shaped the wire request (no-anchor -> BM25_ONLY) but wrote the
    pre-ablation config mode into the row, so a single-channel run was reported as a hybrid one
    and its zero recall looked like a finding instead of an idle request.
    """
    rows, summary, sent = _run_with_fake_transport(tmp_path, ablation)

    adaptive_rows = [row for row in rows if row['method'] == 'ADAPTIVE_HYBRID']
    adaptive_summary = [row for row in summary if row['method'] == 'ADAPTIVE_HYBRID'][0]
    # ABLATION_METHODS runs two methods, so index 0 is not guaranteed to be the adaptive one.
    adaptive_sent = [body for body in sent if body['mode'] == expected_mode]

    assert adaptive_sent, 'the wire request must be ablated to %s' % expected_mode
    assert adaptive_rows[0]['mode'] == expected_mode, \
        'row must record the sent mode, not the pre-ablation %s' % adaptive_rows[0]['requestedMode']
    assert adaptive_rows[0]['requestedMode'] == 'ADAPTIVE_HYBRID', \
        'requestedMode keeps the method identity so the rewrite stays auditable'
    assert adaptive_summary['mode'] == expected_mode, 'summary must agree with the rows'
    assert adaptive_summary['requestedMode'] == 'ADAPTIVE_HYBRID'


def test_unaltered_rows_record_the_configured_mode(tmp_path):
    """Without an ablation the recorded mode equals the method's configured mode."""
    rows, summary, sent = _run_with_fake_transport(tmp_path, None)

    adaptive_rows = [row for row in rows if row['method'] == 'ADAPTIVE_HYBRID']
    adaptive_summary = [row for row in summary if row['method'] == 'ADAPTIVE_HYBRID'][0]

    assert adaptive_rows[0]['mode'] == 'ADAPTIVE_HYBRID'
    assert adaptive_rows[0]['mode'] == adaptive_rows[0]['requestedMode']
    assert adaptive_summary['mode'] == 'ADAPTIVE_HYBRID'
    # Every method's recorded mode must match what its own request carried.
    for method in METHODS:
        bodies = [body for body in sent if body['mode'] == METHOD_CONFIG[method]['mode']]
        assert bodies, method
        assert [row for row in summary if row['method'] == method][0]['mode'] == \
            METHOD_CONFIG[method]['mode']


def test_summary_rejects_a_mixed_mode_within_one_method():
    """A method group that mixes wire modes must fail loudly instead of reporting one of them."""
    from tools.graphrag.benchmark_http import _single_mode

    with pytest.raises(ValueError):
        _single_mode([{'mode': 'BM25_ONLY'}, {'mode': 'GRAPH_ONLY'}], 'ADAPTIVE_HYBRID')
    assert _single_mode([{'mode': 'BM25_ONLY'}, {'mode': 'BM25_ONLY'}],
                        'ADAPTIVE_HYBRID') == 'BM25_ONLY'


def test_answer_metrics_stay_empty_without_an_answer_endpoint(tmp_path):
    """Without --answers-url the runner must not invent EM/F1 from a retrieval response."""
    rows, summary, _sent = _run_with_fake_transport(tmp_path, None)

    adaptive = [row for row in rows if row['method'] == 'ADAPTIVE_HYBRID'][0]
    assert adaptive['predictedAnswer'] is None
    assert adaptive['exactMatch'] is None
    assert adaptive['answerAbstentionReason'] is None
    adaptive_summary = [row for row in summary if row['method'] == 'ADAPTIVE_HYBRID'][0]
    assert adaptive_summary['answerExactMatch'] in (None, '')
    assert adaptive_summary['answerF1'] in (None, '')
    assert adaptive_summary['answerAbstentionCount'] == '0'


def test_answer_endpoint_produces_exact_match_and_f1(tmp_path):
    """With --answers-url the runner scores a real answer, so EM/F1 stop being blank columns."""
    from tools.graphrag import benchmark_http

    queries = tmp_path / 'queries.jsonl'
    queries.write_text(json.dumps({'id': 'q1', 'question': 'q', 'answer': 'Confucius'}) + '\n')
    vectors = tmp_path / 'vectors.npz'
    np.savez(vectors, ids=['q1'], vectors=[[1.0, 0.0]])
    output = tmp_path / 'out-answers.jsonl'
    answer_calls = []

    def fake_request(url, body):
        if url.endswith('/answers'):
            answer_calls.append(body)
            return 200, {'answer': 'Confucius', 'abstentionReason': None}, 1.0, None
        return 200, _fake_response(), 1.0, None

    args = SimpleNamespace(
        url='http://example.test/api/v1/retrievals',
        answers_url='http://example.test/api/v1/answers',
        queries=str(queries), output=str(output), vectors=str(vectors), gold=None, chunks=None,
        graph_name='g', graph_version='gv', index_version='iv', limit=None, top_k=10,
        timeout_ms=1000, max_candidates=100, token_budget=4096,
        primary_constraint='CANDIDATES', workers=1, exact_ranking=False, ablation=None,
        seed=0, config_version='v1', retries=0)
    original = benchmark_http.request
    benchmark_http.request = fake_request
    try:
        benchmark_http.run(args)
    finally:
        benchmark_http.request = original

    rows = [json.loads(line) for line in output.read_text().splitlines() if line.strip()]
    assert answer_calls, 'the answer endpoint must actually be called'
    # One-request-per-method: every method asks the answer layer for its own evidence.
    assert len(answer_calls) == len(METHODS)
    assert all(call['question'] == 'q' for call in answer_calls)
    assert all(isinstance(call['tokenBudget'], int) for call in answer_calls)

    import csv as csv_module
    with benchmark_http._summary_path(output).open(encoding='utf-8') as stream:
        summary = list(csv_module.DictReader(stream))
    graph_only = [row for row in summary if row['method'] == 'GRAPH_ONLY'][0]
    assert float(graph_only['answerExactMatch']) == 1.0
    assert float(graph_only['answerF1']) == 1.0
    assert graph_only['answerAbstentionCount'] == '0'
    for row in rows:
        assert row['predictedAnswer'] == 'Confucius'
        assert row['exactMatch'] == 1.0


def test_answer_abstention_is_counted_not_hidden(tmp_path):
    """An abstention must surface as a counted outcome, distinct from a missing metric."""
    from tools.graphrag import benchmark_http

    queries = tmp_path / 'queries.jsonl'
    queries.write_text(json.dumps({'id': 'q1', 'question': 'q', 'answer': 'Confucius'}) + '\n')
    vectors = tmp_path / 'vectors.npz'
    np.savez(vectors, ids=['q1'], vectors=[[1.0, 0.0]])
    output = tmp_path / 'out-abstain.jsonl'

    def fake_request(url, body):
        if url.endswith('/answers'):
            return 200, {'answer': None, 'abstentionReason': 'NO_EVIDENCE'}, 1.0, None
        return 200, _fake_response(), 1.0, None

    args = SimpleNamespace(
        url='http://example.test/api/v1/retrievals',
        answers_url='http://example.test/api/v1/answers',
        queries=str(queries), output=str(output), vectors=str(vectors), gold=None, chunks=None,
        graph_name='g', graph_version='gv', index_version='iv', limit=None, top_k=10,
        timeout_ms=1000, max_candidates=100, token_budget=4096,
        primary_constraint='CANDIDATES', workers=1, exact_ranking=False, ablation=None,
        seed=0, config_version='v1', retries=0)
    original = benchmark_http.request
    benchmark_http.request = fake_request
    try:
        benchmark_http.run(args)
    finally:
        benchmark_http.request = original

    rows = [json.loads(line) for line in output.read_text().splitlines() if line.strip()]
    assert all(row['answerAbstentionReason'] == 'NO_EVIDENCE' for row in rows)
    assert all(row['predictedAnswer'] is None for row in rows)

    import csv as csv_module
    with benchmark_http._summary_path(output).open(encoding='utf-8') as stream:
        summary = list(csv_module.DictReader(stream))
    assert all(row['answerAbstentionCount'] == '1' for row in summary)


def test_relation_path_target_count_separates_unscoreable_from_missed():
    """A query with no gold relation path must not look like a missed path."""
    from tools.graphrag.eval.retrieval import score_evidence

    # Relation labels are read from each canonical path in the graph trace.
    def response_with(labels):
        return {'evidence': [{'chunks': [{'chunkId': 'c1'}],
                              'graphTraces': [{'path': {'edges': [
                                  {'label': label} for label in labels]}}]}],
                'trace': {}}

    response = response_with(['mentions'])
    no_target = score_evidence(response, ['Fact.'], ['c1'], relation_targets=[])
    assert no_target['relationPathTargetCount'] == 0
    assert no_target['relationPathHit'] == 0

    missed = score_evidence(response, ['Fact.'], ['c1'], relation_targets=['studies', 'born'])
    assert missed['relationPathTargetCount'] == 2
    assert missed['relationPathHit'] == 0

    hit = score_evidence(response, ['Fact.'], ['c1'], relation_targets=['mentions'])
    assert hit['relationPathTargetCount'] == 1
    assert hit['relationPathHit'] == 1


@pytest.mark.parametrize('ablation,expected_mode', [
    ('no-anchor', 'BM25_ONLY'),
    ('lexical', 'BM25_ONLY'),
    ('high-degree', 'GRAPH_ONLY'),
])
def test_single_channel_ablations_drop_the_query_vector(ablation, expected_mode):
    """An ablated request must be accepted by the server, not rejected with UNSUPPORTED_OPTION.

    BM25_ONLY and GRAPH_ONLY reject a `queryVector`, so keeping the hybrid body's vector turned
    every ablated request into an HTTP 400. The runner still recorded it, and the summary reported
    recall 0.0 with `meanEvaluated*=0` -- an idle request read as a measured result.
    """
    body = {'mode': 'ADAPTIVE_HYBRID', 'exactRanking': True,
            'queryVector': [1.0, 0.0],
            'budget': {'topK': 10, 'maxCandidates': 100, 'tokenBudget': 4096,
                       'primaryConstraint': 'CANDIDATES'}}

    shaped = apply_ablation(body, ablation)

    assert shaped['mode'] == expected_mode
    assert 'queryVector' not in shaped, \
        '%s must not carry queryVector, the server rejects it with UNSUPPORTED_OPTION' % expected_mode
    assert shaped['exactRanking'] is False
    # The reference request must not be mutated by shaping.
    assert body['mode'] == 'ADAPTIVE_HYBRID' and 'queryVector' in body


def test_vector_ablations_keep_the_query_vector():
    """Modes that need a vector must still receive one."""
    body = {'mode': 'FIXED_HYBRID', 'exactRanking': True, 'queryVector': [1.0, 0.0],
            'budget': {'topK': 10, 'maxCandidates': 100, 'tokenBudget': 4096,
                       'primaryConstraint': 'CANDIDATES'}}

    assert 'queryVector' in apply_ablation(body, 'semantic')
    # Budget-only ablations do not change the mode, so the vector stays.
    assert 'queryVector' in apply_ablation(body, 'budget-tight')
    assert 'queryVector' in apply_ablation(body, 'budget-loose')
