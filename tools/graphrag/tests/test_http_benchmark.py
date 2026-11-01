import json
import zipfile

import numpy as np
import pytest

from types import SimpleNamespace

from tools.graphrag.benchmark_http import (METHODS, METHOD_CONFIG, _open_output, _summary_path, apply_ablation,
                                            assert_equal_budget, request_body, run_provenance)
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
                              'paths': [{'edges': ['e']}], 'sources': [{'sourceUri': 'source'}]}]}
    metrics = score_evidence(response, ['First sentence.', 'Partial sentence.'])
    assert metrics['supportingSentenceRecall'] == .5
    assert metrics['allSupportingSentencesHit'] == 0
    assert metrics['nonemptyEvidence'] == 1
    assert metrics['pathCount'] == 1
    assert score_evidence({'evidence': []}, ['First sentence.'])['supportingSentenceRecall'] == 0


def test_item_level_chunk_is_counted():
    response = {'evidence': [{'chunkId': 'a', 'text': 'First sentence.'}]}
    assert score_evidence(response, ['First sentence.'])['supportingSentenceRecall'] == 1


def test_top_level_paths_trace_and_provenance_are_counted():
    response = {'evidence': [{'chunks': [{'chunkId': 'a', 'text': 'Fact.',
                                          'sources': [{'documentId': 'd'}]}]}],
                'paths': [{'path': ['a', 'b']}],
                'trace': {'traceVersion': 'v1', 'selectedMode': 'GRAPH_ONLY'}}
    metrics = score_evidence(response, ['Fact.'])
    assert metrics['pathCount'] == 1
    assert metrics['topLevelPathCount'] == 1
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
    assert METHOD_CONFIG['VECTOR_OVERFETCH']['candidateMultiplier'] == 1
    assert METHOD_CONFIG['VECTOR_OVERFETCH']['topKMultiplier'] > 1
    args = SimpleNamespace(graph_name='g', graph_version='gv', index_version='iv',
                           top_k=5, timeout_ms=1000, max_candidates=10, token_budget=100,
                           exact_ranking=True)
    query = {'question': 'q'}
    fixed = request_body(args, query, 'FIXED_HYBRID', [1.0, 0.0])
    adaptive = request_body(args, query, 'ADAPTIVE_HYBRID', [1.0, 0.0])
    overfetch = request_body(args, query, 'VECTOR_OVERFETCH', [1.0, 0.0])
    assert fixed['executionMode'] == 'CASCADED'
    assert adaptive['executionMode'] == fixed['executionMode']
    assert adaptive['mode'] != fixed['mode']
    assert fixed['requestId'] == adaptive['requestId']
    assert fixed['exactRanking'] is True
    assert fixed['budget']['maxCandidates'] == overfetch['budget']['maxCandidates']
    assert overfetch['budget']['topK'] == fixed['budget']['topK'] * 4


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
    response = {'evidence': [{'graphTraces': [{'pathEdges': [
        {'label': 'director'}, {'label': 'mother'}]}]}]}

    assert targets == ['director', 'mother']
    assert score_evidence(response, relation_targets=targets)['relationPathHit'] == 1


def test_run_provenance_records_reproducibility_inputs():
    args = SimpleNamespace(graph_version='gv-1', seed=7, config_version='v2')
    provenance = run_provenance(args)
    assert provenance['datasetRelease'] == 'gv-1'
    assert provenance['randomSeed'] == 7
    assert provenance['configVersion'] == 'v2'
    assert provenance['codeRevision']


def test_equal_budget_rejects_divergent_candidate_multipliers():
    assert_equal_budget(METHODS)
    original = METHOD_CONFIG['VECTOR_OVERFETCH']['candidateMultiplier']
    METHOD_CONFIG['VECTOR_OVERFETCH']['candidateMultiplier'] = 4
    try:
        try:
            assert_equal_budget(METHODS)
        except ValueError as error:
            assert 'candidate budget' in str(error)
        else:
            raise AssertionError('a widened candidate budget must be rejected')
    finally:
        METHOD_CONFIG['VECTOR_OVERFETCH']['candidateMultiplier'] = original


def test_output_stream_is_gzip_when_suffix_says_so(tmp_path):
    import gzip, json
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
    from pathlib import Path
    from tools.graphrag.benchmark_http import _summary_path

    assert _summary_path(Path('run/six.jsonl')).name == 'six.csv'
    assert _summary_path(Path('run/six.jsonl.gz')).name == 'six.csv'
