from tools.graphrag.eval.metrics import evidence_recall_at_k, exact_match, f1, percentile


def test_answer_metrics_and_ranking():
    assert exact_match('The, Answer!', 'the answer') == 1.0
    assert f1('a b', 'a c') == 0.5
    assert evidence_recall_at_k(['a', 'b'], ['b', 'c'], 2) == 0.5
    assert percentile([1, 3, 5], 0.5) == 3


def test_reciprocal_rank_covers_hit_and_miss():
    from tools.graphrag.eval.metrics import reciprocal_rank

    assert reciprocal_rank(['a', 'b'], ['a']) == 1.0
    assert reciprocal_rank(['a', 'b'], ['b']) == 0.5
    assert reciprocal_rank(['a', 'b'], ['c']) == 0.0
    assert reciprocal_rank([], ['a']) == 0.0
    assert reciprocal_rank(['a'], []) == 0.0


def test_percentile_boundaries():
    from tools.graphrag.eval.metrics import percentile

    assert percentile([], 0.95) == 0.0
    assert percentile([7], 0.95) == 7
    assert percentile([1, 2, 3, 4], 0.0) == 1
    assert percentile([1, 2, 3, 4], 1.0) == 4
    assert percentile([1, 2, 3, 4], 0.5) == 2.5


def test_evidence_recall_respects_k_and_empty_targets():
    assert evidence_recall_at_k(['a', 'b', 'c'], ['c'], 2) == 0.0
    assert evidence_recall_at_k(['a', 'b', 'c'], ['c'], 3) == 1.0
    assert evidence_recall_at_k(['a'], [], 5) == 0.0


def test_answer_metrics_handle_empty_predictions():
    assert exact_match('', '') == 1.0
    assert exact_match('', 'answer') == 0.0
    assert f1('', 'answer') == 0.0
    assert f1('answer', 'answer') == 1.0


def test_matches_java_shared_golden_fixture():
    """The Java evaluator reads the same fixture; both must agree on EM/F1."""
    import json
    from pathlib import Path

    fixture = (Path(__file__).resolve().parents[3]
               / 'geaflow-ai' / 'src' / 'test' / 'resources' / 'answer' / 'em-f1-golden.json')
    cases = json.loads(fixture.read_text(encoding='utf-8'))['cases']
    assert len(cases) == 8
    for case in cases:
        assert exact_match(case['prediction'], case['answer']) == case['exactMatch'], case
        assert abs(f1(case['prediction'], case['answer']) - case['f1']) < 1e-9, case
